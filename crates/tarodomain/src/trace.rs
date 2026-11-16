//! Correlation traces (spec §28.3): what happened under one correlation ID,
//! and what caused what.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

use chrono::{DateTime, Utc};

use crate::adapters::EventStoreAdapter;
use crate::error::Result;
use crate::kernel::ids::Uuid;

/// One stored event of a trace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CorrelationTraceEntry {
    /// The event's ID.
    pub event_id: Uuid,
    /// The event's type name.
    pub event_type: String,
    /// What caused it: the ID of a command, or of an event (§28.2).
    pub causation_id: Option<Uuid>,
    /// For an event a command raised, that command's own cause, such as the
    /// event a saga reacted to.
    pub command_causation_id: Option<Uuid>,
    /// When it was raised.
    pub occurred_at: DateTime<Utc>,
}

/// What a node of the tree stands for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeKind {
    /// A stored event.
    Event,
    /// A command, inferred from the events it raised: commands aren't
    /// stored, so only its ID is known (it is the `causation_id` of those
    /// events).
    Command,
}

/// A node of the causation tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CausationTreeNode {
    /// Event or inferred command.
    pub kind: NodeKind,
    /// The event's or command's ID.
    pub id: Uuid,
    /// The event's type name; `None` for a command.
    pub name: Option<String>,
    /// When the event was raised; `None` for a command.
    pub occurred_at: Option<DateTime<Utc>>,
    /// What this node caused, in the order the log recorded it.
    pub children: Vec<CausationTreeNode>,
}

/// The causation tree of a correlation ID: every node nothing in the trace
/// caused is a root. A trace usually has one, but events raised by several
/// independent commands under one correlation ID give several.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CausationTree {
    /// The roots, in the order the log recorded them.
    pub roots: Vec<CausationTreeNode>,
}

/// Debugging helpers over a correlation ID (spec §28.3).
pub struct CorrelationTrace;

impl CorrelationTrace {
    /// Every stored event with this correlation ID, in append order. Events
    /// are read raw, so one that can't be deserialized still shows.
    pub async fn trace(
        store: &dyn EventStoreAdapter,
        correlation_id: Uuid,
    ) -> Result<Vec<CorrelationTraceEntry>> {
        Ok(store
            .load_by_correlation_id(correlation_id)
            .await?
            .into_iter()
            .map(|event| CorrelationTraceEntry {
                event_id: event.event_id,
                event_type: event.event_type,
                causation_id: event.causation_id,
                command_causation_id: event.command_causation_id,
                occurred_at: event.occurred_at,
            })
            .collect())
    }

    /// Builds the causation tree from the entries.
    ///
    /// An event whose `causation_id` is another event of the trace is that
    /// event's child. Otherwise the `causation_id` is a command: the tree
    /// gets a command node with that ID (inferred; commands aren't stored),
    /// the event becomes its child, and the command's parent is the event
    /// named by the event's `command_causation_id`, if the trace has it
    /// (a saga's command hangs under the event the saga reacted to), else
    /// the command is a root. An event with no cause is a root.
    pub fn build_tree(entries: &[CorrelationTraceEntry]) -> CausationTree {
        let event_ids: HashSet<Uuid> = entries.iter().map(|entry| entry.event_id).collect();

        // The tree as a map of parent → children, then assembled bottom-up.
        enum Node<'a> {
            Event(&'a CorrelationTraceEntry),
            Command(Uuid),
        }
        let mut nodes: Vec<Node> = Vec::new();
        let mut index: HashMap<Uuid, usize> = HashMap::new();
        let mut parent: Vec<Option<usize>> = Vec::new();

        for entry in entries {
            index.insert(entry.event_id, nodes.len());
            nodes.push(Node::Event(entry));
            parent.push(None);
        }
        for (position, entry) in entries.iter().enumerate() {
            match entry.causation_id {
                None => {}
                Some(cause) if event_ids.contains(&cause) && cause != entry.event_id => {
                    parent[position] = Some(index[&cause]);
                }
                Some(cause) if cause == entry.event_id => {}
                Some(command) => {
                    let slot = *index.entry(command).or_insert_with(|| {
                        nodes.push(Node::Command(command));
                        parent.push(None);
                        nodes.len() - 1
                    });
                    parent[position] = Some(slot);
                    if parent[slot].is_none()
                        && let Some(origin) = entry.command_causation_id
                        && let Some(origin_slot) =
                            event_ids.contains(&origin).then(|| index[&origin])
                        && origin_slot != position
                    {
                        parent[slot] = Some(origin_slot);
                    }
                }
            }
        }
        // Break cycles from inconsistent data: a node that is its own
        // ancestor becomes a root.
        for start in 0..nodes.len() {
            let mut seen = HashSet::new();
            let mut at = start;
            while let Some(next) = parent[at] {
                if !seen.insert(at) || next == start {
                    parent[start] = None;
                    break;
                }
                at = next;
            }
        }

        let mut children: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
        let mut roots = Vec::new();
        // Position in the log orders siblings; a command takes the place of
        // its first event.
        let mut order: Vec<usize> = (0..nodes.len()).collect();
        let first_event: HashMap<usize, usize> = parent
            .iter()
            .enumerate()
            .filter_map(|(child, p)| p.map(|p| (p, child)))
            .fold(HashMap::new(), |mut acc, (p, child)| {
                acc.entry(p)
                    .and_modify(|first: &mut usize| *first = (*first).min(child))
                    .or_insert(child);
                acc
            });
        order.sort_by_key(|&i| match nodes[i] {
            Node::Event(_) => i,
            Node::Command(_) => first_event.get(&i).copied().unwrap_or(i),
        });
        for &i in &order {
            match parent[i] {
                Some(p) => children[p].push(i),
                None => roots.push(i),
            }
        }

        fn assemble(at: usize, nodes: &[Node], children: &[Vec<usize>]) -> CausationTreeNode {
            let (kind, id, name, occurred_at) = match &nodes[at] {
                Node::Event(entry) => (
                    NodeKind::Event,
                    entry.event_id,
                    Some(entry.event_type.clone()),
                    Some(entry.occurred_at),
                ),
                Node::Command(id) => (NodeKind::Command, *id, None, None),
            };
            CausationTreeNode {
                kind,
                id,
                name,
                occurred_at,
                children: children[at]
                    .iter()
                    .map(|&child| assemble(child, nodes, children))
                    .collect(),
            }
        }
        CausationTree {
            roots: roots
                .into_iter()
                .map(|root| assemble(root, &nodes, &children))
                .collect(),
        }
    }

    /// The tree as indented text, one node per line:
    ///
    /// ```text
    /// command 0190…
    /// └─ event TransferRequested 0190…
    ///    ├─ command 0190…
    ///    │  └─ event MoneyWithdrawn 0190…
    ///    └─ event …
    /// ```
    pub fn format_tree(tree: &CausationTree) -> String {
        fn line(node: &CausationTreeNode) -> String {
            match (&node.kind, &node.name) {
                (NodeKind::Event, Some(name)) => format!("event {name} {}", node.id),
                _ => format!("command {}", node.id),
            }
        }
        fn walk(node: &CausationTreeNode, prefix: &str, last: bool, root: bool, out: &mut String) {
            if root {
                let _ = writeln!(out, "{}", line(node));
            } else {
                let branch = if last { "└─ " } else { "├─ " };
                let _ = writeln!(out, "{prefix}{branch}{}", line(node));
            }
            let next = if root {
                String::new()
            } else {
                format!("{prefix}{}", if last { "   " } else { "│  " })
            };
            for (i, child) in node.children.iter().enumerate() {
                walk(child, &next, i + 1 == node.children.len(), false, out);
            }
        }
        let mut out = String::new();
        for root in &tree.roots {
            walk(root, "", true, true, &mut out);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::kernel::ids::new_v7;

    fn at(n: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + n, 0).unwrap()
    }

    fn entry(
        name: &str,
        cause: Option<Uuid>,
        command_cause: Option<Uuid>,
        n: i64,
    ) -> CorrelationTraceEntry {
        CorrelationTraceEntry {
            event_id: new_v7(),
            event_type: name.into(),
            causation_id: cause,
            command_causation_id: command_cause,
            occurred_at: at(n),
        }
    }

    fn names(node: &CausationTreeNode) -> Vec<String> {
        node.children
            .iter()
            .map(|c| c.name.clone().unwrap_or_else(|| "<command>".into()))
            .collect()
    }

    #[test]
    fn the_saga_chain_is_one_tree_from_the_root_command() {
        let (cmd1, cmd2, cmd3) = (new_v7(), new_v7(), new_v7());
        let requested = entry("TransferRequested", Some(cmd1), None, 0);
        let withdrawn = entry("MoneyWithdrawn", Some(cmd2), Some(requested.event_id), 1);
        let deposited = entry("MoneyDeposited", Some(cmd3), Some(requested.event_id), 2);
        let completed = entry(
            "TransferCompleted",
            Some(new_v7()),
            Some(deposited.event_id),
            3,
        );
        let tree = CorrelationTrace::build_tree(&[
            requested.clone(),
            withdrawn.clone(),
            deposited.clone(),
            completed.clone(),
        ]);

        assert_eq!(tree.roots.len(), 1, "one chain");
        let root = &tree.roots[0];
        assert_eq!((root.kind, root.id), (NodeKind::Command, cmd1));
        let requested_node = &root.children[0];
        assert_eq!(requested_node.name.as_deref(), Some("TransferRequested"));
        assert_eq!(names(requested_node), ["<command>", "<command>"]);
        assert_eq!(requested_node.children[0].id, cmd2);
        assert_eq!(
            requested_node.children[0].children[0].id,
            withdrawn.event_id
        );
        let deposit_command = &requested_node.children[1];
        assert_eq!(deposit_command.children[0].id, deposited.event_id);
        // The completion hangs under the deposit event, through its command.
        let completion = &deposit_command.children[0].children[0].children[0];
        assert_eq!(completion.id, completed.event_id);
    }

    #[test]
    fn an_event_caused_by_an_event_is_its_direct_child() {
        let a = entry("A", None, None, 0);
        let b = entry("B", Some(a.event_id), None, 1);
        let tree = CorrelationTrace::build_tree(&[a.clone(), b.clone()]);
        assert_eq!(tree.roots.len(), 1);
        assert_eq!(tree.roots[0].id, a.event_id);
        assert_eq!(tree.roots[0].children[0].id, b.event_id);
    }

    #[test]
    fn events_of_one_command_share_its_node() {
        let cmd = new_v7();
        let first = entry("First", Some(cmd), None, 0);
        let second = entry("Second", Some(cmd), None, 1);
        let tree = CorrelationTrace::build_tree(&[first, second]);
        assert_eq!(tree.roots.len(), 1);
        assert_eq!(names(&tree.roots[0]), ["First", "Second"]);
    }

    #[test]
    fn independent_roots_stay_separate_and_a_missing_cause_is_a_root() {
        let a = entry("A", None, None, 0);
        let b = entry("B", Some(new_v7()), Some(new_v7()), 1); // origin not in the trace
        let tree = CorrelationTrace::build_tree(&[a, b]);
        assert_eq!(tree.roots.len(), 2);
        assert_eq!(tree.roots[0].kind, NodeKind::Event);
        assert_eq!(tree.roots[1].kind, NodeKind::Command);
        assert!(CorrelationTrace::build_tree(&[]).roots.is_empty());
    }

    #[test]
    fn inconsistent_data_does_not_loop() {
        let mut a = entry("A", None, None, 0);
        let mut b = entry("B", None, None, 1);
        a.causation_id = Some(b.event_id);
        b.causation_id = Some(a.event_id);
        let tree = CorrelationTrace::build_tree(&[a.clone(), b]);
        assert!(!tree.roots.is_empty());
        let own = entry("Own", None, None, 0);
        let mut selfish = own.clone();
        selfish.causation_id = Some(own.event_id);
        assert_eq!(CorrelationTrace::build_tree(&[selfish]).roots.len(), 1);
    }

    #[test]
    fn the_text_form_shows_the_chain() {
        let cmd = new_v7();
        let a = entry("A", Some(cmd), None, 0);
        let b = entry("B", Some(new_v7()), Some(a.event_id), 1);
        let text =
            CorrelationTrace::format_tree(&CorrelationTrace::build_tree(&[a.clone(), b.clone()]));
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 4);
        assert_eq!(lines[0], format!("command {cmd}"));
        assert_eq!(lines[1], format!("└─ event A {}", a.event_id));
        assert!(lines[2].starts_with("   └─ command "));
        assert_eq!(lines[3], format!("      └─ event B {}", b.event_id));
    }
}
