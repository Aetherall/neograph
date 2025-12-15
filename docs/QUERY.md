# Query Declaration Specification

This document defines the semantics for query declarations in Neograph.

## Overview

A query declares:
1. **What** entities to select (root type, filters)
2. **How** to order them (sorts)
3. **Which paths** to traverse (edges)

The edge hierarchy defines the **reactive scope** - mutations to nodes within declared paths trigger view updates.

## Structure

### Query

```
Query {
  root: TypeName           # The root entity type
  id: ?NodeId              # Optional: single node lookup (bypasses filters)
  virtual: bool            # If true, root nodes are transparent (eager, inlined)
  filters: []Filter        # Conditions for root node selection
  sorts: []Sort            # Ordering for root nodes
  edges: []EdgeSelection   # Declared traversal paths
}
```

### EdgeSelection

```
EdgeSelection {
  name: EdgeName           # Edge name (must exist on parent type)
  virtual: bool            # If true, edge is eager and children inlined
  recursive: bool          # If true, selection reapplies on same-type descendants
  filters: []Filter        # Conditions for nodes reached via this edge
  sorts: []Sort            # Ordering for nodes at this level
  edges: []EdgeSelection   # Nested edges (on target type)
}
```

## Edge Declaration Rules

### Rule 1: Type Binding

Each `EdgeSelection` is bound to the type where it's declared:
- **Root-level edges** must exist on the `root` type
- **Nested edges** must exist on the parent edge's target type

```json
{
  "root": "Parent",
  "edges": [
    {
      "name": "children",
      "edges": [
        { "name": "items" }
      ]
    }
  ]
}
```

In this example:
- `children` must be an edge on `Parent` (target: `Child`)
- `items` must be an edge on `Child` (target: `Item`)

### Rule 2: Validation

At query parse/validation time, **reject edges that don't exist on their bound type**.

```json
// INVALID: Parent has no "items" edge
{
  "root": "Parent",
  "edges": [
    { "name": "children" },
    { "name": "items" }
  ]
}
```

This query is malformed because `items` is declared at root level, implying it's an edge on `Parent`. If `Parent` doesn't have an `items` edge, the query must be rejected.

### Rule 3: Recursive Edges

When `recursive: true`, the edge selection (and its nested edges) reapplies whenever the same type is encountered at any depth.

```json
{
  "root": "Frame",
  "edges": [
    {
      "name": "scopes",
      "edges": [
        {
          "name": "variables",
          "recursive": true,
          "edges": [
            { "name": "source" }
          ]
        }
      ]
    }
  ]
}
```

This declares:
- `Frame` -> `scopes` -> `Scope`
- `Scope` -> `variables` -> `Variable`
- `Variable` -> `variables` -> `Variable` (recursive, any depth)
- `Variable` -> `source` -> `Source` (available at any `Variable` depth)

Use case: Exploring graphs with recursive structures like nested variables, tree nodes, or nested comments.

### Rule 4: Virtual Edges

When `virtual: true`:
- The edge is **eager** (auto-expanded, no manual `expand()` needed)
- Children are **inlined** into parent in the reactive tree
- The edge becomes transparent to the user

Use case: Edges that should always be traversed, like `thread->frames` where you always want to see frames when viewing a thread.

## Reactive Scope

The declared hierarchy defines what mutations trigger view updates:

| Event | Triggers Update When |
|-------|---------------------|
| Link to root type | Always (if node passes filters) |
| Link to `parent->edge` | Edge is declared at that level in hierarchy |
| Link to recursive edge | Ancestor in tree has matching recursive selection |
| Unlink | Node is currently in reactive tree |
| Delete | Node is currently in reactive tree |
| Update | Node is currently in reactive tree |

### Expand Behavior

- `expand(node, edge)` succeeds only if `edge` is declared for `node`'s position in the hierarchy
- Expanding an undeclared edge is an **error**
- Virtual edges are implicitly expanded; calling `expand()` on them is a no-op or error

## Examples

### Flat Query (No Traversal)

```json
{
  "root": "Thread",
  "filters": [{ "field": "status", "op": "eq", "value": "active" }],
  "sorts": [{ "field": "created_at", "direction": "desc" }],
  "edges": []
}
```

- Selects active threads, ordered by creation date
- No edge traversal, no nested reactivity
- Only root-level changes trigger updates

### Single-Level Hierarchy

```json
{
  "root": "Parent",
  "edges": [
    { "name": "children", "sorts": [{ "field": "name" }] }
  ]
}
```

- Selects all `Parent` nodes
- Declares `children` edge for traversal
- Reactive to: Parent changes, Child link/unlink on `children` edge

### Multi-Level Hierarchy

```json
{
  "root": "Session",
  "edges": [
    {
      "name": "threads",
      "edges": [
        {
          "name": "frames",
          "edges": [
            { "name": "scopes" }
          ]
        }
      ]
    }
  ]
}
```

- Declares full path: Session -> threads -> frames -> scopes
- Reactive to changes at any declared level
- `expand(thread, "frames")` works because `frames` is nested under `threads`
- `expand(session, "frames")` fails because `frames` is not at root level

### Recursive Hierarchy

```json
{
  "root": "Scope",
  "edges": [
    {
      "name": "variables",
      "recursive": true,
      "edges": [
        { "name": "type" }
      ]
    }
  ]
}
```

- `variables` edge reapplies at any depth
- `type` edge available on any `Variable` at any nesting level
- Supports: Scope -> Variable -> Variable -> Variable -> ... -> type

### Virtual (Eager) Edges

```json
{
  "root": "Thread",
  "edges": [
    {
      "name": "currentFrame",
      "virtual": true,
      "edges": [
        { "name": "scopes" }
      ]
    }
  ]
}
```

- `currentFrame` is auto-expanded (no manual expand needed)
- Frame is inlined into Thread in the view
- `scopes` still requires explicit expansion

## Implementation Notes

### Query Validation

The query validator must:
1. Resolve `root` to a valid type ID
2. For each edge at root level, verify it exists on root type
3. Recursively validate nested edges against their parent's target type
4. Reject queries with invalid edge references

### Reactive Tracking

When a mutation occurs (link/unlink/update/delete):
1. Find subscriptions where the source node is visible
2. Look up edge selection in the declared hierarchy
3. If found, process the mutation and emit callbacks
4. If not found, the mutation is outside reactive scope (no update)

### Expand Validation

When `expand(node_id, edge_name)` is called:
1. Find the node's position in the reactive tree
2. Determine which edge selections apply at that position
3. If `edge_name` is declared (directly or via recursive), proceed
4. Otherwise, return error: edge not in declared hierarchy
