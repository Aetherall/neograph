# Architecture Overview

This document describes the internal architecture of neograph, focusing on how data flows through the system and how reactive updates work.

## Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                         Graph                                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐ │
│  │NodeStore │  │ Indexes  │  │ Rollups  │  │ChangeTracker│ │
│  └──────────┘  └──────────┘  └──────────┘  └─────────────┘ │
│       │              │             │              │         │
│       └──────────────┴─────────────┴──────────────┘         │
│                           │                                  │
│                     ┌─────▼─────┐                           │
│                     │   Views   │                           │
│                     └───────────┘                           │
└─────────────────────────────────────────────────────────────┘
```

### NodeStore

The NodeStore holds all nodes in memory with O(1) lookup by ID.

```
NodeStore
├── nodes: HashMap<NodeId, Node>
├── next_id: NodeId
└── schema: *Schema

Node
├── id: NodeId
├── type_id: TypeId
├── properties: HashMap<string, Value>
├── edges: HashMap<EdgeId, []NodeId>
└── rollup_values: HashMap<string, Value>
```

### IndexManager

Maintains B+ tree indexes for efficient querying. Each index is a sorted map from compound keys to node IDs.

```
IndexManager
├── indexes_by_type: HashMap<TypeId, []Index>
└── schema: *Schema

Index
├── def: IndexDef (fields, directions)
├── tree: BPlusTree<CompoundKey, NodeId>
└── type_id: TypeId

CompoundKey = [field1_value][field2_value]...[node_id]
```

**Key Operations:**
- `onInsert(node)` - Add node to all applicable indexes
- `onUpdate(node, old)` - Remove old key, insert new key
- `onDelete(node)` - Remove from all indexes
- `onLink(node, edge)` - Update cross-entity indexes
- `onUnlink(node, edge)` - Update cross-entity indexes

### RollupCache

Computes and caches derived fields. Rollups are computed at write-time and stored on nodes.

```
RollupCache
├── inverted_index: HashMap<(TypeId, EdgeId, NodeId), []NodeId>
├── schema: *Schema
├── store: *NodeStore
└── indexes: *IndexManager
```

**Rollup Types:**

| Type | Computation | Complexity |
|------|-------------|------------|
| count | Count edge targets | O(1) |
| traverse | Get property from first target | O(1) |
| first | Index scan, take first | O(log n) |
| last | Index scan, take last | O(log n + k) |

**Dependency Tracking:**

The inverted index maps `(source_type, edge, target_id) → [source_ids]` for O(S) reverse lookups when a target changes.

### ChangeTracker

Coordinates reactive updates between data changes and views.

```
ChangeTracker
├── subscriptions: HashMap<TypeId, []Subscription>
├── store: *NodeStore
├── schema: *Schema
├── indexes: *IndexManager
└── rollups: *RollupCache
```

## Data Flow

### Write Path

When data is modified, changes propagate through the system:

```
graph.update(id, props)
       │
       ▼
┌──────────────┐
│  NodeStore   │ ── Update node properties
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Indexes    │ ── Update index keys if indexed field changed
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Rollups    │ ── Recompute dependent rollups (traverse deps)
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ChangeTracker │ ── Notify subscribed views
└──────┬───────┘
       │
       ▼
┌──────────────┐
│    Views     │ ── Re-evaluate affected items
└──────────────┘
```

### Link Path

Edge operations have additional steps:

```
graph.link(src, edge, tgt)
       │
       ▼
┌──────────────┐
│  NodeStore   │ ── Add edge in both directions
└──────┬───────┘
       │
       ├─────────────────────────────────┐
       ▼                                 ▼
┌──────────────┐                 ┌──────────────┐
│   Indexes    │                 │   Rollups    │
│  onLink()    │                 │  onLink()    │
└──────┬───────┘                 └──────┬───────┘
       │                                 │
       │     ┌───────────────────────────┘
       ▼     ▼
┌──────────────┐
│   Rollups    │ ── Recompute for both src and tgt
│recomputeFor  │    (count, first, last, traverse)
│   Edge()     │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ChangeTracker │ ── Notify views about edge change
└──────────────┘
```

### Query Path

Queries flow through indexes to produce results:

```
graph.view(query)
       │
       ▼
┌──────────────┐
│ QueryBuilder │ ── Parse JSON, validate against schema
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Indexes    │ ── Select best index for query
│selectIndex() │    Returns IndexCoverage with score
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Executor   │ ── Scan index, apply post-filters
└──────┬───────┘
       │
       ▼
┌──────────────┐
│    View      │ ── Materialize items for viewport
└──────────────┘
```

## Index Selection

The index selection algorithm finds the best index for a query:

```
selectIndex(type_id, filters, sorts):
    best = null
    for each index on type_id:
        coverage = computeCoverage(index, filters, sorts)
        if coverage.score > best.score:
            best = coverage
    return best

computeCoverage(index, filters, sorts):
    equality_prefix = 0
    sort_coverage = 0

    # Match equality filters to index prefix
    for field in index.fields:
        if field in filters with op=eq:
            equality_prefix++
        else:
            break

    # Match sorts after equality prefix
    for sort in sorts:
        if index.fields[equality_prefix + sort_coverage] == sort:
            sort_coverage++
        else:
            break

    score = equality_prefix * 100 + sort_coverage * 10
    return IndexCoverage { score, post_filters, ... }
```

## Cross-Entity Index Scan

For sorted edge traversal, cross-entity indexes are scanned with a prefix:

```
Query: Thread.stacks sorted by timestamp DESC
Index: Stack(thread[edge], timestamp DESC)

selectNestedIndex(Stack, "thread", [], [timestamp DESC]):
    # Find index starting with "thread" edge field
    # Verify remaining fields cover the sort
    return coverage

scanWithEdgePrefix(coverage, thread_id):
    prefix = encodeEdgePrefix(thread_id)
    return index.prefixScan(prefix)
    # Returns: all Stacks where thread=thread_id, sorted by timestamp DESC
```

## Reactive Update Flow

When a view is active, changes propagate through subscriptions:

```
                    ┌─────────────┐
                    │   Insert    │
                    │   Update    │
                    │   Delete    │
                    │    Link     │
                    └──────┬──────┘
                           │
                           ▼
┌──────────────────────────────────────────────────┐
│                  ChangeTracker                    │
│  ┌────────────────────────────────────────────┐  │
│  │ Subscriptions by Type                      │  │
│  │  User: [View1, View2]                      │  │
│  │  Post: [View1, View3]                      │  │
│  └────────────────────────────────────────────┘  │
└──────────────────────┬───────────────────────────┘
                       │
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
    ┌─────────┐   ┌─────────┐   ┌─────────┐
    │  View1  │   │  View2  │   │  View3  │
    └────┬────┘   └────┬────┘   └────┬────┘
         │             │             │
         ▼             ▼             ▼
    Re-evaluate    Re-evaluate   Re-evaluate
    affected       affected      affected
    items          items         items
         │             │             │
         ▼             ▼             ▼
    ┌─────────┐   ┌─────────┐   ┌─────────┐
    │  Event  │   │  Event  │   │  Event  │
    │Callback │   │Callback │   │Callback │
    └─────────┘   └─────────┘   └─────────┘
```

### Event Types

| Event | Trigger | Data |
|-------|---------|------|
| `enter` | Item matches query | item, index |
| `leave` | Item no longer matches | item, index |
| `update` | Item properties changed | item, index |
| `move` | Item position changed (sort key) | item, old_index, new_index |

## View Materialization

Views lazily materialize items for the viewport:

```
View
├── query: Query
├── root_results: []NodeId          # From index scan
├── expansion_state: HashMap<NodeId, Set<EdgeName>>
├── visible_items: []Item           # Materialized for viewport
├── offset: usize                   # Viewport start
└── limit: usize                    # Viewport size

Item
├── id: NodeId
├── depth: u32
├── path: []PathSegment
├── has_children: bool
├── is_expanded: bool
└── properties: (accessed via node)
```

**Lazy Loading:**

1. Initial load: Materialize `limit` items starting at `offset`
2. On expand: Load children, insert into visible_items
3. On collapse: Remove children from visible_items
4. On scroll: Materialize new viewport range

## Memory Model

### Ownership

```
Graph (owns)
├── Schema (owns types, properties, edges, indexes, rollups)
├── NodeStore (owns all nodes)
├── IndexManager (owns all index trees)
├── RollupCache (owns inverted index, borrows store/indexes)
└── ChangeTracker (borrows everything, owns subscriptions)

View (borrows)
├── Query (may own if parsed from JSON)
├── References to Graph components
└── Owns expansion state and visible items
```

### String Interning

All schema strings (type names, property names, edge names) are interned in a StringInterner owned by the Schema. This allows O(1) string comparison via pointer equality.

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Insert node | O(log n) per index | Update all applicable indexes |
| Update property | O(log n) | If property is indexed |
| Link edge | O(log n) | Update cross-entity indexes |
| Query (indexed) | O(log n + k) | k = result size |
| Query (no index) | Error | Requires index coverage |
| Rollup (count) | O(1) | Cached on node |
| Rollup (first/last) | O(log n) | Index scan |
| View update | O(affected) | Only re-evaluate changed items |
