# neograph

A reactive in-memory graph database written in Zig with Lua bindings.

## Features

- **Reactive queries** - Views automatically update when data changes
- **Tree visualization** - Hierarchical views with expand/collapse, lazy loading
- **Indexed queries** - B-tree indexes for efficient sorting and filtering
- **Cross-entity indexes** - Index across relationships for sorted edge traversal
- **Edge sorting** - Define default sort order for edges by target property
- **Computed rollups** - Derived fields (count, traverse, first, last) computed at write-time
- **Lua bindings** - First-class Lua/Neovim integration
- **SDK** - High-level entity abstraction with signals and reactive collections

## Installation

### Neovim Plugin Manager (lazy.nvim)

```lua
{
    "yourusername/neograph",
    build = "zig build",
}
```

### Manual

```bash
git clone https://github.com/yourusername/neograph
cd neograph
zig build
```

Requires Zig 0.14+. The native library is built to `lua/neograph_lua.so`.

## Quick Start

```lua
local ng = require("neograph_lua")
local sdk = require("neograph-sdk")

-- Define schema
local schema = [[{
    "types": [{
        "name": "User",
        "properties": [{ "name": "name", "type": "string" }],
        "edges": [{
            "name": "posts",
            "target": "Post",
            "reverse": "author",
            "sort": { "property": "createdAt", "direction": "desc" }
        }]
    }, {
        "name": "Post",
        "properties": [
            { "name": "title", "type": "string" },
            { "name": "createdAt", "type": "int" }
        ],
        "edges": [{ "name": "author", "target": "User", "reverse": "posts" }]
    }]
}]]

-- Create database
local graph = ng.graph(schema)
local db = sdk.wrap(graph)

-- Insert and link
local user = db:insert("User", { name = "Alice" })
local post = db:insert("Post", { title = "Hello World" })
user.posts:link(post)

-- React to changes
user.name:use(function(name)
    print("Name:", name)
    return function() print("Cleanup") end
end)
```

## Schema Features

### Indexes

Define indexes for efficient querying. All sorted queries require index coverage.

**Property Index** - Sort/filter by node properties:

```json
{
    "name": "User",
    "properties": [{ "name": "name", "type": "string" }],
    "indexes": [
        { "fields": [{ "field": "name", "direction": "asc" }] }
    ]
}
```

**Compound Index** - Multiple fields for complex queries:

```json
{
    "indexes": [
        { "fields": [
            { "field": "status", "direction": "asc" },
            { "field": "createdAt", "direction": "desc" }
        ]}
    ]
}
```

**Cross-Entity Index** - Index across relationships using `kind: "edge"`:

```json
{
    "name": "Stack",
    "properties": [
        { "name": "timestamp", "type": "int" }
    ],
    "edges": [
        { "name": "thread", "target": "Thread", "reverse": "stacks" }
    ],
    "indexes": [{
        "fields": [
            { "field": "thread", "kind": "edge", "direction": "asc" },
            { "field": "timestamp", "direction": "desc" }
        ]
    }]
}
```

The `kind: "edge"` field references the reverse edge, enabling queries like "all Stacks for Thread X, sorted by timestamp". Without this index, such sorted edge traversals would fail (no in-memory sorting fallback).

**When to use `kind: "edge"`:**
- First field of index references the parent via reverse edge
- Enables sorted traversal of `Thread.stacks` by `timestamp`
- Required for `first`/`last` rollups on that edge

### Edge Sorting

Define a default sort order for edge targets based on a target property:

```json
{
    "name": "User",
    "edges": [{
        "name": "posts",
        "target": "Post",
        "reverse": "author",
        "sort": { "property": "createdAt", "direction": "desc" }
    }]
}
```

When `sort` is specified on an edge:
- Edge targets are stored in sorted order by the target's property value
- When you call `node.edges("posts")`, results are already sorted
- When a target's sort property changes, its position is automatically updated

**Edge sort vs Cross-entity index:**

| Feature | Edge Sort | Cross-Entity Index |
|---------|-----------|-------------------|
| Purpose | Default storage order | Query-time sort override |
| Defined on | Edge definition | Type indexes |
| Auto-updates | Yes, on property change | Yes, on property change |
| Use case | "Posts always sorted by createdAt" | "Query posts by different fields" |

Use edge sort when you have a single canonical ordering. Use cross-entity indexes when queries need different sort orders.

### Rollups

Computed fields that update automatically when data changes:

```json
{
    "name": "Thread",
    "rollups": [
        { "name": "stackCount", "count": "stacks" },
        { "name": "latestTimestamp", "first": { "edge": "stacks", "field": "timestamp", "direction": "desc", "property": "timestamp" } },
        { "name": "authorName", "traverse": { "edge": "author", "property": "name" } }
    ]
}
```

Rollup kinds:
- **count** - Number of edge targets
- **traverse** - Property from first edge target
- **first** - Property from target with highest sort value
- **last** - Property from target with lowest sort value

Rollups are write-time computed and accessible via `node.getProperty("rollupName")`.

## Query Guide

Queries define what data to retrieve and how to present it. All sorted queries require index coverage.

### Basic Query

```json
{
    "root": "User",
    "sort": [{ "field": "name", "direction": "asc" }],
    "filter": [{ "field": "active", "op": "eq", "value": true }]
}
```

### Filter Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `eq` | Equal (default) | `{ "field": "status", "value": "active" }` |
| `neq` | Not equal | `{ "field": "status", "op": "neq", "value": "deleted" }` |
| `gt` | Greater than | `{ "field": "age", "op": "gt", "value": 18 }` |
| `gte` | Greater or equal | `{ "field": "age", "op": "gte", "value": 21 }` |
| `lt` | Less than | `{ "field": "price", "op": "lt", "value": 100 }` |
| `lte` | Less or equal | `{ "field": "price", "op": "lte", "value": 50 }` |
| `in` | In list | `{ "field": "status", "op": "in", "value": ["active", "pending"] }` |

### Edge Traversal

Include related entities using `edges`:

```json
{
    "root": "User",
    "edges": [{
        "name": "posts",
        "sort": [{ "field": "createdAt", "direction": "desc" }],
        "filter": [{ "field": "published", "op": "eq", "value": true }],
        "edges": [{
            "name": "comments",
            "sort": [{ "field": "createdAt", "direction": "asc" }]
        }]
    }]
}
```

Each edge selection can have its own filters, sorts, and nested edges.

### Recursive Edges

For tree structures (e.g., folders, categories), use `recursive: true`:

```json
{
    "root": "Folder",
    "filter": [{ "field": "name", "op": "eq", "value": "root" }],
    "edges": [{
        "name": "children",
        "recursive": true,
        "sort": [{ "field": "name", "direction": "asc" }]
    }]
}
```

This traverses the `children` edge repeatedly, building a tree of arbitrary depth.

### Virtual Mode

Hide the root node from results, showing only its edge targets:

```json
{
    "root": "Thread",
    "id": 42,
    "virtual": true,
    "edges": [{
        "name": "messages",
        "sort": [{ "field": "timestamp", "direction": "asc" }]
    }]
}
```

With `virtual: true`, the Thread node is used for traversal but not included in results.

### Direct ID Lookup

Bypass filters and retrieve a specific node:

```json
{
    "root": "User",
    "id": 123
}
```

## Views (Reactive Queries)

Views are reactive queries that automatically update when underlying data changes.

### Creating Views

```lua
local view = graph:view(query_json, { limit = 100 })
view:activate(true)
```

### View Lifecycle

1. **Create** - `graph:view(query, opts)` - Parses query, creates inactive view
2. **Activate** - `view:activate(true)` - Subscribes to changes, computes initial results
3. **Use** - Iterate items, expand/collapse, scroll
4. **Deactivate** - `view:activate(false)` - Unsubscribes, keeps state
5. **Destroy** - `view:deinit()` - Frees all resources

### View Events

```lua
view:on_event(function(event)
    if event.kind == "enter" then
        -- Item appeared in view
    elseif event.kind == "leave" then
        -- Item removed from view
    elseif event.kind == "update" then
        -- Item properties changed
    end
end)
```

### Tree Expansion

```lua
-- Expand an edge for a node
view:expand(item_index, "children")

-- Collapse
view:collapse(item_index, "children")

-- Expand all to depth
view:expand_all(3)

-- Collapse all
view:collapse_all()
```

### Viewport (Virtual Scrolling)

```lua
-- Set viewport size
local view = graph:view(query, { limit = 50 })

-- Scroll
view:scroll_to(100)  -- Jump to index 100
view:scroll_by(10)   -- Move forward 10 items

-- Iterate visible items
for item in view:items() do
    print(item.id, item.depth)
end
```

## Documentation

- [Architecture Overview](docs/architecture.md) - Internal design, data flow, reactive system
- [SDK Reference](lua/neograph-sdk/README.md) - Entity API, signals, collections
- [SDK Usage Guide](lua/neograph-sdk/SDKUSAGE.md) - Building domain SDKs
- [Schema JSON Schema](docs/schemas/schema.json) - JSON Schema for database schema definitions
- [Query JSON Schema](docs/schemas/query.json) - JSON Schema for query definitions

## Testing

```bash
# All Zig tests
zig build test

# Specific test suites
zig build test-unit          # Unit tests
zig build test-integration   # Integration tests
zig build test-cross-entity  # Cross-entity index tests

# Lua tests
nvim -l test/lua_test.lua    # Binding tests
nvim -l test/sdk_test.lua    # SDK tests
```

## Project Structure

```
├── src/               # Zig implementation
│   ├── index/         # B-tree indexes
│   ├── query/         # Query builder and executor
│   ├── reactive/      # Reactive subscriptions and views
│   └── rollup/        # Computed rollup fields
├── lua/
│   └── neograph-sdk/  # Lua SDK
├── docs/
│   ├── design/        # Design documents
│   └── schemas/       # JSON Schemas for schema and query
├── test/              # Lua tests
├── examples/          # Example code
└── build.zig
```

## License

MIT
