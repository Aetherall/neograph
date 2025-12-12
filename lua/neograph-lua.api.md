# Neograph Lua API

This document describes the Lua API for neograph.

## Overview

```lua
local ng = require("neograph_lua")
local g = ng.graph(schema)
```

## Graph

### Creating a Graph

```lua
-- With schema
local g = ng.graph({
    types = {
        { name = "User", properties = {{ name = "name", type = "string" }} },
        { name = "Post", properties = {{ name = "title", type = "string" }} }
    }
})

-- Or with JSON string
local g = ng.graph('{"types": [...]}')

-- Without schema (set later)
local g = ng.graph()
g:schema({ types = {...} })
```

### Node Operations

```lua
-- Insert a node
local id = g:insert("User", { name = "Alice" })

-- Get a node
local node = g:get(id)
-- Returns: { type = "User", name = "Alice" } or nil

-- Update a node
g:update(id, { name = "Bob" })

-- Delete a node
g:delete(id)
```

### Edge Operations

```lua
-- Link two nodes
g:link(user_id, "posts", post_id)

-- Unlink two nodes
g:unlink(user_id, "posts", post_id)

-- Get edge targets
local post_ids = g:edges(user_id, "posts")
-- Returns: { 1, 2, 3 } or nil

-- Check if edge exists
local exists = g:has_edge(user_id, "posts", post_id)
-- Returns: true or false
```

### Node Events

Low-level reactivity for specific nodes.

```lua
-- Subscribe to node events
local unsub = g:on(id, "change", function(id, node, old_node)
    print("Node changed:", id)
end)

local unsub = g:on(id, "delete", function(id)
    print("Node deleted:", id)
end)

local unsub = g:on(id, "link", function(id, edge, target_id)
    print("Edge added:", id, edge, "->", target_id)
end)

local unsub = g:on(id, "unlink", function(id, edge, target_id)
    print("Edge removed:", id, edge, "->", target_id)
end)

-- Unsubscribe
unsub()

-- Or unsubscribe all listeners for a node
g:off(id)
g:off(id, "link")  -- only "link" events
```

**Note:** Link/unlink events fire bidirectionally. If you link A→B, both A and B
receive the callback (if watched).

## Query

Queries provide high-level reactivity over a set of nodes matching criteria.

### Creating a Query

```lua
local q = g:query({
    root = "User",
    id = 123,                    -- optional: specific node
    virtual = false,             -- optional: hide root in results
    sort = { "name" },           -- optional: sort fields
    filter = { active = true },  -- optional: filter conditions
    edges = {                    -- optional: edge traversals
        { name = "posts", sort = { "created_at" } }
    }
}, {
    limit = 50,      -- viewport size (0 = unlimited)
    expanded = false -- default expansion state
})

-- Or with JSON string
local q = g:query('{"root": "User", "sort": ["name"]}')
```

### Reading Results

```lua
-- Get visible items
local items = q:items()
for _, item in ipairs(items) do
    print(item.id, item.depth, item.edge)
end

-- Get total count (including outside viewport)
local count = q:total()
```

Each item has:
- `id` - Node ID
- `depth` - Nesting depth (0 = root)
- `expandable` - Has expandable edges
- `expanded` - Currently expanded
- `edge` - Edge name this item was reached through (nil for roots)

### Query Events

```lua
-- Item entered result set
local unsub = q:on("enter", function(item, index)
    print("Entered:", item.id, "at", index)
end)

-- Item left result set
local unsub = q:on("leave", function(item, index)
    print("Left:", item.id, "from", index)
end)

-- Item properties changed
local unsub = q:on("change", function(item, index, old_item)
    print("Changed:", item.id)
end)

-- Item position changed (due to sort key change)
local unsub = q:on("move", function(item, new_index, old_index)
    print("Moved:", item.id, old_index, "->", new_index)
end)

-- Unsubscribe
unsub()
q:off("enter")  -- remove all "enter" listeners
q:off()         -- remove all listeners
```

### Viewport (Windowed/Virtualized)

```lua
-- Set viewport size
q:set_limit(100)

-- Get current offset
local offset = q:offset()

-- Scroll
q:scroll_to(50)   -- absolute
q:scroll_by(10)   -- relative (positive = down)
```

### Tree Expansion

```lua
-- Expand/collapse
q:expand(id, "children")
q:collapse(id, "children")
local is_now_expanded = q:toggle(id, "children")

-- Check state
local expanded = q:is_expanded(id, "children")

-- Bulk operations
q:expand_all()        -- expand everything
q:expand_all(2)       -- expand to depth 2
q:collapse_all()
```

### Cleanup

```lua
-- Explicit cleanup (also happens on GC)
q:destroy()
```

## Schema Format

```lua
{
    types = {
        {
            name = "User",
            properties = {
                { name = "name", type = "string" },
                { name = "age", type = "int" },
                { name = "score", type = "number" },
                { name = "active", type = "bool" }
            },
            edges = {
                { name = "posts", target = "Post", reverse = "author" }
            },
            indexes = {
                { fields = {{ name = "name", direction = "asc" }} }
            }
        },
        {
            name = "Post",
            properties = {
                { name = "title", type = "string" }
            },
            edges = {
                { name = "author", target = "User", reverse = "posts" }
            }
        }
    }
}
```

## Query Format

```lua
{
    root = "TypeName",           -- required: root type
    id = 123,                    -- optional: specific node ID
    virtual = false,             -- optional: hide root in results
    sort = { "field1", "field2" },
    filter = {
        field = value,           -- equality
        field = { op = "gt", value = 10 }  -- comparison
    },
    edges = {
        {
            name = "edgeName",
            sort = { "field" },
            filter = {...},
            edges = {...},       -- nested edges
            virtual = false      -- hide this level
        }
    }
}
```

## Migration from Current API

| Current | New |
|---------|-----|
| `g:view(json, opts)` | `g:query(def, opts)` |
| `view:get_visible()` | `query:items()` |
| `view:set_viewport(n)` | `query:set_limit(n)` |
| `view:stats().total` | `query:total()` |
| `view:on_enter(fn)` | `query:on("enter", fn)` |
| `view:on_leave(fn)` | `query:on("leave", fn)` |
| `g:watch_node(id, {on_link=fn})` | `g:on(id, "link", fn)` |
| `handle:unwatch()` | `unsub()` (returned function) |
| `node._type` | `node.type` |

## New in This Version

- `g:edges(id, edge)` - Get edge targets directly
- `g:has_edge(src, edge, tgt)` - Check edge existence
- `g:on(id, event, fn)` / `g:off(id, event)` - Unified node events
- `query:on("change", fn)` - Item property changes
- `query:on("move", fn)` - Item position changes
- `query:is_expanded(id, edge)` - Check expansion state
- `query:expand_all()` / `query:collapse_all()` - Bulk expansion
- `query:total()` - Direct total count
- `query:offset()` - Get current offset
- Lua table support for schema and query definitions
