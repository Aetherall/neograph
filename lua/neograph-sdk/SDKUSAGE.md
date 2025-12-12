# Building Domain SDKs with neograph-sdk

This guide demonstrates how to build domain-specific SDKs using neograph-sdk, using a Debug Adapter Protocol (DAP) implementation as an example.

## Overview

neograph-sdk provides two complementary patterns for working with graph data:

1. **Reactive Collections** (`:each()`, `:use()`) - For flat, dynamic collections where you need to react to additions/removals
2. **View Trees** (`graph:view()`) - For hierarchical data visualization with expand/collapse, lazy loading, and virtual scrolling

## Example: DAP SDK

A debugger has this hierarchy:

```
Debugger
└── sessions (Session)
    └── threads (Thread)
        └── stacks (Stack)
            └── frames (Frame)
                └── scopes (Scope)
                    └── variables (Variable)
                        └── children (Variable, recursive)
```

### Schema Definition

```lua
local schema = [[{
    "types": [
        {
            "name": "Debugger",
            "properties": [],
            "edges": [{ "name": "sessions", "target": "Session", "reverse": "debugger" }]
        },
        {
            "name": "Session",
            "properties": [
                { "name": "name", "type": "string" },
                { "name": "state", "type": "string" }
            ],
            "edges": [
                { "name": "debugger", "target": "Debugger", "reverse": "sessions" },
                { "name": "threads", "target": "Thread", "reverse": "session" }
            ]
        },
        {
            "name": "Thread",
            "properties": [
                { "name": "threadId", "type": "i64" },
                { "name": "name", "type": "string" },
                { "name": "state", "type": "string" }
            ],
            "edges": [
                { "name": "session", "target": "Session", "reverse": "threads" },
                { "name": "stacks", "target": "Stack", "reverse": "thread" }
            ]
        },
        {
            "name": "Stack",
            "properties": [{ "name": "seq", "type": "i64" }],
            "edges": [
                { "name": "thread", "target": "Thread", "reverse": "stacks" },
                { "name": "frames", "target": "Frame", "reverse": "stack" }
            ]
        },
        {
            "name": "Frame",
            "properties": [
                { "name": "frameId", "type": "i64" },
                { "name": "name", "type": "string" },
                { "name": "source", "type": "string" },
                { "name": "line", "type": "i64" },
                { "name": "column", "type": "i64" }
            ],
            "edges": [
                { "name": "stack", "target": "Stack", "reverse": "frames" },
                { "name": "scopes", "target": "Scope", "reverse": "frame" }
            ]
        },
        {
            "name": "Scope",
            "properties": [
                { "name": "name", "type": "string" },
                { "name": "presentationHint", "type": "string" },
                { "name": "expensive", "type": "bool" }
            ],
            "edges": [
                { "name": "frame", "target": "Frame", "reverse": "scopes" },
                { "name": "variables", "target": "Variable", "reverse": "scope" }
            ]
        },
        {
            "name": "Variable",
            "properties": [
                { "name": "name", "type": "string" },
                { "name": "value", "type": "string" },
                { "name": "type", "type": "string" },
                { "name": "variablesReference", "type": "i64" }
            ],
            "edges": [
                { "name": "scope", "target": "Scope", "reverse": "variables" },
                { "name": "parent", "target": "Variable", "reverse": "children" },
                { "name": "children", "target": "Variable", "reverse": "parent" }
            ]
        }
    ]
}]]
```

### Entity Definitions

```lua
local ng = require("neograph_lua")
local sdk = require("neograph-sdk")

local Debugger = sdk.entity("Debugger")
local Session = sdk.entity("Session")
local Thread = sdk.entity("Thread")
local Stack = sdk.entity("Stack")
local Frame = sdk.entity("Frame")
local Scope = sdk.entity("Scope")
local Variable = sdk.entity("Variable")

-- Custom methods
function Session:isRunning()
    return self.state:get() == "running"
end

function Thread:isStopped()
    return self.state:get() == "stopped"
end

function Frame:location()
    return string.format("%s:%d:%d",
        self.source:get(),
        self.line:get(),
        self.column:get())
end

function Variable:hasChildren()
    return self.variablesReference:get() > 0
end

-- Initialize database
local graph = ng.graph(schema)
local db = sdk.wrap(graph, {
    Debugger = Debugger,
    Session = Session,
    Thread = Thread,
    Stack = Stack,
    Frame = Frame,
    Scope = Scope,
    Variable = Variable,
})
```

## Pattern 1: Reactive Collections

Use `:each()` and `:use()` for **flat, dynamic collections** where items come and go.

### When to Use

- Session list (sessions connect/disconnect)
- Thread list (threads start/stop)
- Any collection where you need side effects on enter/leave

### Example: Session Management

```lua
local debugger = Debugger.insert({})

-- React to sessions connecting/disconnecting
debugger.sessions:each(function(session)
    print("Session started:", session.name:get())

    -- Setup UI for this session
    local tab = ui.createTab(session.name:get())

    -- React to session state changes
    local unsub = session.state:use(function(state)
        tab:setStatus(state)
        if state == "terminated" then
            tab:markInactive()
        end
    end)

    -- Cleanup when session disconnects
    return function()
        print("Session ended:", session.name:get())
        unsub()
        tab:close()
    end
end)
```

### Example: Thread Tracking

```lua
session.threads:each(function(thread)
    print("Thread started:", thread.name:get())

    -- React to thread state (running/stopped/exited)
    thread.state:use(function(state)
        if state == "stopped" then
            ui.focusThread(thread)
        end
    end)

    return function()
        print("Thread exited:", thread.name:get())
    end
end)
```

## Pattern 2: View Trees

Use `graph:view()` for **hierarchical visualization** with expand/collapse.

### When to Use

- Variable trees (nested objects/arrays)
- Call stacks with expandable frames
- Any tree where users expand/collapse nodes
- Large datasets requiring virtual scrolling

### Creating Views

```lua
-- Variables tree for a scope
local function createVariablesView(scope)
    local query = string.format([[{
        "root": "Scope",
        "id": %d,
        "virtual": true,
        "edges": [{
            "name": "variables",
            "edges": [{ "name": "children" }]
        }]
    }]], scope:id())

    return db.graph:view(query, { limit = 100 })
end

-- Full stack trace view
local function createStackView(thread)
    local query = string.format([[{
        "root": "Thread",
        "id": %d,
        "virtual": true,
        "edges": [{
            "name": "stacks",
            "edges": [{
                "name": "frames",
                "edges": [{
                    "name": "scopes",
                    "edges": [{
                        "name": "variables",
                        "edges": [{ "name": "children" }]
                    }]
                }]
            }]
        }]
    }]], thread:id())

    return db.graph:view(query, { limit = 50 })
end
```

### Rendering Views

```lua
local view = createVariablesView(scope)

local function render()
    local items = view:get_visible()

    for _, item in ipairs(items) do
        local indent = string.rep("  ", item.depth)
        local entity = db:get(item.id)

        -- Expand/collapse icon
        local icon = ""
        if item.has_children then
            icon = item.expanded and "▼ " or "▶ "
        end

        -- Render based on entity type
        if item.type == "Variable" then
            print(indent .. icon .. entity.name:get() .. " = " .. entity.value:get())
        elseif item.type == "Scope" then
            print(indent .. icon .. "[" .. entity.name:get() .. "]")
        end
    end
end

-- Initial render
render()

-- React to graph changes
view:watch(function()
    render()
end)
```

### User Interactions

```lua
-- Toggle expand/collapse
function onToggle(item_id)
    view:toggle(item_id)
    render()
end

-- Expand specific node
function onExpand(item_id)
    view:expand(item_id)
    render()
end

-- Collapse specific node
function onCollapse(item_id)
    view:collapse(item_id)
    render()
end
```

### View Item Properties

Each item from `view:get_visible()` contains:

| Property | Type | Description |
|----------|------|-------------|
| `id` | number | Entity ID |
| `type` | string | Entity type name |
| `depth` | number | Nesting depth (0 = root) |
| `expanded` | boolean | Whether node is expanded |
| `has_children` | boolean | Whether node has children |

## Combining Patterns

Use reactive collections for top-level navigation and views for deep trees:

```lua
-- Reactive: track active session and thread
local activeView = nil

debugger.sessions:each(function(session)
    session.threads:each(function(thread)
        -- Reactive: respond to thread stopping
        thread.state:use(function(state)
            if state == "stopped" then
                -- View: create tree for stopped thread
                if activeView then
                    activeView:destroy()
                end
                activeView = createStackView(thread)
                activeView:watch(render)
                render()
            end
        end)
    end)
end)
```

## Event Handlers (Populating the Graph)

### On Debug Session Started

```lua
function onSessionStarted(config)
    local session = Session.insert({
        name = config.name,
        state = "initializing"
    })
    debugger.sessions:link(session)
    return session
end
```

### On Thread Created

```lua
function onThreadCreated(session, threadId, name)
    local thread = Thread.insert({
        threadId = threadId,
        name = name,
        state = "running"
    })
    session.threads:link(thread)
    return thread
end
```

### On Stopped (Breakpoint Hit)

```lua
function onStopped(session, threadId, reason)
    local thread = findThread(session, threadId)
    thread:update({ state = "stopped" })

    -- Fetch stack trace from debug adapter
    local stackTrace = adapter:stackTrace(threadId)

    -- Create stack entity
    local stack = Stack.insert({ seq = os.time() })
    thread.stacks:link(stack)

    -- Populate frames
    for i, frameData in ipairs(stackTrace.stackFrames) do
        local frame = Frame.insert({
            frameId = frameData.id,
            name = frameData.name,
            source = frameData.source and frameData.source.path or "<unknown>",
            line = frameData.line,
            column = frameData.column or 0
        })
        stack.frames:link(frame)
    end
end
```

### On Continued

```lua
function onContinued(session, threadId)
    local thread = findThread(session, threadId)
    thread:update({ state = "running" })

    -- Delete old stacks (cleanup happens automatically via :each())
    for stack in thread.stacks:iter() do
        for frame in stack.frames:iter() do
            for scope in frame.scopes:iter() do
                deleteVariablesRecursive(scope)
                scope:delete()
            end
            frame:delete()
        end
        stack:delete()
    end
end
```

### On Variables Requested (Lazy Load)

```lua
function onExpandVariable(variable)
    local ref = variable.variablesReference:get()
    if ref == 0 then return end

    -- Fetch from debug adapter
    local response = adapter:variables(ref)

    -- Populate children
    for _, varData in ipairs(response.variables) do
        local child = Variable.insert({
            name = varData.name,
            value = varData.value,
            type = varData.type or "",
            variablesReference = varData.variablesReference or 0
        })
        variable.children:link(child)
    end
end
```

## Summary

| Pattern | Use Case | Key Methods |
|---------|----------|-------------|
| `:each()` | Flat collections with enter/leave effects | `collection:each(fn)` |
| `:use()` | Property change reactions | `signal:use(fn)` |
| `:onChange()` | Simple property subscriptions | `signal:onChange(fn)` |
| `view()` | Tree visualization | `graph:view(query, opts)` |

**Rule of thumb:**
- **Reactive collections** for managing lifecycle (setup/teardown)
- **Views** for rendering hierarchical UI with user interaction
