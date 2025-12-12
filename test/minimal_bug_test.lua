-- Minimal bug reproduction test using raw native bindings
-- Run with: nvim --headless -u test/minimal_bug_test.lua

local cwd = vim.fn.getcwd()
package.cpath = cwd .. "/lua/?.so;;" .. package.cpath

local ng = require("neograph_lua")

local schema = [[{
    "types": [
        {
            "name": "Debugger",
            "properties": [{ "name": "name", "type": "string" }],
            "edges": [{ "name": "threads", "target": "Thread", "reverse": "debugger" }],
            "indexes": [{ "fields": [{ "name": "name" }] }]
        },
        {
            "name": "Thread",
            "properties": [{ "name": "name", "type": "string" }],
            "edges": [
                { "name": "debugger", "target": "Debugger", "reverse": "threads" },
                { "name": "frames", "target": "Frame", "reverse": "thread" }
            ],
            "indexes": [{ "fields": [{ "name": "name" }] }]
        },
        {
            "name": "Frame",
            "properties": [{ "name": "name", "type": "string" }],
            "edges": [
                { "name": "thread", "target": "Thread", "reverse": "frames" },
                { "name": "scopes", "target": "Scope", "reverse": "frame" }
            ],
            "indexes": [{ "fields": [{ "name": "name" }] }]
        },
        {
            "name": "Scope",
            "properties": [{ "name": "name", "type": "string" }],
            "edges": [
                { "name": "frame", "target": "Frame", "reverse": "scopes" },
                { "name": "variables", "target": "Variable", "reverse": "scope" }
            ],
            "indexes": [{ "fields": [{ "name": "name" }] }]
        },
        {
            "name": "Variable",
            "properties": [{ "name": "name", "type": "string" }],
            "edges": [{ "name": "scope", "target": "Scope", "reverse": "variables" }],
            "indexes": [{ "fields": [{ "name": "name" }] }]
        }
    ]
}]]

local graph = ng.graph(schema)

-- Step 1: Create debugger
local debugger = graph:insert("Debugger", { name = "main" })

-- Step 2: Create view
local query = string.format([[{
    "root": "Debugger",
    "id": %d,
    "virtual": true,
    "edges": [{
        "name": "threads",
        "edges": [{
            "name": "frames",
            "edges": [{
                "name": "scopes",
                "edges": [{
                    "name": "variables"
                }]
            }]
        }]
    }]
}]], debugger)

local view = graph:view(query, { limit = 100, immediate = true })

-- Step 3: Expand debugger→threads
view:expand(debugger, "threads")
local items = view:get_visible()

-- Step 4: Create thread and link (like 't' key)
local thread = graph:insert("Thread", { name = "Thread 1" })
graph:link(debugger, "threads", thread)
items = view:get_visible()

-- Create frame and link
local frame = graph:insert("Frame", { name = "main" })
graph:link(thread, "frames", frame)

-- Create scope and link
local scope = graph:insert("Scope", { name = "Locals" })
graph:link(frame, "scopes", scope)

-- Create 3 existing variables
local var1 = graph:insert("Variable", { name = "x" })
graph:link(scope, "variables", var1)
local var2 = graph:insert("Variable", { name = "y" })
graph:link(scope, "variables", var2)
local var3 = graph:insert("Variable", { name = "z" })
graph:link(scope, "variables", var3)

items = view:get_visible()

-- Expand thread→frames, frame→scopes, scope→variables
view:expand(thread, "frames")
items = view:get_visible()

view:expand(frame, "scopes")
items = view:get_visible()

view:expand(scope, "variables")
items = view:get_visible()

print("Initial: " .. #items .. " items")

-- Now add 5 variables, checking after each
local all_passed = true
for i = 1, 5 do
    -- Simulate get_selected_item
    items = view:get_visible()

    -- Insert and link
    local new_var = graph:insert("Variable", { name = "new_var_" .. i })
    graph:link(scope, "variables", new_var)

    -- Simulate render (get_visible x2)
    items = view:get_visible()
    items = view:get_visible()

    local expected = 7 + i  -- debugger(1) + thread(1) + frame(1) + scope(1) + vars(3+i) = 7+i
    local actual = #items

    if actual == expected then
        print("Link #" .. i .. ": " .. actual .. " items ✓")
    else
        print("Link #" .. i .. ": expected " .. expected .. ", got " .. actual .. " ❌ BUG!")
        all_passed = false
    end
end

if all_passed then
    print("\n=== PASS ===")
    vim.cmd("qa!")
else
    print("\n=== BUG REPRODUCED ===")
    vim.cmd("cq 1")
end
