-- Synchronous test - no feedkeys
-- Run with: nvim --headless -u test/sync_test.lua

local cwd = vim.fn.getcwd()
package.cpath = cwd .. "/lua/?.so;;" .. package.cpath
package.path = cwd .. "/lua/?/init.lua;;" .. package.path

local ng = require("neograph_lua")
local sdk = require("neograph-sdk")

local schema = [[{
    "types": [
        {
            "name": "Parent",
            "properties": [{ "name": "name", "type": "string" }],
            "edges": [{ "name": "children", "target": "Child", "reverse": "parent" }],
            "indexes": [{ "fields": [{ "name": "name" }] }]
        },
        {
            "name": "Child",
            "properties": [{ "name": "name", "type": "string" }],
            "edges": [{ "name": "parent", "target": "Parent", "reverse": "children" }],
            "indexes": [{ "fields": [{ "name": "name" }] }]
        }
    ]
}]]

local Parent = sdk.entity("Parent")
local Child = sdk.entity("Child")

local graph = ng.graph(schema)
local db = sdk.wrap(graph, { Parent = Parent, Child = Child })

local parent = Parent.insert({ name = "root" })

local query = string.format([[{
    "root": "Parent",
    "id": %d,
    "virtual": true,
    "edges": [{ "name": "children" }]
}]], parent:id())

local view = graph:view(query, { limit = 100, immediate = true })
view:expand(parent:id(), "children")

-- Add initial children
for i = 1, 3 do
    local child = Child.insert({ name = "child_" .. i })
    parent.children:link(child)
end

local items = view:get_visible()
-- With virtual=true, parent is NOT shown, only children
local initial_count = 3  -- 3 children (parent is virtual, not shown)
local all_passed = true

if #items == initial_count then
    print("Initial: " .. #items .. " items OK (children only, parent is virtual)")
else
    print("Initial: " .. #items .. " items (expected " .. initial_count .. ") FAIL")
    all_passed = false
end

-- Now add 5 more children synchronously
for i = 1, 5 do
    local new_child = Child.insert({ name = "new_child_" .. i })
    parent.children:link(new_child)

    items = view:get_visible()
    local expected = initial_count + i
    local actual = #items

    if actual == expected then
        print("Link #" .. i .. ": " .. actual .. " items OK")
    else
        print("Link #" .. i .. ": expected " .. expected .. ", got " .. actual .. " FAIL")
        for j, item in ipairs(items) do
            print("  " .. j .. ": " .. item.type .. " id=" .. item.id)
        end
        all_passed = false
    end
end

if all_passed then
    print("\n=== PASS ===")
    vim.cmd("qa!")
else
    print("\n=== FAIL ===")
    vim.cmd("cq 1")
end
