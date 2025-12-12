-- Raw sync test - no SDK wrapper
-- Run with: nvim --headless -u test/raw_sync_test.lua

local cwd = vim.fn.getcwd()
package.cpath = cwd .. "/lua/?.so;;" .. package.cpath

local ng = require("neograph_lua")

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

local graph = ng.graph(schema)

local parent = graph:insert("Parent", { name = "root" })

local query = string.format([[{
    "root": "Parent",
    "id": %d,
    "virtual": true,
    "edges": [{ "name": "children" }]
}]], parent)

local view = graph:view(query, { limit = 100, immediate = true })
view:expand(parent, "children")

-- Add initial children
for i = 1, 3 do
    local child = graph:insert("Child", { name = "child_" .. i })
    graph:link(parent, "children", child)
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
    local new_child = graph:insert("Child", { name = "new_child_" .. i })
    graph:link(parent, "children", new_child)

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
