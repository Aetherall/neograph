-- Isolated race condition reproduction
-- Run with: nvim --headless -u test/isolated_race_test.lua

local cwd = vim.fn.getcwd()
package.cpath = cwd .. "/lua/?.so;;" .. package.cpath
package.path = cwd .. "/lua/?/init.lua;;" .. package.path

local ng = require("neograph_lua")
local sdk = require("neograph-sdk")

-- Minimal schema: Parent -> Children
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

-- Create parent
local parent = Parent.insert({ name = "root" })

-- Create view
local query = string.format([[{
    "root": "Parent",
    "id": %d,
    "virtual": true,
    "edges": [{ "name": "children" }]
}]], parent:id())

local view = graph:view(query, { limit = 100, immediate = true })

-- Expand parent->children
view:expand(parent:id(), "children")

-- Add initial children
for i = 1, 3 do
    local child = Child.insert({ name = "child_" .. i })
    parent.children:link(child)
end

local items = view:get_visible()
print("Initial: " .. #items .. " items (expected 4: parent + 3 children)")

-- The critical variable: how many Signal:get() calls per item
local SIGNAL_GETS_PER_ITEM = 1

-- Simulate render - no Signal:get() calls
local function render()
    items = view:get_visible()
    return items
end

-- Create buffer for feedkeys
local buf = vim.api.nvim_create_buf(false, true)
vim.api.nvim_set_current_buf(buf)

local results = {}
local link_count = 0

vim.keymap.set("n", "a", function()
    link_count = link_count + 1

    -- Insert and link new child
    local new_child = Child.insert({ name = "new_child_" .. link_count })
    parent.children:link(new_child)

    -- Single render call
    items = render()

    local expected = 4 + link_count  -- parent + 3 initial + link_count new
    local actual = #items
    table.insert(results, { expected = expected, actual = actual })
    print("Link #" .. link_count .. ": expected " .. expected .. ", got " .. actual)
    -- Debug: print all items
    for i, item in ipairs(items) do
        print("  " .. i .. ": " .. item.type .. " id=" .. item.id .. " name=" .. (item.name or "?"))
    end
end, { buffer = buf })

vim.schedule(function()
    local function press_a(n)
        if n > 5 then
            local all_passed = true
            for i, r in ipairs(results) do
                if r.actual ~= r.expected then
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
            return
        end

        vim.api.nvim_feedkeys("a", "mtx", false)
        vim.defer_fn(function() press_a(n + 1) end, 50)
    end

    press_a(1)
end)

vim.defer_fn(function()
    print("Timeout!")
    vim.cmd("cq 2")
end, 5000)
