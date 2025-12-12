-- Minimal bug reproduction with feedkeys
-- Run with: nvim --headless -u test/minimal_feedkeys_test.lua

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

-- Create debugger
local debugger = graph:insert("Debugger", { name = "main" })

-- Create view
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

-- Expand debugger→threads
view:expand(debugger, "threads")
local items = view:get_visible()

-- Create initial structure
local thread = graph:insert("Thread", { name = "Thread 1" })
graph:link(debugger, "threads", thread)

local frame = graph:insert("Frame", { name = "main" })
graph:link(thread, "frames", frame)

local scope = graph:insert("Scope", { name = "Locals" })
graph:link(frame, "scopes", scope)

-- Create 3 existing variables
for i = 1, 3 do
    local var = graph:insert("Variable", { name = "var" .. i })
    graph:link(scope, "variables", var)
end

items = view:get_visible()

-- Expand all
view:expand(thread, "frames")
items = view:get_visible()
view:expand(frame, "scopes")
items = view:get_visible()
view:expand(scope, "variables")
items = view:get_visible()

print("Initial: " .. #items .. " items")

-- Create a buffer to trigger nvim events
local buf = vim.api.nvim_create_buf(false, true)
vim.api.nvim_set_current_buf(buf)

-- Track results
local var_count = 0
local results = {}

-- Keymap for 'v' to add variable
vim.keymap.set("n", "v", function()
    var_count = var_count + 1

    -- Simulate get_selected_item
    items = view:get_visible()

    -- Insert and link
    local new_var = graph:insert("Variable", { name = "new_var_" .. var_count })
    graph:link(scope, "variables", new_var)

    -- Simulate render (get_visible x2)
    items = view:get_visible()
    items = view:get_visible()

    local expected = 7 + var_count
    local actual = #items
    table.insert(results, { expected = expected, actual = actual })
    print("Link #" .. var_count .. ": expected " .. expected .. ", got " .. actual)
end, { buffer = buf })

vim.schedule(function()
    -- Use feedkeys to simulate keypresses
    local function press_v(n)
        if n > 5 then
            -- Done, check results
            local all_passed = true
            for i, r in ipairs(results) do
                if r.actual ~= r.expected then
                    print("Link #" .. i .. ": FAILED (expected " .. r.expected .. ", got " .. r.actual .. ")")
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

        vim.api.nvim_feedkeys("v", "mtx", false)
        vim.defer_fn(function() press_v(n + 1) end, 50)
    end

    press_v(1)
end)

vim.defer_fn(function()
    print("Timeout!")
    vim.cmd("cq 2")
end, 5000)
