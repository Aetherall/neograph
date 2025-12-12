-- Bug reproduction test using SDK wrapper + feedkeys
-- Run with: nvim --headless -u test/sdk_feedkeys_test.lua

local cwd = vim.fn.getcwd()
package.cpath = cwd .. "/lua/?.so;;" .. package.cpath
package.path = cwd .. "/lua/?/init.lua;;" .. package.path

local ng = require("neograph_lua")
local sdk = require("neograph-sdk")

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
            "properties": [
                { "name": "name", "type": "string" },
                { "name": "state", "type": "string" }
            ],
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
            "properties": [
                { "name": "name", "type": "string" },
                { "name": "value", "type": "string" }
            ],
            "edges": [{ "name": "scope", "target": "Scope", "reverse": "variables" }],
            "indexes": [{ "fields": [{ "name": "name" }] }]
        }
    ]
}]]

-- Define entity classes
local Debugger = sdk.entity("Debugger")
local Thread = sdk.entity("Thread")
local Frame = sdk.entity("Frame")
local Scope = sdk.entity("Scope")
local Variable = sdk.entity("Variable")

-- Create graph and wrap with SDK
local graph = ng.graph(schema)
local db = sdk.wrap(graph, {
    Debugger = Debugger,
    Thread = Thread,
    Frame = Frame,
    Scope = Scope,
    Variable = Variable,
})

-- Create debugger using SDK
local debugger = Debugger.insert({ name = "main" })

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
}]], debugger:id())

local view = graph:view(query, { limit = 100, immediate = true })

-- Expand debugger→threads
view:expand(debugger:id(), "threads")
local items = view:get_visible()

-- Create initial structure using SDK
local thread = Thread.insert({ name = "Thread 1", state = "running" })
debugger.threads:link(thread)

-- Add a subscription like the demo does
local sub = thread.state:use(function(state)
    print("Thread state changed to: " .. state)
end)

local frame1 = Frame.insert({ name = "main" })
thread.frames:link(frame1)

local frame2 = Frame.insert({ name = "helper" })
thread.frames:link(frame2)

local scope_locals = Scope.insert({ name = "Locals" })
frame1.scopes:link(scope_locals)

local scope_globals = Scope.insert({ name = "Globals" })
frame1.scopes:link(scope_globals)

-- Create 3 existing variables using SDK
for i = 1, 3 do
    local var = Variable.insert({ name = "var" .. i, value = tostring(i) })
    scope_locals.variables:link(var)
end

-- Use scope_locals as the target
local scope = scope_locals

items = view:get_visible()

-- Expand all
view:expand(thread:id(), "frames")
items = view:get_visible()
view:expand(frame1:id(), "scopes")
items = view:get_visible()
view:expand(scope_locals:id(), "variables")
items = view:get_visible()

-- Expected: debugger(1) + thread(1) + frame1(1) + scope_locals(1) + vars(3) + scope_globals(1) + frame2(1) = 9
print("Initial: " .. #items .. " items (expected 9)")

-- Create a buffer
local buf = vim.api.nvim_create_buf(false, true)
vim.api.nvim_set_current_buf(buf)

-- Track results
local var_count = 0
local results = {}

-- Helper to simulate render() which calls get_visible AND writes to buffer (like demo)
local function render()
    items = view:get_visible()

    -- Actually render to buffer like demo does
    local lines = {}
    for _, item in ipairs(items) do
        table.insert(lines, string.format("  %s id=%d", item.type, item.id))
    end
    vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)

    -- Second get_visible call (demo calls it twice due to set_status)
    items = view:get_visible()
    return items
end

-- Store scope id for lookup (like demo does)
local scope_id = scope:id()

-- Keymap for 'v' to add variable (matching demo pattern EXACTLY)
vim.keymap.set("n", "v", function()
    var_count = var_count + 1

    -- Simulate get_selected_item (get_visible before link)
    items = view:get_visible()

    -- Get scope via db:get() like demo does
    local scope_entity = db:get(scope_id)

    -- Insert and link using SDK (like demo does)
    local new_var = Variable.insert({ name = "new_var_" .. var_count, value = "42" })
    scope_entity.variables:link(new_var)

    -- Simulate set_status → render + explicit render at end
    render()
    items = render()

    local expected = 9 + var_count  -- 9 initial + var_count new vars
    local actual = #items
    table.insert(results, { expected = expected, actual = actual })
    print("Link #" .. var_count .. ": expected " .. expected .. ", got " .. actual)
end, { buffer = buf })

vim.schedule(function()
    local function press_v(n)
        if n > 5 then
            local all_passed = true
            for i, r in ipairs(results) do
                if r.actual ~= r.expected then
                    print("Link #" .. i .. ": FAILED")
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
