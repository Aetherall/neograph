-- neograph DAP Interactive Demo
-- Showcases SDK entity patterns with reactive UI
-- Run with: nvim -u examples/dap_interactive.lua

-- ============================================================================
-- Setup
-- ============================================================================

local cwd = vim.fn.getcwd()
package.cpath = cwd .. "/lua/?.so;;" .. package.cpath
package.path = cwd .. "/lua/?/init.lua;;" .. package.path

local ng = require("neograph_lua")
local sdk = require("neograph-sdk")

-- ============================================================================
-- Schema: DAP-like debug structure
-- ============================================================================

local schema = [[{
    "types": [
        {
            "name": "Debugger",
            "properties": [
                { "name": "name", "type": "string" }
            ],
            "edges": [
                { "name": "threads", "target": "Thread", "reverse": "debugger" }
            ],
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
            "properties": [
                { "name": "name", "type": "string" },
                { "name": "source", "type": "string" },
                { "name": "line", "type": "int" }
            ],
            "edges": [
                { "name": "thread", "target": "Thread", "reverse": "frames" },
                { "name": "scopes", "target": "Scope", "reverse": "frame" }
            ],
            "indexes": [{ "fields": [{ "name": "line" }] }]
        },
        {
            "name": "Scope",
            "properties": [
                { "name": "name", "type": "string" },
                { "name": "expensive", "type": "bool" }
            ],
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
                { "name": "value", "type": "string" },
                { "name": "varType", "type": "string" }
            ],
            "edges": [
                { "name": "scope", "target": "Scope", "reverse": "variables" },
                { "name": "parent", "target": "Variable", "reverse": "children" },
                { "name": "children", "target": "Variable", "reverse": "parent" }
            ],
            "indexes": [{ "fields": [{ "name": "name" }] }]
        }
    ]
}]]

-- ============================================================================
-- Entity Definitions
-- ============================================================================

local Debugger = sdk.entity("Debugger")
local Thread = sdk.entity("Thread")
local Frame = sdk.entity("Frame")
local Scope = sdk.entity("Scope")
local Variable = sdk.entity("Variable")

-- Custom methods
function Thread:isStopped()
    return self.state:get() == "stopped"
end

function Thread:statusIcon()
    local state = self.state:get()
    if state == "stopped" then return "⏸" end
    if state == "running" then return "▶" end
    return "?"
end

function Frame:location()
    return string.format("%s:%d", self.source:get(), self.line:get())
end

function Variable:display()
    local t = self.varType:get()
    if t == "table" or t == "function" then
        return string.format("%s (%s)", self.name:get(), t)
    end
    return string.format("%s = %s", self.name:get(), self.value:get())
end

-- ============================================================================
-- Database Setup
-- ============================================================================

local graph = ng.graph(schema)
local db = sdk.wrap(graph, {
    Debugger = Debugger,
    Thread = Thread,
    Frame = Frame,
    Scope = Scope,
    Variable = Variable,
})

-- Create debugger singleton
local debugger = Debugger.insert({ name = "main" })

-- ============================================================================
-- Demo State
-- ============================================================================

local state = {
    buf = nil,
    win = nil,
    view = nil,
    cursor_line = 1,
    status_message = "Ready",
    subscriptions = {},
}

-- ============================================================================
-- View Query
-- ============================================================================

local function create_view()
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
                        "name": "variables",
                        "edges": [{ "name": "children" }]
                    }]
                }]
            }]
        }]
    }]], debugger:id())

    return graph:view(query, { limit = 100, immediate = true })
end

-- ============================================================================
-- Rendering
-- ============================================================================

local function render_item(item, entity)
    local indent = string.rep("  ", item.depth)
    local icon = ""

    if item.has_children then
        icon = item.expanded and "▼ " or "▶ "
    else
        icon = "  "
    end

    local text = ""
    local hl = nil

    if item.type == "Debugger" then
        return nil  -- Skip virtual root
    elseif item.type == "Thread" then
        local thread = entity
        text = string.format("%s Thread: %s [%s]",
            thread:statusIcon(),
            thread.name:get(),
            thread.state:get())
        hl = thread:isStopped() and "DiagnosticWarn" or "DiagnosticOk"
    elseif item.type == "Frame" then
        local frame = entity
        text = string.format("%s() at %s",
            frame.name:get(),
            frame:location())
        hl = "Function"
    elseif item.type == "Scope" then
        local scope = entity
        text = string.format("[%s]", scope.name:get())
        hl = "Type"
    elseif item.type == "Variable" then
        local var = entity
        text = var:display()
        hl = "Identifier"
    end

    return {
        text = indent .. icon .. text,
        hl = hl,
        id = item.id,
    }
end

local function render()
    if not state.buf or not vim.api.nvim_buf_is_valid(state.buf) then
        return
    end

    local lines = {}
    local highlights = {}
    local items = state.view:get_visible()

    -- DEBUG: count new_var items
    local new_var_count = 0
    for _, item in ipairs(items) do
        if item.name and item.name:match("^new_var") then
            new_var_count = new_var_count + 1
        end
    end
    print("render(): " .. #items .. " items, " .. new_var_count .. " new_vars")

    -- Header
    table.insert(lines, "╔═══════════════════════════════════════════════════════════╗")
    table.insert(lines, "║  neograph DAP Demo - SDK Entity Patterns                  ║")
    table.insert(lines, "╠═══════════════════════════════════════════════════════════╣")

    local content_start = #lines

    -- Content
    for _, item in ipairs(items) do
        local entity = db:get(item.id)
        local rendered = render_item(item, entity)
        if rendered then
            table.insert(lines, "║ " .. rendered.text .. string.rep(" ", 58 - #rendered.text) .. "║")
            if rendered.hl then
                table.insert(highlights, {
                    line = #lines - 1,
                    col = 2,
                    end_col = 2 + #rendered.text,
                    hl = rendered.hl,
                })
            end
        end
    end

    if #items <= 1 then
        table.insert(lines, "║   (no threads - press 't' to create one)                  ║")
    end

    -- Footer
    table.insert(lines, "╠═══════════════════════════════════════════════════════════╣")
    table.insert(lines, "║ j/k:navigate  o:toggle  t:thread  s:stop  c:continue       ║")
    table.insert(lines, "║ v:add-var    d:delete  u:update  r:refresh  q:quit         ║")
    table.insert(lines, "╠═══════════════════════════════════════════════════════════╣")

    local status = state.status_message or ""
    table.insert(lines, "║ " .. status .. string.rep(" ", 58 - #status) .. "║")
    table.insert(lines, "╚═══════════════════════════════════════════════════════════╝")

    -- Update buffer
    vim.api.nvim_buf_set_option(state.buf, "modifiable", true)
    vim.api.nvim_buf_set_lines(state.buf, 0, -1, false, lines)
    vim.api.nvim_buf_set_option(state.buf, "modifiable", false)

    -- Apply highlights
    vim.api.nvim_buf_clear_namespace(state.buf, -1, 0, -1)
    for _, hl in ipairs(highlights) do
        vim.api.nvim_buf_add_highlight(state.buf, -1, hl.hl, hl.line, hl.col, hl.end_col)
    end

    -- Position cursor
    local cursor_line = math.min(state.cursor_line + content_start, #lines - 5)
    cursor_line = math.max(content_start + 1, cursor_line)
    vim.api.nvim_win_set_cursor(state.win, { cursor_line, 3 })
end

local function set_status(msg)
    state.status_message = msg
    render()
    -- Clear after delay
    vim.defer_fn(function()
        if state.status_message == msg then
            state.status_message = "Ready"
            render()
        end
    end, 2000)
end

-- ============================================================================
-- Actions
-- ============================================================================

local function get_selected_item()
    local items = state.view:get_visible()
    local idx = state.cursor_line
    -- Skip Debugger virtual root
    local real_items = {}
    for _, item in ipairs(items) do
        if item.type ~= "Debugger" then
            table.insert(real_items, item)
        end
    end
    print("get_selected_item: cursor_line=" .. idx .. " real_items=" .. #real_items)
    for i, item in ipairs(real_items) do
        print("  " .. i .. ": " .. item.type .. " id=" .. item.id)
    end
    if idx >= 1 and idx <= #real_items then
        return real_items[idx]
    end
    return nil
end

local function get_edge_for_type(type_name)
    local edges = {
        Debugger = "threads",
        Thread = "frames",
        Frame = "scopes",
        Scope = "variables",
        Variable = "children",
    }
    return edges[type_name]
end

local function action_toggle()
    local item = get_selected_item()
    if item and item.has_children then
        local edge = get_edge_for_type(item.type)
        if edge then
            state.view:toggle(item.id, edge)
            render()
            set_status("Toggled " .. item.type)
        end
    end
end

local function action_create_thread()
    local thread_num = 1
    for _ in debugger.threads:iter() do
        thread_num = thread_num + 1
    end

    local thread = Thread.insert({
        name = "Thread " .. thread_num,
        state = "running",
    })
    debugger.threads:link(thread)

    -- React to state changes
    local unsub = thread.state:use(function(new_state)
        set_status(string.format("Thread %s -> %s", thread.name:get(), new_state))
        return function()
            -- Cleanup runs before next state or on delete
        end
    end)
    table.insert(state.subscriptions, unsub)

    render()
    set_status("Created " .. thread.name:get())
end

local function action_stop_thread()
    local item = get_selected_item()
    if not item then
        set_status("Select a thread first")
        return
    end

    -- Find the thread for this item
    local thread = nil
    if item.type == "Thread" then
        thread = db:get(item.id)
    elseif item.type == "Frame" then
        local frame = db:get(item.id)
        for t in frame.thread:iter() do
            thread = t
            break
        end
    elseif item.type == "Scope" then
        local scope = db:get(item.id)
        for f in scope.frame:iter() do
            for t in f.thread:iter() do
                thread = t
                break
            end
            break
        end
    end

    if not thread then
        set_status("Could not find thread")
        return
    end

    if thread:isStopped() then
        set_status("Thread already stopped")
        return
    end

    -- Stop the thread and create stack trace
    thread:update({ state = "stopped" })

    -- Create stack frames
    local frame1 = Frame.insert({
        name = "main",
        source = "app.lua",
        line = 42,
    })
    thread.frames:link(frame1)

    local frame2 = Frame.insert({
        name = "init",
        source = "app.lua",
        line = 10,
    })
    thread.frames:link(frame2)

    -- Create scopes for first frame
    local locals = Scope.insert({ name = "Locals", expensive = false })
    frame1.scopes:link(locals)

    local upvalues = Scope.insert({ name = "Upvalues", expensive = false })
    frame1.scopes:link(upvalues)

    -- Add some variables to locals
    local var1 = Variable.insert({ name = "counter", value = "42", varType = "number" })
    locals.variables:link(var1)

    local var2 = Variable.insert({ name = "name", value = '"Alice"', varType = "string" })
    locals.variables:link(var2)

    local var3 = Variable.insert({ name = "user", value = "...", varType = "table" })
    locals.variables:link(var3)

    -- Add children to user table
    local child1 = Variable.insert({ name = "id", value = "123", varType = "number" })
    var3.children:link(child1)

    local child2 = Variable.insert({ name = "active", value = "true", varType = "boolean" })
    var3.children:link(child2)

    render()
    set_status("Stopped " .. thread.name:get() .. " - stack trace created")
end

local function action_continue_thread()
    local item = get_selected_item()
    if not item then
        set_status("Select a thread first")
        return
    end

    -- Find the thread
    local thread = nil
    if item.type == "Thread" then
        thread = db:get(item.id)
    end

    if not thread then
        set_status("Select a thread")
        return
    end

    if not thread:isStopped() then
        set_status("Thread not stopped")
        return
    end

    -- Delete all frames (cascades to scopes and variables)
    local frames_to_delete = {}
    for frame in thread.frames:iter() do
        table.insert(frames_to_delete, frame)
    end

    for _, frame in ipairs(frames_to_delete) do
        -- Delete scopes
        local scopes_to_delete = {}
        for scope in frame.scopes:iter() do
            table.insert(scopes_to_delete, scope)
        end
        for _, scope in ipairs(scopes_to_delete) do
            -- Delete variables (recursive)
            local function delete_vars(parent_scope_or_var, is_scope)
                local vars_to_delete = {}
                local iter = is_scope and parent_scope_or_var.variables:iter() or parent_scope_or_var.children:iter()
                for var in iter do
                    table.insert(vars_to_delete, var)
                end
                for _, var in ipairs(vars_to_delete) do
                    delete_vars(var, false)
                    var:delete()
                end
            end
            delete_vars(scope, true)
            scope:delete()
        end
        frame:delete()
    end

    thread:update({ state = "running" })
    render()
    set_status("Continued " .. thread.name:get() .. " - stack cleared")
end

local add_count = 0
local function action_add_variable()
    add_count = add_count + 1
    local item = get_selected_item()
    if not item then
        set_status("Select a scope or variable")
        return
    end

    print("ADD #" .. add_count .. ": item.type=" .. item.type .. " item.id=" .. item.id)

    if item.type == "Scope" then
        local scope = db:get(item.id)
        local var = Variable.insert({
            name = "new_var_" .. math.random(100),
            value = tostring(math.random(1000)),
            varType = "number",
        })
        print("  linking var " .. var:id() .. " to scope " .. scope:id())
        scope.variables:link(var)
        print("  linked!")
        set_status("Added variable to " .. scope.name:get())
    elseif item.type == "Variable" then
        local parent = db:get(item.id)
        local child = Variable.insert({
            name = "child_" .. math.random(100),
            value = tostring(math.random(1000)),
            varType = "number",
        })
        parent.children:link(child)
        set_status("Added child to " .. parent.name:get())
    else
        set_status("Select a scope or variable")
        return
    end

    render()
end

local function action_delete()
    local item = get_selected_item()
    if not item then
        set_status("Nothing selected")
        return
    end

    local entity = db:get(item.id)
    local name = ""

    if item.type == "Thread" then
        name = entity.name:get()
        -- First continue to clear stack
        if entity:isStopped() then
            action_continue_thread()
        end
        entity:delete()
    elseif item.type == "Variable" then
        name = entity.name:get()
        entity:delete()
    else
        set_status("Can only delete threads or variables")
        return
    end

    render()
    set_status("Deleted " .. name)
end

local function action_update()
    local item = get_selected_item()
    if not item then
        set_status("Nothing selected")
        return
    end

    if item.type == "Variable" then
        local var = db:get(item.id)
        local new_value = tostring(math.random(1000))
        var:update({ value = new_value })
        set_status("Updated " .. var.name:get() .. " = " .. new_value)
    elseif item.type == "Thread" then
        local thread = db:get(item.id)
        local new_name = "Thread-" .. math.random(100)
        thread:update({ name = new_name })
        set_status("Renamed to " .. new_name)
    else
        set_status("Can only update threads or variables")
        return
    end

    render()
end

local function action_navigate(delta)
    local items = state.view:get_visible()
    local max_lines = 0
    for _, item in ipairs(items) do
        if item.type ~= "Debugger" then
            max_lines = max_lines + 1
        end
    end

    state.cursor_line = state.cursor_line + delta
    state.cursor_line = math.max(1, math.min(state.cursor_line, max_lines))
    render()
end

-- ============================================================================
-- UI Setup
-- ============================================================================

local function setup_ui()
    -- Create buffer
    state.buf = vim.api.nvim_create_buf(false, true)
    vim.api.nvim_buf_set_option(state.buf, "buftype", "nofile")
    vim.api.nvim_buf_set_option(state.buf, "bufhidden", "wipe")
    vim.api.nvim_buf_set_option(state.buf, "swapfile", false)
    vim.api.nvim_buf_set_name(state.buf, "DAP Demo")

    -- Create window
    local width = 63
    local height = 25
    local row = math.floor((vim.o.lines - height) / 2)
    local col = math.floor((vim.o.columns - width) / 2)

    state.win = vim.api.nvim_open_win(state.buf, true, {
        relative = "editor",
        width = width,
        height = height,
        row = row,
        col = col,
        style = "minimal",
        border = "none",
    })

    -- Create view
    state.view = create_view()

    -- Auto-expand the debugger root to show threads
    state.view:expand(debugger:id(), "threads")

    -- Keymaps
    local opts = { buffer = state.buf, silent = true }

    vim.keymap.set("n", "j", function() action_navigate(1) end, opts)
    vim.keymap.set("n", "k", function() action_navigate(-1) end, opts)
    vim.keymap.set("n", "o", action_toggle, opts)
    vim.keymap.set("n", "<CR>", action_toggle, opts)
    vim.keymap.set("n", "t", action_create_thread, opts)
    vim.keymap.set("n", "s", action_stop_thread, opts)
    vim.keymap.set("n", "c", action_continue_thread, opts)
    vim.keymap.set("n", "v", action_add_variable, opts)
    vim.keymap.set("n", "d", action_delete, opts)
    vim.keymap.set("n", "u", action_update, opts)
    vim.keymap.set("n", "r", render, opts)
    vim.keymap.set("n", "q", function()
        -- Cleanup subscriptions
        for _, unsub in ipairs(state.subscriptions) do
            unsub()
        end
        vim.api.nvim_win_close(state.win, true)
        vim.cmd("qa!")
    end, opts)

    -- Initial render
    render()
end

-- ============================================================================
-- Main
-- ============================================================================

-- Run setup on next event loop iteration (works both with -u and :luafile)
vim.schedule(function()
    setup_ui()
    set_status("Press 't' to create a thread, 's' to stop it")
end)
