-- Neograph DAP Demo: 100k node virtualized tree
-- Run with: nvim -u demo/dap_demo.lua

-- Setup path
local cwd = vim.fn.getcwd()
package.cpath = cwd .. "/lua/?.so;;" .. package.cpath

local ng = require("neograph_lua")

-- ============================================================================
-- Schema: DAP-like debug structure
-- ============================================================================

local g = ng.graph([[{
    "types": [
        {
            "name": "Thread",
            "properties": [
                { "name": "name", "type": "string" },
                { "name": "tid", "type": "int" },
                { "name": "stopped", "type": "bool" }
            ],
            "edges": [
                { "name": "frames", "target": "StackFrame", "reverse": "thread" }
            ],
            "indexes": [{ "fields": [{ "name": "tid" }] }]
        },
        {
            "name": "StackFrame",
            "properties": [
                { "name": "name", "type": "string" },
                { "name": "line", "type": "int" },
                { "name": "file", "type": "string" }
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
                { "name": "frame", "target": "StackFrame", "reverse": "scopes" },
                { "name": "variables", "target": "Variable", "reverse": "scope" }
            ],
            "indexes": [{ "fields": [{ "name": "name" }] }]
        },
        {
            "name": "Variable",
            "properties": [
                { "name": "name", "type": "string" },
                { "name": "value", "type": "string" },
                { "name": "vtype", "type": "string" }
            ],
            "edges": [
                { "name": "scope", "target": "Scope", "reverse": "variables" },
                { "name": "children", "target": "Variable", "reverse": "parent" },
                { "name": "parent", "target": "Variable", "reverse": "children" }
            ],
            "indexes": [{ "fields": [{ "name": "name" }] }]
        }
    ]
}]])

-- ============================================================================
-- Generate 100k nodes
-- ============================================================================

local function generate_data(target_nodes)
    target_nodes = target_nodes or 100000
    local start = vim.loop.hrtime()
    local node_count = 0

    -- Variable name pools for realistic data
    local var_names = {
        "self", "this", "ctx", "config", "options", "result", "data", "item",
        "key", "value", "index", "count", "size", "length", "buffer", "cache",
        "state", "props", "args", "params", "response", "request", "error",
        "message", "status", "kind", "name", "id", "path", "url", "port",
    }

    local types = {
        "Object", "Array", "String", "Number", "Boolean", "Function",
        "Map", "Set", "WeakMap", "Promise", "Buffer", "Stream",
    }

    local function random_var_name(idx)
        return var_names[(idx % #var_names) + 1] .. "_" .. idx
    end

    local function random_type(idx)
        return types[(idx % #types) + 1]
    end

    local function random_value(typ, idx)
        if typ == "String" then
            return '"value_' .. idx .. '"'
        elseif typ == "Number" then
            return tostring(idx * 17 % 99999)
        elseif typ == "Boolean" then
            return idx % 2 == 0 and "true" or "false"
        elseif typ == "Array" then
            return "Array(" .. (idx % 100) .. ")"
        else
            return typ .. " {...}"
        end
    end

    -- Simpler iterative generation for better control
    local var_idx = 0

    -- Create threads
    local thread_count = 3
    local frames_per_thread = 10
    local scopes_per_frame = 3
    local base_vars_per_scope = 50
    local children_per_var = 8
    local grandchildren_per_var = 5

    -- Calculate to hit target
    local vars_per_scope = math.floor(target_nodes / (thread_count * frames_per_thread * scopes_per_frame))
    vars_per_scope = math.max(10, math.min(vars_per_scope, 200))

    for t = 1, thread_count do
        local thread_id = g:insert("Thread", {
            name = "Thread-" .. t,
            tid = t,
            stopped = t == 1,
        })
        node_count = node_count + 1

        for f = 1, frames_per_thread do
            local frame_id = g:insert("StackFrame", {
                name = f == 1 and "<anonymous>" or ("function_" .. ((t * 100 + f) % 1000)),
                line = (t * f * 17) % 500 + 1,
                file = "/src/module_" .. ((t + f) % 50) .. ".lua",
            })
            node_count = node_count + 1
            g:link(thread_id, "frames", frame_id)

            local scope_names = { "Local", "Closure", "Global" }
            for s, scope_name in ipairs(scope_names) do
                local scope_id = g:insert("Scope", {
                    name = scope_name,
                    expensive = scope_name == "Global",
                })
                node_count = node_count + 1
                g:link(frame_id, "scopes", scope_id)

                -- Create variables
                for v = 1, vars_per_scope do
                    var_idx = var_idx + 1
                    local vtype = random_type(var_idx)
                    local is_container = vtype == "Object" or vtype == "Array" or vtype == "Map"

                    local var_id = g:insert("Variable", {
                        name = random_var_name(var_idx),
                        value = random_value(vtype, var_idx),
                        vtype = vtype,
                    })
                    node_count = node_count + 1
                    g:link(scope_id, "variables", var_id)

                    -- Add children for container types
                    if is_container then
                        for c = 1, children_per_var do
                            var_idx = var_idx + 1
                            local child_type = random_type(var_idx)
                            local child_is_container = child_type == "Object" or child_type == "Array"

                            local child_id = g:insert("Variable", {
                                name = random_var_name(var_idx),
                                value = random_value(child_type, var_idx),
                                vtype = child_type,
                            })
                            node_count = node_count + 1
                            g:link(var_id, "children", child_id)

                            -- Add grandchildren
                            if child_is_container then
                                for gc = 1, grandchildren_per_var do
                                    var_idx = var_idx + 1
                                    local gc_type = random_type(var_idx)

                                    local gc_id = g:insert("Variable", {
                                        name = random_var_name(var_idx),
                                        value = random_value(gc_type, var_idx),
                                        vtype = gc_type,
                                    })
                                    node_count = node_count + 1
                                    g:link(child_id, "children", gc_id)
                                end
                            end
                        end
                    end

                    -- Early exit if we hit target
                    if node_count >= target_nodes then break end
                end
                if node_count >= target_nodes then break end
            end
            if node_count >= target_nodes then break end
        end
        if node_count >= target_nodes then break end
    end

    local elapsed = (vim.loop.hrtime() - start) / 1e6
    return node_count, elapsed
end

-- ============================================================================
-- Tree View UI (Virtualized)
-- ============================================================================

local TreeView = {}
TreeView.__index = TreeView

function TreeView.new(graph, query)
    local self = setmetatable({}, TreeView)

    self.graph = graph
    self.viewport_height = 30
    self.tree = graph:view(query, {limit = self.viewport_height, immediate = false})

    -- UI state
    self.buf = vim.api.nvim_create_buf(false, true)
    self.win = nil
    self.last_toggle_time = nil
    self.total_nodes = 0
    self.header_lines = 2  -- Number of header lines before tree content
    self.footer_lines = 2  -- Number of footer lines after tree content
    self.last_scroll_top = -1  -- Track last scroll position to avoid redundant renders
    self.autocmd_group = vim.api.nvim_create_augroup("NeographTreeView", { clear = true })
    self.line_items = {}  -- Map buffer line (1-indexed) to item data {path, type}

    -- Icons
    self.icons = {
        expanded = "",
        collapsed = "",
        leaf = " ",
        thread = "",
        frame = "",
        scope = "",
        variable = "",
    }

    return self
end

function TreeView:get_icon_for_item(item)
    local type_icons = {
        Thread = self.icons.thread,
        StackFrame = self.icons.frame,
        Scope = self.icons.scope,
        Variable = self.icons.variable,
    }
    return type_icons[item.type] or ""
end

function TreeView:format_item(item)
    local indent = string.rep("  ", item.depth)

    -- Determine expand icon based on type (all but leaf Variables are expandable)
    local is_expandable = item.type == "Thread" or item.type == "StackFrame" or
                          item.type == "Scope" or item.expandable
    local expand_icon
    if is_expandable then
        expand_icon = item.expanded and self.icons.expanded or self.icons.collapsed
    else
        expand_icon = self.icons.leaf
    end

    -- Format based on type
    local type_icon = self:get_icon_for_item(item)

    if item.type == "Thread" then
        local stopped = item.stopped and " [stopped]" or ""
        return string.format("%s%s %s %s%s",
            indent, expand_icon, type_icon, item.name or "?", stopped)
    elseif item.type == "StackFrame" then
        return string.format("%s%s %s %s  %s:%d",
            indent, expand_icon, type_icon, item.name or "?", item.file or "?", item.line or 0)
    elseif item.type == "Scope" then
        local expensive = item.expensive and " [expensive]" or ""
        return string.format("%s%s %s %s%s",
            indent, expand_icon, type_icon, item.name or "?", expensive)
    elseif item.type == "Variable" then
        return string.format("%s%s %s %s: %s  (%s)",
            indent, expand_icon, type_icon,
            item.name or "?", item.value or "?", item.vtype or "?")
    else
        return string.format("%s%s %s", indent, expand_icon, item.name or tostring(item.id))
    end
end

function TreeView:render_full()
    -- Full buffer rebuild - called when total_visible changes (expand/collapse)
    local stats = self.tree:stats()
    local total_lines = self.header_lines + stats.total_visible + self.footer_lines

    -- Build full buffer with empty lines for tree content
    local lines = {}

    -- Header (will be updated in render_viewport)
    table.insert(lines, "")  -- Header line 1 - stats
    table.insert(lines, string.rep("─", 56))  -- Header line 2 - separator

    -- Empty lines for all tree items (virtualized)
    for _ = 1, stats.total_visible do
        table.insert(lines, "")
    end

    -- Footer
    table.insert(lines, string.rep("─", 56))
    table.insert(lines, " <CR>/<Space>: toggle  q: quit")

    -- Update buffer
    vim.api.nvim_buf_set_option(self.buf, "modifiable", true)
    vim.api.nvim_buf_set_lines(self.buf, 0, -1, false, lines)
    vim.api.nvim_buf_set_option(self.buf, "modifiable", false)

    -- Reset scroll tracking to force viewport render
    self.last_scroll_top = -1

    -- Now render the visible viewport
    self:render_viewport()
end

function TreeView:render_viewport()
    -- Only render lines currently visible in the window viewport
    if not self.win or not vim.api.nvim_win_is_valid(self.win) then
        return
    end

    local win_info = vim.fn.getwininfo(self.win)[1]
    local scroll_top = win_info.topline - 1  -- 0-indexed
    local win_height = win_info.height

    -- Skip if scroll position hasn't changed
    if scroll_top == self.last_scroll_top then
        return
    end
    self.last_scroll_top = scroll_top

    local stats = self.tree:stats()
    local start_time = vim.loop.hrtime()

    -- Calculate which tree items are visible
    -- scroll_top is the buffer line at top of window
    -- Tree content starts at line header_lines (0-indexed)
    local tree_start = self.header_lines
    local tree_end = tree_start + stats.total_visible

    -- Viewport in buffer coordinates
    local viewport_start = scroll_top
    local viewport_end = scroll_top + win_height

    -- Calculate tree item range to render
    local first_tree_item = math.max(0, viewport_start - tree_start)
    local last_tree_item = math.min(stats.total_visible - 1, viewport_end - tree_start - 1)

    -- Update tree's internal viewport for get_visible
    self.tree:scroll_to(first_tree_item)
    local visible_count = last_tree_item - first_tree_item + 1
    if visible_count ~= self.viewport_height then
        self.viewport_height = math.max(1, visible_count)
        self.tree:set_viewport(self.viewport_height)
    end

    local visible = self.tree:get_visible()
    local get_visible_time = (vim.loop.hrtime() - start_time) / 1e6

    -- Update header with stats
    local toggle_str = self.last_toggle_time and string.format("  toggle: %.2fms", self.last_toggle_time) or ""
    local header = string.format(
        " DAP (%dk nodes)  %d visible  scroll: %d/%d  render: %.2fms%s",
        math.floor(self.total_nodes / 1000),
        stats.total_visible,
        first_tree_item,
        math.max(0, stats.total_visible - win_height),
        get_visible_time,
        toggle_str
    )

    -- Build lines to update
    vim.api.nvim_buf_set_option(self.buf, "modifiable", true)

    -- Update header
    vim.api.nvim_buf_set_lines(self.buf, 0, 1, false, { header })

    -- Update visible tree items and store item data for toggle
    for i, item in ipairs(visible) do
        local buf_line = tree_start + first_tree_item + i - 1
        local text = self:format_item(item)
        vim.api.nvim_buf_set_lines(self.buf, buf_line, buf_line + 1, false, { text })
        -- Store item info for this buffer line (1-indexed for lookup)
        self.line_items[buf_line + 1] = { id = item.id, type = item.type, name = item.name }
    end

    vim.api.nvim_buf_set_option(self.buf, "modifiable", false)
end

-- Legacy render for compatibility
function TreeView:render()
    self:render_full()
end

function TreeView:get_cursor_tree_index()
    -- Get the tree item index from the current cursor position
    if not self.win or not vim.api.nvim_win_is_valid(self.win) then
        return nil
    end
    local cursor = vim.api.nvim_win_get_cursor(self.win)
    local buf_line = cursor[1] - 1  -- 0-indexed
    local tree_index = buf_line - self.header_lines
    local stats = self.tree:stats()
    if tree_index >= 0 and tree_index < stats.total_visible then
        return tree_index
    end
    return nil
end

function TreeView:toggle_current()
    if not self.win or not vim.api.nvim_win_is_valid(self.win) then
        return
    end

    -- Get current cursor line (1-indexed)
    local cursor = vim.api.nvim_win_get_cursor(self.win)
    local buf_line = cursor[1]

    -- Look up stored item data for this line
    local item = self.line_items[buf_line]
    if not item then return end

    -- Determine edge name based on item type
    local edge_name = nil
    if item.type == "Thread" then
        edge_name = "frames"
    elseif item.type == "StackFrame" then
        edge_name = "scopes"
    elseif item.type == "Scope" then
        edge_name = "variables"
    elseif item.type == "Variable" then
        -- Variables can have children
        edge_name = "children"
    end

    if edge_name and item.id then
        local start = vim.loop.hrtime()
        self.tree:toggle(item.id, edge_name)
        local toggle_time = (vim.loop.hrtime() - start) / 1e6
        self.last_toggle_time = toggle_time

        -- Clear stored line items before rebuild
        self.line_items = {}

        -- Store cursor position before rebuild
        local cursor_pos = vim.api.nvim_win_get_cursor(self.win)

        -- Rebuild buffer with new line count
        self:render_full()

        -- Restore cursor position
        local stats = self.tree:stats()
        local max_line = self.header_lines + stats.total_visible
        if cursor_pos[1] > max_line then
            cursor_pos[1] = max_line
        end
        pcall(vim.api.nvim_win_set_cursor, self.win, cursor_pos)
    end
end

function TreeView:setup_keymaps()
    local opts = { buffer = self.buf, noremap = true, silent = true }

    -- Only toggle and quit - navigation is handled by native vim motions + autocmd
    vim.keymap.set("n", "<CR>", function() self:toggle_current() end, opts)
    vim.keymap.set("n", "<Space>", function() self:toggle_current() end, opts)
    vim.keymap.set("n", "l", function() self:toggle_current() end, opts)
    vim.keymap.set("n", "h", function() self:toggle_current() end, opts)

    -- Quit
    vim.keymap.set("n", "q", function()
        if self.win and vim.api.nvim_win_is_valid(self.win) then
            vim.api.nvim_win_close(self.win, true)
        end
        vim.cmd("qa!")
    end, opts)
end

function TreeView:setup_autocmds()
    -- Disabled for now - manual render on toggle is sufficient
    -- TODO: Re-enable when we figure out why it causes hangs with 100k nodes
end

function TreeView:open()
    -- Create window
    self.win = vim.api.nvim_open_win(self.buf, true, {
        relative = "editor",
        width = vim.o.columns - 4,
        height = vim.o.lines - 4,
        row = 1,
        col = 2,
        style = "minimal",
        border = "rounded",
        title = " Neograph DAP Demo ",
        title_pos = "center",
    })

    -- Window options
    vim.api.nvim_win_set_option(self.win, "cursorline", true)
    vim.api.nvim_win_set_option(self.win, "wrap", false)

    -- Buffer options
    vim.api.nvim_buf_set_option(self.buf, "buftype", "nofile")
    vim.api.nvim_buf_set_option(self.buf, "bufhidden", "wipe")
    vim.api.nvim_buf_set_option(self.buf, "filetype", "neograph")

    self:setup_keymaps()
    self:setup_autocmds()
    self:render_full()

    -- Position cursor on first tree item (after header)
    vim.api.nvim_win_set_cursor(self.win, { self.header_lines + 1, 0 })
end

-- ============================================================================
-- Main
-- ============================================================================

-- Note: 100k nodes works but tree creation is slow (~30s)
-- Use 10k for quick testing, 100k for full demo
local node_count, gen_time = generate_data(10000)
-- Query: All threads, sorted by tid, with nested frames/scopes/variables
local query = [[{
    "root": "Thread",
    "sort": [{ "field": "tid" }],
    "edges": [
        {
            "name": "frames",
            "sort": [{ "field": "line" }],
            "edges": [
                {
                    "name": "scopes",
                    "sort": [{ "field": "name" }],
                    "edges": [
                        {
                            "name": "variables",
                            "sort": [{ "field": "name" }],
                            "edges": [
                                {
                                    "name": "children",
                                    "sort": [{ "field": "name" }],
                                    "edges": [
                                        {
                                            "name": "children",
                                            "sort": [{ "field": "name" }]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]
}]]

local view = TreeView.new(g, query)
view.total_nodes = node_count

-- Open the view
vim.schedule(function()
    view:open()
end)
