-- BUG REPRODUCTION TEST: Linking new nodes to expanded edge stops updating after 3 links
--
-- See: docs/BUG-reactive-link-expanded-edge.md
--
-- The bug: After expanding an edge and linking 3 new nodes, subsequent links
-- don't appear in get_visible() until collapse/re-expand.
--
-- This test uses feedkeys to simulate real keypresses, which is required to
-- trigger the bug. Direct function calls don't reproduce it.
--
-- Run with: nvim --headless -u test/bug_link_expanded_edge.lua

local cwd = vim.fn.getcwd()

-- Track render output
local render_data = {}

-- Hook print to capture render() output
local original_print = print
_G.print = function(...)
    local args = {...}
    local msg = table.concat(vim.tbl_map(tostring, args), " ")

    local items, new_vars = msg:match("render%(%):%s*(%d+)%s*items,%s*(%d+)%s*new_vars")
    if items then
        table.insert(render_data, { items = tonumber(items), new_vars = tonumber(new_vars) })
    end

    original_print(...)
end

-- Load the demo
vim.schedule(function()
    dofile(cwd .. "/test/demo_bisect.lua")

    vim.defer_fn(function()
        original_print("=== BUG REPRODUCTION TEST ===")
        original_print("Using feedkeys to simulate real keypresses")
        original_print("")

        render_data = {}

        -- Setup: t (create thread), s (stop), o j o j o (expand tree to [Locals])
        vim.api.nvim_feedkeys("tsojojo", "mtx", false)

        vim.defer_fn(function()
            local initial = render_data[#render_data]
            original_print("Initial: " .. (initial and initial.items or "?") .. " items")
            original_print("")

            local before_count = #render_data
            local v_pressed = 0
            local NUM_VARS = 5

            local function press_v()
                v_pressed = v_pressed + 1
                if v_pressed > NUM_VARS then
                    vim.defer_fn(function()
                        original_print("")
                        original_print("=== RESULTS ===")

                        -- Analyze results
                        local all_passed = true
                        local first_fail = nil
                        for i = 1, NUM_VARS do
                            local idx = before_count + (i * 2)  -- Each 'v' generates 2 render calls
                            local r = render_data[idx]
                            if r then
                                local expected = 8 + i  -- 8 initial + i new vars
                                local pass = (r.items == expected)
                                original_print(string.format("v #%d: expected %d items, got %d %s",
                                    i, expected, r.items, pass and "✓" or "❌ BUG!"))
                                if not pass then
                                    all_passed = false
                                    if not first_fail then first_fail = i end
                                end
                            end
                        end

                        original_print("")
                        if all_passed then
                            original_print("=== PASS ===")
                            vim.cmd("qa!")
                        else
                            original_print("=== BUG REPRODUCED ===")
                            original_print("Links stopped appearing after #" .. (first_fail - 1))

                            -- Verify with collapse/re-expand
                            original_print("")
                            original_print("Verifying with collapse/re-expand (o o)...")
                            vim.api.nvim_feedkeys("oo", "mtx", false)

                            vim.defer_fn(function()
                                local final = render_data[#render_data]
                                original_print("After collapse/re-expand: " .. final.items .. " items, " .. final.new_vars .. " new_vars")
                                original_print("Expected: " .. (8 + NUM_VARS) .. " items, " .. NUM_VARS .. " new_vars")
                                vim.cmd("cq 1")
                            end, 300)
                        end
                    end, 300)
                    return
                end

                vim.api.nvim_feedkeys("v", "mtx", false)
                vim.defer_fn(press_v, 100)
            end

            press_v()
        end, 800)
    end, 300)
end)

vim.defer_fn(function()
    original_print("Test timeout!")
    vim.cmd("cq 3")
end, 15000)
