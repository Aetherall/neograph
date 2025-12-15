#!/usr/bin/env -S nvim --headless -u NONE -l
-- Test: view:on("enter") and view:on("leave") for expanded edges
--
-- Run: nvim --headless -u NONE -l test/view_events_test.lua

local cwd = vim.fn.getcwd()
package.cpath = cwd .. "/lua/?.so;" .. package.cpath

local ng = require("neograph_lua")

local passed = 0
local failed = 0

local function test(name, fn)
    local ok, err = pcall(fn)
    if ok then
        print("  PASS: " .. name)
        passed = passed + 1
    else
        print("  FAIL: " .. name .. " - " .. tostring(err))
        failed = failed + 1
    end
end

local function assert_eq(a, b, msg)
    if a ~= b then
        error((msg or "assertion failed") .. ": " .. tostring(a) .. " ~= " .. tostring(b))
    end
end

local function assert_true(v, msg)
    if not v then
        error(msg or "expected true")
    end
end

-- Schema with Parent/Child relationship
local schema = [[{
  "types": [
    {
      "name": "Parent",
      "properties": [{ "name": "name", "type": "string" }],
      "edges": [{ "name": "children", "target": "Child", "reverse": "parent" }],
      "indexes": [{ "fields": [{ "field": "name" }] }]
    },
    {
      "name": "Child",
      "properties": [{ "name": "name", "type": "string" }],
      "edges": [{ "name": "parent", "target": "Parent", "reverse": "children" }],
      "indexes": [{ "fields": [{ "field": "name" }] }]
    }
  ]
}]]

print("\n=== View Events Tests ===\n")

print("1. Enter events for expanded edges")

test("enter fires when linking to already-expanded edge", function()
    local g = ng.graph(schema)

    local parent = g:insert("Parent", { name = "p" })
    local child1 = g:insert("Child", { name = "c1" })
    g:link(parent, "children", child1)

    local view = g:query(string.format(
        [[{ "root": "Parent", "id": %d, "edges": [{ "name": "children" }] }]],
        parent
    ), { limit = 100 })

    -- Expand BEFORE registering callback
    view:expand(parent, "children")

    local enter_count = 0
    local entered_ids = {}
    local unsub = view:on("enter", function(item, index)
        enter_count = enter_count + 1
        table.insert(entered_ids, item.id)
    end)

    -- Link new child to already-expanded edge
    local child2 = g:insert("Child", { name = "c2" })
    g:link(parent, "children", child2)

    -- Verify child2 is in the view
    local items = view:items()
    local found = false
    for _, item in ipairs(items) do
        if item.id == child2 then found = true break end
    end
    assert_true(found, "child2 should appear in view:items()")

    -- Verify enter callback fired
    assert_eq(enter_count, 1, "enter callback should fire once")
    assert_eq(entered_ids[1], child2, "entered item should be child2")

    unsub()
end)

test("enter fires for multiple links to expanded edge", function()
    local g = ng.graph(schema)

    local parent = g:insert("Parent", { name = "p" })

    local view = g:query(string.format(
        [[{ "root": "Parent", "id": %d, "edges": [{ "name": "children" }] }]],
        parent
    ), { limit = 100 })

    view:expand(parent, "children")

    local enter_count = 0
    local unsub = view:on("enter", function() enter_count = enter_count + 1 end)

    -- Link multiple children
    for i = 1, 5 do
        local child = g:insert("Child", { name = "c" .. i })
        g:link(parent, "children", child)
    end

    assert_eq(enter_count, 5, "enter should fire for each linked child")

    unsub()
end)

print("\n2. Leave events for expanded edges")

test("leave fires when unlinking from already-expanded edge", function()
    local g = ng.graph(schema)

    local parent = g:insert("Parent", { name = "p" })
    local child1 = g:insert("Child", { name = "c1" })
    local child2 = g:insert("Child", { name = "c2" })
    g:link(parent, "children", child1)
    g:link(parent, "children", child2)

    local view = g:query(string.format(
        [[{ "root": "Parent", "id": %d, "edges": [{ "name": "children" }] }]],
        parent
    ), { limit = 100 })

    view:expand(parent, "children")

    local leave_count = 0
    local left_ids = {}
    local unsub = view:on("leave", function(item, index)
        leave_count = leave_count + 1
        table.insert(left_ids, item.id)
    end)

    -- Unlink child2
    g:unlink(parent, "children", child2)

    assert_eq(leave_count, 1, "leave callback should fire once")
    assert_eq(left_ids[1], child2, "left item should be child2")

    unsub()
end)

test("leave fires when deleting linked child", function()
    local g = ng.graph(schema)

    local parent = g:insert("Parent", { name = "p" })
    local child = g:insert("Child", { name = "c1" })
    g:link(parent, "children", child)

    local view = g:query(string.format(
        [[{ "root": "Parent", "id": %d, "edges": [{ "name": "children" }] }]],
        parent
    ), { limit = 100 })

    view:expand(parent, "children")

    local leave_count = 0
    local unsub = view:on("leave", function() leave_count = leave_count + 1 end)

    g:delete(child)

    assert_eq(leave_count, 1, "leave should fire when child is deleted")

    unsub()
end)

print("\n3. Edge cases")

test("no enter event when edge is collapsed", function()
    local g = ng.graph(schema)

    local parent = g:insert("Parent", { name = "p" })

    local view = g:query(string.format(
        [[{ "root": "Parent", "id": %d, "edges": [{ "name": "children" }] }]],
        parent
    ), { limit = 100 })

    -- Do NOT expand - edge remains collapsed

    local enter_count = 0
    local unsub = view:on("enter", function() enter_count = enter_count + 1 end)

    local child = g:insert("Child", { name = "c1" })
    g:link(parent, "children", child)

    -- Child should not trigger enter since edge is collapsed
    assert_eq(enter_count, 0, "enter should NOT fire when edge is collapsed")

    unsub()
end)

test("enter fires after expand even if linked before expand", function()
    local g = ng.graph(schema)

    local parent = g:insert("Parent", { name = "p" })
    local child = g:insert("Child", { name = "c1" })
    g:link(parent, "children", child)

    local view = g:query(string.format(
        [[{ "root": "Parent", "id": %d, "edges": [{ "name": "children" }] }]],
        parent
    ), { limit = 100 })

    local enter_count = 0
    local unsub = view:on("enter", function() enter_count = enter_count + 1 end)

    -- Expand AFTER registering callback - should fire for existing child
    view:expand(parent, "children")

    assert_eq(enter_count, 1, "enter should fire for existing child when expanding")

    unsub()
end)

-- Summary
print("\n=== Test Summary ===")
print(string.format("Passed: %d", passed))
print(string.format("Failed: %d", failed))
print("")

if failed > 0 then
    vim.cmd("cq 1")
else
    print("All tests passed!")
    vim.cmd("qa!")
end
