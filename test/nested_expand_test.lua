#!/usr/bin/env -S nvim --headless -u NONE -l
-- Test: Nested edge expansion reactivity
--
-- BUG: When expanding a nested edge (child->items after parent->children),
-- new links to that nested edge are not tracked - they don't appear in
-- view:items() and don't fire enter events.
--
-- Run: nvim --headless -u NONE -l test/nested_expand_test.lua

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

local function find_in_items(view, id)
    for _, item in ipairs(view:items()) do
        if item.id == id then return true end
    end
    return false
end

-- Schema with 3-level hierarchy: Parent -> Child -> Item
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
      "edges": [
        { "name": "parent", "target": "Parent", "reverse": "children" },
        { "name": "items", "target": "Item", "reverse": "child" }
      ],
      "indexes": [{ "fields": [{ "field": "name" }] }]
    },
    {
      "name": "Item",
      "properties": [{ "name": "name", "type": "string" }],
      "edges": [{ "name": "child", "target": "Child", "reverse": "items" }],
      "indexes": [{ "fields": [{ "field": "name" }] }]
    }
  ]
}]]

print("\n=== Nested Edge Expansion Tests ===\n")

print("1. Basic nested expansion")

test("expanding nested edge shows existing items", function()
    local g = ng.graph(schema)

    local parent = g:insert("Parent", { name = "p" })
    local child = g:insert("Child", { name = "c" })
    local item = g:insert("Item", { name = "i" })
    g:link(parent, "children", child)
    g:link(child, "items", item)

    local view = g:query(string.format(
        [[{ "root": "Parent", "id": %d, "edges": [{ "name": "children", "edges": [{ "name": "items" }] }] }]],
        parent
    ), { limit = 100 })

    -- Expand first level
    view:expand(parent, "children")
    assert_eq(#view:items(), 2, "should have parent + child")

    -- Expand nested level
    view:expand(child, "items")
    assert_eq(#view:items(), 3, "should have parent + child + item")
    assert_true(find_in_items(view, item), "item should be in view")
end)

print("\n2. Reactivity on nested expanded edges")

test("linking to nested expanded edge adds item to view", function()
    local g = ng.graph(schema)

    local parent = g:insert("Parent", { name = "p" })
    local child = g:insert("Child", { name = "c" })
    g:link(parent, "children", child)

    local view = g:query(string.format(
        [[{ "root": "Parent", "id": %d, "edges": [{ "name": "children", "edges": [{ "name": "items" }] }] }]],
        parent
    ), { limit = 100 })

    -- Expand both levels
    view:expand(parent, "children")
    view:expand(child, "items")
    assert_eq(#view:items(), 2, "should have parent + child")

    -- Link new item to nested expanded edge
    local item = g:insert("Item", { name = "i1" })
    g:link(child, "items", item)

    -- Item should appear in view
    assert_eq(#view:items(), 3, "should have parent + child + item")
    assert_true(find_in_items(view, item), "newly linked item should be in view")
end)

test("enter fires when linking to nested expanded edge", function()
    local g = ng.graph(schema)

    local parent = g:insert("Parent", { name = "p" })
    local child = g:insert("Child", { name = "c" })
    g:link(parent, "children", child)

    local view = g:query(string.format(
        [[{ "root": "Parent", "id": %d, "edges": [{ "name": "children", "edges": [{ "name": "items" }] }] }]],
        parent
    ), { limit = 100 })

    view:expand(parent, "children")
    view:expand(child, "items")

    local enter_count = 0
    local entered_ids = {}
    local unsub = view:on("enter", function(item)
        enter_count = enter_count + 1
        table.insert(entered_ids, item.id)
    end)

    -- Link new item
    local item = g:insert("Item", { name = "i1" })
    g:link(child, "items", item)

    assert_eq(enter_count, 1, "enter should fire once")
    assert_eq(entered_ids[1], item, "entered item should be the new item")

    unsub()
end)

test("leave fires when unlinking from nested expanded edge", function()
    local g = ng.graph(schema)

    local parent = g:insert("Parent", { name = "p" })
    local child = g:insert("Child", { name = "c" })
    local item = g:insert("Item", { name = "i1" })
    g:link(parent, "children", child)
    g:link(child, "items", item)

    local view = g:query(string.format(
        [[{ "root": "Parent", "id": %d, "edges": [{ "name": "children", "edges": [{ "name": "items" }] }] }]],
        parent
    ), { limit = 100 })

    view:expand(parent, "children")
    view:expand(child, "items")
    assert_eq(#view:items(), 3, "should have parent + child + item")

    local leave_count = 0
    local left_ids = {}
    local unsub = view:on("leave", function(item)
        leave_count = leave_count + 1
        table.insert(left_ids, item.id)
    end)

    -- Unlink item
    g:unlink(child, "items", item)

    assert_eq(leave_count, 1, "leave should fire once")
    assert_eq(left_ids[1], item, "left item should be the unlinked item")
    assert_eq(#view:items(), 2, "should have parent + child")

    unsub()
end)

print("\n3. Multiple nested levels")

test("linking to deeply nested expanded edge (3+ levels)", function()
    -- Extended schema with 4 levels
    local deep_schema = [[{
      "types": [
        {
          "name": "Root",
          "properties": [{ "name": "name", "type": "string" }],
          "edges": [{ "name": "level1", "target": "L1", "reverse": "root" }],
          "indexes": [{ "fields": [{ "field": "name" }] }]
        },
        {
          "name": "L1",
          "properties": [{ "name": "name", "type": "string" }],
          "edges": [
            { "name": "root", "target": "Root", "reverse": "level1" },
            { "name": "level2", "target": "L2", "reverse": "l1" }
          ],
          "indexes": [{ "fields": [{ "field": "name" }] }]
        },
        {
          "name": "L2",
          "properties": [{ "name": "name", "type": "string" }],
          "edges": [
            { "name": "l1", "target": "L1", "reverse": "level2" },
            { "name": "level3", "target": "L3", "reverse": "l2" }
          ],
          "indexes": [{ "fields": [{ "field": "name" }] }]
        },
        {
          "name": "L3",
          "properties": [{ "name": "name", "type": "string" }],
          "edges": [{ "name": "l2", "target": "L2", "reverse": "level3" }],
          "indexes": [{ "fields": [{ "field": "name" }] }]
        }
      ]
    }]]

    local g = ng.graph(deep_schema)

    local root = g:insert("Root", { name = "r" })
    local l1 = g:insert("L1", { name = "l1" })
    local l2 = g:insert("L2", { name = "l2" })
    g:link(root, "level1", l1)
    g:link(l1, "level2", l2)

    local view = g:query(string.format(
        [[{ "root": "Root", "id": %d, "edges": [{ "name": "level1", "edges": [{ "name": "level2", "edges": [{ "name": "level3" }] }] }] }]],
        root
    ), { limit = 100 })

    -- Expand all levels
    view:expand(root, "level1")
    view:expand(l1, "level2")
    view:expand(l2, "level3")
    assert_eq(#view:items(), 3, "should have root + l1 + l2")

    local enter_count = 0
    local unsub = view:on("enter", function() enter_count = enter_count + 1 end)

    -- Link at deepest level
    local l3 = g:insert("L3", { name = "l3" })
    g:link(l2, "level3", l3)

    assert_eq(#view:items(), 4, "should have root + l1 + l2 + l3")
    assert_true(find_in_items(view, l3), "l3 should be in view")
    assert_eq(enter_count, 1, "enter should fire for deeply nested link")

    unsub()
end)

print("\n4. Edge cases")

test("no enter when nested edge is collapsed", function()
    local g = ng.graph(schema)

    local parent = g:insert("Parent", { name = "p" })
    local child = g:insert("Child", { name = "c" })
    g:link(parent, "children", child)

    local view = g:query(string.format(
        [[{ "root": "Parent", "id": %d, "edges": [{ "name": "children", "edges": [{ "name": "items" }] }] }]],
        parent
    ), { limit = 100 })

    -- Expand only first level, keep items collapsed
    view:expand(parent, "children")

    local enter_count = 0
    local unsub = view:on("enter", function() enter_count = enter_count + 1 end)

    -- Link item - should NOT trigger enter since items edge is collapsed
    local item = g:insert("Item", { name = "i1" })
    g:link(child, "items", item)

    assert_eq(enter_count, 0, "enter should NOT fire when nested edge is collapsed")
    assert_eq(#view:items(), 2, "view should still have parent + child only")

    unsub()
end)

test("multiple items linked to same nested edge", function()
    local g = ng.graph(schema)

    local parent = g:insert("Parent", { name = "p" })
    local child = g:insert("Child", { name = "c" })
    g:link(parent, "children", child)

    local view = g:query(string.format(
        [[{ "root": "Parent", "id": %d, "edges": [{ "name": "children", "edges": [{ "name": "items" }] }] }]],
        parent
    ), { limit = 100 })

    view:expand(parent, "children")
    view:expand(child, "items")

    local enter_count = 0
    local unsub = view:on("enter", function() enter_count = enter_count + 1 end)

    -- Link multiple items
    for i = 1, 5 do
        local item = g:insert("Item", { name = "i" .. i })
        g:link(child, "items", item)
    end

    assert_eq(enter_count, 5, "enter should fire for each linked item")
    assert_eq(#view:items(), 7, "should have parent + child + 5 items")

    unsub()
end)

test("items appear after collapse and re-expand of nested edge", function()
    local g = ng.graph(schema)

    local parent = g:insert("Parent", { name = "p" })
    local child = g:insert("Child", { name = "c" })
    g:link(parent, "children", child)

    local view = g:query(string.format(
        [[{ "root": "Parent", "id": %d, "edges": [{ "name": "children", "edges": [{ "name": "items" }] }] }]],
        parent
    ), { limit = 100 })

    view:expand(parent, "children")
    view:expand(child, "items")

    -- Link item while expanded
    local item1 = g:insert("Item", { name = "i1" })
    g:link(child, "items", item1)
    assert_eq(#view:items(), 3, "should have 3 items")

    -- Collapse and re-expand
    view:collapse(child, "items")
    assert_eq(#view:items(), 2, "should have 2 after collapse")

    view:expand(child, "items")
    assert_eq(#view:items(), 3, "should have 3 after re-expand")
    assert_true(find_in_items(view, item1), "item1 should still be visible")
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
