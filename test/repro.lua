#!/usr/bin/env -S nvim --headless -u NONE -l
-- Template for creating minimal bug reproductions
--
-- Instructions:
--   1. Copy this file to bug_<description>.lua
--   2. Modify the schema, setup, and test to reproduce your bug
--   3. Run: nvim --headless -u NONE -l bug_<description>.lua
--   4. Share the reproduction when reporting issues
--
-- Tips:
--   - Keep the schema minimal (only types/edges needed to reproduce)
--   - Document expected vs actual behavior clearly
--   - Exit with code 1 on failure, 0 on success

local cwd = vim.fn.getcwd()
package.cpath = cwd .. "/lua/?.so;" .. package.cpath

local ng = require("neograph_lua")

-- ============================================================================
-- Schema: Define minimal types and edges to reproduce the bug
-- ============================================================================

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

local g = ng.graph(schema)

-- ============================================================================
-- Setup: Create entities and relationships
-- ============================================================================

print("=== Bug Reproduction ===")
print("")

local parent = g:insert("Parent", { name = "p" })
local child = g:insert("Child", { name = "c" })
g:link(parent, "children", child)

print("Setup: Parent -> Child")
print("")

-- ============================================================================
-- Test: Create view and demonstrate the bug
-- ============================================================================

local query = string.format([[{
  "root": "Parent",
  "id": %d,
  "edges": [{ "name": "children" }]
}]], parent)

local view = g:query(query, { limit = 100 })

-- Track events
local enter_count = 0
local leave_count = 0

view:on("enter", function(item)
    enter_count = enter_count + 1
    print(string.format("  ENTER: id=%d type=%s", item.id, item.type))
end)

view:on("leave", function(item)
    leave_count = leave_count + 1
    print(string.format("  LEAVE: id=%d type=%s", item.id, item.type))
end)

-- ============================================================================
-- Actions: Perform operations that trigger the bug
-- ============================================================================

print("1. Initial state:")
print("   items: " .. #view:items())

print("")
print("2. Expand parent->children:")
view:expand(parent, "children")
print("   items: " .. #view:items())

print("")
print("3. Link new child:")
local child2 = g:insert("Child", { name = "c2" })
g:link(parent, "children", child2)
print("   items: " .. #view:items())

-- ============================================================================
-- Validation: Check expected vs actual
-- ============================================================================

print("")
print("=== Results ===")

local items = view:items()
local expected_items = 3  -- parent + 2 children
local expected_enters = 1 -- child2 should trigger enter

print(string.format("Items: %d (expected: %d)", #items, expected_items))
print(string.format("Enter events: %d (expected: %d)", enter_count, expected_enters))

print("")

if #items == expected_items and enter_count >= expected_enters then
    print("PASS: Behavior is correct")
    vim.cmd("qa!")
else
    print("FAIL: Bug reproduced")
    print("")
    print("Expected behavior:")
    print("  - " .. expected_items .. " items in view after linking")
    print("  - Enter event fires for newly linked child")
    print("")
    print("Actual behavior:")
    print("  - " .. #items .. " items in view")
    print("  - " .. enter_count .. " enter events fired")
    vim.cmd("cq 1")
end
