#!/usr/bin/env -S nvim --headless -u NONE -l
-- BUG: Calling view:items() inside "enter" event callback causes infinite recursion
--
-- Problem:
--   view:items() triggers "enter" events for each item it visits.
--   If you call view:items() from an "enter" callback, it triggers more "enter" events,
--   which call the callback again, causing infinite recursion.
--
-- Expected behavior:
--   Calling view:items() from a callback should NOT trigger additional events,
--   OR events should be suppressed while inside a callback (re-entrancy guard).
--
-- Run: nvim --headless -u NONE -l test/bug_items_in_callback.lua

local cwd = vim.fn.getcwd()
package.cpath = cwd .. "/lua/?.so;" .. package.cpath

local ng = require("neograph_lua")

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

-- Setup: Parent with 2 children
local parent = g:insert("Parent", { name = "parent" })
local child1 = g:insert("Child", { name = "child1" })
local child2 = g:insert("Child", { name = "child2" })
g:link(parent, "children", child1)
g:link(parent, "children", child2)

-- Query
local query = string.format([[{
  "root": "Parent",
  "id": %d,
  "edges": [{ "name": "children" }]
}]], parent)

local view = g:query(query, { limit = 50 })

-- Track callback invocations
local callback_count = 0
local max_callbacks = 5

-- Subscribe to "enter" event that calls view:items()
view:on("enter", function()
  callback_count = callback_count + 1
  if callback_count > max_callbacks then
    -- If we get here, infinite recursion is happening
    print("")
    print("========================================")
    print("BUG REPRODUCED: Infinite recursion detected")
    print("========================================")
    print("")
    print("view:items() in 'enter' callback triggers more 'enter' events.")
    print("Callback was invoked " .. callback_count .. " times (limit: " .. max_callbacks .. ")")
    print("")
    vim.cmd("cq 1")
  end
  -- This call to items() triggers more "enter" events
  local items = view:items()
end)

-- Trigger the bug by expanding
view:expand(parent, "children")

-- If we reach here, no infinite recursion occurred
print("")
print("========================================")
print("BUG SOLVED: No infinite recursion")
print("========================================")
print("")
print("view:items() in callback did not cause runaway recursion.")
print("Callback count: " .. callback_count)
print("")
vim.cmd("qa!")
