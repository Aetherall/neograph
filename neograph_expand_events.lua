#!/usr/bin/env -S nvim --headless -u NONE -l
--------------------------------------------------------------------------------
-- NEOGRAPH BUG REPORT: View events not firing after expand()
--------------------------------------------------------------------------------
--
-- BUG SUMMARY:
-- After calling view:expand(), the "enter" events do not fire for newly
-- visible children. The children ARE returned by view:items() after expand,
-- but the event callbacks never execute.
--
-- EXPECTED BEHAVIOR:
-- When expand() makes new items visible in the view, "enter" should fire
-- for each newly visible item.
--
-- ACTUAL BEHAVIOR:
-- No events fire. Users must call items() and manually diff against
-- previous state to detect what changed.
--
-- IMPACT:
-- This makes it impossible to efficiently react to view changes. The only
-- workaround is to poll items() and manually track state.
--
-- USAGE:
--   nvim --headless -u NONE -l neograph_expand_events.lua
--
-- NOTE: Adjust package.cpath below to point to your neograph_lua.so location
--------------------------------------------------------------------------------

-- Add path to neograph_lua.so (adjust this for your setup)
local script_dir = debug.getinfo(1, "S").source:match("@?(.*/)") or "./"
-- Try common locations
package.cpath = table.concat({
  script_dir .. "?.so",
  script_dir .. "../?.so",
  script_dir .. "../../.tests/plugins/neograph/lua/?.so",
  script_dir .. ".tests/plugins/neograph/lua/?.so",
  package.cpath
}, ";")

local ok, ng = pcall(require, "neograph_lua")
if not ok then
  print("ERROR: Could not load neograph_lua")
  print("Adjust package.cpath in the script to point to neograph_lua.so")
  vim.cmd("qa!")
  return
end

-- Simple parent-child schema
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

-- Create test data
local parent_id = g:insert("Parent", { name = "parent1" })
local child1_id = g:insert("Child", { name = "child1" })
local child2_id = g:insert("Child", { name = "child2" })
g:link(parent_id, "children", child1_id)
g:link(parent_id, "children", child2_id)

print("Created parent (id=" .. parent_id .. ") with 2 children")

-- Create view with event tracking
local events = {
  enter = {},
  leave = {},
  change = {},
}

local query = string.format([[{
  "root": "Parent",
  "id": %d,
  "edges": [{ "name": "children" }]
}]], parent_id)

local view = g:query(query, { limit = 100 })

-- Subscribe to ALL events
view:on("enter", function(item, index)
  table.insert(events.enter, { id = item.id, index = index, type = item.type })
  print(string.format("  EVENT: enter id=%d type=%s index=%d", item.id, item.type or "?", index or -1))
end)

view:on("leave", function(item, index)
  table.insert(events.leave, { id = item.id, index = index })
  print(string.format("  EVENT: leave id=%d index=%d", item.id, index or -1))
end)

view:on("change", function(item, index)
  table.insert(events.change, { id = item.id, index = index })
  print(string.format("  EVENT: change id=%d index=%d", item.id, index or -1))
end)

print("\n=== Initial state (before expand) ===")
local items = view:items()
print("Items in view: " .. #items)
for i, item in ipairs(items) do
  print(string.format("  [%d] id=%d type=%s", i, item.id, item.type or "?"))
end
print("Enter events so far: " .. #events.enter)

-- Clear events before expand
events.enter = {}
events.leave = {}
events.change = {}

print("\n=== Calling view:expand(parent_id, 'children') ===")
view:expand(parent_id, "children")

print("\n=== After expand (before items() call) ===")
print("Enter events fired: " .. #events.enter)
print("Leave events fired: " .. #events.leave)
print("Change events fired: " .. #events.change)

print("\n=== After items() call ===")
items = view:items()
print("Items in view: " .. #items)
for i, item in ipairs(items) do
  print(string.format("  [%d] id=%d type=%s depth=%d", i, item.id, item.type or "?", item.depth or 0))
end

print("\n=== BUG ANALYSIS ===")
if #events.enter == 0 then
  print("BUG CONFIRMED: expand() added children to view but 'enter' events never fired!")
  print("")
  print("Expected: 2 'enter' events (one for each child)")
  print("Actual: 0 'enter' events")
  print("")
  print("The children ARE visible in items() after expand,")
  print("but the event callbacks were never invoked.")
  vim.cmd("cq!")  -- Exit with error code
else
  print("Events fired correctly: " .. #events.enter .. " enter events")
  vim.cmd("qa!")  -- Exit success
end
