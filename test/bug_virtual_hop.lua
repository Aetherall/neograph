#!/usr/bin/env -S nvim --headless -u NONE -l
-- BUG: Virtual hop expand/collapse issues
--
-- Virtual hops allow hiding intermediate nodes in a tree view.
-- Pattern: Parent -> (virtual) intermediate -> items
-- Expected: Items appear directly under Parent, intermediate is hidden
--
-- Issues:
--   1. After expand, items don't appear in view:items()
--   2. Collapse on virtual edge crashes (segfault)
--
-- Real scenario: neodap's Stdio node uses virtual hop to show Session's outputs
-- as direct children: Stdio -> (virtual) session -> outputs
--
-- Run: nvim --headless -u NONE -l test/bug_virtual_hop.lua

local cwd = vim.fn.getcwd()
package.cpath = cwd .. "/lua/?.so;" .. package.cpath

local ng = require("neograph_lua")

-- ============================================================================
-- Schema: Mirrors neodap's Debugger -> Session -> Stdio -> outputs structure
-- ============================================================================

local schema = [[{
  "types": [
    {
      "name": "Debugger",
      "properties": [{ "name": "name", "type": "string" }],
      "edges": [{ "name": "sessions", "target": "Session", "reverse": "debugger" }],
      "indexes": [{ "fields": [{ "field": "name" }] }]
    },
    {
      "name": "Session",
      "properties": [{ "name": "name", "type": "string" }],
      "edges": [
        { "name": "debugger", "target": "Debugger", "reverse": "sessions" },
        { "name": "stdio", "target": "Stdio", "reverse": "session" },
        { "name": "outputs", "target": "Output", "reverse": "session" }
      ],
      "indexes": [{ "fields": [{ "field": "name" }] }]
    },
    {
      "name": "Stdio",
      "properties": [{ "name": "name", "type": "string" }],
      "edges": [{ "name": "session", "target": "Session", "reverse": "stdio" }],
      "indexes": [{ "fields": [{ "field": "name" }] }]
    },
    {
      "name": "Output",
      "properties": [{ "name": "text", "type": "string" }],
      "edges": [{ "name": "session", "target": "Session", "reverse": "outputs" }],
      "indexes": [{ "fields": [{ "field": "text" }] }]
    }
  ]
}]]

local g = ng.graph(schema)

-- ============================================================================
-- Setup: Debugger -> Session -> Stdio, Session -> 2 Outputs
-- ============================================================================

print("=== Virtual Hop Bug Reproduction ===")
print("")

local debugger = g:insert("Debugger", { name = "dbg" })
local session = g:insert("Session", { name = "sess" })
local stdio = g:insert("Stdio", { name = "stdio" })
local output1 = g:insert("Output", { text = "Hello" })
local output2 = g:insert("Output", { text = "World" })

g:link(debugger, "sessions", session)
g:link(session, "stdio", stdio)
g:link(session, "outputs", output1)
g:link(session, "outputs", output2)

print("Setup:")
print("  Debugger -> Session -> Stdio")
print("  Session -> Output1, Output2")
print("")

-- ============================================================================
-- Query: Debugger tree with virtual hop on Stdio->session->outputs
-- ============================================================================

-- This matches neodap's actual query structure:
-- Stdio has edge "session" marked virtual, with nested edge "outputs"
-- So outputs should appear as direct children of Stdio (session hidden)

local query = string.format([[{
  "root": "Debugger",
  "id": %d,
  "edges": [{
    "name": "sessions",
    "edges": [{
      "name": "stdio",
      "edges": [{
        "name": "session",
        "virtual": true,
        "edges": [{ "name": "outputs" }]
      }]
    }]
  }]
}]], debugger)

local view = g:query(query, { limit = 100 })

-- ============================================================================
-- Test 1: Expand path to Stdio and check if outputs appear
-- ============================================================================

print("Test 1: Virtual hop expand")
print("  Expanding debugger->sessions...")
view:expand(debugger, "sessions")

print("  Expanding session->stdio...")
view:expand(session, "stdio")

print("  Expanding stdio->session (VIRTUAL HOP)...")
view:expand(stdio, "session")

local items = view:items()
print("")
print("  Items in view after expand:")
for i, item in ipairs(items) do
  print(string.format("    [%d] type=%s id=%d", i, item.type, item.id))
end

local output_count = 0
for _, item in ipairs(items) do
  if item.type == "Output" then output_count = output_count + 1 end
end

print("")
print(string.format("  Outputs visible: %d (expected: 2)", output_count))

local expand_pass = output_count == 2

-- ============================================================================
-- Test 2: Collapse virtual hop (this crashes in neodap)
-- ============================================================================

print("")
print("Test 2: Virtual hop collapse")
print("  Collapsing stdio->session (VIRTUAL HOP)...")

local collapse_ok, collapse_err = pcall(function()
  view:collapse(stdio, "session")
end)

if not collapse_ok then
  print("  CRASH: " .. tostring(collapse_err))
end

local items_after = view:items()
print("")
print("  Items in view after collapse:")
for i, item in ipairs(items_after) do
  print(string.format("    [%d] type=%s id=%d", i, item.type, item.id))
end

-- After collapse, outputs should disappear, only Debugger->Session->Stdio remain
local expected_after_collapse = 3  -- Debugger, Session, Stdio
local collapse_pass = collapse_ok and #items_after == expected_after_collapse

-- ============================================================================
-- Results
-- ============================================================================

print("")
print("=== Results ===")
print("")

if expand_pass and collapse_pass then
  print("PASS: Virtual hop works correctly")
  print("  - Expand shows outputs as children of Stdio")
  print("  - Collapse hides outputs without crash")
  vim.cmd("qa!")
else
  print("FAIL: Virtual hop bug reproduced")
  print("")
  if not expand_pass then
    print("  BUG 1 - Expand: Outputs don't appear after virtual hop expand")
    print("    Expected: 2 outputs visible under Stdio")
    print("    Actual: " .. output_count .. " outputs visible")
  end
  if not collapse_ok then
    print("  BUG 2 - Collapse: Crash on virtual hop collapse")
    print("    Error: " .. tostring(collapse_err))
  elseif not collapse_pass then
    print("  BUG 2 - Collapse: Wrong item count after collapse")
    print("    Expected: " .. expected_after_collapse .. " items")
    print("    Actual: " .. #items_after .. " items")
  end
  vim.cmd("cq 1")
end
