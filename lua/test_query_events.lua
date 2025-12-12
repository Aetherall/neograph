-- Test script for the new query:on/query:off API
package.cpath = "lua/?.so"

local ng = require("neograph_lua")

-- Create graph with schema (using JSON string)
local g = ng.graph([[{
    "types": [{
        "name": "User",
        "properties": [{ "name": "name", "type": "string" }],
        "indexes": [{ "fields": [{ "name": "name" }] }]
    }]
}]])

-- Create a query (using JSON string, with immediate mode for reactive events)
local q = g:query('{"root": "User", "sort": [{"field": "name", "direction": "asc"}]}', { immediate = true })

-- Track events
local events = {}

-- Test 1: Subscribe using on()
print("Test 1: Subscribe using query:on()")
local unsub_enter = q:on("enter", function(item, index)
    table.insert(events, "enter:" .. item.id)
end)
local unsub_leave = q:on("leave", function(item, index)
    table.insert(events, "leave:" .. item.id)
end)

-- Insert a user - should trigger enter
local id = g:insert("User", { name = "Alice" })
print("  Inserted user:", id)
print("  Events:", table.concat(events, ", "))
assert(#events == 1, "Expected 1 event")
assert(events[1] == "enter:" .. id, "Expected enter event")

-- Test 2: Unsubscribe using returned function
print("Test 2: Unsubscribe using returned function")
events = {}
unsub_enter()
local id2 = g:insert("User", { name = "Bob" })
print("  Inserted another user:", id2)
print("  Events:", table.concat(events, ", "))
-- Only leave callbacks should still fire (for the new item entering)
-- Actually, we only unsubscribed enter, so leave should fire if something leaves
-- Since we're inserting, nothing leaves, so events should be empty for enter
assert(#events == 0, "Expected 0 enter events after unsubscribe")

-- Test 3: query:off() to clear specific event
print("Test 3: query:off('leave')")
events = {}
-- Re-subscribe to enter
local unsub3 = q:on("enter", function(item, index)
    table.insert(events, "enter:" .. item.id)
end)
q:off("enter")  -- Clear the enter callback we just set
local id3 = g:insert("User", { name = "Charlie" })
print("  Events after q:off('enter'):", table.concat(events, ", "))
assert(#events == 0, "Expected 0 enter events after q:off('enter')")

-- Test 4: query:off() to clear all events
print("Test 4: query:off() to clear all")
q:on("enter", function(item, index)
    table.insert(events, "enter:" .. item.id)
end)
q:on("leave", function(item, index)
    table.insert(events, "leave:" .. item.id)
end)
events = {}
q:off()  -- Clear all
local id4 = g:insert("User", { name = "David" })
print("  Events after q:off():", table.concat(events, ", "))
assert(#events == 0, "Expected 0 events after q:off()")

print("All tests passed!")
