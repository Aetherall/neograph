-- Neograph Lua API Test Suite
-- Run with: nvim -l test/lua_test.lua

-- Setup path
local cwd = vim.fn.getcwd()
package.cpath = cwd .. "/lua/?.so;;" .. package.cpath
package.path = cwd .. "/lua/?/init.lua;;" .. package.path

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

-- ============================================================================
-- Schema for tests
-- ============================================================================

local schema = [[{
    "types": [
        {
            "name": "User",
            "properties": [
                { "name": "name", "type": "string" },
                { "name": "age", "type": "int" },
                { "name": "active", "type": "bool" }
            ],
            "edges": [
                { "name": "posts", "target": "Post", "reverse": "author" }
            ],
            "indexes": [
                { "fields": [{ "name": "name" }] },
                { "fields": [{ "name": "age" }] },
                { "fields": [{ "name": "active" }, { "name": "name" }] }
            ]
        },
        {
            "name": "Post",
            "properties": [
                { "name": "title", "type": "string" },
                { "name": "views", "type": "int" }
            ],
            "edges": [
                { "name": "author", "target": "User", "reverse": "posts" }
            ],
            "indexes": [
                { "fields": [{ "name": "title" }] },
                { "fields": [{ "name": "views" }] }
            ]
        }
    ]
}]]

-- ============================================================================
-- Tests
-- ============================================================================

print("\n=== Neograph Lua API Tests ===\n")

print("1. Graph Creation")
test("create graph with schema", function()
    local g = ng.graph(schema)
    assert_true(g ~= nil, "graph should not be nil")
end)

print("\n2. Node Operations")
local g = ng.graph(schema)

test("insert node", function()
    local id = g:insert("User")
    assert_true(id ~= nil, "id should not be nil")
    assert_eq(type(id), "number", "id should be number")
end)

test("insert node with properties", function()
    local id = g:insert("User", {name = "Alice", age = 30, active = true})
    assert_true(id > 0, "id should be positive")
end)

test("update node", function()
    local id = g:insert("User", {name = "Bob", age = 25})
    g:update(id, {age = 26})
end)

test("delete node", function()
    local id = g:insert("User", {name = "ToDelete"})
    local ok = g:delete(id)
    assert_true(ok, "delete should return true")
end)

test("get node returns properties and type", function()
    local id = g:insert("User", {name = "GetTest", age = 42, active = true})
    local data = g:get(id)
    assert_true(data ~= nil, "get should return data")
    assert_eq(data.name, "GetTest", "name should match")
    assert_eq(data.age, 42, "age should match")
    assert_eq(data.active, true, "active should match")
    assert_eq(data.type, "User", "type should be User")
end)

test("get non-existent node returns nil", function()
    local data = g:get(99999)
    assert_true(data == nil, "get should return nil for non-existent node")
end)

print("\n3. Schema Introspection")

test("field_type returns property for properties", function()
    assert_eq(g:field_type("User", "name"), "property")
    assert_eq(g:field_type("User", "age"), "property")
    assert_eq(g:field_type("Post", "title"), "property")
end)

test("field_type returns edge for edges", function()
    assert_eq(g:field_type("User", "posts"), "edge")
    assert_eq(g:field_type("Post", "author"), "edge")
end)

test("field_type returns nil for unknown fields", function()
    assert_eq(g:field_type("User", "nonexistent"), nil)
end)

test("field_type returns nil for unknown types", function()
    assert_eq(g:field_type("UnknownType", "name"), nil)
end)

print("\n4. Edge Operations")

test("link nodes", function()
    local user_id = g:insert("User", {name = "Author", age = 35, active = true})
    local post_id = g:insert("Post", {title = "Hello World", views = 100})
    local ok = g:link(user_id, "posts", post_id)
    assert_true(ok, "link should return true")
end)

print("\n5. Query Creation")

test("create query with definition", function()
    for i = 1, 5 do
        g:insert("User", {name = "User" .. i, age = 20 + i, active = i % 2 == 0})
    end

    local qdef = [[{"root": "User", "sort": [{"field": "name"}]}]]
    local q = g:query(qdef, {limit = 10})
    assert_true(q ~= nil, "query should not be nil")
end)

test("create query with filter", function()
    local qdef = [[{"root": "User", "filter": [{"field": "active", "value": true}], "sort": [{"field": "name"}]}]]
    local q = g:query(qdef, {limit = 10})
    assert_true(q ~= nil, "query should not be nil")
end)

print("\n6. Query Operations")

local qdef = [[{"root": "User", "sort": [{"field": "name"}]}]]
local view = g:query(qdef, {limit = 10})

test("items returns items", function()
    local items = view:items()
    assert_true(type(items) == "table", "items should be table")
    assert_true(#items > 0, "should have items")
end)

test("items have expected fields", function()
    local items = view:items()
    local item = items[1]
    assert_true(item.id ~= nil, "item should have id")
    assert_true(item.depth ~= nil, "item should have depth")
    assert_true(item.expandable ~= nil, "item should have expandable")
end)

test("stats returns query info", function()
    local stats = view:stats()
    assert_true(stats.total ~= nil, "should have total")
    assert_true(stats.offset ~= nil, "should have offset")
    assert_true(stats.height ~= nil, "should have height")
end)

test("set_limit changes height", function()
    view:set_limit(5)
    local stats = view:stats()
    assert_eq(stats.height, 5, "height should be 5")
end)

test("scroll_to changes offset", function()
    view:scroll_to(2)
    local stats = view:stats()
    assert_eq(stats.offset, 2, "offset should be 2")
end)

-- Clean up view to avoid subscription interference
view = nil
collectgarbage()

print("\n7. Tree Expansion")

-- Use simple query without filter for expansion test
local g2 = ng.graph(schema)
local author_id = g2:insert("User", {name = "Blogger", age = 28, active = true})
for i = 1, 3 do
    local post_id = g2:insert("Post", {title = "Post " .. i, views = i * 10})
    g2:link(author_id, "posts", post_id)
end

-- Query all users (no filter) sorted by name
local tree_query = [[{
    "root": "User",
    "sort": [{"field": "name"}],
    "edges": [{"name": "posts", "sort": [{"field": "title"}]}]
}]]

test("tree view shows items", function()
    local tree = g2:query(tree_query, {limit = 20})
    local items = tree:items()
    assert_true(#items > 0, "should have items")
    print("    Found " .. #items .. " items")
end)

test("expand node", function()
    local tree = g2:query(tree_query, {limit = 20})
    local items = tree:items()

    -- Find a user with has_children
    for _, item in ipairs(items) do
        if item.expandable then
            print("    Expanding node " .. item.id .. " (" .. tostring(item.name) .. ")")
            local ok = tree:expand(item.id, "posts")
            assert_true(ok, "expand should succeed")

            -- Check that children appeared
            local after = tree:items()
            print("    Items after expand: " .. #after)
            assert_true(#after > #items, "should have more items after expand")
            return
        end
    end
    error("no expandable nodes found")
end)

test("collapse node", function()
    local tree = g2:query(tree_query, {limit = 20})
    local items = tree:items()

    for _, item in ipairs(items) do
        if item.expandable then
            tree:expand(item.id, "posts")
            local expanded_count = #tree:items()

            local ok = tree:collapse(item.id, "posts")
            assert_true(ok, "collapse should succeed")

            local collapsed_count = #tree:items()
            assert_true(collapsed_count < expanded_count, "should have fewer items after collapse")
            return
        end
    end
    error("no expandable nodes found")
end)

test("toggle node", function()
    local tree = g2:query(tree_query, {limit = 20})
    local items = tree:items()

    for _, item in ipairs(items) do
        if item.expandable then
            local expanded = tree:toggle(item.id, "posts")
            assert_true(expanded == true, "first toggle should expand")

            local collapsed = tree:toggle(item.id, "posts")
            assert_true(collapsed == false, "second toggle should collapse")
            return
        end
    end
    error("no expandable nodes found")
end)

print("\n8. Reactivity")

-- Create a fresh graph for reactivity tests
local gr = ng.graph(schema)

test("view reflects data inserted before creation", function()
    gr:insert("User", {name = "ReactUser1", age = 40, active = true})
    gr:insert("User", {name = "ReactUser2", age = 41, active = true})

    local query = [[{"root": "User", "sort": [{"field": "name"}]}]]
    local v = gr:query(query, {limit = 30})

    local items = v:items()
    local found1, found2 = false, false
    for _, item in ipairs(items) do
        if item.name == "ReactUser1" then found1 = true end
        if item.name == "ReactUser2" then found2 = true end
    end
    assert_true(found1 and found2, "view should show previously inserted items")
end)
collectgarbage()

test("multiple views see same data", function()
    local query = [[{"root": "User", "sort": [{"field": "name"}]}]]
    local v1 = gr:query(query, {limit = 30})
    local v2 = gr:query(query, {limit = 30})

    local count1 = #v1:items()
    local count2 = #v2:items()

    assert_eq(count1, count2, "both views should see same item count")
    assert_true(count1 > 0, "views should have items")
end)
collectgarbage()

test("filtered view shows correct subset", function()
    local gf = ng.graph(schema)
    gf:insert("User", {name = "Active1", age = 25, active = true})
    gf:insert("User", {name = "Active2", age = 26, active = true})
    gf:insert("User", {name = "Inactive1", age = 27, active = false})

    local all_query = [[{"root": "User", "sort": [{"field": "name"}]}]]
    local active_query = [[{"root": "User", "filter": [{"field": "active", "value": true}], "sort": [{"field": "name"}]}]]

    local all_view = gf:query(all_query, {limit = 30})
    local active_view = gf:query(active_query, {limit = 30})

    local all_count = #all_view:items()
    local active_count = #active_view:items()

    assert_eq(all_count, 3, "all view should have 3 users")
    assert_eq(active_count, 2, "active view should have 2 users")
end)
collectgarbage()

test("sorted view maintains order", function()
    local gs = ng.graph(schema)
    gs:insert("User", {name = "Charlie", age = 30, active = true})
    gs:insert("User", {name = "Alice", age = 25, active = true})
    gs:insert("User", {name = "Bob", age = 35, active = true})

    local query = [[{"root": "User", "sort": [{"field": "name"}]}]]
    local v = gs:query(query, {limit = 30})

    local items = v:items()
    assert_eq(items[1].name, "Alice", "first should be Alice")
    assert_eq(items[2].name, "Bob", "second should be Bob")
    assert_eq(items[3].name, "Charlie", "third should be Charlie")
end)
collectgarbage()

print("\n9. Live Reactivity (data modified while view active)")

test("reactive insert updates view", function()
    local gl = ng.graph(schema)
    gl:insert("User", {name = "Initial", age = 20, active = true})

    local query = [[{"root": "User", "sort": [{"field": "name"}]}]]
    local v = gl:query(query, {limit = 30, immediate = true})

    local before = #v:items()
    assert_eq(before, 1, "should have 1 item before insert")

    -- Insert while view is active
    gl:insert("User", {name = "Added", age = 25, active = true})

    local after = #v:items()
    assert_eq(after, 2, "should have 2 items after insert")

    -- Check order (Added < Initial alphabetically)
    local items = v:items()
    assert_eq(items[1].name, "Added", "Added should be first")
    assert_eq(items[2].name, "Initial", "Initial should be second")
end)
collectgarbage()

test("reactive delete updates view (lazy)", function()
    local gl = ng.graph(schema)
    local id1 = gl:insert("User", {name = "User1", age = 20, active = true})
    local id2 = gl:insert("User", {name = "User2", age = 25, active = true})

    local query = [[{"root": "User", "sort": [{"field": "name"}]}]]
    local v = gl:query(query, {limit = 30})

    -- Delete BEFORE loading viewport
    gl:delete(id1)

    local after = #v:items()
    assert_eq(after, 1, "should have 1 item after delete")

    local items = v:items()
    assert_eq(items[1].name, "User2", "only User2 should remain")
end)

test("reactive delete updates view (loaded)", function()
    local gl = ng.graph(schema)
    local id1 = gl:insert("User", {name = "User1", age = 20, active = true})
    local id2 = gl:insert("User", {name = "User2", age = 25, active = true})

    local query = [[{"root": "User", "sort": [{"field": "name"}]}]]
    local v = gl:query(query, {limit = 30})

    -- Load viewport FIRST
    local before = v:items()
    assert_eq(#before, 2, "should have 2 items before delete")

    -- Delete AFTER loading viewport (tests the viewport.first fix)
    gl:delete(id1)

    local after = v:items()
    assert_eq(#after, 1, "should have 1 item after delete")
    assert_eq(after[1].name, "User2", "only User2 should remain")
end)

test("reactive update triggers view refresh", function()
    local gl = ng.graph(schema)
    local id = gl:insert("User", {name = "Original", age = 20, active = true})

    local query = [[{"root": "User", "sort": [{"field": "name"}]}]]
    local v = gl:query(query, {limit = 30, immediate = true})

    local before = v:items()
    assert_eq(before[1].name, "Original", "should show Original before update")

    -- Update while view is active
    gl:update(id, {name = "Updated"})

    local after = v:items()
    assert_eq(after[1].name, "Updated", "should show Updated after update")
end)

test("filtered view reacts to filter field change", function()
    local gl = ng.graph(schema)
    local id = gl:insert("User", {name = "Test", age = 20, active = false})

    -- View filtering active users only
    local query = [[{"root": "User", "filter": [{"field": "active", "value": true}], "sort": [{"field": "name"}]}]]
    local v = gl:query(query, {limit = 30, immediate = true})

    local before = #v:items()
    assert_eq(before, 0, "inactive user should not appear in active filter")

    -- Update active field while view is active
    gl:update(id, {active = true})

    local after = #v:items()
    assert_eq(after, 1, "user should appear after becoming active")
end)

test("multiple views react independently", function()
    local gl = ng.graph(schema)
    gl:insert("User", {name = "Shared", age = 20, active = true})

    local query = [[{"root": "User", "sort": [{"field": "name"}]}]]
    local v1 = gl:query(query, {limit = 30, immediate = true})
    local v2 = gl:query(query, {limit = 30, immediate = true})

    assert_eq(#v1:items(), 1, "v1 should have 1 item")
    assert_eq(#v2:items(), 1, "v2 should have 1 item")

    -- Insert while both views are active
    gl:insert("User", {name = "New", age = 25, active = true})

    assert_eq(#v1:items(), 2, "v1 should have 2 items after insert")
    assert_eq(#v2:items(), 2, "v2 should have 2 items after insert")
end)

-- ============================================================================
-- BUG: onEnter not firing for linked children on expanded edge
-- ============================================================================

print("\n10. SDK Integration - Linking to expanded edges")

-- This reproduces the bug from dap_interactive.lua:
-- When an edge is already expanded and you link a new child to it,
-- the new child does NOT appear in get_visible() until you collapse and re-expand.

local dap_schema = [[{
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
                { "name": "name", "type": "string" }
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
                { "name": "value", "type": "string" }
            ],
            "edges": [
                { "name": "scope", "target": "Scope", "reverse": "variables" }
            ],
            "indexes": [{ "fields": [{ "name": "name" }] }]
        }
    ]
}]]

test("rapid links to expanded edge should all appear (regression test)", function()
    -- This test verifies that rapid linking to an expanded edge works correctly.
    --
    -- NOTE: A bug exists in the interactive nvim demo (dap_interactive.lua):
    -- When pressing 'v' 20 times rapidly, only ~6 new variables appear.
    -- The rest are "lost" until collapse/re-expand.
    --
    -- This test passes in synchronous execution but the bug manifests in the
    -- async nvim environment. This serves as a regression test to catch if
    -- the bug gets worse or affects synchronous execution.
    --
    -- Demo flow: VIEW created before entities, then entities added dynamically.

    local sdk = require("neograph-sdk")
    local gd = ng.graph(dap_schema)

    -- Define entity types
    local Debugger = sdk.entity("Debugger")
    local Thread = sdk.entity("Thread")
    local Frame = sdk.entity("Frame")
    local Scope = sdk.entity("Scope")
    local Variable = sdk.entity("Variable")

    -- Wrap graph with SDK
    local db = sdk.wrap(gd, {
        Debugger = Debugger,
        Thread = Thread,
        Frame = Frame,
        Scope = Scope,
        Variable = Variable,
    })

    -- Create debugger singleton FIRST (like the demo)
    local debugger = Debugger.insert({name = "main"})

    -- Create the MAIN VIEW BEFORE any other entities exist (like setup_ui())
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
                        "name": "variables"
                    }]
                }]
            }]
        }]
    }]], debugger:id())

    local main_view = gd:query(query, {limit = 100, immediate = true})

    -- Auto-expand debugger->threads (like setup_ui line 645)
    main_view:expand(debugger:id(), "threads")
    main_view:items()  -- render()

    -- Simulate pressing 't' - create thread
    local thread = Thread.insert({name = "Thread 1", state = "running"})
    debugger.threads:link(thread)
    main_view:items()  -- render()

    -- Simulate pressing 's' - stop thread (creates Frame, Scope, Variables)
    thread:update({state = "stopped"})

    local frame = Frame.insert({name = "main", source = "app.lua", line = 42})
    thread.frames:link(frame)

    local scope = Scope.insert({name = "Locals", expensive = false})
    frame.scopes:link(scope)

    -- Add initial variables (like action_stop_thread does)
    local var1 = Variable.insert({name = "counter", value = "42", varType = "number"})
    scope.variables:link(var1)
    local var2 = Variable.insert({name = "name", value = '"Alice"', varType = "string"})
    scope.variables:link(var2)
    local var3 = Variable.insert({name = "user", value = "table", varType = "table"})
    scope.variables:link(var3)

    main_view:items()  -- render()

    -- Simulate user expanding the tree (pressing 'o' multiple times)
    main_view:expand(thread:id(), "frames")
    main_view:items()
    main_view:expand(frame:id(), "scopes")
    main_view:items()
    main_view:expand(scope:id(), "variables")
    main_view:items()

    -- Count items before rapid linking
    local items_before = main_view:items()
    local count_before = #items_before
    print("    Items before rapid linking: " .. count_before)

    -- CRITICAL: Initialize the SDK EdgeCollection's internal view
    -- This happens in the demo when the first 'v' press calls scope.variables:link()
    -- The EdgeCollection creates its own view with expand on the same edge
    local scope_from_db = db:get(scope:id())
    for _ in scope_from_db.variables:iter() do end  -- This initializes the internal view
    print("    EdgeCollection internal view initialized")

    -- NOW: Simulate pressing 'v' 20 times rapidly (like in the demo)
    -- Each press: db:get(item.id) + insert + link + render (get_visible)
    local num_new_vars = 20
    local scope_id = scope:id()
    for i = 1, num_new_vars do
        -- This is EXACTLY what action_add_variable() does:
        local scope_from_db = db:get(scope_id)  -- Get entity (may be cached or new)
        local new_var = Variable.insert({
            name = "new_var_" .. i,
            value = tostring(i * 100),
            varType = "number"
        })
        scope_from_db.variables:link(new_var)  -- Link via the db:get() result
        main_view:items()  -- This is what render() does
    end

    -- Check how many items are visible now
    local items_after = main_view:items()
    local count_after = #items_after
    print("    Items after " .. num_new_vars .. " links: " .. count_after)

    -- Count how many new_var_* are visible
    local visible_new_vars = 0
    for _, item in ipairs(items_after) do
        if item.name and item.name:match("^new_var_") then
            visible_new_vars = visible_new_vars + 1
        end
    end
    print("    Visible new variables: " .. visible_new_vars .. " / " .. num_new_vars)

    -- All 20 new variables should be visible
    assert_eq(visible_new_vars, num_new_vars,
        "all " .. num_new_vars .. " new variables should appear, but only " .. visible_new_vars .. " visible")
end)

test("WORKAROUND: collapse and re-expand shows new child", function()
    local gd = ng.graph(dap_schema)

    -- Same setup as above
    local debugger_id = gd:insert("Debugger", {name = "main"})
    local thread_id = gd:insert("Thread", {name = "Thread 1", state = "stopped"})
    gd:link(debugger_id, "threads", thread_id)
    local frame_id = gd:insert("Frame", {name = "main", line = 42})
    gd:link(thread_id, "frames", frame_id)
    local scope_id = gd:insert("Scope", {name = "Locals"})
    gd:link(frame_id, "scopes", scope_id)
    local var1_id = gd:insert("Variable", {name = "counter", value = "42"})
    gd:link(scope_id, "variables", var1_id)

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
                        "name": "variables"
                    }]
                }]
            }]
        }]
    }]], debugger_id)

    local view = gd:query(query, {limit = 100, immediate = true})

    -- Expand all the way to variables
    view:expand(debugger_id, "threads")
    view:expand(thread_id, "frames")
    view:expand(frame_id, "scopes")
    view:expand(scope_id, "variables")

    -- Add new variable (won't appear due to bug)
    local new_var_id = gd:insert("Variable", {name = "new_var", value = "999"})
    gd:link(scope_id, "variables", new_var_id)

    -- WORKAROUND: Collapse and re-expand
    view:collapse(scope_id, "variables")
    view:expand(scope_id, "variables")

    -- Now check if new variable appears
    local items = view:items()
    local found_new = false
    for _, item in ipairs(items) do
        if item.name == "new_var" then
            found_new = true
            break
        end
    end

    -- This should pass (workaround works)
    assert_true(found_new, "new variable should appear after collapse+expand workaround")
end)

-- ============================================================================
-- 11. Unified Node Events API (g:on / g:off)
-- ============================================================================

print("\n11. Unified Node Events API (g:on / g:off)")

test("g:on('change') fires on property update", function()
    local gn = ng.graph(schema)
    local id = gn:insert("User", {name = "Original", age = 30, active = true})

    local change_count = 0
    local last_new = nil
    local last_old = nil

    local unsub = gn:on(id, "change", function(node_id, new_node, old_node)
        change_count = change_count + 1
        last_new = new_node
        last_old = old_node
    end)

    gn:update(id, {name = "Updated"})

    assert_eq(change_count, 1, "change callback should fire once")
    assert_eq(last_new.name, "Updated", "new node should have updated name")
    assert_eq(last_old.name, "Original", "old node should have original name")

    unsub()
end)

test("g:on('delete') fires on node deletion", function()
    local gn = ng.graph(schema)
    local id = gn:insert("User", {name = "ToDelete", age = 25, active = true})

    local delete_count = 0
    local deleted_id = nil

    local unsub = gn:on(id, "delete", function(node_id)
        delete_count = delete_count + 1
        deleted_id = node_id
    end)

    gn:delete(id)

    assert_eq(delete_count, 1, "delete callback should fire once")
    assert_eq(deleted_id, id, "deleted id should match")
end)

test("g:on('link') fires bidirectionally", function()
    local gn = ng.graph(schema)
    local user_id = gn:insert("User", {name = "Author", age = 35, active = true})
    local post_id = gn:insert("Post", {title = "My Post", views = 0})

    local user_links = {}
    local post_links = {}

    local unsub_user = gn:on(user_id, "link", function(node_id, edge_name, target_id)
        table.insert(user_links, {id = node_id, edge = edge_name, target = target_id})
    end)

    local unsub_post = gn:on(post_id, "link", function(node_id, edge_name, target_id)
        table.insert(post_links, {id = node_id, edge = edge_name, target = target_id})
    end)

    gn:link(user_id, "posts", post_id)

    assert_eq(#user_links, 1, "user should receive one link callback")
    assert_eq(user_links[1].edge, "posts", "user link edge should be 'posts'")
    assert_eq(user_links[1].target, post_id, "user link target should be post_id")

    assert_eq(#post_links, 1, "post should receive one link callback")
    assert_eq(post_links[1].edge, "author", "post link edge should be 'author' (reverse)")
    assert_eq(post_links[1].target, user_id, "post link target should be user_id")

    unsub_user()
    unsub_post()
end)

test("g:on('unlink') fires bidirectionally", function()
    local gn = ng.graph(schema)
    local user_id = gn:insert("User", {name = "Author", age = 35, active = true})
    local post_id = gn:insert("Post", {title = "My Post", views = 0})
    gn:link(user_id, "posts", post_id)

    local user_unlinks = {}
    local post_unlinks = {}

    local unsub_user = gn:on(user_id, "unlink", function(node_id, edge_name, target_id)
        table.insert(user_unlinks, {id = node_id, edge = edge_name, target = target_id})
    end)

    local unsub_post = gn:on(post_id, "unlink", function(node_id, edge_name, target_id)
        table.insert(post_unlinks, {id = node_id, edge = edge_name, target = target_id})
    end)

    gn:unlink(user_id, "posts", post_id)

    assert_eq(#user_unlinks, 1, "user should receive one unlink callback")
    assert_eq(user_unlinks[1].edge, "posts", "user unlink edge should be 'posts'")

    assert_eq(#post_unlinks, 1, "post should receive one unlink callback")
    assert_eq(post_unlinks[1].edge, "author", "post unlink edge should be 'author' (reverse)")

    unsub_user()
    unsub_post()
end)

test("g:on returns unsubscribe function that works", function()
    local gn = ng.graph(schema)
    local id = gn:insert("User", {name = "Test", age = 20, active = true})

    local change_count = 0
    local unsub = gn:on(id, "change", function()
        change_count = change_count + 1
    end)

    gn:update(id, {age = 21})
    assert_eq(change_count, 1, "callback should fire before unsub")

    unsub()

    gn:update(id, {age = 22})
    assert_eq(change_count, 1, "callback should NOT fire after unsub")
end)

test("g:off clears specific event", function()
    local gn = ng.graph(schema)
    local id = gn:insert("User", {name = "Test", age = 20, active = true})

    local change_count = 0
    local delete_count = 0

    gn:on(id, "change", function() change_count = change_count + 1 end)
    gn:on(id, "delete", function() delete_count = delete_count + 1 end)

    gn:update(id, {age = 21})
    assert_eq(change_count, 1, "change should fire")

    gn:off(id, "change")

    gn:update(id, {age = 22})
    assert_eq(change_count, 1, "change should NOT fire after g:off('change')")

    gn:delete(id)
    assert_eq(delete_count, 1, "delete should still fire (not cleared)")
end)

test("g:off clears all events for node", function()
    local gn = ng.graph(schema)
    local id = gn:insert("User", {name = "Test", age = 20, active = true})

    local change_count = 0
    gn:on(id, "change", function() change_count = change_count + 1 end)

    gn:update(id, {age = 21})
    assert_eq(change_count, 1, "change should fire before g:off()")

    gn:off(id)

    gn:update(id, {age = 22})
    assert_eq(change_count, 1, "change should NOT fire after g:off()")
end)

-- ============================================================================
-- 12. Query Events API (query:on('change') / query:on('move'))
-- ============================================================================

print("\n12. Query Events API (change/move)")

test("query:on('change') fires on property update", function()
    local gq = ng.graph(schema)
    local id = gq:insert("User", {name = "Alice", age = 30, active = true})

    local q = gq:query([[{"root": "User", "sort": [{"field": "name"}]}]], {immediate = true})

    local change_events = {}
    local unsub = q:on("change", function(item, index, old_item)
        table.insert(change_events, {item = item, index = index, old_item = old_item})
    end)

    gq:update(id, {age = 31})

    assert_eq(#change_events, 1, "change callback should fire once")
    assert_eq(change_events[1].item.age, 31, "new item should have updated age")
    assert_eq(change_events[1].old_item.age, 30, "old item should have original age")

    unsub()
end)

test("query:on('move') fires on sort position change", function()
    local gq = ng.graph(schema)
    local id_alice = gq:insert("User", {name = "Alice", age = 30, active = true})
    local id_bob = gq:insert("User", {name = "Bob", age = 25, active = true})
    local id_charlie = gq:insert("User", {name = "Charlie", age = 35, active = true})

    -- Sort by name ascending: Alice(1), Bob(2), Charlie(3)
    local q = gq:query([[{"root": "User", "sort": [{"field": "name"}]}]], {immediate = true})

    local move_events = {}
    local unsub = q:on("move", function(item, new_index, old_index)
        table.insert(move_events, {item = item, new_index = new_index, old_index = old_index})
    end)

    -- Rename Alice to Zoe - should move from position 1 to position 3
    gq:update(id_alice, {name = "Zoe"})

    assert_true(#move_events > 0, "move callback should fire")
    -- Find the move event for Zoe
    local found_zoe_move = false
    for _, ev in ipairs(move_events) do
        if ev.item.name == "Zoe" then
            found_zoe_move = true
            assert_true(ev.new_index > ev.old_index, "Zoe should move to higher index")
        end
    end
    assert_true(found_zoe_move, "should have move event for Zoe")

    unsub()
end)

test("query:on('enter') fires when item enters result set", function()
    local gq = ng.graph(schema)
    gq:insert("User", {name = "Existing", age = 30, active = true})

    local q = gq:query([[{"root": "User", "sort": [{"field": "name"}]}]], {immediate = true})

    local enter_events = {}
    local unsub = q:on("enter", function(item, index)
        table.insert(enter_events, {item = item, index = index})
    end)

    local new_id = gq:insert("User", {name = "NewUser", age = 25, active = true})

    assert_eq(#enter_events, 1, "enter callback should fire once")
    assert_eq(enter_events[1].item.id, new_id, "entered item should be the new user")

    unsub()
end)

test("query:on('leave') fires when item leaves result set", function()
    local gq = ng.graph(schema)
    local id = gq:insert("User", {name = "ToDelete", age = 30, active = true})

    local q = gq:query([[{"root": "User", "sort": [{"field": "name"}]}]], {immediate = true})

    local leave_events = {}
    local unsub = q:on("leave", function(item, index)
        table.insert(leave_events, {item = item, index = index})
    end)

    gq:delete(id)

    assert_eq(#leave_events, 1, "leave callback should fire once")
    assert_eq(leave_events[1].item.id, id, "left item should be the deleted user")

    unsub()
end)

test("query:off clears specific event", function()
    local gq = ng.graph(schema)
    gq:insert("User", {name = "Test", age = 30, active = true})

    local q = gq:query([[{"root": "User", "sort": [{"field": "name"}]}]], {immediate = true})

    local enter_count = 0
    local leave_count = 0

    q:on("enter", function() enter_count = enter_count + 1 end)
    q:on("leave", function() leave_count = leave_count + 1 end)

    local id = gq:insert("User", {name = "New", age = 25, active = true})
    assert_eq(enter_count, 1, "enter should fire")

    q:off("enter")

    gq:insert("User", {name = "Another", age = 26, active = true})
    assert_eq(enter_count, 1, "enter should NOT fire after q:off('enter')")

    gq:delete(id)
    assert_eq(leave_count, 1, "leave should still fire")
end)

test("query:off clears all events", function()
    local gq = ng.graph(schema)
    gq:insert("User", {name = "Test", age = 30, active = true})

    local q = gq:query([[{"root": "User", "sort": [{"field": "name"}]}]], {immediate = true})

    local enter_count = 0
    q:on("enter", function() enter_count = enter_count + 1 end)

    gq:insert("User", {name = "New1", age = 25, active = true})
    assert_eq(enter_count, 1, "enter should fire before q:off()")

    q:off()

    gq:insert("User", {name = "New2", age = 26, active = true})
    assert_eq(enter_count, 1, "enter should NOT fire after q:off()")
end)

-- ============================================================================
-- 13. Expand All / Collapse All
-- ============================================================================

print("\n13. Expand All / Collapse All")

test("expand_all expands all nodes", function()
    local ge = ng.graph(schema)
    local user_id = ge:insert("User", {name = "Author", age = 35, active = true})
    for i = 1, 3 do
        local post_id = ge:insert("Post", {title = "Post " .. i, views = i * 10})
        ge:link(user_id, "posts", post_id)
    end

    local q = ge:query([[{
        "root": "User",
        "sort": [{"field": "name"}],
        "edges": [{"name": "posts", "sort": [{"field": "title"}]}]
    }]], {limit = 20})

    local before = #q:items()
    assert_eq(before, 1, "should have 1 item before expand_all")

    q:expand_all()

    local after = #q:items()
    assert_eq(after, 4, "should have 4 items after expand_all (1 user + 3 posts)")
end)

test("expand_all respects max_depth", function()
    local ge = ng.graph(dap_schema)
    local debugger_id = ge:insert("Debugger", {name = "main"})
    local thread_id = ge:insert("Thread", {name = "Thread 1", state = "running"})
    ge:link(debugger_id, "threads", thread_id)
    local frame_id = ge:insert("Frame", {name = "main", line = 42})
    ge:link(thread_id, "frames", frame_id)
    local scope_id = ge:insert("Scope", {name = "Locals"})
    ge:link(frame_id, "scopes", scope_id)

    local q = ge:query(string.format([[{
        "root": "Debugger",
        "id": %d,
        "edges": [{
            "name": "threads",
            "edges": [{
                "name": "frames",
                "edges": [{"name": "scopes"}]
            }]
        }]
    }]], debugger_id), {limit = 20})

    -- Expand only 1 level deep
    q:expand_all(1)

    local items = q:items()
    -- Should have: Debugger + Thread (depth 1), but NOT Frame or Scope
    local max_depth = 0
    for _, item in ipairs(items) do
        if item.depth > max_depth then max_depth = item.depth end
    end
    assert_eq(max_depth, 1, "max depth should be 1 when expand_all(1)")
end)

test("collapse_all collapses all nodes", function()
    local ge = ng.graph(schema)
    local user_id = ge:insert("User", {name = "Author", age = 35, active = true})
    for i = 1, 3 do
        local post_id = ge:insert("Post", {title = "Post " .. i, views = i * 10})
        ge:link(user_id, "posts", post_id)
    end

    local q = ge:query([[{
        "root": "User",
        "sort": [{"field": "name"}],
        "edges": [{"name": "posts", "sort": [{"field": "title"}]}]
    }]], {limit = 20})

    q:expand_all()
    local expanded = #q:items()
    assert_eq(expanded, 4, "should have 4 items when expanded")

    q:collapse_all()
    local collapsed = #q:items()
    assert_eq(collapsed, 1, "should have 1 item after collapse_all")
end)

test("collapse_all then expand works correctly", function()
    local ge = ng.graph(schema)
    local user_id = ge:insert("User", {name = "Author", age = 35, active = true})
    for i = 1, 2 do
        local post_id = ge:insert("Post", {title = "Post " .. i, views = i * 10})
        ge:link(user_id, "posts", post_id)
    end

    local q = ge:query([[{
        "root": "User",
        "sort": [{"field": "name"}],
        "edges": [{"name": "posts", "sort": [{"field": "title"}]}]
    }]], {limit = 20})

    q:expand(user_id, "posts")
    assert_eq(#q:items(), 3, "should have 3 items when expanded")

    q:collapse_all()
    assert_eq(#q:items(), 1, "should have 1 item after collapse_all")

    q:expand(user_id, "posts")
    assert_eq(#q:items(), 3, "should have 3 items when re-expanded")
end)

-- ============================================================================
-- 14. SDK Signal Tests
-- ============================================================================

print("\n14. SDK Signal Tests")

local sdk = require("neograph-sdk")

-- Simple schema for SDK tests
local sdk_schema = [[{
    "types": [
        {
            "name": "Person",
            "properties": [
                { "name": "name", "type": "string" },
                { "name": "age", "type": "int" }
            ],
            "edges": [
                { "name": "friends", "target": "Person", "reverse": "friends" }
            ],
            "indexes": [{ "fields": [{ "name": "name" }] }]
        }
    ]
}]]

test("signal:get() returns property value", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})
    assert_eq(alice.name:get(), "Alice", "signal:get() should return name")
    assert_eq(alice.age:get(), 30, "signal:get() should return age")
end)

test("signal:onChange() fires on property update", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})

    local change_count = 0
    local last_new, last_old = nil, nil

    local unsub = alice.age:onChange(function(new_val, old_val)
        change_count = change_count + 1
        last_new = new_val
        last_old = old_val
    end)

    alice:update({age = 31})

    assert_eq(change_count, 1, "onChange should fire once")
    assert_eq(last_new, 31, "new value should be 31")
    assert_eq(last_old, 30, "old value should be 30")

    -- Cleanup
    unsub()
end)

test("signal:onChange() unsubscribe stops callbacks", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})

    local change_count = 0
    local unsub = alice.age:onChange(function(new_val, old_val)
        change_count = change_count + 1
    end)

    alice:update({age = 31})
    assert_eq(change_count, 1, "should fire before unsub")

    unsub()

    alice:update({age = 32})
    assert_eq(change_count, 1, "should NOT fire after unsub")
end)

test("signal:use() runs effect immediately and on change", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})

    local effect_values = {}
    local cleanup_count = 0

    local unsub = alice.age:use(function(age)
        table.insert(effect_values, age)
        return function()
            cleanup_count = cleanup_count + 1
        end
    end)

    assert_eq(#effect_values, 1, "effect should run immediately")
    assert_eq(effect_values[1], 30, "initial value should be 30")

    alice:update({age = 31})

    assert_eq(#effect_values, 2, "effect should run on change")
    assert_eq(effect_values[2], 31, "changed value should be 31")
    assert_eq(cleanup_count, 1, "cleanup should run before second effect")

    unsub()
    assert_eq(cleanup_count, 2, "cleanup should run on unsubscribe")
end)

test("signal:get() errors on deleted entity", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})
    local name_signal = alice.name

    alice:delete()

    local ok, err = pcall(function() name_signal:get() end)
    assert_true(not ok, "signal:get() should error on deleted entity")
end)

-- ============================================================================
-- 15. SDK EdgeCollection Tests
-- ============================================================================

print("\n15. SDK EdgeCollection Tests")

test("EdgeCollection:iter() returns linked entities", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})
    local bob = Person.insert({name = "Bob", age = 25})
    local carol = Person.insert({name = "Carol", age = 35})

    alice.friends:link(bob)
    alice.friends:link(carol)

    local friend_names = {}
    for friend in alice.friends:iter() do
        table.insert(friend_names, friend.name:get())
    end

    assert_eq(#friend_names, 2, "should have 2 friends")
    -- Check both are present (order may vary)
    local has_bob = false
    local has_carol = false
    for _, name in ipairs(friend_names) do
        if name == "Bob" then has_bob = true end
        if name == "Carol" then has_carol = true end
    end
    assert_true(has_bob, "should have Bob")
    assert_true(has_carol, "should have Carol")
end)

test("EdgeCollection:unlink() removes link", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})
    local bob = Person.insert({name = "Bob", age = 25})

    alice.friends:link(bob)

    local count_before = 0
    for _ in alice.friends:iter() do count_before = count_before + 1 end
    assert_eq(count_before, 1, "should have 1 friend before unlink")

    alice.friends:unlink(bob)

    local count_after = 0
    for _ in alice.friends:iter() do count_after = count_after + 1 end
    assert_eq(count_after, 0, "should have 0 friends after unlink")
end)

test("EdgeCollection:onEnter() fires when entity linked", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})
    local bob = Person.insert({name = "Bob", age = 25})

    local entered_names = {}
    local unsub = alice.friends:onEnter(function(entity)
        table.insert(entered_names, entity.name:get())
    end)

    alice.friends:link(bob)

    assert_eq(#entered_names, 1, "onEnter should fire once")
    assert_eq(entered_names[1], "Bob", "entered entity should be Bob")

    unsub()
end)

test("EdgeCollection:onLeave() fires when entity unlinked", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})
    local bob = Person.insert({name = "Bob", age = 25})

    alice.friends:link(bob)

    local left_names = {}
    local unsub = alice.friends:onLeave(function(entity)
        table.insert(left_names, entity.name:get())
    end)

    alice.friends:unlink(bob)

    assert_eq(#left_names, 1, "onLeave should fire once")
    assert_eq(left_names[1], "Bob", "left entity should be Bob")

    unsub()
end)

test("EdgeCollection:each() runs effect for all and reacts", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})
    local bob = Person.insert({name = "Bob", age = 25})

    alice.friends:link(bob)

    local effect_runs = {}
    local cleanup_runs = {}

    local unsub = alice.friends:each(function(entity)
        table.insert(effect_runs, entity.name:get())
        return function()
            table.insert(cleanup_runs, entity.name:get())
        end
    end)

    assert_eq(#effect_runs, 1, "effect should run for existing friend")
    assert_eq(effect_runs[1], "Bob", "effect should run for Bob")

    -- Add new friend
    local carol = Person.insert({name = "Carol", age = 35})
    alice.friends:link(carol)

    assert_eq(#effect_runs, 2, "effect should run for new friend")
    assert_eq(effect_runs[2], "Carol", "effect should run for Carol")

    -- Remove friend
    alice.friends:unlink(bob)

    assert_eq(#cleanup_runs, 1, "cleanup should run for removed friend")
    assert_eq(cleanup_runs[1], "Bob", "cleanup should run for Bob")

    unsub()
    -- Carol's cleanup should run on unsub
    assert_eq(#cleanup_runs, 2, "cleanup should run for remaining on unsub")
end)

-- ============================================================================
-- 16. SDK Entity Tests
-- ============================================================================

print("\n16. SDK Entity Tests")

test("entity:type() returns type name", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})
    assert_eq(alice:type(), "Person", "type() should return Person")
end)

test("entity:isDeleted() tracks deletion state", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})
    assert_eq(alice:isDeleted(), false, "should not be deleted initially")

    alice:delete()
    assert_eq(alice:isDeleted(), true, "should be deleted after delete()")
end)

test("entity:delete() removes from graph", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})
    local id = alice:id()

    alice:delete()

    local data = g:get(id)
    assert_eq(data, nil, "node should be deleted from graph")
end)

test("entity:onLink() fires when edge linked", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})
    local bob = Person.insert({name = "Bob", age = 25})

    local link_events = {}
    local unsub = alice:onLink("friends", function(target_id)
        table.insert(link_events, target_id)
    end)

    g:link(alice:id(), "friends", bob:id())

    assert_eq(#link_events, 1, "onLink should fire once")
    assert_eq(link_events[1], bob:id(), "target_id should be Bob's id")

    unsub()
end)

test("entity:onUnlink() fires when edge unlinked", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})
    local bob = Person.insert({name = "Bob", age = 25})

    g:link(alice:id(), "friends", bob:id())

    local unlink_events = {}
    local unsub = alice:onUnlink("friends", function(target_id)
        table.insert(unlink_events, target_id)
    end)

    g:unlink(alice:id(), "friends", bob:id())

    assert_eq(#unlink_events, 1, "onUnlink should fire once")
    assert_eq(unlink_events[1], bob:id(), "target_id should be Bob's id")

    unsub()
end)

test("entity:unwatch() stops all callbacks", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})

    local change_count = 0
    alice.age:onChange(function() change_count = change_count + 1 end)

    alice:update({age = 31})
    assert_eq(change_count, 1, "should fire before unwatch")

    alice:unwatch()

    alice:update({age = 32})
    assert_eq(change_count, 1, "should NOT fire after unwatch")
end)

test("entity update on deleted entity errors", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = Person.insert({name = "Alice", age = 30})
    alice:delete()

    local ok, err = pcall(function() alice:update({age = 31}) end)
    assert_true(not ok, "update() on deleted entity should error")
end)

-- ============================================================================
-- 17. SDK Database Tests
-- ============================================================================

print("\n17. SDK Database Tests")

test("db:get() returns cached entity", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local id = g:insert("Person", {name = "Alice", age = 30})

    local entity1 = db:get(id)
    local entity2 = db:get(id)

    assert_true(entity1 == entity2, "same entity should be returned (cached)")
end)

test("db:get() returns nil for non-existent id", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local entity = db:get(99999)
    assert_eq(entity, nil, "should return nil for non-existent id")
end)

test("db:find() returns entity with explicit type", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local id = g:insert("Person", {name = "Alice", age = 30})

    local entity = db:find("Person", id)
    assert_true(entity ~= nil, "should find entity")
    assert_eq(entity:id(), id, "should have correct id")
end)

test("db:evict() removes entity from cache", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local id = g:insert("Person", {name = "Alice", age = 30})

    local entity1 = db:get(id)
    db:evict(id)
    local entity2 = db:get(id)

    assert_true(entity1 ~= entity2, "different entity instances after evict")
end)

test("db.graph exposes underlying graph", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    assert_true(db.graph == g, "db.graph should be the original graph")
end)

test("db:insert() returns entity and caches it", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    local alice = db:insert("Person", {name = "Alice", age = 30})
    local cached = db:get(alice:id())

    assert_true(alice == cached, "inserted entity should be cached")
end)

-- ============================================================================
-- 18. SDK Custom Entity Class Tests
-- ============================================================================

print("\n18. SDK Custom Entity Class Tests")

test("custom entity class methods work", function()
    local g = ng.graph(sdk_schema)

    local Person = sdk.entity("Person")

    -- Add custom method
    function Person:greet()
        return "Hello, " .. self.name:get() .. "!"
    end

    local db = sdk.wrap(g, { Person = Person })
    local alice = Person.insert({name = "Alice", age = 30})

    assert_eq(alice:greet(), "Hello, Alice!", "custom method should work")
end)

test("custom entity __tostring metamethod works", function()
    local g = ng.graph(sdk_schema)

    local Person = sdk.entity("Person")
    Person.__tostring = function(self)
        return "Person:" .. self.name:get()
    end

    local db = sdk.wrap(g, { Person = Person })
    local alice = Person.insert({name = "Alice", age = 30})

    assert_eq(tostring(alice), "Person:Alice", "__tostring should work")
end)

test("default entity class used for unknown types", function()
    local g = ng.graph(sdk_schema)
    local db = sdk.wrap(g, {})  -- No custom types registered

    local id = g:insert("Person", {name = "Alice", age = 30})
    local entity = db:get(id)

    assert_true(entity ~= nil, "should get entity with default class")
    assert_eq(entity:id(), id, "should have correct id")
    assert_eq(entity.name:get(), "Alice", "should access properties")
end)

test("Entity.insert() static method works", function()
    local g = ng.graph(sdk_schema)
    local Person = sdk.entity("Person")
    local db = sdk.wrap(g, { Person = Person })

    -- insert() is added by sdk.wrap
    local alice = Person.insert({name = "Alice", age = 30})

    assert_true(alice ~= nil, "static insert should return entity")
    assert_eq(alice.name:get(), "Alice", "entity should have correct data")
end)

-- ============================================================================
-- Summary
-- ============================================================================

print("\n=== Test Summary ===")
print(string.format("Passed: %d", passed))
print(string.format("Failed: %d", failed))
print("")

if failed > 0 then
    os.exit(1)
else
    print("All tests passed!")
    os.exit(0)
end
