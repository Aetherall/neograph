-- neograph-sdk test suite
-- Tests the entity mapper SDK layer on top of neograph_lua bindings
-- Run with: nvim -l test/sdk_test.lua

-- Setup path
local cwd = vim.fn.getcwd()
package.cpath = cwd .. "/lua/?.so;;" .. package.cpath

local ng = require("neograph_lua")

-- Add lua/ to package path for SDK
package.path = package.path .. ";./lua/?/init.lua;./lua/?.lua"
local sdk = require("neograph-sdk")

-- Test schema (same format as lua_test.lua)
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
                { "name": "posts", "target": "Post", "reverse": "author" },
                { "name": "friends", "target": "User", "reverse": "friends" }
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
                { "fields": [{ "name": "title" }] }
            ]
        }
    ]
}]]

-- Test helpers
local passed = 0
local failed = 0

local function test(name, fn)
    local ok, err = pcall(fn)
    if ok then
        print("  PASS: " .. name)
        passed = passed + 1
    else
        print("  FAIL: " .. name)
        print("        " .. tostring(err))
        failed = failed + 1
    end
end

local function assert_eq(actual, expected, msg)
    if actual ~= expected then
        error(string.format("%s: expected %s, got %s", msg or "assertion failed", tostring(expected), tostring(actual)))
    end
end

local function assert_true(val, msg)
    if not val then
        error(msg or "expected true")
    end
end

local function assert_false(val, msg)
    if val then
        error(msg or "expected false")
    end
end

-- ============================================================================
-- Tests
-- ============================================================================

print("\n=== Neograph SDK Tests ===")

print("\n1. Database and Entity Creation")

test("wrap graph creates database", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)
    assert_true(db ~= nil, "database should be created")
    assert_true(db.graph == g, "database should expose underlying graph")
end)

test("insert creates entity with properties", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    assert_true(user ~= nil, "entity should be created")
    assert_eq(user:id(), 1, "entity should have ID")
    assert_false(user:isDeleted(), "entity should not be deleted")
end)

test("entity property access returns signal", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local name_signal = user.name

    assert_true(name_signal ~= nil, "should return signal")
    assert_true(name_signal.get ~= nil, "signal should have get method")
    assert_true(name_signal.onChange ~= nil, "signal should have onChange method")
end)

test("signal:get returns property value", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    assert_eq(user.name:get(), "Alice", "name should be Alice")
    assert_eq(user.age:get(), 30, "age should be 30")
    assert_eq(user.active:get(), true, "active should be true")
end)

print("\n2. Signal Reactivity")

test("signal:get always returns fresh data without subscribing", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    assert_eq(user.name:get(), "Alice", "initial value")

    -- Update directly via low-level graph API (no subscription)
    g:update(user:id(), {name = "Bob"})

    -- Should see fresh value without any subscription
    assert_eq(user.name:get(), "Bob", "should see updated value without subscribing")
end)

test("signal:onChange fires on update", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local changes = {}
    user.name:onChange(function(new_val, old_val)
        table.insert(changes, {new = new_val, old = old_val})
    end)

    user:update({name = "Bob"})

    assert_eq(#changes, 1, "should have one change")
    assert_eq(changes[1].new, "Bob", "new value should be Bob")
    assert_eq(changes[1].old, "Alice", "old value should be Alice")
end)

test("signal:onChange unsubscribe works", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local count = 0
    local unsub = user.name:onChange(function(new_val, old_val)
        count = count + 1
    end)

    user:update({name = "Bob"})
    assert_eq(count, 1, "should fire once")

    unsub()

    user:update({name = "Charlie"})
    assert_eq(count, 1, "should not fire after unsubscribe")
end)

test("multiple signals can be subscribed", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local name_changes = 0
    local age_changes = 0

    user.name:onChange(function() name_changes = name_changes + 1 end)
    user.age:onChange(function() age_changes = age_changes + 1 end)

    user:update({name = "Bob"})
    assert_eq(name_changes, 1, "name should change once")
    assert_eq(age_changes, 0, "age should not change")

    user:update({age = 31})
    assert_eq(name_changes, 1, "name should still be 1")
    assert_eq(age_changes, 1, "age should change once")
end)

test("signal reflects updated value after change", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    -- Subscribe to trigger watching
    user.name:onChange(function() end)

    user:update({name = "Bob"})

    assert_eq(user.name:get(), "Bob", "signal should return updated value")
end)

print("\n3. Entity Deletion")

test("entity:delete marks entity as deleted", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    -- Subscribe to trigger watching
    user.name:onChange(function() end)

    assert_false(user:isDeleted(), "should not be deleted initially")

    user:delete()

    assert_true(user:isDeleted(), "should be deleted after delete()")
end)

test("signal:get throws after entity deleted", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    user.name:onChange(function() end)

    user:delete()

    local ok, err = pcall(function()
        user.name:get()
    end)

    assert_false(ok, "should throw error")
    assert_true(string.find(err, "deleted") ~= nil, "error should mention deleted")
end)

test("signal:onChange fires with nil on delete", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local final_value = "not_called"
    user.name:onChange(function(new_val, old_val)
        final_value = new_val
    end)

    user:delete()

    assert_eq(final_value, nil, "should receive nil on delete")
end)

print("\n4. Entity Caching")

test("db:get returns same entity for same ID", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user1 = db:insert("User", {name = "Alice", age = 30, active = true})
    local id = user1:id()

    local user2 = db:get(id)

    -- They should be the same Lua table (cached)
    assert_true(user1 == user2, "should return same entity from cache")
end)

test("subscribers see changes from any reference", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user1 = db:insert("User", {name = "Alice", age = 30, active = true})
    local id = user1:id()
    local user2 = db:get(id)

    local changes = {}
    user1.name:onChange(function(new_val)
        table.insert(changes, new_val)
    end)

    -- Update through second reference
    user2:update({name = "Bob"})

    assert_eq(#changes, 1, "subscriber should see change")
    assert_eq(changes[1], "Bob", "should see new value")
end)

test("db:evict removes entity from cache", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user1 = db:insert("User", {name = "Alice", age = 30, active = true})
    local id = user1:id()

    db:evict(id)

    local user2 = db:get(id)

    -- After eviction, we get a new entity instance
    assert_true(user1 ~= user2, "should return different entity after evict")
end)

test("db:get fetches existing node data from graph", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    -- Create node directly via low-level graph API
    local id = g:insert("User", {name = "DirectInsert", age = 99, active = false})

    -- Fetch via SDK - should have all properties
    local user = db:get(id)

    assert_true(user ~= nil, "should return entity")
    assert_eq(user.name:get(), "DirectInsert", "name should be fetched from graph")
    assert_eq(user.age:get(), 99, "age should be fetched from graph")
    assert_eq(user.active:get(), false, "active should be fetched from graph")
end)

test("db:get returns nil for non-existent node", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:get(99999)
    assert_true(user == nil, "should return nil for non-existent node")
end)

test("db:get uses correct entity class based on type", function()
    local User = sdk.entity("User")

    function User:greet()
        return "Hi, " .. self.name:get()
    end

    local g = ng.graph(schema)
    local db = sdk.wrap(g, { User = User })

    -- Create via low-level API
    local id = g:insert("User", {name = "TypedUser", age = 25, active = true})

    -- Fetch via SDK - should use User entity class
    local user = db:get(id)

    assert_eq(user:greet(), "Hi, TypedUser", "should use custom entity class methods")
end)

print("\n5. Custom Entity Types")

test("sdk.entity creates custom entity class", function()
    local User = sdk.entity("User")

    function User:greet()
        return "Hello, " .. self.name:get()
    end

    assert_true(User ~= nil, "should create entity class")
    assert_eq(User._type_name, "User", "should have type name")
end)

test("custom methods work on entity", function()
    local User = sdk.entity("User")

    function User:greet()
        return "Hello, " .. self.name:get()
    end

    local g = ng.graph(schema)
    local db = sdk.wrap(g, { User = User })

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    assert_eq(user:greet(), "Hello, Alice", "custom method should work")
end)

test("custom method uses updated signal value", function()
    local User = sdk.entity("User")

    function User:greet()
        return "Hello, " .. self.name:get()
    end

    local g = ng.graph(schema)
    local db = sdk.wrap(g, { User = User })

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    user.name:onChange(function() end)  -- Start watching

    user:update({name = "Bob"})

    assert_eq(user:greet(), "Hello, Bob", "custom method should use updated value")
end)

print("\n6. Class Extension Pattern")

test("methods can be added after sdk.entity() call", function()
    local User = sdk.entity("User")

    -- Add method after creation
    function User:greet()
        return "Hello, " .. self.name:get()
    end

    local g = ng.graph(schema)
    local db = sdk.wrap(g, { User = User })

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    assert_eq(user:greet(), "Hello, Alice", "method added after sdk.entity() should work")
end)

test("multiple methods can be added incrementally", function()
    local User = sdk.entity("User")

    function User:greet()
        return "Hello, " .. self.name:get()
    end

    function User:isAdult()
        return self.age:get() >= 18
    end

    function User:summary()
        return string.format("%s (age %d)", self.name:get(), self.age:get())
    end

    local g = ng.graph(schema)
    local db = sdk.wrap(g, { User = User })

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    assert_eq(user:greet(), "Hello, Alice", "greet should work")
    assert_eq(user:isAdult(), true, "isAdult should work")
    assert_eq(user:summary(), "Alice (age 30)", "summary should work")
end)

test("__tostring metamethod works", function()
    local User = sdk.entity("User")

    User.__tostring = function(self)
        return "User#" .. self:id() .. ": " .. self.name:get()
    end

    local g = ng.graph(schema)
    local db = sdk.wrap(g, { User = User })

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    assert_eq(tostring(user), "User#1: Alice", "__tostring should be used")
end)

test("default __tostring when not provided", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local str = tostring(user)
    assert_true(string.find(str, "Entity") ~= nil, "default should contain Entity")
    assert_true(string.find(str, "1") ~= nil, "default should contain ID")
end)

test("__eq metamethod works", function()
    local User = sdk.entity("User")

    User.__eq = function(a, b)
        return a:id() == b:id()
    end

    local g = ng.graph(schema)
    local db = sdk.wrap(g, { User = User })

    local user1 = db:insert("User", {name = "Alice", age = 30, active = true})
    local id = user1:id()

    -- Evict and re-fetch to get a different table
    db:evict(id)
    local user2 = db:get(id)

    -- Different tables but same ID
    assert_true(rawequal(user1, user2) == false, "should be different tables")
    assert_true(user1 == user2, "__eq should compare by ID")
end)

test("methods and metamethods combined", function()
    local User = sdk.entity("User")

    function User:greet()
        return "Hello, " .. self.name:get()
    end

    User.__tostring = function(self)
        return self:greet()
    end

    local g = ng.graph(schema)
    local db = sdk.wrap(g, { User = User })

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    assert_eq(user:greet(), "Hello, Alice", "method should work")
    assert_eq(tostring(user), "Hello, Alice", "__tostring should call method")
end)

test("base methods still work with custom class", function()
    local User = sdk.entity("User")

    function User:customMethod()
        return "custom"
    end

    local g = ng.graph(schema)
    local db = sdk.wrap(g, { User = User })

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    -- Base methods should still work
    assert_eq(user:id(), 1, "id() should work")
    assert_eq(user:type(), "User", "type() should work")
    assert_false(user:isDeleted(), "isDeleted() should work")

    -- Custom method should work
    assert_eq(user:customMethod(), "custom", "custom method should work")

    -- Signals should work
    assert_eq(user.name:get(), "Alice", "signals should work")
end)

test("Class.insert() creates entity", function()
    local User = sdk.entity("User")

    function User:greet()
        return "Hello, " .. self.name:get()
    end

    local g = ng.graph(schema)
    local db = sdk.wrap(g, { User = User })

    -- Use Class.insert() instead of db:insert()
    local user = User.insert({name = "Alice", age = 30, active = true})

    assert_true(user ~= nil, "should create entity")
    assert_eq(user:id(), 1, "should have ID")
    assert_eq(user.name:get(), "Alice", "should have properties")
    assert_eq(user:greet(), "Hello, Alice", "should have custom methods")
end)

test("Class.insert() works with multiple entity types", function()
    local User = sdk.entity("User")
    local Post = sdk.entity("Post")

    function User:greet()
        return "Hello, " .. self.name:get()
    end

    function Post:headline()
        return "POST: " .. self.title:get()
    end

    local g = ng.graph(schema)
    local db = sdk.wrap(g, { User = User, Post = Post })

    local user = User.insert({name = "Alice", age = 30, active = true})
    local post = Post.insert({title = "My First Post", views = 0})

    assert_eq(user:greet(), "Hello, Alice", "User should work")
    assert_eq(post:headline(), "POST: My First Post", "Post should work")
end)

print("\n7. Edge Events")

test("entity:onLink fires on edge creation", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local post_id = g:insert("Post", {title = "Hello", views = 0})

    local linked_posts = {}
    user:onLink("posts", function(target_id)
        table.insert(linked_posts, target_id)
    end)

    g:link(user:id(), "posts", post_id)

    assert_eq(#linked_posts, 1, "should receive link callback")
    assert_eq(linked_posts[1], post_id, "should receive correct target id")
end)

test("entity:onUnlink fires on edge removal", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local post_id = g:insert("Post", {title = "Hello", views = 0})

    g:link(user:id(), "posts", post_id)

    local unlinked_posts = {}
    user:onUnlink("posts", function(target_id)
        table.insert(unlinked_posts, target_id)
    end)

    g:unlink(user:id(), "posts", post_id)

    assert_eq(#unlinked_posts, 1, "should receive unlink callback")
    assert_eq(unlinked_posts[1], post_id, "should receive correct target id")
end)

test("edge event unsubscribe works", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local count = 0
    local unsub = user:onLink("posts", function(target_id)
        count = count + 1
    end)

    local post1 = g:insert("Post", {title = "Post 1", views = 0})
    g:link(user:id(), "posts", post1)
    assert_eq(count, 1, "should fire once")

    unsub()

    local post2 = g:insert("Post", {title = "Post 2", views = 0})
    g:link(user:id(), "posts", post2)
    assert_eq(count, 1, "should not fire after unsubscribe")
end)

test("bidirectional edge events work through SDK", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local post = db:insert("Post", {title = "Hello", views = 0})

    local user_linked = {}
    local post_linked = {}

    user:onLink("posts", function(target_id)
        table.insert(user_linked, target_id)
    end)

    post:onLink("author", function(target_id)
        table.insert(post_linked, target_id)
    end)

    g:link(user:id(), "posts", post:id())

    assert_eq(#user_linked, 1, "user should receive link callback")
    assert_eq(user_linked[1], post:id(), "user should see post linked")

    assert_eq(#post_linked, 1, "post should receive link callback")
    assert_eq(post_linked[1], user:id(), "post should see user as author")
end)

print("\n8. EdgeCollection")

test("edge access returns EdgeCollection (not Signal)", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local posts = user.posts

    -- Should be an EdgeCollection, not a Signal
    assert_true(posts.iter ~= nil, "should have iter method")
    assert_true(posts.onEnter ~= nil, "should have onEnter method")
    assert_true(posts.onLeave ~= nil, "should have onLeave method")
    assert_true(posts.get == nil, "should NOT have get method (not a Signal)")
end)

test("EdgeCollection is cached (same instance returned)", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local posts1 = user.posts
    local posts2 = user.posts

    assert_true(posts1 == posts2, "should return same EdgeCollection instance")
end)

test("EdgeCollection:iter() returns entities", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local post1 = db:insert("Post", {title = "Post 1", views = 10})
    local post2 = db:insert("Post", {title = "Post 2", views = 20})

    g:link(user:id(), "posts", post1:id())
    g:link(user:id(), "posts", post2:id())

    local found = {}
    for post in user.posts:iter() do
        table.insert(found, post.title:get())
    end

    assert_eq(#found, 2, "should iterate two posts")
    -- Posts are sorted by insertion order in edge targets
    assert_true(found[1] == "Post 1" or found[1] == "Post 2", "should find post titles")
end)

test("EdgeCollection:iter() returns empty for no links", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local count = 0
    for post in user.posts:iter() do
        count = count + 1
    end

    assert_eq(count, 0, "should have no posts")
end)

test("EdgeCollection:onEnter fires on link", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local entered = {}
    user.posts:onEnter(function(post)
        table.insert(entered, post.title:get())
    end)

    local post = db:insert("Post", {title = "New Post", views = 0})
    g:link(user:id(), "posts", post:id())

    assert_eq(#entered, 1, "should fire onEnter once")
    assert_eq(entered[1], "New Post", "should receive entity with correct title")
end)

test("EdgeCollection:onLeave fires on unlink", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local post = db:insert("Post", {title = "My Post", views = 0})
    g:link(user:id(), "posts", post:id())

    local left = {}
    user.posts:onLeave(function(post)
        table.insert(left, post.title:get())
    end)

    g:unlink(user:id(), "posts", post:id())

    assert_eq(#left, 1, "should fire onLeave once")
    assert_eq(left[1], "My Post", "should receive entity with correct title")
end)

test("EdgeCollection unsubscribe works", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local count = 0
    local unsub = user.posts:onEnter(function(post)
        count = count + 1
    end)

    local post1 = db:insert("Post", {title = "Post 1", views = 0})
    g:link(user:id(), "posts", post1:id())
    assert_eq(count, 1, "should fire once")

    unsub()

    local post2 = db:insert("Post", {title = "Post 2", views = 0})
    g:link(user:id(), "posts", post2:id())
    assert_eq(count, 1, "should not fire after unsubscribe")
end)

test("property access still returns Signal", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    -- Property should return Signal
    local name = user.name
    assert_true(name.get ~= nil, "property should have get method")
    assert_true(name.onChange ~= nil, "property should have onChange method")
    assert_eq(name:get(), "Alice", "should return correct value")
end)

test("EdgeCollection works with custom entity types", function()
    local User = sdk.entity("User")
    local Post = sdk.entity("Post")

    function Post:headline()
        return ">> " .. self.title:get()
    end

    local g = ng.graph(schema)
    local db = sdk.wrap(g, { User = User, Post = Post })

    local user = User.insert({name = "Alice", age = 30, active = true})
    local post = Post.insert({title = "My Post", views = 0})
    g:link(user:id(), "posts", post:id())

    for p in user.posts:iter() do
        assert_eq(p:headline(), ">> My Post", "should use custom entity class")
    end
end)

test("EdgeCollection:link() with entity", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local post = db:insert("Post", {title = "My Post", views = 0})

    user.posts:link(post)

    local found = {}
    for p in user.posts:iter() do
        table.insert(found, p.title:get())
    end

    assert_eq(#found, 1, "should have one linked post")
    assert_eq(found[1], "My Post", "should find correct post")
end)

test("EdgeCollection:link() with ID", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local post_id = g:insert("Post", {title = "Post by ID", views = 0})

    user.posts:link(post_id)

    local found = {}
    for p in user.posts:iter() do
        table.insert(found, p.title:get())
    end

    assert_eq(#found, 1, "should have one linked post")
    assert_eq(found[1], "Post by ID", "should find correct post")
end)

test("EdgeCollection:unlink() with entity", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local post = db:insert("Post", {title = "My Post", views = 0})

    user.posts:link(post)

    -- Verify linked
    local count = 0
    for _ in user.posts:iter() do count = count + 1 end
    assert_eq(count, 1, "should have one post before unlink")

    user.posts:unlink(post)

    -- Verify unlinked
    count = 0
    for _ in user.posts:iter() do count = count + 1 end
    assert_eq(count, 0, "should have no posts after unlink")
end)

test("EdgeCollection:unlink() with ID", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local post = db:insert("Post", {title = "My Post", views = 0})
    local post_id = post:id()

    user.posts:link(post)

    user.posts:unlink(post_id)

    local count = 0
    for _ in user.posts:iter() do count = count + 1 end
    assert_eq(count, 0, "should have no posts after unlink by ID")
end)

test("EdgeCollection:link() fires onEnter", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local entered = {}
    user.posts:onEnter(function(post)
        table.insert(entered, post.title:get())
    end)

    local post = db:insert("Post", {title = "New Post", views = 0})
    user.posts:link(post)

    assert_eq(#entered, 1, "should fire onEnter")
    assert_eq(entered[1], "New Post", "should receive correct entity")
end)

test("EdgeCollection:unlink() fires onLeave", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local post = db:insert("Post", {title = "My Post", views = 0})
    user.posts:link(post)

    local left = {}
    user.posts:onLeave(function(post)
        table.insert(left, post.title:get())
    end)

    user.posts:unlink(post)

    assert_eq(#left, 1, "should fire onLeave")
    assert_eq(left[1], "My Post", "should receive correct entity")
end)

test("EdgeCollection:link() throws on deleted entity", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local post = db:insert("Post", {title = "My Post", views = 0})

    user.name:onChange(function() end)  -- Start watching
    user:delete()

    local ok, err = pcall(function()
        user.posts:link(post)
    end)

    assert_false(ok, "should throw error")
    assert_true(string.find(err, "deleted") ~= nil, "error should mention deleted")
end)

print("\n9. Effect Management")

test("Signal:use() calls effect immediately", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local called_with = nil
    user.name:use(function(value)
        called_with = value
    end)

    assert_eq(called_with, "Alice", "should call effect immediately with current value")
end)

test("Signal:use() calls effect on change", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local values = {}
    user.name:use(function(value)
        table.insert(values, value)
    end)

    user:update({name = "Bob"})
    user:update({name = "Charlie"})

    assert_eq(#values, 3, "should call effect 3 times")
    assert_eq(values[1], "Alice", "first call with initial value")
    assert_eq(values[2], "Bob", "second call with Bob")
    assert_eq(values[3], "Charlie", "third call with Charlie")
end)

test("Signal:use() runs cleanup on value change", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local cleanups = {}
    user.name:use(function(value)
        return function()
            table.insert(cleanups, value)
        end
    end)

    user:update({name = "Bob"})

    assert_eq(#cleanups, 1, "should run cleanup once")
    assert_eq(cleanups[1], "Alice", "cleanup should have previous value")
end)

test("Signal:use() runs cleanup on unsubscribe", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local cleanup_ran = false
    local unsub = user.name:use(function(value)
        return function()
            cleanup_ran = true
        end
    end)

    assert_false(cleanup_ran, "cleanup should not run yet")

    unsub()

    assert_true(cleanup_ran, "cleanup should run on unsubscribe")
end)

test("Signal:use() unsubscribe stops updates", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local count = 0
    local unsub = user.name:use(function(value)
        count = count + 1
    end)

    assert_eq(count, 1, "immediate call")

    user:update({name = "Bob"})
    assert_eq(count, 2, "after first update")

    unsub()

    user:update({name = "Charlie"})
    assert_eq(count, 2, "should not fire after unsubscribe")
end)

test("Signal:use() runs cleanup on delete but not effect", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local effect_values = {}
    local cleanup_values = {}

    user.name:use(function(value)
        table.insert(effect_values, value)
        return function()
            table.insert(cleanup_values, value)
        end
    end)

    assert_eq(#effect_values, 1, "effect called once initially")
    assert_eq(effect_values[1], "Alice", "initial value")
    assert_eq(#cleanup_values, 0, "no cleanup yet")

    user:delete()

    assert_eq(#effect_values, 1, "effect should NOT run with nil on delete")
    assert_eq(#cleanup_values, 1, "cleanup should run on delete")
    assert_eq(cleanup_values[1], "Alice", "cleanup has last value")
end)

test("Signal:use() auto-unsubscribes on delete", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local count = 0
    local unsub = user.name:use(function(value)
        count = count + 1
    end)

    assert_eq(count, 1, "initial call")

    user:delete()

    -- Calling unsub again should be safe (no-op)
    unsub()
    unsub()

    assert_eq(count, 1, "effect should not have run again")
end)

test("EdgeCollection:each() calls effect for existing items", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local post1 = db:insert("Post", {title = "Post 1", views = 0})
    local post2 = db:insert("Post", {title = "Post 2", views = 0})
    user.posts:link(post1)
    user.posts:link(post2)

    local entered = {}
    user.posts:each(function(post)
        table.insert(entered, post.title:get())
    end)

    assert_eq(#entered, 2, "should call for both existing posts")
end)

test("EdgeCollection:each() calls effect for new items", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local entered = {}
    user.posts:each(function(post)
        table.insert(entered, post.title:get())
    end)

    assert_eq(#entered, 0, "no items initially")

    local post = db:insert("Post", {title = "New Post", views = 0})
    user.posts:link(post)

    assert_eq(#entered, 1, "should call for new post")
    assert_eq(entered[1], "New Post", "should have correct title")
end)

test("EdgeCollection:each() runs cleanup on leave", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local post = db:insert("Post", {title = "My Post", views = 0})
    user.posts:link(post)

    local left = {}
    user.posts:each(function(p)
        local title = p.title:get()
        return function()
            table.insert(left, title)
        end
    end)

    assert_eq(#left, 0, "no cleanups yet")

    user.posts:unlink(post)

    assert_eq(#left, 1, "should run cleanup on leave")
    assert_eq(left[1], "My Post", "cleanup should have post title")
end)

test("EdgeCollection:each() runs all cleanups on unsubscribe", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})
    local post1 = db:insert("Post", {title = "Post 1", views = 0})
    local post2 = db:insert("Post", {title = "Post 2", views = 0})
    user.posts:link(post1)
    user.posts:link(post2)

    local cleanups = {}
    local unsub = user.posts:each(function(p)
        local title = p.title:get()
        return function()
            table.insert(cleanups, title)
        end
    end)

    assert_eq(#cleanups, 0, "no cleanups yet")

    unsub()

    assert_eq(#cleanups, 2, "should run cleanup for both posts")
end)

test("EdgeCollection:each() unsubscribe stops callbacks", function()
    local g = ng.graph(schema)
    local db = sdk.wrap(g)

    local user = db:insert("User", {name = "Alice", age = 30, active = true})

    local count = 0
    local unsub = user.posts:each(function(post)
        count = count + 1
    end)

    local post1 = db:insert("Post", {title = "Post 1", views = 0})
    user.posts:link(post1)
    assert_eq(count, 1, "first link")

    unsub()

    local post2 = db:insert("Post", {title = "Post 2", views = 0})
    user.posts:link(post2)
    assert_eq(count, 1, "should not fire after unsubscribe")
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
