# neograph-sdk

A reactive entity abstraction layer over the neograph_lua bindings. Provides a clean, object-oriented API for working with graph data using signals and reactive collections.

## Installation

```lua
-- Add to package path
package.path = package.path .. ";./lua/?/init.lua;./lua/?.lua"

local ng = require("neograph_lua")
local sdk = require("neograph-sdk")
```

## Quick Start

```lua
-- Define custom entity types (optional)
local User = sdk.entity("User")
local Post = sdk.entity("Post")

-- Add custom methods
function User:greet()
    return "Hello, " .. self.name:get()
end

-- Create database from graph
local graph = ng.graph(schema)
local db = sdk.wrap(graph, { User = User, Post = Post })

-- Insert entities
local user = User.insert({ name = "Alice", age = 30 })
local post = Post.insert({ title = "My First Post" })

-- Access properties via signals
print(user.name:get())  -- "Alice"

-- React to changes
user.name:onChange(function(new_val, old_val)
    print("Name changed from", old_val, "to", new_val)
end)

-- Link entities
user.posts:link(post)

-- Iterate edges
for p in user.posts:iter() do
    print(p.title:get())
end
```

## Core Concepts

### Database

The `Database` wraps a low-level neograph and provides entity caching and type mapping.

```lua
-- Create with optional entity type mappings
local db = sdk.wrap(graph, { User = User, Post = Post })

-- Access underlying graph
db.graph:insert("User", { name = "Direct" })

-- Get entity by ID (uses cache)
local user = db:get(1)

-- Insert new entity
local user = db:insert("User", { name = "Alice", age = 30 })

-- Find entity with type hint
local user = db:find("User", 1)

-- Evict from cache (forces re-fetch on next access)
db:evict(1)
```

### Entity

Entities represent graph nodes with signal-based property access.

```lua
local user = db:insert("User", { name = "Alice", age = 30 })

-- Get entity metadata
user:id()        -- numeric ID
user:type()      -- "User"
user:isDeleted() -- false

-- Update properties
user:update({ name = "Bob", age = 31 })

-- Delete entity
user:delete()

-- Stop watching for changes
user:unwatch()
```

### Signal

Signals provide reactive access to entity properties. Accessing any property on an entity returns a Signal.

```lua
local name = user.name  -- Returns a Signal

-- Get current value (always fetches fresh from graph)
name:get()  -- "Alice"

-- Subscribe to changes
local unsubscribe = name:onChange(function(new_val, old_val)
    print("Changed:", old_val, "->", new_val)
end)

-- Unsubscribe when done
unsubscribe()
```

**Note:** `signal:get()` always returns fresh data from the graph, even without subscribing. Subscribing via `onChange` enables reactive notifications.

#### Effect Pattern with `use()`

The `use()` method provides a convenient pattern for effects with cleanup, similar to React's `useEffect`:

```lua
local unsub = user.name:use(function(value)
    print("Name is now:", value)  -- Runs immediately and on each change

    return function()
        print("Cleaning up for:", value)  -- Runs before next effect or on unsubscribe
    end
end)

-- Later: cleanup runs and effect stops
unsub()
```

The cleanup function is optional. If provided, it runs:
- Before each subsequent effect call (when value changes)
- When `unsub()` is called
- When the entity is deleted (auto-unsubscribes, effect does NOT run with `nil`)

### EdgeCollection

Accessing an edge property returns an EdgeCollection for reactive traversal.

```lua
local posts = user.posts  -- Returns an EdgeCollection

-- Iterate linked entities
for post in posts:iter() do
    print(post.title:get())
end

-- Link entities (accepts entity or ID)
posts:link(post)
posts:link(post_id)

-- Unlink entities
posts:unlink(post)
posts:unlink(post_id)

-- React to links/unlinks
local unsub = posts:onEnter(function(entity)
    print("Added:", entity.title:get())
end)

posts:onLeave(function(entity)
    print("Removed:", entity.title:get())
end)

-- Unsubscribe
unsub()
```

#### Effect Pattern with `each()`

The `each()` method runs an effect for each item in the collection, with automatic cleanup:

```lua
local unsub = user.posts:each(function(post)
    print("Post entered:", post.title:get())  -- Runs for existing + new items

    return function()
        print("Post left:", post.title:get())  -- Runs when item leaves or on unsubscribe
    end
end)

-- Later: all cleanups run and tracking stops
unsub()
```

The cleanup function is optional. If provided, it runs:
- When the specific item leaves the collection (unlinked)
- For all remaining items when `unsub()` is called

## Custom Entity Types

Define custom entity classes to add domain-specific methods and metamethods.

### Basic Custom Type

```lua
local User = sdk.entity("User")

-- Add methods
function User:greet()
    return "Hello, " .. self.name:get()
end

function User:isAdult()
    return self.age:get() >= 18
end

-- Use with database
local db = sdk.wrap(graph, { User = User })
local user = db:insert("User", { name = "Alice", age = 30 })

print(user:greet())   -- "Hello, Alice"
print(user:isAdult()) -- true
```

### Metamethods

```lua
local User = sdk.entity("User")

-- Custom string representation
User.__tostring = function(self)
    return string.format("User<%s>#%d", self.name:get(), self:id())
end

-- Custom equality (compare by ID)
User.__eq = function(a, b)
    return a:id() == b:id()
end

local user = db:insert("User", { name = "Alice" })
print(user)  -- "User<Alice>#1"
```

### Class.insert() Shorthand

After wrapping, entity classes gain an `insert()` method:

```lua
local db = sdk.wrap(graph, { User = User, Post = Post })

-- These are equivalent:
local user = db:insert("User", { name = "Alice" })
local user = User.insert({ name = "Alice" })
```

## Edge Events (Low-Level)

For fine-grained control, entities provide direct edge event callbacks:

```lua
-- React to any link on this edge
user:onLink("posts", function(target_id)
    print("Linked to post:", target_id)
end)

-- React to any unlink on this edge
user:onUnlink("posts", function(target_id)
    print("Unlinked from post:", target_id)
end)
```

**Note:** These callbacks receive raw IDs. For entity objects, use `EdgeCollection:onEnter/onLeave` instead.

## Complete Example

```lua
local ng = require("neograph_lua")
local sdk = require("neograph-sdk")

-- Schema
local schema = [[{
    "types": [
        {
            "name": "User",
            "properties": [
                { "name": "name", "type": "string" },
                { "name": "email", "type": "string" }
            ],
            "edges": [
                { "name": "posts", "target": "Post", "reverse": "author" }
            ],
            "indexes": [{ "fields": [{ "name": "name" }] }]
        },
        {
            "name": "Post",
            "properties": [
                { "name": "title", "type": "string" },
                { "name": "published", "type": "bool" }
            ],
            "edges": [
                { "name": "author", "target": "User", "reverse": "posts" }
            ],
            "indexes": [{ "fields": [{ "name": "title" }] }]
        }
    ]
}]]

-- Define entities
local User = sdk.entity("User")
local Post = sdk.entity("Post")

function User:postCount()
    local count = 0
    for _ in self.posts:iter() do
        count = count + 1
    end
    return count
end

function Post:byline()
    for author in self.author:iter() do
        return "By " .. author.name:get()
    end
    return "By Anonymous"
end

-- Setup
local graph = ng.graph(schema)
local db = sdk.wrap(graph, { User = User, Post = Post })

-- Create data
local alice = User.insert({ name = "Alice", email = "alice@example.com" })
local post1 = Post.insert({ title = "Hello World", published = true })
local post2 = Post.insert({ title = "Draft Post", published = false })

-- Link
alice.posts:link(post1)
alice.posts:link(post2)

-- Query
print(alice:postCount())  -- 2
print(post1:byline())     -- "By Alice"

-- React to changes
alice.name:onChange(function(new_name)
    print("Alice is now", new_name)
end)

alice:update({ name = "Alicia" })  -- Prints: "Alice is now Alicia"

-- React to new posts
alice.posts:onEnter(function(post)
    print("New post:", post.title:get())
end)

local post3 = Post.insert({ title = "Another Post" })
alice.posts:link(post3)  -- Prints: "New post: Another Post"
```

## API Reference

### sdk

| Function | Description |
|----------|-------------|
| `sdk.entity(type_name)` | Create a custom entity class |
| `sdk.wrap(graph, entity_types?)` | Wrap graph with SDK, returns Database |

### Database

| Method | Description |
|--------|-------------|
| `db:get(id)` | Get entity by ID (cached) |
| `db:find(type_name, id)` | Get entity with type hint |
| `db:insert(type_name, props)` | Create new entity |
| `db:evict(id)` | Remove from cache |
| `db.graph` | Underlying neograph instance |

### Entity

| Method | Description |
|--------|-------------|
| `entity:id()` | Get numeric ID |
| `entity:type()` | Get type name |
| `entity:isDeleted()` | Check if deleted |
| `entity:update(props)` | Update properties |
| `entity:delete()` | Delete from graph |
| `entity:unwatch()` | Stop watching for changes |
| `entity:onLink(edge, callback)` | Subscribe to link events |
| `entity:onUnlink(edge, callback)` | Subscribe to unlink events |
| `entity.<property>` | Returns Signal |
| `entity.<edge>` | Returns EdgeCollection |

### Signal

| Method | Description |
|--------|-------------|
| `signal:get()` | Get current value |
| `signal:onChange(callback)` | Subscribe to changes, returns unsubscribe function |
| `signal:use(effect)` | Run effect immediately and on changes, with optional cleanup |

### EdgeCollection

| Method | Description |
|--------|-------------|
| `collection:iter()` | Iterator over linked entities |
| `collection:link(target)` | Link entity or ID |
| `collection:unlink(target)` | Unlink entity or ID |
| `collection:onEnter(callback)` | Subscribe to additions, returns unsubscribe |
| `collection:onLeave(callback)` | Subscribe to removals, returns unsubscribe |
| `collection:each(effect)` | Run effect for each item, with optional cleanup on leave |
