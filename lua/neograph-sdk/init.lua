-- neograph-sdk: Reactive entity abstraction over neograph_lua bindings
--
-- Provides:
--   - Signal: reactive primitive for property values
--   - Entity: graph node with signal-based property access
--   - Database: wrapped graph with entity caching

local sdk = {}

-- ============================================================================
-- Signal: Reactive property primitive
-- ============================================================================

local Signal = {}
Signal.__index = Signal

function Signal.new(entity, prop_name)
    local self = setmetatable({}, Signal)
    self._entity = entity
    self._prop_name = prop_name
    self._subscribers = {}
    return self
end

function Signal:get()
    if self._entity._deleted then
        error("Entity deleted")
    end
    -- Always fetch fresh from graph
    local data = self._entity._graph:get(self._entity._id)
    if not data then
        error("Entity deleted")
    end
    return data[self._prop_name]
end

function Signal:onChange(callback)
    -- Start watching the entity if not already
    self._entity:_startWatching()

    -- Add subscriber
    table.insert(self._subscribers, callback)

    -- Return unsubscribe function
    local subscribers = self._subscribers
    return function()
        for i, cb in ipairs(subscribers) do
            if cb == callback then
                table.remove(subscribers, i)
                break
            end
        end
    end
end

function Signal:_notify(new_val, old_val)
    for _, cb in ipairs(self._subscribers) do
        local ok, err = pcall(cb, new_val, old_val)
        if not ok then
            print("Signal callback error:", err)
        end
    end
end

function Signal:use(effect)
    local cleanup = nil
    local unsubscribed = false

    -- Helper to run cleanup safely
    local function runCleanup()
        if cleanup then
            local ok, err = pcall(cleanup)
            if not ok then
                print("Signal cleanup error:", err)
            end
            cleanup = nil
        end
    end

    -- Run effect immediately with current value
    local ok, result = pcall(effect, self:get())
    if ok and type(result) == "function" then
        cleanup = result
    elseif not ok then
        print("Signal effect error:", result)
    end

    -- Subscribe to changes
    local unsubscribe
    unsubscribe = self:onChange(function(new_val, old_val)
        if unsubscribed then return end

        -- Run cleanup from previous effect
        runCleanup()

        -- On delete: only cleanup, then auto-unsubscribe (don't run effect with nil)
        if self._entity._deleted then
            unsubscribed = true
            unsubscribe()
            return
        end

        -- Run new effect
        local ok2, result2 = pcall(effect, new_val)
        if ok2 and type(result2) == "function" then
            cleanup = result2
        elseif not ok2 then
            print("Signal effect error:", result2)
        end
    end)

    -- Return unsubscribe that also runs final cleanup
    return function()
        if unsubscribed then return end
        unsubscribed = true
        runCleanup()
        unsubscribe()
    end
end

-- ============================================================================
-- EdgeCollection: Reactive collection of linked entities (lightweight)
-- Uses direct edge access instead of views for better performance.
-- ============================================================================

local EdgeCollection = {}
EdgeCollection.__index = EdgeCollection

function EdgeCollection.new(entity, edge_name)
    local self = setmetatable({}, EdgeCollection)
    self._entity = entity
    self._edge_name = edge_name
    self._on_enter_callbacks = {}
    self._on_leave_callbacks = {}
    self._subscribed = false
    self._unsub_link = nil
    self._unsub_unlink = nil
    return self
end

function EdgeCollection:_ensureSubscribed()
    if self._subscribed then return end
    self._subscribed = true

    local collection = self
    local db = self._entity._db
    local edge_name = self._edge_name

    -- Subscribe to link events on this specific edge
    self._unsub_link = self._entity:onLink(edge_name, function(target_id)
        local target_entity = db:get(target_id)
        if target_entity then
            for _, cb in ipairs(collection._on_enter_callbacks) do
                local ok, err = pcall(cb, target_entity)
                if not ok then
                    print("EdgeCollection onEnter callback error:", err)
                end
            end
        end
    end)

    -- Subscribe to unlink events on this specific edge
    self._unsub_unlink = self._entity:onUnlink(edge_name, function(target_id)
        local target_entity = db:get(target_id)
        if target_entity then
            for _, cb in ipairs(collection._on_leave_callbacks) do
                local ok, err = pcall(cb, target_entity)
                if not ok then
                    print("EdgeCollection onLeave callback error:", err)
                end
            end
        end
    end)
end

function EdgeCollection:iter()
    if self._entity._deleted then
        error("Entity deleted")
    end

    -- Use direct edge access - no view needed
    local targets = self._entity._graph:edges(self._entity._id, self._edge_name) or {}
    local db = self._entity._db

    local i = 0
    local n = #targets

    return function()
        i = i + 1
        if i <= n then
            return db:get(targets[i])
        end
        return nil
    end
end

function EdgeCollection:onEnter(callback)
    self:_ensureSubscribed()
    table.insert(self._on_enter_callbacks, callback)

    -- Return unsubscribe function
    local callbacks = self._on_enter_callbacks
    return function()
        for i, cb in ipairs(callbacks) do
            if cb == callback then
                table.remove(callbacks, i)
                break
            end
        end
    end
end

function EdgeCollection:onLeave(callback)
    self:_ensureSubscribed()
    table.insert(self._on_leave_callbacks, callback)

    -- Return unsubscribe function
    local callbacks = self._on_leave_callbacks
    return function()
        for i, cb in ipairs(callbacks) do
            if cb == callback then
                table.remove(callbacks, i)
                break
            end
        end
    end
end

function EdgeCollection:link(target)
    if self._entity._deleted then
        error("Entity deleted")
    end

    -- Accept either an entity or an ID
    local target_id = type(target) == "table" and target._id or target
    self._entity._graph:link(self._entity._id, self._edge_name, target_id)
end

function EdgeCollection:unlink(target)
    if self._entity._deleted then
        error("Entity deleted")
    end

    -- Accept either an entity or an ID
    local target_id = type(target) == "table" and target._id or target
    self._entity._graph:unlink(self._entity._id, self._edge_name, target_id)
end

function EdgeCollection:each(effect)
    -- Track cleanup functions per entity ID
    local cleanups = {}

    -- Run effect for existing items
    for entity in self:iter() do
        local ok, result = pcall(effect, entity)
        if ok and type(result) == "function" then
            cleanups[entity:id()] = result
        elseif not ok then
            print("EdgeCollection effect error:", result)
        end
    end

    -- Subscribe to enter
    local unsubEnter = self:onEnter(function(entity)
        local ok, result = pcall(effect, entity)
        if ok and type(result) == "function" then
            cleanups[entity:id()] = result
        elseif not ok then
            print("EdgeCollection effect error:", result)
        end
    end)

    -- Subscribe to leave
    local unsubLeave = self:onLeave(function(entity)
        local id = entity:id()
        if cleanups[id] then
            local ok, err = pcall(cleanups[id])
            if not ok then
                print("EdgeCollection cleanup error:", err)
            end
            cleanups[id] = nil
        end
    end)

    -- Return unsubscribe that cleans up all
    return function()
        -- Run all pending cleanups
        for id, cleanup in pairs(cleanups) do
            local ok, err = pcall(cleanup)
            if not ok then
                print("EdgeCollection cleanup error:", err)
            end
        end
        cleanups = {}
        unsubEnter()
        unsubLeave()
    end
end

-- ============================================================================
-- Entity: Graph node with signal-based property access
-- ============================================================================

-- Create Entity class that users can extend with methods and metamethods
local function createEntityClass(type_name)
    local class = {}

    -- Instance metatable - separate from the class table
    -- This allows users to add methods/metamethods to `class` directly
    local instance_mt = {
        __index = function(self, key)
            -- Check class methods first
            if class[key] then
                return class[key]
            end
            -- Check internal fields (start with _)
            if type(key) == "string" and key:sub(1, 1) == "_" then
                return rawget(self, key)
            end
            -- Check if this is an edge or property using schema introspection
            -- Use actual_type (from graph) for default entities, class type_name for custom
            local actual_type = rawget(self, "_actual_type") or type_name
            local field_type = self._graph:field_type(actual_type, key)
            if field_type == "edge" then
                return self:_getEdgeCollection(key)
            end
            -- Otherwise, return a signal for the property (or rollup)
            return self:_getSignal(key)
        end,

        -- Delegate metamethods to class table
        __tostring = function(self)
            if class.__tostring then
                return class.__tostring(self)
            end
            return string.format("Entity<%s>#%d", class._type_name, self._id)
        end,

        __eq = function(a, b)
            if class.__eq then
                return class.__eq(a, b)
            end
            return rawequal(a, b)
        end,

        __lt = function(a, b)
            if class.__lt then
                return class.__lt(a, b)
            end
            return false
        end,

        __le = function(a, b)
            if class.__le then
                return class.__le(a, b)
            end
            return rawequal(a, b)
        end,

        __call = function(self, ...)
            if class.__call then
                return class.__call(self, ...)
            end
            error("Entity is not callable")
        end,

        __len = function(self)
            if class.__len then
                return class.__len(self)
            end
            return 0
        end,
    }

    -- Store metadata on class
    class._type_name = type_name
    class._instance_mt = instance_mt

    -- Constructor - creates new entity instance
    -- actual_type is the graph type name (may differ from class._type_name for default entities)
    function class.new(db, id, props, actual_type)
        local self = setmetatable({}, instance_mt)
        self._db = db
        self._graph = db._graph
        self._id = id
        self._props = props or {}
        self._signals = {}
        self._edge_collections = {}
        self._deleted = false
        self._watch_unsubs = nil
        self._edge_subscribers = {}  -- edge_name -> {callbacks}
        self._actual_type = actual_type  -- Store actual graph type
        return self
    end

    -- Base methods - users can override these if needed
    function class:id()
        return self._id
    end

    function class:type()
        return self._actual_type or class._type_name
    end

    function class:isDeleted()
        return self._deleted
    end

    function class:_getSignal(prop_name)
        if not self._signals[prop_name] then
            self._signals[prop_name] = Signal.new(self, prop_name)
        end
        return self._signals[prop_name]
    end

    function class:_getEdgeCollection(edge_name)
        if not self._edge_collections[edge_name] then
            self._edge_collections[edge_name] = EdgeCollection.new(self, edge_name)
        end
        return self._edge_collections[edge_name]
    end

    function class:_startWatching()
        if self._watch_unsubs then return end

        local entity = self
        self._watch_unsubs = {}

        -- Subscribe to all node events using unified g:on API
        self._watch_unsubs.change = self._graph:on(self._id, "change", function(id, new_props, old_props)
            entity:_handleChange(new_props, old_props)
        end)

        self._watch_unsubs.delete = self._graph:on(self._id, "delete", function(id)
            entity:_handleDelete()
        end)

        self._watch_unsubs.link = self._graph:on(self._id, "link", function(id, edge_name, target_id)
            entity:_handleLink(edge_name, target_id)
        end)

        self._watch_unsubs.unlink = self._graph:on(self._id, "unlink", function(id, edge_name, target_id)
            entity:_handleUnlink(edge_name, target_id)
        end)
    end

    function class:_handleChange(new_props, old_props)
        -- Update internal props
        for k, v in pairs(new_props) do
            self._props[k] = v
        end

        -- Notify signals for changed properties
        for prop_name, signal in pairs(self._signals) do
            local new_val = new_props[prop_name]
            local old_val = old_props[prop_name]
            if new_val ~= old_val then
                signal:_notify(new_val, old_val)
            end
        end
    end

    function class:_handleDelete()
        self._deleted = true
        -- Notify all signals with nil
        for prop_name, signal in pairs(self._signals) do
            signal:_notify(nil, self._props[prop_name])
        end
        -- Remove from cache
        self._db:_evict(self._id)
    end

    function class:_handleLink(edge_name, target_id)
        local subs = self._edge_subscribers[edge_name]
        if subs and subs.on_link then
            for _, cb in ipairs(subs.on_link) do
                local ok, err = pcall(cb, target_id)
                if not ok then
                    print("Edge link callback error:", err)
                end
            end
        end
    end

    function class:_handleUnlink(edge_name, target_id)
        local subs = self._edge_subscribers[edge_name]
        if subs and subs.on_unlink then
            for _, cb in ipairs(subs.on_unlink) do
                local ok, err = pcall(cb, target_id)
                if not ok then
                    print("Edge unlink callback error:", err)
                end
            end
        end
    end

    function class:onLink(edge_name, callback)
        self:_startWatching()

        if not self._edge_subscribers[edge_name] then
            self._edge_subscribers[edge_name] = { on_link = {}, on_unlink = {} }
        end
        table.insert(self._edge_subscribers[edge_name].on_link, callback)

        local subs = self._edge_subscribers[edge_name].on_link
        return function()
            for i, cb in ipairs(subs) do
                if cb == callback then
                    table.remove(subs, i)
                    break
                end
            end
        end
    end

    function class:onUnlink(edge_name, callback)
        self:_startWatching()

        if not self._edge_subscribers[edge_name] then
            self._edge_subscribers[edge_name] = { on_link = {}, on_unlink = {} }
        end
        table.insert(self._edge_subscribers[edge_name].on_unlink, callback)

        local subs = self._edge_subscribers[edge_name].on_unlink
        return function()
            for i, cb in ipairs(subs) do
                if cb == callback then
                    table.remove(subs, i)
                    break
                end
            end
        end
    end

    function class:update(props)
        if self._deleted then
            error("Entity deleted")
        end
        self._graph:update(self._id, props)
    end

    function class:delete()
        if self._deleted then
            error("Entity already deleted")
        end
        self._deleted = true
        self._graph:delete(self._id)
        -- Evict from cache
        self._db:_evict(self._id)
    end

    function class:unwatch()
        if self._watch_unsubs then
            -- Call all unsubscribe functions
            for _, unsub in pairs(self._watch_unsubs) do
                unsub()
            end
            self._watch_unsubs = nil
        end
    end

    return class
end

-- Default entity class (no custom methods)
local DefaultEntity = createEntityClass("_default")

-- ============================================================================
-- Database: Wrapped graph with entity caching
-- ============================================================================

local Database = {}
Database.__index = Database

function Database.new(graph, entity_types)
    local self = setmetatable({}, Database)
    self._graph = graph
    self._entity_types = entity_types or {}
    self._cache = setmetatable({}, { __mode = "v" })  -- Weak value table for GC
    self.graph = graph  -- Expose underlying graph
    return self
end

function Database:_getEntityClass(type_name)
    return self._entity_types[type_name] or DefaultEntity
end

function Database:_evict(id)
    self._cache[id] = nil
end

function Database:get(id)
    -- Check cache first
    if self._cache[id] then
        return self._cache[id]
    end

    -- Fetch node data from graph
    local data = self._graph:get(id)
    if not data then
        return nil
    end

    -- Get entity class based on type
    local type_name = data.type
    local EntityClass = self:_getEntityClass(type_name)

    -- Remove metadata fields before passing to entity
    data.type = nil
    data._id = nil

    local entity = EntityClass.new(self, id, data, type_name)
    self._cache[id] = entity
    return entity
end

function Database:find(type_name, id)
    -- Check cache first
    if self._cache[id] then
        return self._cache[id]
    end

    -- Fetch node data from graph
    local data = self._graph:get(id)
    if not data then
        return nil
    end

    -- Remove metadata fields
    data._type = nil
    data._id = nil

    local EntityClass = self:_getEntityClass(type_name)
    local entity = EntityClass.new(self, id, data, type_name)
    self._cache[id] = entity
    return entity
end

function Database:insert(type_name, props)
    local id = self._graph:insert(type_name, props)

    local EntityClass = self:_getEntityClass(type_name)
    local entity = EntityClass.new(self, id, props or {}, type_name)
    self._cache[id] = entity
    return entity
end

function Database:evict(id)
    self:_evict(id)
end

-- ============================================================================
-- SDK Module Exports
-- ============================================================================

-- Define a custom entity type
-- Usage:
--   local User = sdk.entity("User")
--   function User:greet() return "Hello, " .. self.name:get() end
--   User.__tostring = function(self) return "User#" .. self:id() end
function sdk.entity(type_name)
    return createEntityClass(type_name)
end

-- Wrap a low-level graph with SDK features
-- Adds insert() method to each entity class for convenient creation:
--   local user = User.insert({name = "Alice", age = 30})
function sdk.wrap(graph, entity_types)
    local db = Database.new(graph, entity_types)

    -- Add insert method to each entity class
    if entity_types then
        for type_name, class in pairs(entity_types) do
            class.insert = function(props)
                return db:insert(type_name, props)
            end
        end
    end

    return db
end

-- Export classes for advanced usage
sdk.Signal = Signal
sdk.EdgeCollection = EdgeCollection
sdk.Entity = DefaultEntity
sdk.Database = Database

return sdk
