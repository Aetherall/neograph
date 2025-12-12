---@meta

-- neograph-sdk type annotations for LuaLS

-- ============================================================================
-- Query/View (accessed via db.graph:query())
-- ============================================================================

---@class neograph.ViewItem
---@field id integer Node ID
---@field depth integer Depth in tree (0 = root)
---@field type string Type name
---@field expanded boolean Whether any edge is expanded
---@field expandable boolean Whether node has children
---@field [string] any Property values

---@class neograph.ViewStats
---@field total integer Total number of items
---@field offset integer Current viewport offset
---@field height integer Viewport height

---@class neograph.ViewOptions
---@field limit? integer Viewport size (0 = unlimited)
---@field immediate? boolean Activate immediately

---@class neograph.View
local View = {}

---Get visible items as a table.
---@return neograph.ViewItem[]
function View:items() end

---Get total item count.
---@return integer
function View:total() end

---Get current viewport offset.
---@return integer
function View:offset() end

---Set viewport height.
---@param height integer
function View:set_limit(height) end

---Scroll to absolute position.
---@param index integer
function View:scroll_to(index) end

---Scroll by relative amount.
---@param delta integer
function View:scroll_by(delta) end

---Expand a node's edge.
---@param id integer Node ID
---@param edge string Edge name
function View:expand(id, edge) end

---Collapse a node's edge.
---@param id integer Node ID
---@param edge string Edge name
function View:collapse(id, edge) end

---Toggle a node's edge expansion.
---@param id integer Node ID
---@param edge string Edge name
---@return boolean expanded New expansion state
function View:toggle(id, edge) end

---Check if a node's edge is expanded.
---@param id integer Node ID
---@param edge string Edge name
---@return boolean
function View:is_expanded(id, edge) end

---Expand all nodes up to max depth.
---@param max_depth? integer
function View:expand_all(max_depth) end

---Collapse all expanded nodes.
function View:collapse_all() end

---Get viewport statistics.
---@return neograph.ViewStats
function View:stats() end

---Subscribe to view events.
---@param event "enter"|"leave"|"change"|"move"
---@param callback fun(item: neograph.ViewItem, index: integer, old_item?: neograph.ViewItem)
---@return fun() unsubscribe
function View:on(event, callback) end

---Unsubscribe from view events.
---@param event? "enter"|"leave"|"change"|"move" Specific event or all
function View:off(event) end

-- ============================================================================
-- Graph (accessed via db.graph)
-- ============================================================================

---@class neograph.Graph
local Graph = {}

---Set schema from JSON string or table.
---@param schema string|table
---@return boolean
function Graph:schema(schema) end

---Get a node by ID.
---@param id integer
---@return table|nil Node properties with `type` and `_id` fields
function Graph:get(id) end

---Insert a new node.
---@param type_name string
---@param props? table<string, any>
---@return integer id
function Graph:insert(type_name, props) end

---Update node properties.
---@param id integer
---@param props table<string, any>
---@return boolean
function Graph:update(id, props) end

---Link two nodes via an edge.
---@param src integer Source node ID
---@param edge string Edge name
---@param tgt integer Target node ID
---@return boolean
function Graph:link(src, edge, tgt) end

---Unlink two nodes.
---@param src integer Source node ID
---@param edge string Edge name
---@param tgt integer Target node ID
---@return boolean
function Graph:unlink(src, edge, tgt) end

---Get edge targets for a node.
---@param id integer Node ID
---@param edge string Edge name
---@return integer[]|nil
function Graph:edges(id, edge) end

---Check if an edge exists between two nodes.
---@param src integer
---@param edge string
---@param tgt integer
---@return boolean
function Graph:has_edge(src, edge, tgt) end

---Delete a node.
---@param id integer
---@return boolean
function Graph:delete(id) end

---Get field type for a type's field.
---@param type_name string
---@param field_name string
---@return "edge"|"property"|"rollup"|nil
function Graph:field_type(type_name, field_name) end

---Create a reactive view from a query.
---@param query string|table Query JSON or table
---@param opts? neograph.ViewOptions
---@return neograph.View
function Graph:query(query, opts) end

---Subscribe to node events.
---@param id integer Node ID
---@param event "change"|"delete"|"link"|"unlink"
---@param callback function
---@return fun() unsubscribe
function Graph:on(id, event, callback) end

---Unsubscribe from node events.
---@param id integer Node ID
---@param event? "change"|"delete"|"link"|"unlink" Specific event or all
---@return boolean
function Graph:off(id, event) end

-- ============================================================================
-- Signal
-- ============================================================================

---@class neograph.Signal
---@field private _entity neograph.Entity
---@field private _prop_name string
---@field private _subscribers function[]
local Signal = {}

---Get the current value of the signal.
---@return any
function Signal:get() end

---Subscribe to value changes.
---@param callback fun(new_value: any, old_value: any)
---@return fun() unsubscribe Unsubscribe function
function Signal:onChange(callback) end

---Run an effect immediately and on each change.
---The effect receives the current value and may return a cleanup function.
---@param effect fun(value: any): fun()|nil
---@return fun() unsubscribe Unsubscribe function (also runs cleanup)
function Signal:use(effect) end

---@class neograph.EdgeCollection
---@field private _entity neograph.Entity
---@field private _edge_name string
local EdgeCollection = {}

---Iterate over all linked entities.
---@return fun(): neograph.Entity|nil iterator
function EdgeCollection:iter() end

---Subscribe to entities being linked.
---@param callback fun(entity: neograph.Entity)
---@return fun() unsubscribe
function EdgeCollection:onEnter(callback) end

---Subscribe to entities being unlinked.
---@param callback fun(entity: neograph.Entity)
---@return fun() unsubscribe
function EdgeCollection:onLeave(callback) end

---Link a target entity.
---@param target neograph.Entity|integer Entity or ID
function EdgeCollection:link(target) end

---Unlink a target entity.
---@param target neograph.Entity|integer Entity or ID
function EdgeCollection:unlink(target) end

---Run an effect for each entity (existing and future).
---The effect may return a cleanup function called on leave.
---@param effect fun(entity: neograph.Entity): fun()|nil
---@return fun() unsubscribe Unsubscribes and runs all cleanups
function EdgeCollection:each(effect) end

---@class neograph.Entity
---@field private _id integer
---@field private _db neograph.Database
---@field private _graph neograph.Graph
---@field private _deleted boolean
---@field [string] neograph.Signal|neograph.EdgeCollection Property signals or edge collections
local Entity = {}

---Get the entity ID.
---@return integer
function Entity:id() end

---Get the entity type name.
---@return string
function Entity:type() end

---Check if the entity has been deleted.
---@return boolean
function Entity:isDeleted() end

---Update entity properties.
---@param props table<string, any>
function Entity:update(props) end

---Delete the entity from the graph.
function Entity:delete() end

---Stop watching this entity for changes.
function Entity:unwatch() end

---Subscribe to link events on a specific edge.
---@param edge_name string
---@param callback fun(target_id: integer)
---@return fun() unsubscribe
function Entity:onLink(edge_name, callback) end

---Subscribe to unlink events on a specific edge.
---@param edge_name string
---@param callback fun(target_id: integer)
---@return fun() unsubscribe
function Entity:onUnlink(edge_name, callback) end

---@class neograph.EntityClass
---@field _type_name string
---@field insert fun(props?: table): neograph.Entity Static insert method (added by sdk.wrap)
local EntityClass = {}

---Create a new entity instance (internal).
---@param db neograph.Database
---@param id integer
---@param props? table
---@param actual_type? string
---@return neograph.Entity
function EntityClass.new(db, id, props, actual_type) end

---@class neograph.Database
---@field graph neograph.Graph The underlying low-level graph
---@field private _cache table<integer, neograph.Entity>
---@field private _entity_types table<string, neograph.EntityClass>
local Database = {}

---Get an entity by ID.
---@param id integer
---@return neograph.Entity|nil
function Database:get(id) end

---Find an entity by type and ID.
---@param type_name string
---@param id integer
---@return neograph.Entity|nil
function Database:find(type_name, id) end

---Insert a new entity.
---@param type_name string
---@param props? table<string, any>
---@return neograph.Entity
function Database:insert(type_name, props) end

---Evict an entity from the cache.
---@param id integer
function Database:evict(id) end

---@class neograph.sdk
local sdk = {}

---Define a custom entity class.
---@param type_name string
---@return neograph.EntityClass
function sdk.entity(type_name) end

---Wrap a low-level graph with SDK features.
---@param graph neograph.Graph
---@param entity_types? table<string, neograph.EntityClass>
---@return neograph.Database
function sdk.wrap(graph, entity_types) end

---@type neograph.Signal
sdk.Signal = nil

---@type neograph.EdgeCollection
sdk.EdgeCollection = nil

---@type neograph.EntityClass
sdk.Entity = nil

---@type neograph.Database
sdk.Database = nil

return sdk
