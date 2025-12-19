//! Lua bindings for neograph - thin shim layer over the Zig public API.
//!
//! This module provides Lua/Neovim integration by exposing the neograph API.
//! Schema and query definitions can be passed as JSON strings or Lua tables.

const std = @import("std");
const Allocator = std.mem.Allocator;
const neograph = @import("neograph");
const zlua = @import("zlua");
const Lua = zlua.Lua;

// Use page allocator - Lua's allocator causes segfaults with neograph
const lua_allocator = std.heap.page_allocator;

// ============================================================================
// Graph Context - wraps neograph.Graph
// ============================================================================

const Context = struct {
    graph: ?*neograph.Graph = null,
    /// Map of node ID -> watch handle for unified g:on/g:off API
    node_watches: std.AutoHashMapUnmanaged(neograph.NodeId, *NodeWatchHandle) = .{},
    lua: ?*Lua = null,

    fn init(lua: *Lua) *Context {
        const ctx = lua_allocator.create(Context) catch @panic("OOM");
        ctx.* = .{ .lua = lua };
        return ctx;
    }

    fn deinit(self: *Context) void {
        // Cleanup all node watches
        var iter = self.node_watches.valueIterator();
        while (iter.next()) |handle_ptr| {
            handle_ptr.*.cleanup();
            lua_allocator.destroy(handle_ptr.*);
        }
        self.node_watches.deinit(lua_allocator);

        if (self.graph) |g| g.deinit();
        lua_allocator.destroy(self);
    }

    fn initWithSchema(self: *Context, schema: neograph.Schema) !void {
        self.graph = try neograph.Graph.init(lua_allocator, schema);
    }

    /// Get or create a NodeWatchHandle for a node
    fn getOrCreateWatch(self: *Context, id: neograph.NodeId) !*NodeWatchHandle {
        if (self.node_watches.get(id)) |handle| {
            return handle;
        }

        const graph = self.graph orelse return error.GraphNotInitialized;
        const lua = self.lua orelse return error.LuaNotInitialized;

        // Create new handle
        const handle = try lua_allocator.create(NodeWatchHandle);
        handle.* = .{
            .lua = lua,
            .graph = graph,
            .node_id = id,
        };

        // Register with graph (with bridge callbacks)
        const callbacks = neograph.NodeCallbacks{
            .on_change = NodeWatchHandle.onChangeBridge,
            .on_delete = NodeWatchHandle.onDeleteBridge,
            .on_link = NodeWatchHandle.onLinkBridge,
            .on_unlink = NodeWatchHandle.onUnlinkBridge,
            .context = handle,
        };
        try graph.watchNode(id, callbacks);

        try self.node_watches.put(lua_allocator, id, handle);
        return handle;
    }

    /// Remove watch for a node
    fn removeWatch(self: *Context, id: neograph.NodeId) void {
        if (self.node_watches.fetchRemove(id)) |kv| {
            kv.value.cleanup();
            lua_allocator.destroy(kv.value);
        }
    }
};

// ============================================================================
// Lua Table to JSON Serialization
// ============================================================================

/// Serialize a Lua value at the given stack index to JSON.
/// Returns owned slice that must be freed by caller.
fn luaToJson(lua: *Lua, idx: i32, allocator: Allocator) ![]const u8 {
    var buffer = std.ArrayListUnmanaged(u8){};
    errdefer buffer.deinit(allocator);

    try luaValueToJson(lua, idx, &buffer, allocator);
    return buffer.toOwnedSlice(allocator);
}

const JsonError = error{OutOfMemory};

fn luaValueToJson(lua: *Lua, idx: i32, buffer: *std.ArrayListUnmanaged(u8), allocator: Allocator) JsonError!void {
    const abs_idx = if (idx < 0) @as(i32, @intCast(@as(i64, @intCast(lua.getTop())) + idx + 1)) else idx;

    switch (lua.typeOf(abs_idx)) {
        .nil => try buffer.appendSlice(allocator, "null"),
        .boolean => {
            if (lua.toBoolean(abs_idx)) {
                try buffer.appendSlice(allocator, "true");
            } else {
                try buffer.appendSlice(allocator, "false");
            }
        },
        .number => {
            const num = lua.toNumber(abs_idx) catch 0;
            // Check if it's an integer
            const int_val: i64 = @intFromFloat(num);
            var num_buf: [32]u8 = undefined;
            if (@as(f64, @floatFromInt(int_val)) == num) {
                const slice = std.fmt.bufPrint(&num_buf, "{d}", .{int_val}) catch "0";
                try buffer.appendSlice(allocator, slice);
            } else {
                const slice = std.fmt.bufPrint(&num_buf, "{d}", .{num}) catch "0";
                try buffer.appendSlice(allocator, slice);
            }
        },
        .string => {
            const str = lua.toString(abs_idx) catch "";
            try buffer.append(allocator, '"');
            for (str) |c| {
                switch (c) {
                    '"' => try buffer.appendSlice(allocator, "\\\""),
                    '\\' => try buffer.appendSlice(allocator, "\\\\"),
                    '\n' => try buffer.appendSlice(allocator, "\\n"),
                    '\r' => try buffer.appendSlice(allocator, "\\r"),
                    '\t' => try buffer.appendSlice(allocator, "\\t"),
                    else => try buffer.append(allocator, c),
                }
            }
            try buffer.append(allocator, '"');
        },
        .table => {
            // Determine if array or object by checking keys
            const is_array = luaIsArray(lua, abs_idx);
            if (is_array) {
                try luaArrayToJson(lua, abs_idx, buffer, allocator);
            } else {
                try luaObjectToJson(lua, abs_idx, buffer, allocator);
            }
        },
        else => try buffer.appendSlice(allocator, "null"), // Unsupported types become null
    }
}

/// Check if a Lua table is an array (consecutive integer keys starting at 1)
fn luaIsArray(lua: *Lua, idx: i32) bool {
    // Get the raw length of the table
    const len = lua.rawLen(idx);
    if (len == 0) {
        // Could be empty array or empty object - check if any keys exist
        lua.pushNil();
        if (lua.next(idx)) {
            lua.pop(2); // pop key and value
            return false; // Has keys, treat as object
        }
        return true; // Empty, treat as array
    }

    // Has length, check if all keys are sequential integers
    var count: usize = 0;
    lua.pushNil();
    while (lua.next(idx)) {
        lua.pop(1); // pop value, keep key
        count += 1;
    }
    return count == len;
}

fn luaArrayToJson(lua: *Lua, idx: i32, buffer: *std.ArrayListUnmanaged(u8), allocator: Allocator) JsonError!void {
    try buffer.append(allocator, '[');
    const len = lua.rawLen(idx);
    for (1..len + 1) |i| {
        if (i > 1) try buffer.append(allocator, ',');
        _ = lua.rawGetIndex(idx, @intCast(i));
        try luaValueToJson(lua, -1, buffer, allocator);
        lua.pop(1);
    }
    try buffer.append(allocator, ']');
}

fn luaObjectToJson(lua: *Lua, idx: i32, buffer: *std.ArrayListUnmanaged(u8), allocator: Allocator) JsonError!void {
    try buffer.append(allocator, '{');
    var first = true;

    lua.pushNil();
    while (lua.next(idx)) {
        // key at -2, value at -1
        if (lua.typeOf(-2) == .string) {
            if (!first) try buffer.append(allocator, ',');
            first = false;

            // Write key
            const key = lua.toString(-2) catch "";
            try buffer.append(allocator, '"');
            try buffer.appendSlice(allocator, key);
            try buffer.append(allocator, '"');
            try buffer.append(allocator, ':');

            // Write value
            try luaValueToJson(lua, -1, buffer, allocator);
        }
        lua.pop(1); // pop value, keep key for next iteration
    }
    try buffer.append(allocator, '}');
}

/// Get a string from the Lua stack, converting table to JSON if needed.
/// If the value is a string, returns it directly.
/// If the value is a table, serializes it to JSON and returns owned slice.
/// Returns error if neither string nor table.
fn getStringOrTableAsJson(lua: *Lua, idx: i32, allocator: Allocator) !struct { str: []const u8, owned: bool } {
    switch (lua.typeOf(idx)) {
        .string => {
            const str = lua.toString(idx) catch return error.InvalidType;
            return .{ .str = str, .owned = false };
        },
        .table => {
            const json = try luaToJson(lua, idx, allocator);
            return .{ .str = json, .owned = true };
        },
        else => return error.InvalidType,
    }
}

// ============================================================================
// Query Handle - wraps neograph.View (internal)
// ============================================================================

const TreeHandle = struct {
    view: neograph.View,
    graph: *neograph.Graph,
    lua: *Lua,
    on_enter_ref: ?i32 = null,
    on_leave_ref: ?i32 = null,
    on_change_ref: ?i32 = null,
    on_move_ref: ?i32 = null,

    fn deinit(self: *TreeHandle) void {
        // Release Lua function references
        if (self.on_enter_ref) |ref| self.lua.unref(zlua.registry_index, ref);
        if (self.on_leave_ref) |ref| self.lua.unref(zlua.registry_index, ref);
        if (self.on_change_ref) |ref| self.lua.unref(zlua.registry_index, ref);
        if (self.on_move_ref) |ref| self.lua.unref(zlua.registry_index, ref);

        self.view.deinit();
        lua_allocator.destroy(self);
    }

    /// Bridge callback for on_enter
    fn onEnterBridge(ctx: ?*anyopaque, item: neograph.Item, index: u32) void {
        const self: *TreeHandle = @ptrCast(@alignCast(ctx));
        const ref = self.on_enter_ref orelse return;

        _ = self.lua.rawGetIndex(zlua.registry_index, ref);
        if (!self.lua.isFunction(-1)) {
            self.lua.pop(1);
            return;
        }

        // Push item as table
        pushItem(self.lua, item, self.graph);
        self.lua.pushInteger(@intCast(index));

        self.lua.protectedCall(.{ .args = 2, .results = 0 }) catch {
            self.lua.pop(1);
        };
    }

    /// Bridge callback for on_leave
    fn onLeaveBridge(ctx: ?*anyopaque, item: neograph.Item, index: u32) void {
        const self: *TreeHandle = @ptrCast(@alignCast(ctx));
        const ref = self.on_leave_ref orelse return;

        _ = self.lua.rawGetIndex(zlua.registry_index, ref);
        if (!self.lua.isFunction(-1)) {
            self.lua.pop(1);
            return;
        }

        // Push item as table
        pushItem(self.lua, item, self.graph);
        self.lua.pushInteger(@intCast(index));

        self.lua.protectedCall(.{ .args = 2, .results = 0 }) catch {
            self.lua.pop(1);
        };
    }

    /// Bridge callback for on_change
    fn onChangeBridge(ctx: ?*anyopaque, item: neograph.Item, index: u32, old_item: neograph.Item) void {
        const self: *TreeHandle = @ptrCast(@alignCast(ctx));
        const ref = self.on_change_ref orelse return;

        _ = self.lua.rawGetIndex(zlua.registry_index, ref);
        if (!self.lua.isFunction(-1)) {
            self.lua.pop(1);
            return;
        }

        // Push item as table, index, old_item
        pushItem(self.lua, item, self.graph);
        self.lua.pushInteger(@intCast(index));
        pushItem(self.lua, old_item, self.graph);

        self.lua.protectedCall(.{ .args = 3, .results = 0 }) catch {
            self.lua.pop(1);
        };
    }

    /// Bridge callback for on_move
    fn onMoveBridge(ctx: ?*anyopaque, item: neograph.Item, from: u32, to: u32) void {
        const self: *TreeHandle = @ptrCast(@alignCast(ctx));
        const ref = self.on_move_ref orelse return;

        _ = self.lua.rawGetIndex(zlua.registry_index, ref);
        if (!self.lua.isFunction(-1)) {
            self.lua.pop(1);
            return;
        }

        // Push item as table, new_index, old_index
        pushItem(self.lua, item, self.graph);
        self.lua.pushInteger(@intCast(to));
        self.lua.pushInteger(@intCast(from));

        self.lua.protectedCall(.{ .args = 3, .results = 0 }) catch {
            self.lua.pop(1);
        };
    }
};

/// Push an Item as a Lua table
fn pushItem(lua: *Lua, item: neograph.Item, graph: *neograph.Graph) void {
    lua.createTable(0, 6);

    _ = lua.pushString("id");
    lua.pushInteger(@intCast(item.id));
    lua.setTable(-3);

    _ = lua.pushString("depth");
    lua.pushInteger(@intCast(item.depth));
    lua.setTable(-3);

    // Get type name
    if (graph.schema.getTypeName(item.type_id)) |type_name| {
        _ = lua.pushString("type");
        _ = lua.pushString(type_name);
        lua.setTable(-3);
    }

    // Add fields (properties)
    var iter = item.fields.iterator();
    while (iter.next()) |entry| {
        _ = lua.pushString(entry.key_ptr.*);
        pushValue(lua, entry.value_ptr.*);
        lua.setTable(-3);
    }
}

// ============================================================================
// Node Watch Handle - stores Lua callback references for node watching
// ============================================================================

const NodeWatchHandle = struct {
    lua: *Lua,
    graph: *neograph.Graph,
    node_id: neograph.NodeId,
    on_change_ref: ?i32 = null,
    on_delete_ref: ?i32 = null,
    on_link_ref: ?i32 = null,
    on_unlink_ref: ?i32 = null,
    active: bool = true,

    fn cleanup(self: *NodeWatchHandle) void {
        // Guard against double-cleanup (can happen if unwatch() called before GC)
        if (!self.active) return;
        self.active = false;

        // Unregister from graph first (prevents callbacks from firing)
        self.graph.unwatchNode(self.node_id);

        // Release Lua function references
        if (self.on_change_ref) |ref| self.lua.unref(zlua.registry_index, ref);
        if (self.on_delete_ref) |ref| self.lua.unref(zlua.registry_index, ref);
        if (self.on_link_ref) |ref| self.lua.unref(zlua.registry_index, ref);
        if (self.on_unlink_ref) |ref| self.lua.unref(zlua.registry_index, ref);

        // Note: memory is managed by Lua's userdata GC, no need to free
    }

    /// Bridge callback for on_change
    fn onChangeBridge(ctx: ?*anyopaque, id: neograph.NodeId, node: *const neograph.Node, old_node: *const neograph.Node) void {
        const self: *NodeWatchHandle = @ptrCast(@alignCast(ctx));
        if (!self.active) return;
        const ref = self.on_change_ref orelse return;

        // Push callback function from registry
        _ = self.lua.rawGetIndex(zlua.registry_index, ref);
        if (!self.lua.isFunction(-1)) {
            self.lua.pop(1);
            return;
        }

        // Push arguments: id, current_props, old_props
        self.lua.pushInteger(@intCast(id));
        pushNodeProperties(self.lua, node);
        pushNodeProperties(self.lua, old_node);

        // Call with 3 arguments, 0 returns
        self.lua.protectedCall(.{ .args = 3, .results = 0 }) catch {
            self.lua.pop(1); // Pop error message
        };
    }

    /// Bridge callback for on_delete
    fn onDeleteBridge(ctx: ?*anyopaque, id: neograph.NodeId) void {
        const self: *NodeWatchHandle = @ptrCast(@alignCast(ctx));
        if (!self.active) return;
        const ref = self.on_delete_ref orelse return;

        _ = self.lua.rawGetIndex(zlua.registry_index, ref);
        if (!self.lua.isFunction(-1)) {
            self.lua.pop(1);
            return;
        }

        self.lua.pushInteger(@intCast(id));

        self.lua.protectedCall(.{ .args = 1, .results = 0 }) catch {
            self.lua.pop(1);
        };
    }

    /// Bridge callback for on_link
    fn onLinkBridge(ctx: ?*anyopaque, id: neograph.NodeId, edge_name: []const u8, target: neograph.NodeId) void {
        const self: *NodeWatchHandle = @ptrCast(@alignCast(ctx));
        if (!self.active) return;
        const ref = self.on_link_ref orelse return;

        _ = self.lua.rawGetIndex(zlua.registry_index, ref);
        if (!self.lua.isFunction(-1)) {
            self.lua.pop(1);
            return;
        }

        self.lua.pushInteger(@intCast(id));
        _ = self.lua.pushString(edge_name);
        self.lua.pushInteger(@intCast(target));

        self.lua.protectedCall(.{ .args = 3, .results = 0 }) catch {
            self.lua.pop(1);
        };
    }

    /// Bridge callback for on_unlink
    fn onUnlinkBridge(ctx: ?*anyopaque, id: neograph.NodeId, edge_name: []const u8, target: neograph.NodeId) void {
        const self: *NodeWatchHandle = @ptrCast(@alignCast(ctx));
        if (!self.active) return;
        const ref = self.on_unlink_ref orelse return;

        _ = self.lua.rawGetIndex(zlua.registry_index, ref);
        if (!self.lua.isFunction(-1)) {
            self.lua.pop(1);
            return;
        }

        self.lua.pushInteger(@intCast(id));
        _ = self.lua.pushString(edge_name);
        self.lua.pushInteger(@intCast(target));

        self.lua.protectedCall(.{ .args = 3, .results = 0 }) catch {
            self.lua.pop(1);
        };
    }
};

/// Push node properties as a Lua table
fn pushNodeProperties(lua: *Lua, node: *const neograph.Node) void {
    const total_count = node.properties.count() + node.rollup_values.count();
    lua.createTable(0, @intCast(total_count));

    // Push properties
    var prop_iter = node.properties.iterator();
    while (prop_iter.next()) |entry| {
        _ = lua.pushString(entry.key_ptr.*);
        pushValue(lua, entry.value_ptr.*);
        lua.setTable(-3);
    }

    // Push rollup values
    var rollup_iter = node.rollup_values.iterator();
    while (rollup_iter.next()) |entry| {
        _ = lua.pushString(entry.key_ptr.*);
        pushValue(lua, entry.value_ptr.*);
        lua.setTable(-3);
    }
}

// ============================================================================
// String Interning - for Lua string lifetime management
// ============================================================================

const StringInterner = struct {
    strings: std.StringHashMapUnmanaged(void),
    allocator: Allocator,

    fn init(allocator: Allocator) StringInterner {
        return .{
            .strings = .{},
            .allocator = allocator,
        };
    }

    fn deinit(self: *StringInterner) void {
        var iter = self.strings.keyIterator();
        while (iter.next()) |key| {
            self.allocator.free(key.*);
        }
        self.strings.deinit(self.allocator);
    }

    fn intern(self: *StringInterner, str: []const u8) ![]const u8 {
        if (self.strings.getKey(str)) |existing| {
            return existing;
        }
        const owned = try self.allocator.dupe(u8, str);
        try self.strings.put(self.allocator, owned, {});
        return owned;
    }
};

// Global string interner for property names
var global_interner: ?StringInterner = null;

fn getInterner() *StringInterner {
    if (global_interner == null) {
        global_interner = StringInterner.init(lua_allocator);
    }
    return &global_interner.?;
}

// ============================================================================
// Lua API Functions
// ============================================================================

/// Create a new graph context, optionally with a schema JSON string.
/// Usage: local g = neograph.graph()  or  neograph.graph(schema_json)
fn graphNew(lua: *Lua) i32 {
    const ctx = Context.init(lua);

    const ptr = lua.newUserdata(*Context);
    ptr.* = ctx;

    const mt_type = lua.getMetatableRegistry("neograph.Context");
    if (mt_type == .nil) {
        lua.pop(1);
        lua.pushNil();
        _ = lua.pushString("Metatable not found");
        return 2;
    }
    lua.setMetatable(-2);

    // If a schema was passed (string or table), parse and init
    if (lua.isString(1) or lua.isTable(1)) {
        const json_result = getStringOrTableAsJson(lua, 1, lua_allocator) catch
            return luaErr(lua, "Schema must be a JSON string or table");
        defer if (json_result.owned) lua_allocator.free(@constCast(json_result.str));

        const schema = neograph.parseSchema(lua_allocator, json_result.str) catch |err| {
            const msg: [:0]const u8 = switch (err) {
                error.InvalidJson => "Invalid JSON syntax",
                error.InvalidPropertyType => "Invalid property type",
                error.MissingReverseEdge => "Missing reverse edge",
                error.InvalidRollupDefinition => "Invalid rollup definition",
                else => "Schema parsing failed",
            };
            return luaErr(lua, msg);
        };
        ctx.initWithSchema(schema) catch return luaErr(lua, "Failed to init graph");
    }

    return 1;
}

/// Set schema from JSON string or Lua table.
/// Usage: g:schema(json_string) or g:schema({ types = {...} })
fn graphSchema(lua: *Lua) i32 {
    const ctx = lua.toUserdata(*Context, 1) catch return luaErr(lua, "Invalid context");

    const json_result = getStringOrTableAsJson(lua, 2, lua_allocator) catch
        return luaErr(lua, "Schema must be a JSON string or table");
    defer if (json_result.owned) lua_allocator.free(@constCast(json_result.str));

    const schema = neograph.parseSchema(lua_allocator, json_result.str) catch |err| {
        const msg: [:0]const u8 = switch (err) {
            error.InvalidJson => "Invalid JSON syntax",
            error.InvalidPropertyType => "Invalid property type",
            error.MissingReverseEdge => "Missing reverse edge",
            error.InvalidRollupDefinition => "Invalid rollup definition",
            else => "Schema parsing failed",
        };
        return luaErr(lua, msg);
    };
    ctx.*.initWithSchema(schema) catch return luaErr(lua, "Failed to init graph");

    lua.pushBoolean(true);
    return 1;
}

/// Get a node by ID.
/// Usage: local data = g:get(id)
/// Returns nil if node doesn't exist, otherwise a table with properties and type field.
fn graphGet(lua: *Lua) i32 {
    const ctx = lua.toUserdata(*Context, 1) catch return luaErr(lua, "Invalid context");
    const graph = ctx.*.graph orelse return luaErr(lua, "Graph not initialized");
    const id: neograph.NodeId = @intCast(lua.toInteger(2) catch return luaErr(lua, "ID required"));

    const node = graph.get(id) orelse {
        lua.pushNil();
        return 1;
    };

    // Create result table with properties
    pushNodeProperties(lua, node);

    // Add type field
    if (graph.getTypeName(id)) |type_name| {
        _ = lua.pushString("type");
        _ = lua.pushString(type_name);
        lua.setTable(-3);
    }

    // Add _id field
    _ = lua.pushString("_id");
    lua.pushInteger(@intCast(id));
    lua.setTable(-3);

    return 1;
}

/// Insert a new node.
/// Usage: local id = g:insert("TypeName")  or  g:insert("TypeName", {prop = val})
fn graphInsert(lua: *Lua) i32 {
    const ctx = lua.toUserdata(*Context, 1) catch return luaErr(lua, "Invalid context");
    const graph = ctx.*.graph orelse return luaErr(lua, "Graph not initialized");
    const type_name = lua.toString(2) catch return luaErr(lua, "Type name required");

    const id = graph.insert(type_name) catch return luaErr(lua, "Insert failed");

    // If properties table is provided, update the node
    if (lua.isTable(3)) {
        updateFromTable(graph, id, lua, 3) catch return luaErr(lua, "Update failed");
    }

    lua.pushInteger(@intCast(id));
    return 1;
}

/// Update node properties from a Lua table using graph.setProperty().
fn updateFromTable(graph: *neograph.Graph, id: neograph.NodeId, lua: *Lua, table_idx: i32) !void {
    // Iterate properties table and call graph.setProperty for each
    lua.pushNil();
    while (lua.next(table_idx)) {
        const lua_key = lua.toString(-2) catch {
            lua.pop(1);
            continue;
        };
        // Intern the key so it outlives Lua's stack
        const key = getInterner().intern(lua_key) catch {
            lua.pop(1);
            continue;
        };
        const val = luaToValueInterned(lua, -1);

        // Use public API - properly notifies indexes and reactive subscriptions
        graph.setProperty(id, key, val) catch {};
        lua.pop(1);
    }
}

/// Update node properties.
/// Usage: g:update(id, {prop = val})
fn graphUpdate(lua: *Lua) i32 {
    const ctx = lua.toUserdata(*Context, 1) catch return luaErr(lua, "Invalid context");
    const graph = ctx.*.graph orelse return luaErr(lua, "Graph not initialized");
    const id: neograph.NodeId = @intCast(lua.toInteger(2) catch return luaErr(lua, "ID required"));
    if (!lua.isTable(3)) return luaErr(lua, "Properties table required");

    // Use graph.setProperty for each property - properly notifies reactive subscriptions
    lua.pushNil();
    while (lua.next(3)) {
        const lua_key = lua.toString(-2) catch {
            lua.pop(1);
            continue;
        };
        const key = getInterner().intern(lua_key) catch {
            lua.pop(1);
            continue;
        };
        const val = luaToValueInterned(lua, -1);

        graph.setProperty(id, key, val) catch {};
        lua.pop(1);
    }

    lua.pushBoolean(true);
    return 1;
}

/// Link two nodes via an edge.
/// Usage: g:link(src_id, "edge_name", tgt_id)
fn graphLink(lua: *Lua) i32 {
    const ctx = lua.toUserdata(*Context, 1) catch return luaErr(lua, "Invalid context");
    const graph = ctx.*.graph orelse return luaErr(lua, "Graph not initialized");
    const src: neograph.NodeId = @intCast(lua.toInteger(2) catch return luaErr(lua, "Source ID required"));
    const edge = lua.toString(3) catch return luaErr(lua, "Edge name required");
    const tgt: neograph.NodeId = @intCast(lua.toInteger(4) catch return luaErr(lua, "Target ID required"));

    graph.link(src, edge, tgt) catch return luaErr(lua, "Link failed");
    lua.pushBoolean(true);
    return 1;
}

/// Unlink two nodes.
/// Usage: g:unlink(src_id, "edge_name", tgt_id)
fn graphUnlink(lua: *Lua) i32 {
    const ctx = lua.toUserdata(*Context, 1) catch return luaErr(lua, "Invalid context");
    const graph = ctx.*.graph orelse return luaErr(lua, "Graph not initialized");
    const src: neograph.NodeId = @intCast(lua.toInteger(2) catch return luaErr(lua, "Source ID required"));
    const edge = lua.toString(3) catch return luaErr(lua, "Edge name required");
    const tgt: neograph.NodeId = @intCast(lua.toInteger(4) catch return luaErr(lua, "Target ID required"));

    graph.unlink(src, edge, tgt) catch return luaErr(lua, "Unlink failed");
    lua.pushBoolean(true);
    return 1;
}

/// Get edge targets for a node.
/// Usage: g:edges(id, "edge_name") -> {target_ids} or nil
fn graphEdges(lua: *Lua) i32 {
    const ctx = lua.toUserdata(*Context, 1) catch return luaErr(lua, "Invalid context");
    const graph = ctx.*.graph orelse return luaErr(lua, "Graph not initialized");
    const id: neograph.NodeId = @intCast(lua.toInteger(2) catch return luaErr(lua, "Node ID required"));
    const edge = lua.toString(3) catch return luaErr(lua, "Edge name required");

    const targets = graph.getEdgeTargets(id, edge) orelse {
        lua.pushNil();
        return 1;
    };

    // Create Lua table with targets
    lua.createTable(@intCast(targets.len), 0);
    for (targets, 1..) |target_id, i| {
        lua.pushInteger(@intCast(target_id));
        lua.rawSetIndex(-2, @intCast(i));
    }
    return 1;
}

/// Check if an edge exists between two nodes.
/// Usage: g:has_edge(src_id, "edge_name", tgt_id) -> boolean
fn graphHasEdge(lua: *Lua) i32 {
    const ctx = lua.toUserdata(*Context, 1) catch return luaErr(lua, "Invalid context");
    const graph = ctx.*.graph orelse return luaErr(lua, "Graph not initialized");
    const src: neograph.NodeId = @intCast(lua.toInteger(2) catch return luaErr(lua, "Source ID required"));
    const edge = lua.toString(3) catch return luaErr(lua, "Edge name required");
    const tgt: neograph.NodeId = @intCast(lua.toInteger(4) catch return luaErr(lua, "Target ID required"));

    lua.pushBoolean(graph.hasEdge(src, edge, tgt));
    return 1;
}

/// Delete a node.
/// Usage: g:delete(id)
fn graphDelete(lua: *Lua) i32 {
    const ctx = lua.toUserdata(*Context, 1) catch return luaErr(lua, "Invalid context");
    const graph = ctx.*.graph orelse return luaErr(lua, "Graph not initialized");
    const id: neograph.NodeId = @intCast(lua.toInteger(2) catch return luaErr(lua, "ID required"));

    graph.delete(id) catch return luaErr(lua, "Delete failed");
    lua.pushBoolean(true);
    return 1;
}

/// Get the field type for a type's field.
/// Usage: g:field_type("User", "posts") -> "edge" | "property" | "rollup" | nil
fn graphFieldType(lua: *Lua) i32 {
    const ctx = lua.toUserdata(*Context, 1) catch return luaErr(lua, "Invalid context");
    const graph = ctx.*.graph orelse return luaErr(lua, "Graph not initialized");
    const type_name = lua.toString(2) catch return luaErr(lua, "Type name required");
    const field_name = lua.toString(3) catch return luaErr(lua, "Field name required");

    const type_def = graph.schema.getType(type_name) orelse {
        lua.pushNil();
        return 1;
    };

    if (type_def.getProperty(field_name) != null) {
        _ = lua.pushString("property");
        return 1;
    }

    if (type_def.getEdge(field_name) != null) {
        _ = lua.pushString("edge");
        return 1;
    }

    if (type_def.getRollup(field_name) != null) {
        _ = lua.pushString("rollup");
        return 1;
    }

    lua.pushNil();
    return 1;
}

/// Event type enum for unified node event API
const NodeEventType = enum { change, delete, link, unlink };

fn parseNodeEventType(name: []const u8) ?NodeEventType {
    if (std.mem.eql(u8, name, "change")) return .change;
    if (std.mem.eql(u8, name, "delete")) return .delete;
    if (std.mem.eql(u8, name, "link")) return .link;
    if (std.mem.eql(u8, name, "unlink")) return .unlink;
    return null;
}

/// Unified event subscription for nodes.
/// Usage: local unsub = g:on(id, "change", function(id, node, old_node) ... end)
/// Returns an unsubscribe function.
fn graphOn(lua: *Lua) i32 {
    const ctx = lua.toUserdata(*Context, 1) catch return luaErr(lua, "Invalid context");
    const id: neograph.NodeId = @intCast(lua.toInteger(2) catch return luaErr(lua, "Node ID required"));
    const event_name = lua.toString(3) catch return luaErr(lua, "Event name required");
    if (!lua.isFunction(4)) return luaErr(lua, "Callback function required");

    const event_type = parseNodeEventType(event_name) orelse
        return luaErr(lua, "Unknown event type. Use: change, delete, link, unlink");

    // Get or create watch handle for this node
    const handle = ctx.*.getOrCreateWatch(id) catch return luaErr(lua, "Failed to create watch");

    // Store the callback ref
    lua.pushValue(4);
    const callback_ref = lua.ref(zlua.registry_index) catch return luaErr(lua, "Failed to store callback");

    // Set up the callback based on event type
    switch (event_type) {
        .change => {
            if (handle.on_change_ref) |old_ref| handle.lua.unref(zlua.registry_index, old_ref);
            handle.on_change_ref = callback_ref;
        },
        .delete => {
            if (handle.on_delete_ref) |old_ref| handle.lua.unref(zlua.registry_index, old_ref);
            handle.on_delete_ref = callback_ref;
        },
        .link => {
            if (handle.on_link_ref) |old_ref| handle.lua.unref(zlua.registry_index, old_ref);
            handle.on_link_ref = callback_ref;
        },
        .unlink => {
            if (handle.on_unlink_ref) |old_ref| handle.lua.unref(zlua.registry_index, old_ref);
            handle.on_unlink_ref = callback_ref;
        },
    }

    // Create unsubscribe closure with upvalues: context userdata, node id, event type
    lua.pushValue(1); // Push context (upvalue 1)
    lua.pushInteger(@intCast(id)); // Push node id (upvalue 2)
    lua.pushInteger(@intFromEnum(event_type)); // Push event type (upvalue 3)
    lua.pushClosure(zlua.wrap(graphNodeUnsubscribe), 3);

    return 1;
}

/// Unsubscribe function (returned by g:on)
fn graphNodeUnsubscribe(lua: *Lua) i32 {
    // Get upvalues
    const ctx = lua.toUserdata(*Context, Lua.upvalueIndex(1)) catch return 0;
    const id: neograph.NodeId = @intCast(lua.toInteger(Lua.upvalueIndex(2)) catch return 0);
    const event_idx = lua.toInteger(Lua.upvalueIndex(3)) catch return 0;
    const event_type: NodeEventType = @enumFromInt(event_idx);

    // Find the handle for this node
    const handle = ctx.*.node_watches.get(id) orelse return 0;

    // Clear the callback
    switch (event_type) {
        .change => {
            if (handle.on_change_ref) |ref| {
                handle.lua.unref(zlua.registry_index, ref);
                handle.on_change_ref = null;
            }
        },
        .delete => {
            if (handle.on_delete_ref) |ref| {
                handle.lua.unref(zlua.registry_index, ref);
                handle.on_delete_ref = null;
            }
        },
        .link => {
            if (handle.on_link_ref) |ref| {
                handle.lua.unref(zlua.registry_index, ref);
                handle.on_link_ref = null;
            }
        },
        .unlink => {
            if (handle.on_unlink_ref) |ref| {
                handle.lua.unref(zlua.registry_index, ref);
                handle.on_unlink_ref = null;
            }
        },
    }

    // If all callbacks are cleared, remove the watch entirely
    if (handle.on_change_ref == null and handle.on_delete_ref == null and
        handle.on_link_ref == null and handle.on_unlink_ref == null)
    {
        ctx.*.removeWatch(id);
    }

    return 0;
}

/// Unsubscribe all listeners for a node (or specific event type).
/// Usage: g:off(id) or g:off(id, "change")
fn graphOff(lua: *Lua) i32 {
    const ctx = lua.toUserdata(*Context, 1) catch return luaErr(lua, "Invalid context");
    const id: neograph.NodeId = @intCast(lua.toInteger(2) catch return luaErr(lua, "Node ID required"));

    // Find the handle for this node
    const handle = ctx.*.node_watches.get(id) orelse {
        lua.pushBoolean(true);
        return 1;
    };

    // If event name provided, only clear that one
    if (lua.isString(3)) {
        const event_name = lua.toString(3) catch return luaErr(lua, "Invalid event name");
        const event_type = parseNodeEventType(event_name) orelse
            return luaErr(lua, "Unknown event type. Use: change, delete, link, unlink");

        switch (event_type) {
            .change => {
                if (handle.on_change_ref) |ref| {
                    handle.lua.unref(zlua.registry_index, ref);
                    handle.on_change_ref = null;
                }
            },
            .delete => {
                if (handle.on_delete_ref) |ref| {
                    handle.lua.unref(zlua.registry_index, ref);
                    handle.on_delete_ref = null;
                }
            },
            .link => {
                if (handle.on_link_ref) |ref| {
                    handle.lua.unref(zlua.registry_index, ref);
                    handle.on_link_ref = null;
                }
            },
            .unlink => {
                if (handle.on_unlink_ref) |ref| {
                    handle.lua.unref(zlua.registry_index, ref);
                    handle.on_unlink_ref = null;
                }
            },
        }

        // If all callbacks are cleared, remove the watch entirely
        if (handle.on_change_ref == null and handle.on_delete_ref == null and
            handle.on_link_ref == null and handle.on_unlink_ref == null)
        {
            ctx.*.removeWatch(id);
        }
    } else {
        // Clear all callbacks and remove watch
        ctx.*.removeWatch(id);
    }

    lua.pushBoolean(true);
    return 1;
}

/// Create a reactive view from a query definition.
/// Usage: local v = g:view(query, {limit = 30, immediate = false})
/// Query can be a JSON string or Lua table.
fn graphView(lua: *Lua) i32 {
    const ctx = lua.toUserdata(*Context, 1) catch return luaErr(lua, "Invalid context");
    const graph = ctx.*.graph orelse return luaErr(lua, "Graph not initialized");

    const json_result = getStringOrTableAsJson(lua, 2, lua_allocator) catch
        return luaErr(lua, "Query must be a JSON string or table");
    defer if (json_result.owned) lua_allocator.free(@constCast(json_result.str));

    // Parse options from table (arg 3)
    var limit: u32 = 0;
    var immediate: bool = false;

    if (lua.isTable(3)) {
        // Get limit
        _ = lua.getField(3, "limit");
        if (lua.isNumber(-1)) {
            limit = @intCast(lua.toInteger(-1) catch 0);
        }
        lua.pop(1);

        // Get immediate
        _ = lua.getField(3, "immediate");
        if (lua.isBoolean(-1)) {
            immediate = lua.toBoolean(-1);
        }
        lua.pop(1);
    }

    // Create view through public API
    const view = graph.viewFromJson(json_result.str, .{ .limit = limit }) catch |err| {
        const msg: [:0]const u8 = switch (err) {
            error.TypeNotFound => "Unknown node type in query",
            error.NoIndexCoverage => "No index covers this query",
            error.InvalidJson => "Invalid query JSON",
            error.OutOfMemory => "Out of memory",
            else => "View creation failed",
        };
        return luaErr(lua, msg);
    };

    // IMPORTANT: Create handle FIRST, copy view into it, THEN activate.
    // activate() stores a pointer to the View in subscription callbacks.
    // If we activate before copying, the pointer points to the local `view`
    // variable which becomes invalid when this function returns.
    const handle = lua_allocator.create(TreeHandle) catch return luaErr(lua, "Out of memory");
    handle.* = .{ .view = view, .graph = graph, .lua = lua };

    // Now activate on the handle's view (at its final memory location)
    handle.view.activate(immediate);

    const ud = lua.newUserdata(*TreeHandle);
    ud.* = handle;
    _ = lua.getMetatableRegistry("neograph.Query");
    lua.setMetatable(-2);
    return 1;
}

fn graphGc(lua: *Lua) i32 {
    const ctx = lua.toUserdata(*Context, 1) catch return 0;
    ctx.*.deinit();
    return 0;
}

// ============================================================================
// Tree methods
// ============================================================================

fn treeSetViewport(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid tree");
    h.*.view.setHeight(@intCast(lua.toInteger(2) catch 30));
    lua.pushBoolean(true);
    return 1;
}

fn treeScrollTo(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid tree");
    h.*.view.scrollTo(@intCast(lua.toInteger(2) catch 0));
    lua.pushBoolean(true);
    return 1;
}

fn treeScrollBy(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid tree");
    h.*.view.move(@intCast(lua.toInteger(2) catch 0));
    lua.pushBoolean(true);
    return 1;
}

fn treeExpand(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid tree");
    const id: neograph.NodeId = @intCast(lua.toInteger(2) catch return luaErr(lua, "Node ID required"));
    const edge = lua.toString(3) catch return luaErr(lua, "Edge name required");
    h.*.view.expandById(id, edge) catch return luaErr(lua, "Expand failed");
    lua.pushBoolean(true);
    return 1;
}

fn treeCollapse(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid tree");
    const id: neograph.NodeId = @intCast(lua.toInteger(2) catch return luaErr(lua, "Node ID required"));
    const edge = lua.toString(3) catch return luaErr(lua, "Edge name required");

    h.*.view.collapseById(id, edge);
    lua.pushBoolean(true);
    return 1;
}

fn treeToggle(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid tree");
    const id: neograph.NodeId = @intCast(lua.toInteger(2) catch return luaErr(lua, "Node ID required"));
    const edge = lua.toString(3) catch return luaErr(lua, "Edge name required");
    const expanded = h.*.view.toggleById(id, edge) catch return luaErr(lua, "Toggle failed");
    lua.pushBoolean(expanded);
    return 1;
}

/// Expand all nodes up to a maximum depth.
/// Usage: query:expand_all() or query:expand_all(2)
fn queryExpandAll(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid query");

    // Get optional max_depth argument
    var max_depth: ?u32 = null;
    if (lua.isNumber(2)) {
        const depth = lua.toInteger(2) catch 0;
        if (depth > 0) {
            max_depth = @intCast(depth);
        }
    }

    h.*.view.expandAll(max_depth) catch return luaErr(lua, "Expand all failed");
    lua.pushBoolean(true);
    return 1;
}

/// Collapse all expanded nodes.
/// Usage: query:collapse_all()
fn queryCollapseAll(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid query");
    h.*.view.collapseAll();
    lua.pushBoolean(true);
    return 1;
}

fn treeGetVisible(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid tree");

    // Use actual visible count, not viewport height (which could be maxInt)
    lua.createTable(@intCast(h.*.view.total()), 0);

    var iter = h.*.view.items();
    var i: i32 = 1;
    while (iter.next()) |item| {
        lua.createTable(0, 7);

        _ = lua.pushString("id");
        lua.pushInteger(@intCast(item.id));
        lua.setTable(-3);

        _ = lua.pushString("depth");
        lua.pushInteger(@intCast(item.depth));
        lua.setTable(-3);

        _ = lua.pushString("expanded");
        lua.pushBoolean(item.expanded_edges.len > 0);
        lua.setTable(-3);

        _ = lua.pushString("expandable");
        lua.pushBoolean(item.has_children);
        lua.setTable(-3);

        // Get type name through public API
        if (h.*.graph.getTypeName(item.id)) |type_name| {
            _ = lua.pushString("type");
            _ = lua.pushString(type_name);
            lua.setTable(-3);
        }

        // Get properties through public API
        if (h.*.graph.get(item.id)) |node| {
            var prop_iter = node.properties.iterator();
            while (prop_iter.next()) |entry| {
                _ = lua.pushString(entry.key_ptr.*);
                pushValue(lua, entry.value_ptr.*);
                lua.setTable(-3);
            }
        }

        lua.rawSetIndex(-2, i);
        i += 1;
    }
    return 1;
}

fn treeStats(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid tree");
    lua.createTable(0, 3);

    _ = lua.pushString("total");
    lua.pushInteger(@intCast(h.*.view.total()));
    lua.setTable(-3);

    _ = lua.pushString("offset");
    lua.pushInteger(@intCast(h.*.view.getOffset()));
    lua.setTable(-3);

    _ = lua.pushString("height");
    lua.pushInteger(@intCast(h.*.view.getHeight()));
    lua.setTable(-3);

    return 1;
}

/// Get total item count.
/// Usage: query:total() -> integer
fn queryTotal(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid query");
    lua.pushInteger(@intCast(h.*.view.total()));
    return 1;
}

/// Get current viewport offset.
/// Usage: query:offset() -> integer
fn queryOffset(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid query");
    lua.pushInteger(@intCast(h.*.view.getOffset()));
    return 1;
}

/// Check if a node's edge is expanded.
/// Usage: query:is_expanded(id, "edge") -> boolean
fn queryIsExpanded(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid query");
    const id: neograph.NodeId = @intCast(lua.toInteger(2) catch return luaErr(lua, "Node ID required"));
    const edge = lua.toString(3) catch return luaErr(lua, "Edge name required");
    lua.pushBoolean(h.*.view.isExpandedById(id, edge));
    return 1;
}

/// Event type enum for unified event API
const QueryEventType = enum { enter, leave, change, move };

fn parseEventType(name: []const u8) ?QueryEventType {
    if (std.mem.eql(u8, name, "enter")) return .enter;
    if (std.mem.eql(u8, name, "leave")) return .leave;
    if (std.mem.eql(u8, name, "change")) return .change;
    if (std.mem.eql(u8, name, "move")) return .move;
    return null;
}

/// Unified event subscription for queries.
/// Usage: local unsub = query:on("enter", function(item, index) ... end)
/// Returns an unsubscribe function.
fn queryOn(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid query");
    const event_name = lua.toString(2) catch return luaErr(lua, "Event name required");
    if (!lua.isFunction(3)) return luaErr(lua, "Callback function required");

    const event_type = parseEventType(event_name) orelse
        return luaErr(lua, "Unknown event type. Use: enter, leave, change, move");

    // Store the callback ref
    lua.pushValue(3);
    const callback_ref = lua.ref(zlua.registry_index) catch return luaErr(lua, "Failed to store callback");

    // Set up the callback based on event type
    switch (event_type) {
        .enter => {
            // Release old ref if exists
            if (h.*.on_enter_ref) |old_ref| h.*.lua.unref(zlua.registry_index, old_ref);
            h.*.on_enter_ref = callback_ref;
            h.*.view.onEnter(TreeHandle.onEnterBridge, h.*);
        },
        .leave => {
            if (h.*.on_leave_ref) |old_ref| h.*.lua.unref(zlua.registry_index, old_ref);
            h.*.on_leave_ref = callback_ref;
            h.*.view.onLeave(TreeHandle.onLeaveBridge, h.*);
        },
        .change => {
            if (h.*.on_change_ref) |old_ref| h.*.lua.unref(zlua.registry_index, old_ref);
            h.*.on_change_ref = callback_ref;
            h.*.view.onChange(TreeHandle.onChangeBridge, h.*);
        },
        .move => {
            if (h.*.on_move_ref) |old_ref| h.*.lua.unref(zlua.registry_index, old_ref);
            h.*.on_move_ref = callback_ref;
            h.*.view.onMove(TreeHandle.onMoveBridge, h.*);
        },
    }

    // Create unsubscribe closure with upvalues: handle userdata, event type
    lua.pushValue(1); // Push handle (upvalue 1)
    lua.pushInteger(@intFromEnum(event_type)); // Push event type (upvalue 2)
    lua.pushClosure(zlua.wrap(queryUnsubscribe), 2);

    return 1;
}

/// Unsubscribe function (returned by query:on)
fn queryUnsubscribe(lua: *Lua) i32 {
    // Get upvalues
    const h = lua.toUserdata(*TreeHandle, Lua.upvalueIndex(1)) catch return 0;
    const event_idx = lua.toInteger(Lua.upvalueIndex(2)) catch return 0;
    const event_type: QueryEventType = @enumFromInt(event_idx);

    // Clear the callback
    switch (event_type) {
        .enter => {
            if (h.*.on_enter_ref) |ref| {
                h.*.lua.unref(zlua.registry_index, ref);
                h.*.on_enter_ref = null;
                h.*.view.onEnter(null, null);
            }
        },
        .leave => {
            if (h.*.on_leave_ref) |ref| {
                h.*.lua.unref(zlua.registry_index, ref);
                h.*.on_leave_ref = null;
                h.*.view.onLeave(null, null);
            }
        },
        .change => {
            if (h.*.on_change_ref) |ref| {
                h.*.lua.unref(zlua.registry_index, ref);
                h.*.on_change_ref = null;
                h.*.view.onChange(null, null);
            }
        },
        .move => {
            if (h.*.on_move_ref) |ref| {
                h.*.lua.unref(zlua.registry_index, ref);
                h.*.on_move_ref = null;
                h.*.view.onMove(null, null);
            }
        },
    }
    return 0;
}

/// Unsubscribe all listeners (or specific event type).
/// Usage: query:off() or query:off("enter")
fn queryOff(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return luaErr(lua, "Invalid query");

    // If event name provided, only clear that one
    if (lua.isString(2)) {
        const event_name = lua.toString(2) catch return luaErr(lua, "Invalid event name");
        const event_type = parseEventType(event_name) orelse
            return luaErr(lua, "Unknown event type. Use: enter, leave, change, move");

        switch (event_type) {
            .enter => {
                if (h.*.on_enter_ref) |ref| {
                    h.*.lua.unref(zlua.registry_index, ref);
                    h.*.on_enter_ref = null;
                    h.*.view.onEnter(null, null);
                }
            },
            .leave => {
                if (h.*.on_leave_ref) |ref| {
                    h.*.lua.unref(zlua.registry_index, ref);
                    h.*.on_leave_ref = null;
                    h.*.view.onLeave(null, null);
                }
            },
            .change => {
                if (h.*.on_change_ref) |ref| {
                    h.*.lua.unref(zlua.registry_index, ref);
                    h.*.on_change_ref = null;
                    h.*.view.onChange(null, null);
                }
            },
            .move => {
                if (h.*.on_move_ref) |ref| {
                    h.*.lua.unref(zlua.registry_index, ref);
                    h.*.on_move_ref = null;
                    h.*.view.onMove(null, null);
                }
            },
        }
    } else {
        // Clear all callbacks
        if (h.*.on_enter_ref) |ref| {
            h.*.lua.unref(zlua.registry_index, ref);
            h.*.on_enter_ref = null;
            h.*.view.onEnter(null, null);
        }
        if (h.*.on_leave_ref) |ref| {
            h.*.lua.unref(zlua.registry_index, ref);
            h.*.on_leave_ref = null;
            h.*.view.onLeave(null, null);
        }
        if (h.*.on_change_ref) |ref| {
            h.*.lua.unref(zlua.registry_index, ref);
            h.*.on_change_ref = null;
            h.*.view.onChange(null, null);
        }
        if (h.*.on_move_ref) |ref| {
            h.*.lua.unref(zlua.registry_index, ref);
            h.*.on_move_ref = null;
            h.*.view.onMove(null, null);
        }
    }

    lua.pushBoolean(true);
    return 1;
}

fn treeGc(lua: *Lua) i32 {
    const h = lua.toUserdata(*TreeHandle, 1) catch return 0;
    h.*.deinit();
    return 0;
}

// ============================================================================
// Helpers
// ============================================================================

fn luaErr(lua: *Lua, msg: [:0]const u8) noreturn {
    lua.raiseErrorStr(msg, .{});
}

fn luaToValueInterned(lua: *Lua, idx: i32) neograph.Value {
    return switch (lua.typeOf(idx)) {
        .number => .{ .int = lua.toInteger(idx) catch 0 },
        .boolean => .{ .bool = lua.toBoolean(idx) },
        .string => blk: {
            const lua_str = lua.toString(idx) catch "";
            const interned = getInterner().intern(lua_str) catch "";
            break :blk .{ .string = interned };
        },
        else => .{ .null = {} },
    };
}

fn pushValue(lua: *Lua, val: neograph.Value) void {
    switch (val) {
        .int => |v| lua.pushInteger(@intCast(v)),
        .number => |v| lua.pushNumber(v),
        .bool => |v| lua.pushBoolean(v),
        .string => |v| _ = lua.pushString(v),
        .null => lua.pushNil(),
    }
}

// ============================================================================
// Module Registration
// ============================================================================

const graph_methods = [_]zlua.FnReg{
    .{ .name = "schema", .func = zlua.wrap(graphSchema) },
    .{ .name = "get", .func = zlua.wrap(graphGet) },
    .{ .name = "insert", .func = zlua.wrap(graphInsert) },
    .{ .name = "update", .func = zlua.wrap(graphUpdate) },
    .{ .name = "link", .func = zlua.wrap(graphLink) },
    .{ .name = "unlink", .func = zlua.wrap(graphUnlink) },
    .{ .name = "edges", .func = zlua.wrap(graphEdges) },
    .{ .name = "has_edge", .func = zlua.wrap(graphHasEdge) },
    .{ .name = "delete", .func = zlua.wrap(graphDelete) },
    .{ .name = "field_type", .func = zlua.wrap(graphFieldType) },
    .{ .name = "query", .func = zlua.wrap(graphView) },
    // Unified node event API
    .{ .name = "on", .func = zlua.wrap(graphOn) },
    .{ .name = "off", .func = zlua.wrap(graphOff) },
    .{ .name = "__gc", .func = zlua.wrap(graphGc) },
};

const query_methods = [_]zlua.FnReg{
    .{ .name = "items", .func = zlua.wrap(treeGetVisible) },
    .{ .name = "total", .func = zlua.wrap(queryTotal) },
    .{ .name = "offset", .func = zlua.wrap(queryOffset) },
    .{ .name = "set_limit", .func = zlua.wrap(treeSetViewport) },
    .{ .name = "scroll_to", .func = zlua.wrap(treeScrollTo) },
    .{ .name = "scroll_by", .func = zlua.wrap(treeScrollBy) },
    .{ .name = "expand", .func = zlua.wrap(treeExpand) },
    .{ .name = "collapse", .func = zlua.wrap(treeCollapse) },
    .{ .name = "toggle", .func = zlua.wrap(treeToggle) },
    .{ .name = "is_expanded", .func = zlua.wrap(queryIsExpanded) },
    // Bulk expansion methods
    .{ .name = "expand_all", .func = zlua.wrap(queryExpandAll) },
    .{ .name = "collapse_all", .func = zlua.wrap(queryCollapseAll) },
    .{ .name = "stats", .func = zlua.wrap(treeStats) },
    // Unified event API
    .{ .name = "on", .func = zlua.wrap(queryOn) },
    .{ .name = "off", .func = zlua.wrap(queryOff) },
    .{ .name = "__gc", .func = zlua.wrap(treeGc) },
};

fn initModule(lua: *Lua) i32 {
    // Create Context metatable
    _ = lua.newMetatable("neograph.Context") catch return 0;
    lua.pushValue(-1);
    lua.setField(-2, "__index");
    lua.setFuncs(&graph_methods, 0);
    lua.pop(1);

    // Create Query metatable
    _ = lua.newMetatable("neograph.Query") catch return 0;
    lua.pushValue(-1);
    lua.setField(-2, "__index");
    lua.setFuncs(&query_methods, 0);
    lua.pop(1);

    // Return module table
    lua.createTable(0, 1);
    _ = lua.pushString("graph");
    lua.pushFunction(zlua.wrap(graphNew));
    lua.setTable(-3);

    return 1;
}

pub export fn luaopen_neograph_lua(state: *anyopaque) i32 {
    const lua: *Lua = @ptrCast(state);
    return initModule(lua);
}
