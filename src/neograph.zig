///! Neograph - A reactive in-memory graph database
///!
///! This module provides the public API for the Neograph database.
///! The main entry point is `Graph`.

const std = @import("std");
const Allocator = std.mem.Allocator;

// Internal modules (not exported)
const value_mod = @import("value.zig");
const schema_mod = @import("schema.zig");
const node_mod = @import("node.zig");
const node_store_mod = @import("node_store.zig");
const index_mod = @import("index.zig");
const rollup_mod = @import("rollup.zig");
const query_mod = @import("query.zig");
const reactive_mod = @import("reactive.zig");
const json_mod = @import("json.zig");

// ============================================================================
// Public API Types
// ============================================================================

// Core types
pub const NodeId = node_mod.NodeId;
pub const Node = node_mod.Node;
pub const Value = value_mod.Value;
pub const NodeStoreError = node_store_mod.NodeStoreError;

// Schema (returned by parseSchema, passed to Graph.init)
pub const Schema = schema_mod.Schema;
pub const SortDir = schema_mod.SortDir;
pub const ParseError = json_mod.ParseError;

// View types (returned by Graph.view)
pub const View = reactive_mod.View;
pub const ViewOpts = reactive_mod.ViewOpts;
pub const Callbacks = reactive_mod.subscription.Callbacks;
pub const Item = query_mod.Item;

// Node callbacks (for SDK entity mapper)
pub const NodeCallbacks = reactive_mod.NodeCallbacks;

// Query types (for building queries)
pub const QueryInput = query_mod.QueryInput;
pub const FilterInput = query_mod.FilterInput;
pub const FilterOp = query_mod.FilterOp;
pub const EdgeInput = query_mod.EdgeInput;

// Schema parsing
pub const parseSchema = json_mod.parseSchema;

// ============================================================================
// EdgeSortIndex - Tracks which edges sort by which properties
// ============================================================================

/// Reference to an edge definition that sorts by a specific property.
const EdgeSortRef = struct {
    source_type_id: node_mod.TypeId,
    edge_id: node_mod.EdgeId,
};

/// Key for the edge sort index: (target_type_id, property_name)
const EdgeSortKey = struct {
    type_id: node_mod.TypeId,
    property: []const u8,
};

const EdgeSortKeyContext = struct {
    pub fn hash(_: EdgeSortKeyContext, key: EdgeSortKey) u64 {
        var h = std.hash.Wyhash.init(0);
        h.update(std.mem.asBytes(&key.type_id));
        h.update(key.property);
        return h.final();
    }

    pub fn eql(_: EdgeSortKeyContext, a: EdgeSortKey, b: EdgeSortKey) bool {
        return a.type_id == b.type_id and std.mem.eql(u8, a.property, b.property);
    }
};

/// Index that maps (type_id, property) -> list of edges that sort by that property.
/// Used to efficiently find which edges need re-sorting when a property changes.
const EdgeSortIndex = struct {
    /// Maps (target_type_id, property_name) -> list of edges sorting by this property
    index: std.HashMapUnmanaged(EdgeSortKey, std.ArrayListUnmanaged(EdgeSortRef), EdgeSortKeyContext, 80),
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .index = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        var iter = self.index.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.index.deinit(self.allocator);
    }

    /// Build the index from the schema.
    /// Finds all edges with sort specifications and indexes them by (target_type, property).
    pub fn buildFromSchema(self: *Self, schema: *const schema_mod.Schema) !void {
        for (schema.types) |type_def| {
            for (type_def.edges) |edge_def| {
                if (edge_def.sort) |sort| {
                    const key = EdgeSortKey{
                        .type_id = edge_def.target_type_id,
                        .property = sort.property,
                    };

                    const result = try self.index.getOrPut(self.allocator, key);
                    if (!result.found_existing) {
                        result.value_ptr.* = std.ArrayListUnmanaged(EdgeSortRef){};
                    }

                    try result.value_ptr.append(self.allocator, .{
                        .source_type_id = type_def.id,
                        .edge_id = edge_def.id,
                    });
                }
            }
        }
    }

    /// Get edges that sort by a property on a given type.
    pub fn getEdgesSortingBy(self: *const Self, type_id: node_mod.TypeId, property: []const u8) []const EdgeSortRef {
        const key = EdgeSortKey{ .type_id = type_id, .property = property };
        if (self.index.get(key)) |refs| {
            return refs.items;
        }
        return &.{};
    }
};

// ============================================================================
// Graph - The Public API
// ============================================================================

/// Graph is the main entry point for the neograph API.
/// It wraps all internal components and provides a clean interface.
pub const Graph = struct {
    allocator: Allocator,
    schema: schema_mod.Schema,
    store: node_store_mod.NodeStore,
    indexes: index_mod.IndexManager,
    rollups: rollup_mod.RollupCache,
    tracker: reactive_mod.ChangeTracker,
    allocated_queries: std.ArrayListUnmanaged(*query_mod.Query),
    edge_sort_index: EdgeSortIndex,

    const Self = @This();

    /// Initialize a new graph with the given schema.
    /// Returns a heap-allocated Graph to ensure stable pointers.
    pub fn init(allocator: Allocator, schema_input: schema_mod.Schema) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        self.allocator = allocator;
        self.schema = schema_input;
        self.allocated_queries = .{};

        self.store = node_store_mod.NodeStore.init(allocator, &self.schema);
        errdefer self.store.deinit();

        self.indexes = try index_mod.IndexManager.init(allocator, &self.schema);
        errdefer self.indexes.deinit();

        self.rollups = rollup_mod.RollupCache.init(allocator, &self.schema, &self.store, &self.indexes);
        errdefer self.rollups.deinit();

        self.tracker = reactive_mod.ChangeTracker.init(allocator, &self.store, &self.schema, &self.indexes, &self.rollups);

        // Build edge sort index for re-sorting edges when properties change
        self.edge_sort_index = EdgeSortIndex.init(allocator);
        errdefer self.edge_sort_index.deinit();
        try self.edge_sort_index.buildFromSchema(&self.schema);

        return self;
    }

    /// Deinitialize the graph and free all resources.
    pub fn deinit(self: *Self) void {
        // Free allocated queries
        for (self.allocated_queries.items) |q| {
            q.deinit(self.allocator);
            self.allocator.destroy(q);
        }
        self.allocated_queries.deinit(self.allocator);

        self.edge_sort_index.deinit();
        self.tracker.deinit();
        self.rollups.deinit();
        self.indexes.deinit();
        self.store.deinit();
        self.schema.deinit();
        self.allocator.destroy(self);
    }

    /// Insert a new node of the given type.
    pub fn insert(self: *Self, type_name: []const u8) !NodeId {
        const id = try self.store.insert(type_name);
        const n = self.store.get(id).?;
        try self.indexes.onInsert(n);
        self.tracker.onInsert(n);
        // Initialize rollup values (count=0, traverse=null, etc.)
        try self.rollups.initializeRollups(id);
        return id;
    }

    /// Update a node's properties.
    pub fn update(self: *Self, id: NodeId, props: anytype) !void {
        const n = self.store.get(id) orelse return error.NodeNotFound;
        var old = try n.clone();
        defer old.deinit();

        try self.store.update(id, props);

        const updated = self.store.get(id).?;
        try self.indexes.onUpdate(updated, &old);
        self.tracker.onUpdate(updated, &old);

        // Recompute rollups on other nodes that traverse to this node's properties
        const fields = @typeInfo(@TypeOf(props)).@"struct".fields;
        inline for (fields) |field| {
            try self.rollups.recomputeTraverseDeps(id, field.name);
        }
    }

    /// Link two nodes via an edge.
    pub fn link(self: *Self, src: NodeId, edge_name: []const u8, tgt: NodeId) !void {
        const n = self.store.get(src) orelse return error.NodeNotFound;
        var old_src = try n.clone();
        defer old_src.deinit();

        // Also clone target node - store.link() adds reverse edge to target
        const tgt_n = self.store.get(tgt) orelse return error.EdgeTargetNotFound;
        var old_tgt = try tgt_n.clone();
        defer old_tgt.deinit();

        try self.store.link(src, edge_name, tgt);

        const updated_src = self.store.get(src).?;
        const updated_tgt = self.store.get(tgt).?;
        const edge_def = self.schema.getEdgeDef(updated_src.type_id, edge_name).?;

        // Update indexes on source node (for indexes that use this edge)
        try self.indexes.onLink(updated_src, &old_src, edge_name);
        // Also update indexes on target node (for cross-entity indexes using the reverse edge)
        try self.indexes.onLink(updated_tgt, &old_tgt, edge_def.reverse_name);

        self.tracker.onLink(src, edge_def.id, tgt);

        // Maintain rollup inverted index (for recomputeTraverseDeps lookups)
        self.rollups.onLink(src, updated_src.type_id, edge_def.id, tgt);
        // Also track the reverse edge
        self.rollups.onLink(tgt, updated_tgt.type_id, edge_def.reverse_edge_id, src);

        // Eagerly recompute rollups that depend on this edge
        try self.rollups.recomputeForEdge(src, edge_name);
        // Also recompute rollups on target that depend on the reverse edge
        try self.rollups.recomputeForEdge(tgt, edge_def.reverse_name);
    }

    /// Unlink two nodes.
    pub fn unlink(self: *Self, src: NodeId, edge_name: []const u8, tgt: NodeId) !void {
        const n = self.store.get(src) orelse return error.NodeNotFound;
        const edge_def = self.schema.getEdgeDef(n.type_id, edge_name) orelse return error.EdgeNotFound;
        var old_src = try n.clone();
        defer old_src.deinit();

        // Also clone target node - store.unlink() removes reverse edge from target
        const tgt_n = self.store.get(tgt) orelse return error.NodeNotFound;
        var old_tgt = try tgt_n.clone();
        defer old_tgt.deinit();

        try self.store.unlink(src, edge_name, tgt);

        const updated_src = self.store.get(src).?;
        const updated_tgt = self.store.get(tgt).?;

        // Update indexes on source node
        try self.indexes.onUnlink(updated_src, &old_src, edge_name);
        // Also update indexes on target node (for cross-entity indexes using the reverse edge)
        try self.indexes.onUnlink(updated_tgt, &old_tgt, edge_def.reverse_name);

        self.tracker.onUnlink(src, edge_def.id, tgt);

        // Maintain rollup inverted index
        self.rollups.onUnlink(src, updated_src.type_id, edge_def.id, tgt);
        // Also remove the reverse edge tracking
        self.rollups.onUnlink(tgt, updated_tgt.type_id, edge_def.reverse_edge_id, src);

        // Eagerly recompute rollups that depend on this edge
        try self.rollups.recomputeForEdge(src, edge_name);
        // Also recompute rollups on target that depend on the reverse edge
        try self.rollups.recomputeForEdge(tgt, edge_def.reverse_name);
    }

    /// Delete a node.
    pub fn delete(self: *Self, id: NodeId) !void {
        const n = self.store.get(id) orelse return error.NodeNotFound;

        // Emit unlink events for all edges before deletion.
        // This ensures reactive callbacks (like EdgeCollection:each cleanup) fire.
        self.emitUnlinksForDelete(id, n);

        self.tracker.onDelete(n);
        self.indexes.onDelete(n);
        self.rollups.removeNode(id);
        try self.store.delete(id);
    }

    /// Emit unlink events for all edges pointing TO the node being deleted.
    /// This triggers cleanup callbacks (like EdgeCollection:each onLeave).
    fn emitUnlinksForDelete(self: *Self, id: NodeId, _: *const Node) void {
        // Only emit for incoming edges: sources → this node (deleted node is target)
        // We do NOT emit for outgoing edges because that would tell the tracker
        // "this node unlinked its targets" which removes THEM from views.
        const sources = self.rollups.inverted_index.getSourcesFor(id);
        for (sources) |src| {
            self.tracker.onUnlink(src.source_id, src.edge_id, id);
        }
    }

    /// Get a node by ID.
    pub fn get(self: *Self, id: NodeId) ?*const Node {
        return self.store.get(id);
    }

    /// Get a rollup value for a node.
    /// Rollups are computed values (like first/last by sort, count, traverse).
    pub fn getRollup(self: *Self, id: NodeId, rollup_name: []const u8) !Value {
        const node = self.store.get(id) orelse return Value{ .null = {} };
        return self.rollups.get(node, rollup_name);
    }

    /// Get edge targets from a node by edge name.
    /// Returns null if node doesn't exist or edge name is invalid.
    pub fn getEdgeTargets(self: *Self, id: NodeId, edge_name: []const u8) ?[]const NodeId {
        const n = self.store.get(id) orelse return null;
        const edge_def = self.schema.getEdgeDef(n.type_id, edge_name) orelse return null;
        return n.getEdgeTargets(edge_def.id);
    }

    /// Check if an edge exists between two nodes.
    pub fn hasEdge(self: *Self, src: NodeId, edge_name: []const u8, tgt: NodeId) bool {
        const targets = self.getEdgeTargets(src, edge_name) orelse return false;
        for (targets) |t| {
            if (t == tgt) return true;
        }
        return false;
    }

    /// Get total node count.
    pub fn count(self: *Self) usize {
        return self.store.count();
    }

    /// Create a view from a query input.
    pub fn view(self: *Self, input: query_mod.QueryInput, opts: ViewOpts) !View {
        var qb = query_mod.QueryBuilder.init(self.allocator);

        const q = try qb.build(input);

        const q_ptr = try self.allocator.create(query_mod.Query);
        q_ptr.* = q;
        try self.allocated_queries.append(self.allocator, q_ptr);

        // Validate query: resolves types, validates edges, selects index
        const validation = query_mod.validate(q_ptr, &self.schema, &self.indexes) catch |err| {
            return switch (err) {
                error.UnknownType => error.TypeNotFound,
                error.UnknownEdge => error.InvalidQuery,
                error.UnknownProperty => error.InvalidQuery,
                error.NoSuitableIndex => error.NoIndexCoverage,
                error.TypeMismatch => error.InvalidQuery,
            };
        };

        return try View.init(self.allocator, &self.tracker, validation.query, validation.coverage, opts);
    }

    // ========================================================================
    // Runtime-dynamic API (for FFI/Lua bindings)
    // ========================================================================

    /// Set a single property on a node (runtime-dynamic version of update).
    /// Properly notifies indexes and reactive subscriptions.
    pub fn setProperty(self: *Self, id: NodeId, key: []const u8, value: Value) !void {
        const n = self.store.get(id) orelse return error.NodeNotFound;
        var old = try n.clone();
        defer old.deinit();

        // Update the property (store.get returns mutable pointer)
        try n.setProperty(key, value);

        // Notify indexes and tracker
        const updated = self.store.get(id).?;
        try self.indexes.onUpdate(updated, &old);
        self.tracker.onUpdate(updated, &old);

        // Recompute rollups on other nodes that traverse to this property
        try self.rollups.recomputeTraverseDeps(id, key);

        // Re-sort edges that sort by this property
        try self.resortEdgesForProperty(id, key);
    }

    /// Re-position target in EdgeTargets that sort by this property.
    /// Called when a target node's property changes.
    fn resortEdgesForProperty(self: *Self, target_id: NodeId, property: []const u8) !void {
        const target_node = self.store.get(target_id) orelse return;

        // Find edges that sort by this property on this node's type
        const edge_refs = self.edge_sort_index.getEdgesSortingBy(target_node.type_id, property);
        if (edge_refs.len == 0) return;

        // Use the rollups' inverted index to find sources pointing to this target
        const sources = self.rollups.inverted_index.getSourcesFor(target_id);

        for (sources) |src| {
            // Check if this source has an edge that sorts by the changed property
            for (edge_refs) |ref| {
                if (src.source_type_id == ref.source_type_id and src.edge_id == ref.edge_id) {
                    // This source has an edge to target that sorts by the changed property
                    const source_node = self.store.get(src.source_id) orelse continue;
                    if (source_node.edges.getPtr(ref.edge_id)) |edge_targets| {
                        try edge_targets.resortItem(target_id);
                    }
                    break;
                }
            }
        }
    }

    /// Create a view from a JSON query string.
    /// This is the runtime-dynamic version of view() for FFI/Lua bindings.
    pub fn viewFromJson(self: *Self, json_str: []const u8, opts: ViewOpts) !View {
        const q = json_mod.parseQuery(self.allocator, json_str) catch |err| {
            return switch (err) {
                error.InvalidJson => error.InvalidJson,
                error.OutOfMemory => error.OutOfMemory,
                else => error.InvalidQuery,
            };
        };

        const q_ptr = try self.allocator.create(query_mod.Query);
        q_ptr.* = q;
        try self.allocated_queries.append(self.allocator, q_ptr);

        // Validate edges (also resolves root type ID)
        query_mod.validateEdges(q_ptr, &self.schema) catch |err| {
            return switch (err) {
                error.UnknownType => error.TypeNotFound,
                error.UnknownEdge => error.InvalidQuery,
                error.UnknownProperty => error.InvalidQuery,
                error.TypeMismatch => error.InvalidQuery,
                else => error.InvalidQuery,
            };
        };

        // For direct node ID lookups, use any available index (bypasses filter/sort matching)
        const coverage = if (q_ptr.root_id != null)
            self.indexes.getAnyIndex(q_ptr.root_type_id) orelse return error.NoIndexCoverage
        else
            self.indexes.selectIndex(q_ptr.root_type_id, q_ptr.filters, q_ptr.sorts) orelse
                return error.NoIndexCoverage;

        return try View.init(self.allocator, &self.tracker, q_ptr, coverage, opts);
    }

    /// Get the type name for a node.
    pub fn getTypeName(self: *Self, id: NodeId) ?[]const u8 {
        const n = self.store.get(id) orelse return null;
        return self.schema.getTypeName(n.type_id);
    }

    /// Get a property value from a node.
    pub fn getProperty(self: *Self, id: NodeId, key: []const u8) ?Value {
        const n = self.store.get(id) orelse return null;
        return n.getProperty(key);
    }

    // ========================================================================
    // Node Callbacks - For SDK entity mapper
    // ========================================================================

    /// Register callbacks for a specific node.
    /// Callbacks fire when the node is updated, deleted, or edges are linked/unlinked.
    /// Link/unlink callbacks fire for both ends of the edge (bidirectional).
    pub fn watchNode(self: *Self, id: NodeId, callbacks: NodeCallbacks) !void {
        try self.tracker.watchNode(id, callbacks);
    }

    /// Unregister callbacks for a specific node.
    pub fn unwatchNode(self: *Self, id: NodeId) void {
        self.tracker.unwatchNode(id);
    }
};

test {
    std.testing.refAllDecls(@This());
}
