///! Query executor.
///!
///! Executes validated queries using indexes and materializes results.

const std = @import("std");
const Allocator = std.mem.Allocator;

const builder = @import("builder.zig");
pub const Query = builder.Query;
pub const Filter = builder.Filter;
pub const FilterOp = builder.FilterOp;
pub const Sort = builder.Sort;
pub const EdgeSelection = builder.EdgeSelection;

const Value = @import("../value.zig").Value;

// ============================================================================
// Result Types
// ============================================================================

/// Path to an item in the result tree.
pub const PathSegment = union(enum) {
    root: u64, // NodeId
    edge: struct {
        name: []const u8,
        index: u32,
    },
};

pub const Path = []const PathSegment;

/// A result item.
pub const Item = struct {
    id: u64, // NodeId
    type_id: u16,
    path: Path,
    depth: u8,
    fields: std.StringHashMapUnmanaged(Value),
    edges: std.StringHashMapUnmanaged(EdgeResult),
    allocator: Allocator,

    pub fn init(allocator: Allocator, id: u64, type_id: u16) Item {
        return .{
            .id = id,
            .type_id = type_id,
            .path = &.{},
            .depth = 0,
            .fields = .{},
            .edges = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Item) void {
        if (self.path.len > 0) {
            self.allocator.free(self.path);
        }
        self.fields.deinit(self.allocator);
        var edge_iter = self.edges.iterator();
        while (edge_iter.next()) |entry| {
            switch (entry.value_ptr.*) {
                .items => |items| {
                    for (items) |*item| {
                        var mutable = item.*;
                        mutable.deinit();
                    }
                    self.allocator.free(items);
                },
                .lazy, .cycle => {},
            }
        }
        self.edges.deinit(self.allocator);
    }
};

/// Result of an edge traversal.
pub const EdgeResult = union(enum) {
    items: []Item,
    lazy: LazyEdge,
    cycle: CycleRef,
};

/// Lazy edge (not yet loaded).
pub const LazyEdge = struct {
    has_children: bool,
    count: u32,
};

/// Cycle reference (prevents infinite recursion).
pub const CycleRef = struct {
    to_id: u64, // NodeId
};

const Schema = @import("../schema.zig").Schema;
const SortDir = @import("../schema.zig").SortDir;
const Node = @import("../node.zig").Node;
const NodeId = @import("../node.zig").NodeId;
const TypeId = @import("../node.zig").TypeId;
const NodeStore = @import("../node_store.zig").NodeStore;
const IndexManager = @import("../index/index.zig").IndexManager;
const IndexCoverage = @import("../index/index.zig").IndexCoverage;
const RollupCache = @import("../rollup/cache.zig").RollupCache;

/// Set of visited nodes for cycle detection.
const VisitedSet = std.AutoHashMapUnmanaged(NodeId, void);

/// Query executor.
pub const Executor = struct {
    store: *const NodeStore,
    schema: *const Schema,
    indexes: *const IndexManager,
    rollups: *RollupCache,
    allocator: Allocator,

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        store: *const NodeStore,
        schema: *const Schema,
        indexes: *const IndexManager,
        rollups: *RollupCache,
    ) Self {
        return .{
            .store = store,
            .schema = schema,
            .indexes = indexes,
            .rollups = rollups,
            .allocator = allocator,
        };
    }

    /// Execute a query and return results.
    /// Returns NoIndexCoverage if the index doesn't fully cover the required sort order.
    pub fn execute(self: *Self, query: *const Query, coverage: IndexCoverage) ExecuteError![]Item {
        // Verify index fully covers the sort order - no in-memory sorting allowed
        if (query.sorts.len > 0 and coverage.sort_prefix < query.sorts.len) {
            return error.NoIndexCoverage;
        }

        var results: std.ArrayList(Item) = .{};
        errdefer {
            for (results.items) |*item| {
                item.deinit();
            }
            results.deinit(self.allocator);
        }

        // Scan index - results are already in sorted order
        var iter = self.indexes.scan(coverage, query.filters);

        while (iter.next()) |node_id| {
            const node = self.store.get(node_id) orelse continue;

            // Apply post-filters
            if (!self.matchesFilters(node, coverage.post_filters)) continue;
            if (!self.matchesFilters(node, query.filters)) continue;

            // Materialize
            var visited = VisitedSet{};
            defer visited.deinit(self.allocator);

            const path = try self.allocator.alloc(PathSegment, 1);
            path[0] = .{ .root = node_id };

            const item = try self.materialize(node, query.selections, path, &visited);
            try results.append(self.allocator, item);
        }

        // No in-memory sort needed - index provides order
        return results.toOwnedSlice(self.allocator) catch return &.{};
    }

    /// Materialize a node into an Item with all fields.
    pub fn materialize(
        self: *Self,
        node: *const Node,
        selections: []const EdgeSelection,
        path: Path,
        visited: *VisitedSet,
    ) ExecuteError!Item {
        var item = Item.init(self.allocator, node.id, node.type_id);
        errdefer item.deinit();

        item.path = path;
        item.depth = @intCast(path.len - 1);

        // Track visited for cycle detection
        try visited.put(self.allocator, node.id, {});

        // Load all properties from the node
        const type_def = self.schema.getTypeById(node.type_id);
        if (type_def) |td| {
            for (td.properties) |prop| {
                if (node.getProperty(prop.name)) |value| {
                    try item.fields.put(self.allocator, prop.name, value);
                }
            }
            // Load rollups
            for (td.rollups) |rollup| {
                const value = self.rollups.get(node, rollup.name) catch continue;
                try item.fields.put(self.allocator, rollup.name, value);
            }
        }

        // Process edge selections
        for (selections) |edge_sel| {
            const edge_result = try self.materializeEdge(node, edge_sel, path, visited);
            try item.edges.put(self.allocator, edge_sel.name, edge_result);
        }

        return item;
    }

    pub const ExecuteError = error{
        OutOfMemory,
        NoIndexCoverage,
    };

    fn materializeEdge(
        self: *Self,
        node: *const Node,
        edge_sel: EdgeSelection,
        parent_path: Path,
        visited: *VisitedSet,
    ) ExecuteError!EdgeResult {
        const edge_def = self.schema.getEdgeDef(node.type_id, edge_sel.name) orelse
            return EdgeResult{ .items = &.{} };

        var items: std.ArrayList(Item) = .{};
        errdefer {
            for (items.items) |*item| {
                item.deinit();
            }
            items.deinit(self.allocator);
        }

        // For recursive edges, build child selections that include the recursive edge itself.
        // This allows traversing an edge repeatedly (e.g., children -> children -> children).
        var child_selections: []const EdgeSelection = edge_sel.selections;
        var recursive_selections: ?[]EdgeSelection = null;
        defer if (recursive_selections) |rs| self.allocator.free(rs);

        if (edge_sel.recursive) {
            // Allocate new selections array: original selections + the recursive edge
            recursive_selections = try self.allocator.alloc(EdgeSelection, edge_sel.selections.len + 1);
            @memcpy(recursive_selections.?[0..edge_sel.selections.len], edge_sel.selections);
            recursive_selections.?[edge_sel.selections.len] = edge_sel;
            child_selections = recursive_selections.?;
        }

        // If sorts are specified, we MUST use a cross-entity index
        if (edge_sel.sorts.len > 0) {
            // Try to find a cross-entity index for this edge traversal
            const reverse_edge = edge_def.reverse_name;
            if (reverse_edge.len > 0) {
                if (self.indexes.selectNestedIndex(
                    edge_def.target_type_id,
                    reverse_edge,
                    edge_sel.filters,
                    edge_sel.sorts,
                )) |coverage| {
                    defer {
                        var cov = coverage;
                        cov.deinit();
                    }

                    // Use index scan for sorted traversal
                    var iter = self.indexes.scanWithEdgePrefix(coverage, node.id, edge_sel.filters);
                    var idx: u32 = 0;

                    while (iter.next()) |target_id| {
                        // Cycle detection
                        if (visited.contains(target_id)) continue;

                        const target = self.store.get(target_id) orelse continue;

                        // Apply post-filters not covered by index
                        if (!self.matchesFilters(target, coverage.post_filters)) continue;

                        // Build path for child
                        const child_path = try self.allocator.alloc(PathSegment, parent_path.len + 1);
                        @memcpy(child_path[0..parent_path.len], parent_path);
                        child_path[parent_path.len] = .{ .edge = .{
                            .name = edge_sel.name,
                            .index = idx,
                        } };

                        const item = try self.materialize(target, child_selections, child_path, visited);
                        try items.append(self.allocator, item);
                        idx += 1;
                    }

                    // Already sorted by index - no in-memory sort needed
                    return EdgeResult{ .items = items.toOwnedSlice(self.allocator) catch &.{} };
                }
            }

            // No cross-entity index available for sorted traversal - error
            return error.NoIndexCoverage;
        }

        // No sorts - iterate through targets in NodeId order (no index needed)
        const targets = node.getEdgeTargets(edge_def.id);

        for (targets, 0..) |target_id, idx| {
            // Cycle detection
            if (visited.contains(target_id)) {
                continue; // Skip cycles
            }

            const target = self.store.get(target_id) orelse continue;

            // Apply edge filters
            if (!self.matchesFilters(target, edge_sel.filters)) continue;

            // Build path for child
            const child_path = try self.allocator.alloc(PathSegment, parent_path.len + 1);
            @memcpy(child_path[0..parent_path.len], parent_path);
            child_path[parent_path.len] = .{ .edge = .{
                .name = edge_sel.name,
                .index = @intCast(idx),
            } };

            const item = try self.materialize(target, child_selections, child_path, visited);
            try items.append(self.allocator, item);
        }

        return EdgeResult{ .items = items.toOwnedSlice(self.allocator) catch &.{} };
    }

    /// Check if a node matches all filters.
    pub fn matchesFilters(self: *Self, node: *const Node, filters: []const Filter) bool {
        for (filters) |filter| {
            if (!self.matchesFilter(node, filter)) return false;
        }
        return true;
    }

    fn matchesFilter(self: *Self, node: *const Node, filter: Filter) bool {
        // Get the value to compare
        const value = blk: {
            if (filter.path.len == 1) {
                // Special handling for node id
                if (std.mem.eql(u8, filter.path[0], "id")) {
                    break :blk Value{ .int = @intCast(node.id) };
                }
                // Simple property
                if (node.getProperty(filter.path[0])) |v| {
                    break :blk v;
                }
                // Try rollup
                break :blk self.rollups.get(node, filter.path[0]) catch Value{ .null = {} };
            } else {
                // Cross-edge filter - traverse to get value
                break :blk self.traversePath(node, filter.path) orelse Value{ .null = {} };
            }
        };

        return self.compareValues(value, filter.op, filter.value, filter.values);
    }

    fn traversePath(self: *Self, start_node: *const Node, path: []const []const u8) ?Value {
        var current_node = start_node;

        // Traverse edges
        for (path[0 .. path.len - 1]) |edge_name| {
            const edge_def = self.schema.getEdgeDef(current_node.type_id, edge_name) orelse return null;
            const targets = current_node.getEdgeTargets(edge_def.id);
            if (targets.len == 0) return null;
            current_node = self.store.get(targets[0]) orelse return null;
        }

        // Get final property
        return current_node.getProperty(path[path.len - 1]);
    }

    fn compareValues(self: *Self, value: Value, op: FilterOp, filter_value: Value, filter_values: ?[]const Value) bool {
        _ = self;
        switch (op) {
            .eq => return value.eql(filter_value),
            .neq => return !value.eql(filter_value),
            .gt => return value.order(filter_value) == .gt,
            .gte => {
                const ord = value.order(filter_value);
                return ord == .gt or ord == .eq;
            },
            .lt => return value.order(filter_value) == .lt,
            .lte => {
                const ord = value.order(filter_value);
                return ord == .lt or ord == .eq;
            },
            .in => {
                if (filter_values) |values| {
                    for (values) |v| {
                        if (value.eql(v)) return true;
                    }
                }
                return false;
            },
        }
    }

};

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;
const parseSchema = @import("../json.zig").parseSchema;

fn createTestSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "User",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "age", "type": "int" }
        \\      ],
        \\      "edges": [{ "name": "posts", "target": "Post", "reverse": "author" }],
        \\      "indexes": [{ "fields": [{ "field": "name", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Post",
        \\      "properties": [
        \\        { "name": "title", "type": "string" },
        \\        { "name": "views", "type": "int" }
        \\      ],
        \\      "edges": [{ "name": "author", "target": "User", "reverse": "posts" }],
        \\      "indexes": [{ "fields": [{ "field": "views", "direction": "desc" }] }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

test "Executor filter matching" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    defer store.deinit();

    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();

    var rollups = RollupCache.init(testing.allocator, &schema, &store, &indexes);
    defer rollups.deinit();

    var executor = Executor.init(testing.allocator, &store, &schema, &indexes, &rollups);

    // Create test node
    const user_id = try store.insert("User");
    try store.update(user_id, .{ .name = "Alice", .age = @as(i64, 30) });

    const user = store.get(user_id).?;

    // Test equality filter
    const eq_filter = Filter{
        .path = &.{"name"},
        .op = .eq,
        .value = .{ .string = "Alice" },
    };
    try testing.expect(executor.matchesFilter(user, eq_filter));

    // Test inequality
    const neq_filter = Filter{
        .path = &.{"name"},
        .op = .neq,
        .value = .{ .string = "Bob" },
    };
    try testing.expect(executor.matchesFilter(user, neq_filter));

    // Test greater than
    const gt_filter = Filter{
        .path = &.{"age"},
        .op = .gt,
        .value = .{ .int = 25 },
    };
    try testing.expect(executor.matchesFilter(user, gt_filter));
}

