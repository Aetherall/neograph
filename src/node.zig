///! Node types for the graph database.
///!
///! Nodes are the fundamental data units, containing properties and edges
///! to other nodes.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Order = std.math.Order;
const Value = @import("value.zig").Value;
const ds = @import("ds.zig");
const SortedSet = ds.SortedSet;
const SortedSetContext = ds.SortedSetContext;
const SortDir = @import("schema.zig").SortDir;

/// Unique identifier for a node.
pub const NodeId = u64;

/// Type identifier within the schema.
pub const TypeId = u16;

/// Edge identifier within a type.
pub const EdgeId = u16;

/// Context for property-based edge target sorting.
/// Uses type erasure to avoid circular dependency with NodeStore.
pub const EdgeSortContext = struct {
    /// Opaque pointer to the NodeStore
    store_ptr: *const anyopaque,
    /// Function to get a node by ID from the store
    get_node_fn: *const fn (*const anyopaque, NodeId) ?*const Node,
    /// Property name on target node to sort by
    property: []const u8,
    /// Sort direction
    direction: SortDir,

    const Self = @This();

    /// Compare two node IDs by looking up their property values.
    pub fn compare(self: Self, a: NodeId, b: NodeId) Order {
        const val_a = self.getPropertyValue(a);
        const val_b = self.getPropertyValue(b);

        var result = val_a.order(val_b);

        // Apply descending direction
        if (self.direction == .desc) {
            result = switch (result) {
                .lt => .gt,
                .gt => .lt,
                .eq => .eq,
            };
        }

        // Tie-breaker: sort by node ID for stable ordering
        if (result == .eq) {
            result = std.math.order(a, b);
        }

        return result;
    }

    fn getPropertyValue(self: Self, id: NodeId) Value {
        if (self.get_node_fn(self.store_ptr, id)) |node| {
            return node.getProperty(self.property) orelse Value{ .null = {} };
        }
        return Value{ .null = {} };
    }
};

/// Sorted edge targets using custom property-based comparison.
pub const SortedEdgeTargets = SortedSetContext(NodeId, EdgeSortContext, EdgeSortContext.compare);

/// Sorted list of edge targets for fast lookup.
/// Maintains sorted order for binary search on contains().
/// Supports two modes: default (sorted by NodeId) or property-sorted.
pub const EdgeTargets = struct {
    /// Standard sorted set (by node ID) - used when no sort spec
    inner: ?SortedSet(NodeId),
    /// Property-sorted set - used when edge has sort spec
    sorted_inner: ?SortedEdgeTargets,
    /// Allocator for memory operations
    allocator: Allocator,

    const Self = @This();

    /// Initialize with default NodeId sorting.
    pub fn init(allocator: Allocator) Self {
        return .{
            .inner = SortedSet(NodeId).init(allocator),
            .sorted_inner = null,
            .allocator = allocator,
        };
    }

    /// Initialize with property-based sorting.
    pub fn initWithSort(allocator: Allocator, context: EdgeSortContext) Self {
        return .{
            .inner = null,
            .sorted_inner = SortedEdgeTargets.initWithContext(allocator, context),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.inner) |*i| i.deinit();
        if (self.sorted_inner) |*si| si.deinit();
    }

    /// Add a target, maintaining sorted order. No-op if already present.
    pub fn add(self: *Self, target: NodeId) !void {
        if (self.inner) |*i| {
            try i.add(target);
        } else if (self.sorted_inner) |*si| {
            try si.add(target);
        }
    }

    /// Remove a target. Returns true if it was present.
    pub fn remove(self: *Self, target: NodeId) bool {
        if (self.inner) |*i| {
            return i.remove(target);
        } else if (self.sorted_inner) |*si| {
            return si.remove(target);
        }
        return false;
    }

    /// Check if a target is present. O(log n).
    pub fn contains(self: *const Self, target: NodeId) bool {
        if (self.inner) |i| {
            return i.contains(target);
        } else if (self.sorted_inner) |si| {
            return si.contains(target);
        }
        return false;
    }

    /// Get the number of targets.
    pub fn count(self: *const Self) usize {
        if (self.inner) |i| {
            return i.count();
        } else if (self.sorted_inner) |si| {
            return si.count();
        }
        return 0;
    }

    /// Get the first target (smallest by sort order), or null if empty.
    pub fn first(self: *const Self) ?NodeId {
        if (self.inner) |i| {
            return i.first();
        } else if (self.sorted_inner) |si| {
            return si.first();
        }
        return null;
    }

    /// Get all targets as a slice.
    pub fn items(self: *const Self) []const NodeId {
        if (self.inner) |i| {
            return i.slice();
        } else if (self.sorted_inner) |si| {
            return si.slice();
        }
        return &.{};
    }

    /// Clone the edge targets.
    pub fn clone(self: *const Self) !Self {
        return .{
            .inner = if (self.inner) |i| try i.clone() else null,
            .sorted_inner = if (self.sorted_inner) |si| try si.clone() else null,
            .allocator = self.allocator,
        };
    }

    /// Re-position a single item after its sort property changed.
    /// Only affects property-sorted EdgeTargets; no-op for NodeId-sorted.
    pub fn resortItem(self: *Self, target_id: NodeId) !void {
        if (self.sorted_inner) |*si| {
            // Remove and re-add to get correct position with new property value
            _ = si.remove(target_id);
            try si.add(target_id);
        }
        // No-op for unsorted edges (sorted by NodeId, which doesn't change)
    }

    /// Check if this EdgeTargets uses property-based sorting.
    pub fn isPropertySorted(self: *const Self) bool {
        return self.sorted_inner != null;
    }
};

/// Properties storage - maps property names to values.
pub const Properties = std.StringHashMapUnmanaged(Value);

/// Edge storage - maps edge ids to their targets.
pub const Edges = std.AutoHashMapUnmanaged(EdgeId, EdgeTargets);

/// A node in the graph database.
pub const Node = struct {
    id: NodeId,
    type_id: TypeId,
    properties: Properties,
    edges: Edges,
    rollup_values: Properties,
    allocator: Allocator,

    const Self = @This();

    /// Create a new node with the given id and type.
    pub fn init(allocator: Allocator, id: NodeId, type_id: TypeId) Self {
        return .{
            .id = id,
            .type_id = type_id,
            .properties = .{},
            .edges = .{},
            .rollup_values = .{},
            .allocator = allocator,
        };
    }

    /// Free all node resources.
    pub fn deinit(self: *Self) void {
        self.properties.deinit(self.allocator);
        self.rollup_values.deinit(self.allocator);

        var edge_iter = self.edges.iterator();
        while (edge_iter.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.edges.deinit(self.allocator);
    }

    /// Get a property value by name (also checks rollup values).
    pub fn getProperty(self: *const Self, name: []const u8) ?Value {
        // Check properties first, then rollups
        return self.properties.get(name) orelse self.rollup_values.get(name);
    }

    /// Get a rollup value by name.
    pub fn getRollup(self: *const Self, name: []const u8) ?Value {
        return self.rollup_values.get(name);
    }

    /// Set a rollup value.
    pub fn setRollup(self: *Self, name: []const u8, value: Value) !void {
        try self.rollup_values.put(self.allocator, name, value);
    }

    /// Set a property value. If value is null, removes the property.
    pub fn setProperty(self: *Self, name: []const u8, value: ?Value) !void {
        if (value) |v| {
            try self.properties.put(self.allocator, name, v);
        } else {
            _ = self.properties.remove(name);
        }
    }

    /// Get edge targets for an edge id.
    pub fn getEdgeTargets(self: *const Self, edge_id: EdgeId) []const NodeId {
        if (self.edges.get(edge_id)) |targets| {
            return targets.items();
        }
        return &.{};
    }

    /// Get or create edge targets for an edge id.
    /// If sort_context is provided and edge targets don't exist, creates property-sorted targets.
    pub fn getOrCreateEdgeTargets(self: *Self, edge_id: EdgeId, sort_context: ?EdgeSortContext) !*EdgeTargets {
        const result = try self.edges.getOrPut(self.allocator, edge_id);
        if (!result.found_existing) {
            if (sort_context) |ctx| {
                result.value_ptr.* = EdgeTargets.initWithSort(self.allocator, ctx);
            } else {
                result.value_ptr.* = EdgeTargets.init(self.allocator);
            }
        }
        return result.value_ptr;
    }

    /// Add an edge target.
    pub fn addEdge(self: *Self, edge_id: EdgeId, target: NodeId) !void {
        const targets = try self.getOrCreateEdgeTargets(edge_id, null);
        try targets.add(target);
    }

    /// Add an edge target with optional sort context.
    pub fn addEdgeWithSort(self: *Self, edge_id: EdgeId, target: NodeId, sort_context: ?EdgeSortContext) !void {
        const targets = try self.getOrCreateEdgeTargets(edge_id, sort_context);
        try targets.add(target);
    }

    /// Remove an edge target. Returns true if it was present.
    pub fn removeEdge(self: *Self, edge_id: EdgeId, target: NodeId) bool {
        if (self.edges.getPtr(edge_id)) |targets| {
            return targets.remove(target);
        }
        return false;
    }

    /// Clone the node with all its properties and edges.
    pub fn clone(self: *const Self) !Self {
        var new_node = Self.init(self.allocator, self.id, self.type_id);
        errdefer new_node.deinit();

        // Clone properties
        var prop_iter = self.properties.iterator();
        while (prop_iter.next()) |entry| {
            try new_node.properties.put(self.allocator, entry.key_ptr.*, entry.value_ptr.*);
        }

        // Clone rollup values
        var rollup_iter = self.rollup_values.iterator();
        while (rollup_iter.next()) |entry| {
            try new_node.rollup_values.put(self.allocator, entry.key_ptr.*, entry.value_ptr.*);
        }

        // Clone edges
        var edge_iter = self.edges.iterator();
        while (edge_iter.next()) |entry| {
            const cloned_targets = try entry.value_ptr.clone();
            try new_node.edges.put(self.allocator, entry.key_ptr.*, cloned_targets);
        }

        return new_node;
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "EdgeTargets add and contains" {
    var targets = EdgeTargets.init(std.testing.allocator);
    defer targets.deinit();

    try targets.add(5);
    try targets.add(3);
    try targets.add(7);
    try targets.add(1);

    try std.testing.expect(targets.contains(1));
    try std.testing.expect(targets.contains(3));
    try std.testing.expect(targets.contains(5));
    try std.testing.expect(targets.contains(7));
    try std.testing.expect(!targets.contains(2));
    try std.testing.expect(!targets.contains(10));

    try std.testing.expectEqual(@as(usize, 4), targets.count());
}

test "EdgeTargets maintains sorted order" {
    var targets = EdgeTargets.init(std.testing.allocator);
    defer targets.deinit();

    try targets.add(50);
    try targets.add(10);
    try targets.add(30);
    try targets.add(20);
    try targets.add(40);

    const items = targets.items();
    try std.testing.expectEqual(@as(usize, 5), items.len);
    try std.testing.expectEqual(@as(NodeId, 10), items[0]);
    try std.testing.expectEqual(@as(NodeId, 20), items[1]);
    try std.testing.expectEqual(@as(NodeId, 30), items[2]);
    try std.testing.expectEqual(@as(NodeId, 40), items[3]);
    try std.testing.expectEqual(@as(NodeId, 50), items[4]);
}

test "EdgeTargets add is idempotent" {
    var targets = EdgeTargets.init(std.testing.allocator);
    defer targets.deinit();

    try targets.add(5);
    try targets.add(5);
    try targets.add(5);

    try std.testing.expectEqual(@as(usize, 1), targets.count());
}

test "EdgeTargets remove" {
    var targets = EdgeTargets.init(std.testing.allocator);
    defer targets.deinit();

    try targets.add(1);
    try targets.add(2);
    try targets.add(3);

    try std.testing.expect(targets.remove(2));
    try std.testing.expect(!targets.contains(2));
    try std.testing.expectEqual(@as(usize, 2), targets.count());

    // Remove non-existent
    try std.testing.expect(!targets.remove(99));
}

test "EdgeTargets first" {
    var targets = EdgeTargets.init(std.testing.allocator);
    defer targets.deinit();

    try std.testing.expect(targets.first() == null);

    try targets.add(5);
    try targets.add(3);

    try std.testing.expectEqual(@as(NodeId, 3), targets.first().?);
}

test "Node properties" {
    var node = Node.init(std.testing.allocator, 1, 0);
    defer node.deinit();

    try node.setProperty("name", .{ .string = "Alice" });
    try node.setProperty("age", .{ .int = 30 });

    try std.testing.expectEqualStrings("Alice", node.getProperty("name").?.string);
    try std.testing.expectEqual(@as(i64, 30), node.getProperty("age").?.int);
    try std.testing.expect(node.getProperty("unknown") == null);

    // Remove property
    try node.setProperty("age", null);
    try std.testing.expect(node.getProperty("age") == null);
}

test "Node edges" {
    var node = Node.init(std.testing.allocator, 1, 0);
    defer node.deinit();

    try node.addEdge(0, 10);
    try node.addEdge(0, 20);
    try node.addEdge(1, 30);

    const edge0_targets = node.getEdgeTargets(0);
    try std.testing.expectEqual(@as(usize, 2), edge0_targets.len);

    const edge1_targets = node.getEdgeTargets(1);
    try std.testing.expectEqual(@as(usize, 1), edge1_targets.len);

    const edge2_targets = node.getEdgeTargets(2);
    try std.testing.expectEqual(@as(usize, 0), edge2_targets.len);
}

test "Node clone" {
    var node = Node.init(std.testing.allocator, 1, 0);
    defer node.deinit();

    try node.setProperty("name", .{ .string = "Alice" });
    try node.addEdge(0, 10);

    var cloned = try node.clone();
    defer cloned.deinit();

    try std.testing.expectEqual(node.id, cloned.id);
    try std.testing.expectEqual(node.type_id, cloned.type_id);
    try std.testing.expectEqualStrings("Alice", cloned.getProperty("name").?.string);
    try std.testing.expectEqual(@as(usize, 1), cloned.getEdgeTargets(0).len);

    // Modifications to clone don't affect original
    try cloned.setProperty("name", .{ .string = "Bob" });
    try std.testing.expectEqualStrings("Alice", node.getProperty("name").?.string);
}
