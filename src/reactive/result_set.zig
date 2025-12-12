///! Result set with linked list for O(1) reordering.
///!
///! Maintains a doubly linked list of result nodes with a hash map
///! for O(1) lookup by id. Supports efficient insertion, removal, and reordering.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Order = std.math.Order;

const CompoundKey = @import("../index/key.zig").CompoundKey;
const NodeId = @import("../node.zig").NodeId;
const IntrusiveList = @import("../ds.zig").IntrusiveList;

/// A node in the result set linked list.
pub const ResultNode = struct {
    id: NodeId,
    key: CompoundKey, // cached for comparison during insert
    /// Path from root: [root_id, level1_id, ...] (excludes self).
    /// Used for composite key recomputation when virtual ancestors change.
    ancestry: []NodeId = &.{},
    /// The edge name used by the parent to link to this node.
    /// Used by lazy loading to track which edge to decrement on removal.
    edge_name: ?[]const u8 = null,
    /// Number of parent edges pointing to this node (for DAG multi-parent support).
    /// Node is only removed from result set when parent_count reaches 0.
    parent_count: u32 = 1,
    prev: ?*ResultNode = null,
    next: ?*ResultNode = null,
};

/// Doubly linked list of results with O(1) operations.
pub const ResultSet = struct {
    list: IntrusiveList(ResultNode, "prev", "next") = .{},
    by_id: std.AutoHashMapUnmanaged(NodeId, *ResultNode) = .{},
    allocator: Allocator,

    const Self = @This();
    const List = IntrusiveList(ResultNode, "prev", "next");

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        // Free all nodes
        var current = self.list.head;
        while (current) |node| {
            const next_node = List.next(node);
            if (node.ancestry.len > 0) {
                self.allocator.free(node.ancestry);
            }
            self.allocator.destroy(node);
            current = next_node;
        }
        self.by_id.deinit(self.allocator);
    }

    /// Clear all nodes without deallocating the result set.
    /// Used for viewport-based reloading.
    pub fn clear(self: *Self) void {
        // Free all nodes
        var current = self.list.head;
        while (current) |node| {
            const next_node = List.next(node);
            if (node.ancestry.len > 0) {
                self.allocator.free(node.ancestry);
            }
            self.allocator.destroy(node);
            current = next_node;
        }
        self.list.head = null;
        self.list.tail = null;
        self.list.len = 0;
        self.by_id.clearRetainingCapacity();
    }

    /// Get the count of nodes. O(1).
    pub fn count(self: *const Self) u32 {
        return @intCast(self.list.len);
    }

    /// Check if an id is in the result set. O(1).
    pub fn contains(self: *const Self, id: NodeId) bool {
        return self.by_id.contains(id);
    }

    /// Get the node for an id. O(1).
    pub fn getNode(self: *const Self, id: NodeId) ?*ResultNode {
        return self.by_id.get(id);
    }

    /// Get the index of a node. O(n) - walk from head.
    pub fn indexOf(self: *const Self, id: NodeId) ?u32 {
        const target = self.by_id.get(id) orelse return null;

        var idx: u32 = 0;
        var current = self.list.head;
        while (current) |node| : (idx += 1) {
            if (node == target) return idx;
            current = List.next(node);
        }
        return null;
    }

    /// Insert a node in sorted position. O(n) walk to find position.
    /// If ancestry is provided, it is owned by the ResultNode and will be freed on removal.
    pub fn insertSorted(self: *Self, id: NodeId, key: CompoundKey, ancestry: ?[]NodeId) !*ResultNode {
        return self.insertSortedWithEdge(id, key, ancestry, null);
    }

    /// Insert a node in sorted position with edge name tracking.
    /// If ancestry is provided, it is owned by the ResultNode and will be freed on removal.
    /// edge_name should be a stable string (not freed during result_set lifetime).
    pub fn insertSortedWithEdge(self: *Self, id: NodeId, key: CompoundKey, ancestry: ?[]NodeId, edge_name: ?[]const u8) !*ResultNode {
        const node = try self.allocator.create(ResultNode);
        node.* = .{ .id = id, .key = key, .ancestry = ancestry orelse &.{}, .edge_name = edge_name };

        // Find insert position by walking list from tail
        const after = self.findInsertPos(key);
        self.list.insertAfter(node, after);

        try self.by_id.put(self.allocator, id, node);
        return node;
    }

    /// Remove a node by id. O(1).
    /// Returns the removed node. Caller must free ancestry if non-empty and destroy the node.
    pub fn remove(self: *Self, id: NodeId) ?*ResultNode {
        const node = self.by_id.get(id) orelse return null;
        self.list.remove(node);
        _ = self.by_id.remove(id);
        return node;
    }

    /// Remove a node by id and free all its resources.
    pub fn removeAndFree(self: *Self, id: NodeId) void {
        if (self.remove(id)) |node| {
            if (node.ancestry.len > 0) {
                self.allocator.free(node.ancestry);
            }
            self.allocator.destroy(node);
        }
    }

    /// Increment parent count for a node (multi-parent DAG support).
    /// Returns the new parent count, or null if node doesn't exist.
    pub fn incrementParentCount(self: *Self, id: NodeId) ?u32 {
        if (self.by_id.get(id)) |node| {
            node.parent_count += 1;
            return node.parent_count;
        }
        return null;
    }

    /// Decrement parent count for a node (multi-parent DAG support).
    /// Returns true if node should be removed (parent_count reached 0).
    /// Returns false if node still has parents or doesn't exist.
    pub fn decrementParentCount(self: *Self, id: NodeId) bool {
        if (self.by_id.get(id)) |node| {
            if (node.parent_count > 0) {
                node.parent_count -= 1;
            }
            return node.parent_count == 0;
        }
        return false;
    }

    /// Get the parent count for a node.
    pub fn getParentCount(self: *const Self, id: NodeId) ?u32 {
        if (self.by_id.get(id)) |node| {
            return node.parent_count;
        }
        return null;
    }

    /// Move a node to a new position. O(1).
    pub fn move(self: *Self, node: *ResultNode, after: ?*ResultNode) void {
        self.list.moveAfter(node, after);
    }

    /// Find insert position for key. O(n) walk from tail.
    pub fn findInsertPos(self: *const Self, key: CompoundKey) ?*ResultNode {
        var current = self.list.tail;
        while (current) |node| {
            if (node.key.order(key) != .gt) {
                return node;
            }
            current = List.prev(node);
        }
        return null; // insert at head
    }

    /// Iterator for traversal.
    pub fn iterator(self: *const Self) List.Iterator {
        return self.list.iterator();
    }

    /// Get all ids as a slice. Caller must free.
    pub fn toSlice(self: *const Self) ![]NodeId {
        const ids = try self.allocator.alloc(NodeId, self.list.len);
        var idx: usize = 0;
        var current = self.list.head;
        while (current) |node| : (idx += 1) {
            ids[idx] = node.id;
            current = List.next(node);
        }
        return ids;
    }

};

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;
const Value = @import("../value.zig").Value;
const SortDir = @import("../schema.zig").SortDir;

test "ResultSet insert and contains" {
    var rs = ResultSet.init(testing.allocator);
    defer rs.deinit();

    const key1 = CompoundKey.encodePartial(&.{Value{ .int = 10 }}, &.{.asc});
    const key2 = CompoundKey.encodePartial(&.{Value{ .int = 20 }}, &.{.asc});
    const key3 = CompoundKey.encodePartial(&.{Value{ .int = 15 }}, &.{.asc});

    _ = try rs.insertSorted(1, key1, null);
    _ = try rs.insertSorted(2, key2, null);
    _ = try rs.insertSorted(3, key3, null);

    try testing.expect(rs.contains(1));
    try testing.expect(rs.contains(2));
    try testing.expect(rs.contains(3));
    try testing.expect(!rs.contains(99));
    try testing.expectEqual(@as(u32, 3), rs.count());
}

test "ResultSet maintains sorted order" {
    var rs = ResultSet.init(testing.allocator);
    defer rs.deinit();

    // Insert out of order
    const key3 = CompoundKey.encodePartial(&.{Value{ .int = 30 }}, &.{.asc});
    const key1 = CompoundKey.encodePartial(&.{Value{ .int = 10 }}, &.{.asc});
    const key2 = CompoundKey.encodePartial(&.{Value{ .int = 20 }}, &.{.asc});

    _ = try rs.insertSorted(3, key3, null);
    _ = try rs.insertSorted(1, key1, null);
    _ = try rs.insertSorted(2, key2, null);

    // Should be in order: 1, 2, 3
    const ids = try rs.toSlice();
    defer testing.allocator.free(ids);

    try testing.expectEqual(@as(u64, 1), ids[0]);
    try testing.expectEqual(@as(u64, 2), ids[1]);
    try testing.expectEqual(@as(u64, 3), ids[2]);
}

test "ResultSet remove" {
    var rs = ResultSet.init(testing.allocator);
    defer rs.deinit();

    const key1 = CompoundKey.encodePartial(&.{Value{ .int = 10 }}, &.{.asc});
    const key2 = CompoundKey.encodePartial(&.{Value{ .int = 20 }}, &.{.asc});

    _ = try rs.insertSorted(1, key1, null);
    _ = try rs.insertSorted(2, key2, null);

    const removed = rs.remove(1);
    try testing.expect(removed != null);
    testing.allocator.destroy(removed.?);

    try testing.expect(!rs.contains(1));
    try testing.expect(rs.contains(2));
    try testing.expectEqual(@as(u32, 1), rs.count());
}

test "ResultSet move" {
    var rs = ResultSet.init(testing.allocator);
    defer rs.deinit();

    const key1 = CompoundKey.encodePartial(&.{Value{ .int = 10 }}, &.{.asc});
    const key2 = CompoundKey.encodePartial(&.{Value{ .int = 20 }}, &.{.asc});
    const key3 = CompoundKey.encodePartial(&.{Value{ .int = 30 }}, &.{.asc});

    const node1 = try rs.insertSorted(1, key1, null);
    const node2 = try rs.insertSorted(2, key2, null);
    _ = try rs.insertSorted(3, key3, null);

    // Move node 1 after node 2
    rs.move(node1, node2);

    const ids = try rs.toSlice();
    defer testing.allocator.free(ids);

    try testing.expectEqual(@as(u64, 2), ids[0]);
    try testing.expectEqual(@as(u64, 1), ids[1]);
    try testing.expectEqual(@as(u64, 3), ids[2]);
}

test "ResultSet indexOf" {
    var rs = ResultSet.init(testing.allocator);
    defer rs.deinit();

    const key1 = CompoundKey.encodePartial(&.{Value{ .int = 10 }}, &.{.asc});
    const key2 = CompoundKey.encodePartial(&.{Value{ .int = 20 }}, &.{.asc});
    const key3 = CompoundKey.encodePartial(&.{Value{ .int = 30 }}, &.{.asc});

    _ = try rs.insertSorted(1, key1, null);
    _ = try rs.insertSorted(2, key2, null);
    _ = try rs.insertSorted(3, key3, null);

    try testing.expectEqual(@as(?u32, 0), rs.indexOf(1));
    try testing.expectEqual(@as(?u32, 1), rs.indexOf(2));
    try testing.expectEqual(@as(?u32, 2), rs.indexOf(3));
    try testing.expectEqual(@as(?u32, null), rs.indexOf(99));
}

test "ResultSet iterator" {
    var rs = ResultSet.init(testing.allocator);
    defer rs.deinit();

    const key1 = CompoundKey.encodePartial(&.{Value{ .int = 10 }}, &.{.asc});
    const key2 = CompoundKey.encodePartial(&.{Value{ .int = 20 }}, &.{.asc});

    _ = try rs.insertSorted(1, key1, null);
    _ = try rs.insertSorted(2, key2, null);

    var iter = rs.iterator();
    var count: u32 = 0;
    while (iter.next()) |_| {
        count += 1;
    }
    try testing.expectEqual(@as(u32, 2), count);
}
