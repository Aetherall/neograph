///! Inverted edge index for efficient reverse lookups.
///!
///! Maps target_id -> [(source_id, source_type_id, edge_id), ...]
///! Enables O(S) cache invalidation instead of O(N) full scans,
///! where S = number of sources pointing to a target.

const std = @import("std");
const Allocator = std.mem.Allocator;

const NodeId = @import("../node.zig").NodeId;
const TypeId = @import("../node.zig").TypeId;
const EdgeId = @import("../node.zig").EdgeId;

/// Reference to a source node that points to a target via an edge.
pub const EdgeRef = struct {
    source_id: NodeId,
    source_type_id: TypeId,
    edge_id: EdgeId,

    pub fn eql(self: EdgeRef, other: EdgeRef) bool {
        return self.source_id == other.source_id and
            self.source_type_id == other.source_type_id and
            self.edge_id == other.edge_id;
    }
};

/// List of edge references for a single target.
const EdgeRefList = std.ArrayListUnmanaged(EdgeRef);

/// Inverted edge index for O(1) reverse lookups.
///
/// Usage:
/// ```
/// var index = InvertedEdgeIndex.init(allocator);
/// defer index.deinit();
///
/// // When linking source -> target
/// index.onLink(source_id, source_type_id, edge_id, target_id);
///
/// // When unlinking
/// index.onUnlink(source_id, source_type_id, edge_id, target_id);
///
/// // Find all sources pointing to a target
/// const sources = index.getSourcesFor(target_id);
/// for (sources) |ref| {
///     // ref.source_id points to target_id via ref.edge_id
/// }
/// ```
pub const InvertedEdgeIndex = struct {
    /// target_id -> list of (source_id, source_type_id, edge_id)
    index: std.AutoHashMapUnmanaged(NodeId, EdgeRefList),
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

    /// Record that source_id now points to target_id via edge_id.
    /// Called when a link is created.
    pub fn onLink(
        self: *Self,
        source_id: NodeId,
        source_type_id: TypeId,
        edge_id: EdgeId,
        target_id: NodeId,
    ) void {
        const ref = EdgeRef{
            .source_id = source_id,
            .source_type_id = source_type_id,
            .edge_id = edge_id,
        };

        const result = self.index.getOrPut(self.allocator, target_id) catch return;
        if (!result.found_existing) {
            result.value_ptr.* = EdgeRefList{};
        }

        // Check if already exists (idempotent)
        for (result.value_ptr.items) |existing| {
            if (existing.eql(ref)) return;
        }

        result.value_ptr.append(self.allocator, ref) catch return;
    }

    /// Remove the record that source_id points to target_id via edge_id.
    /// Called when a link is removed.
    pub fn onUnlink(
        self: *Self,
        source_id: NodeId,
        source_type_id: TypeId,
        edge_id: EdgeId,
        target_id: NodeId,
    ) void {
        const list = self.index.getPtr(target_id) orelse return;

        const ref = EdgeRef{
            .source_id = source_id,
            .source_type_id = source_type_id,
            .edge_id = edge_id,
        };

        // Find and remove
        for (list.items, 0..) |existing, i| {
            if (existing.eql(ref)) {
                _ = list.swapRemove(i);
                break;
            }
        }

        // Clean up empty lists
        if (list.items.len == 0) {
            list.deinit(self.allocator);
            _ = self.index.remove(target_id);
        }
    }

    /// Remove all references where source_id is the source.
    /// Called when a node is deleted.
    pub fn removeSource(self: *Self, source_id: NodeId) void {
        var targets_to_clean = std.ArrayListUnmanaged(NodeId){};
        defer targets_to_clean.deinit(self.allocator);

        // Find all entries that reference this source
        var iter = self.index.iterator();
        while (iter.next()) |entry| {
            const target_id = entry.key_ptr.*;
            const list = entry.value_ptr;

            // Remove all refs from this source
            var i: usize = 0;
            while (i < list.items.len) {
                if (list.items[i].source_id == source_id) {
                    _ = list.swapRemove(i);
                } else {
                    i += 1;
                }
            }

            // Mark for cleanup if empty
            if (list.items.len == 0) {
                targets_to_clean.append(self.allocator, target_id) catch {};
            }
        }

        // Clean up empty lists
        for (targets_to_clean.items) |target_id| {
            if (self.index.getPtr(target_id)) |list| {
                list.deinit(self.allocator);
            }
            _ = self.index.remove(target_id);
        }
    }

    /// Remove all references to a target.
    /// Called when a target node is deleted.
    pub fn removeTarget(self: *Self, target_id: NodeId) void {
        if (self.index.fetchRemove(target_id)) |entry| {
            var list = entry.value;
            list.deinit(self.allocator);
        }
    }

    /// Get all sources that point to a target.
    /// Returns empty slice if no sources.
    pub fn getSourcesFor(self: *const Self, target_id: NodeId) []const EdgeRef {
        if (self.index.get(target_id)) |list| {
            return list.items;
        }
        return &.{};
    }

    /// Get all sources of a specific type that point to a target.
    pub fn getSourcesForByType(
        self: *const Self,
        target_id: NodeId,
        source_type_id: TypeId,
        allocator: Allocator,
    ) ![]EdgeRef {
        const all = self.getSourcesFor(target_id);
        var filtered = std.ArrayListUnmanaged(EdgeRef){};

        for (all) |ref| {
            if (ref.source_type_id == source_type_id) {
                try filtered.append(allocator, ref);
            }
        }

        return filtered.toOwnedSlice(allocator);
    }

    /// Get count of sources pointing to a target.
    pub fn countSourcesFor(self: *const Self, target_id: NodeId) usize {
        if (self.index.get(target_id)) |list| {
            return list.items.len;
        }
        return 0;
    }

    /// Get total number of tracked edges.
    pub fn totalEdges(self: *const Self) usize {
        var total: usize = 0;
        var iter = self.index.iterator();
        while (iter.next()) |entry| {
            total += entry.value_ptr.items.len;
        }
        return total;
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;

test "InvertedEdgeIndex basic operations" {
    var index = InvertedEdgeIndex.init(testing.allocator);
    defer index.deinit();

    // Link: post(1) --author--> user(100)
    index.onLink(1, 0, 0, 100);

    // Link: post(2) --author--> user(100)
    index.onLink(2, 0, 0, 100);

    // Link: post(3) --author--> user(200)
    index.onLink(3, 0, 0, 200);

    // Check sources for user 100
    const sources_100 = index.getSourcesFor(100);
    try testing.expectEqual(@as(usize, 2), sources_100.len);

    // Check sources for user 200
    const sources_200 = index.getSourcesFor(200);
    try testing.expectEqual(@as(usize, 1), sources_200.len);
    try testing.expectEqual(@as(NodeId, 3), sources_200[0].source_id);

    // Check non-existent target
    const sources_999 = index.getSourcesFor(999);
    try testing.expectEqual(@as(usize, 0), sources_999.len);
}

test "InvertedEdgeIndex onUnlink" {
    var index = InvertedEdgeIndex.init(testing.allocator);
    defer index.deinit();

    // Create links
    index.onLink(1, 0, 0, 100);
    index.onLink(2, 0, 0, 100);
    try testing.expectEqual(@as(usize, 2), index.countSourcesFor(100));

    // Unlink one
    index.onUnlink(1, 0, 0, 100);
    try testing.expectEqual(@as(usize, 1), index.countSourcesFor(100));

    // Unlink the other
    index.onUnlink(2, 0, 0, 100);
    try testing.expectEqual(@as(usize, 0), index.countSourcesFor(100));
}

test "InvertedEdgeIndex removeSource" {
    var index = InvertedEdgeIndex.init(testing.allocator);
    defer index.deinit();

    // Node 1 points to multiple targets
    index.onLink(1, 0, 0, 100);
    index.onLink(1, 0, 1, 200);
    index.onLink(1, 0, 0, 300);

    // Node 2 also points to target 100
    index.onLink(2, 0, 0, 100);

    try testing.expectEqual(@as(usize, 2), index.countSourcesFor(100));
    try testing.expectEqual(@as(usize, 1), index.countSourcesFor(200));
    try testing.expectEqual(@as(usize, 1), index.countSourcesFor(300));

    // Remove source 1
    index.removeSource(1);

    try testing.expectEqual(@as(usize, 1), index.countSourcesFor(100)); // Only node 2 left
    try testing.expectEqual(@as(usize, 0), index.countSourcesFor(200)); // Cleaned up
    try testing.expectEqual(@as(usize, 0), index.countSourcesFor(300)); // Cleaned up
}

test "InvertedEdgeIndex removeTarget" {
    var index = InvertedEdgeIndex.init(testing.allocator);
    defer index.deinit();

    // Multiple sources point to target 100
    index.onLink(1, 0, 0, 100);
    index.onLink(2, 0, 0, 100);
    index.onLink(3, 0, 0, 100);

    try testing.expectEqual(@as(usize, 3), index.countSourcesFor(100));

    // Remove target
    index.removeTarget(100);

    try testing.expectEqual(@as(usize, 0), index.countSourcesFor(100));
}

test "InvertedEdgeIndex idempotent onLink" {
    var index = InvertedEdgeIndex.init(testing.allocator);
    defer index.deinit();

    // Same link multiple times
    index.onLink(1, 0, 0, 100);
    index.onLink(1, 0, 0, 100);
    index.onLink(1, 0, 0, 100);

    // Should only have one entry
    try testing.expectEqual(@as(usize, 1), index.countSourcesFor(100));
}

test "InvertedEdgeIndex different edge types" {
    var index = InvertedEdgeIndex.init(testing.allocator);
    defer index.deinit();

    // Same source to same target but different edges
    index.onLink(1, 0, 0, 100); // edge 0
    index.onLink(1, 0, 1, 100); // edge 1

    const sources = index.getSourcesFor(100);
    try testing.expectEqual(@as(usize, 2), sources.len);

    // Verify different edge IDs
    var edge_ids = [_]EdgeId{ sources[0].edge_id, sources[1].edge_id };
    std.mem.sort(EdgeId, &edge_ids, {}, std.sort.asc(EdgeId));
    try testing.expectEqual(@as(EdgeId, 0), edge_ids[0]);
    try testing.expectEqual(@as(EdgeId, 1), edge_ids[1]);
}

test "InvertedEdgeIndex totalEdges" {
    var index = InvertedEdgeIndex.init(testing.allocator);
    defer index.deinit();

    try testing.expectEqual(@as(usize, 0), index.totalEdges());

    index.onLink(1, 0, 0, 100);
    index.onLink(2, 0, 0, 100);
    index.onLink(3, 0, 0, 200);

    try testing.expectEqual(@as(usize, 3), index.totalEdges());

    index.onUnlink(1, 0, 0, 100);
    try testing.expectEqual(@as(usize, 2), index.totalEdges());
}
