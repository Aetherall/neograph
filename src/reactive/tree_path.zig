///! Tree path parsing and resolution.
///!
///! Paths address nodes and edges within a reactive tree using a string format:
///!   - `type:id` - root node (e.g., "user:123")
///!   - `type:id/edge` - edge of root (e.g., "user:123/posts")
///!   - `type:id/edge:id` - child node (e.g., "user:123/posts:456")
///!   - `type:id/edge:id/edge` - edge of child (e.g., "user:123/posts:456/comments")
///!
///! The pattern alternates: node → edge → node → edge → ...
///! A path ending with `:id` points to a node; without `:id` points to an edge.

const std = @import("std");
const Allocator = std.mem.Allocator;

const NodeId = @import("../node.zig").NodeId;

pub const TreePath = struct {
    /// Original path string (not owned)
    raw: []const u8,

    /// Parsed segments
    segments: []Segment,

    /// Allocator used for segments
    allocator: Allocator,

    const Self = @This();

    pub const Segment = struct {
        /// Type name (first segment) or edge name (subsequent segments)
        name: []const u8,
        /// Node ID - required for first segment, optional for subsequent
        /// If null on non-first segment, this is the terminal edge
        id: ?NodeId,
    };

    pub const Target = union(enum) {
        /// Path points to a node
        node: NodeId,
        /// Path points to an edge
        edge: struct {
            parent_id: NodeId,
            edge_name: []const u8,
        },
    };

    pub const ParseError = error{
        EmptyPath,
        InvalidFormat,
        InvalidNodeId,
        MissingRootId,
    };

    /// Parse a path string.
    /// The path string must remain valid for the lifetime of the TreePath.
    pub fn parse(allocator: Allocator, path_str: []const u8) ParseError!Self {
        if (path_str.len == 0) return ParseError.EmptyPath;

        // Count segments by counting '/' separators
        var segment_count: usize = 1;
        for (path_str) |c| {
            if (c == '/') segment_count += 1;
        }

        const segments = allocator.alloc(Segment, segment_count) catch return ParseError.InvalidFormat;
        errdefer allocator.free(segments);

        var seg_idx: usize = 0;
        var iter = std.mem.splitScalar(u8, path_str, '/');

        while (iter.next()) |part| {
            if (part.len == 0) return ParseError.InvalidFormat;

            // Find ':' separator for id
            if (std.mem.indexOfScalar(u8, part, ':')) |colon_pos| {
                const name = part[0..colon_pos];
                const id_str = part[colon_pos + 1 ..];

                if (name.len == 0) return ParseError.InvalidFormat;
                if (id_str.len == 0) return ParseError.InvalidFormat;

                const id = std.fmt.parseInt(NodeId, id_str, 10) catch return ParseError.InvalidNodeId;

                segments[seg_idx] = .{ .name = name, .id = id };
            } else {
                // No ':' - only valid for non-first segments (edge without child id)
                if (seg_idx == 0) return ParseError.MissingRootId;

                segments[seg_idx] = .{ .name = part, .id = null };
            }

            seg_idx += 1;
        }

        return Self{
            .raw = path_str,
            .segments = segments[0..seg_idx],
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.segments);
    }

    /// Get what this path points to.
    pub fn target(self: *const Self) Target {
        const last = self.segments[self.segments.len - 1];

        if (self.segments.len == 1) {
            // Just root node - must have id
            return .{ .node = last.id.? };
        }

        if (last.id) |id| {
            // Ends at a node (has :id)
            return .{ .node = id };
        } else {
            // Ends at an edge (no :id) - parent is previous segment
            const parent_seg = self.segments[self.segments.len - 2];
            return .{ .edge = .{
                .parent_id = parent_seg.id.?,
                .edge_name = last.name,
            } };
        }
    }

    /// Check if path points to a node.
    pub fn isNode(self: *const Self) bool {
        return switch (self.target()) {
            .node => true,
            .edge => false,
        };
    }

    /// Check if path points to an edge.
    pub fn isEdge(self: *const Self) bool {
        return !self.isNode();
    }

    /// Get the root type name.
    pub fn rootType(self: *const Self) []const u8 {
        return self.segments[0].name;
    }

    /// Get the root node ID.
    pub fn rootId(self: *const Self) NodeId {
        return self.segments[0].id.?;
    }

    /// Get the depth (number of edge traversals from root).
    pub fn depth(self: *const Self) usize {
        // First segment is root node, each subsequent segment is an edge traversal
        return self.segments.len - 1;
    }

    /// Get parent path (one level up). Returns null if already at root.
    pub fn parent(self: *const Self, allocator: Allocator) ?Self {
        if (self.segments.len <= 1) return null;

        const parent_segments = allocator.alloc(Segment, self.segments.len - 1) catch return null;
        @memcpy(parent_segments, self.segments[0 .. self.segments.len - 1]);

        // Find the raw string length for parent
        var raw_len: usize = 0;
        var count: usize = 0;
        for (self.raw, 0..) |c, i| {
            if (c == '/') {
                count += 1;
                if (count == self.segments.len - 1) {
                    raw_len = i;
                    break;
                }
            }
        }
        if (raw_len == 0) raw_len = self.raw.len;

        return Self{
            .raw = self.raw[0..raw_len],
            .segments = parent_segments,
            .allocator = allocator,
        };
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;

test "TreePath: parse root node" {
    var path = try TreePath.parse(testing.allocator, "user:123");
    defer path.deinit();

    try testing.expectEqual(@as(usize, 1), path.segments.len);
    try testing.expectEqualStrings("user", path.segments[0].name);
    try testing.expectEqual(@as(NodeId, 123), path.segments[0].id.?);
    try testing.expect(path.isNode());
    try testing.expectEqual(@as(NodeId, 123), path.target().node);
}

test "TreePath: parse root edge" {
    var path = try TreePath.parse(testing.allocator, "user:123/posts");
    defer path.deinit();

    try testing.expectEqual(@as(usize, 2), path.segments.len);
    try testing.expectEqualStrings("user", path.segments[0].name);
    try testing.expectEqual(@as(NodeId, 123), path.segments[0].id.?);
    try testing.expectEqualStrings("posts", path.segments[1].name);
    try testing.expect(path.segments[1].id == null);

    try testing.expect(path.isEdge());
    const tgt = path.target();
    try testing.expectEqual(@as(NodeId, 123), tgt.edge.parent_id);
    try testing.expectEqualStrings("posts", tgt.edge.edge_name);
}

test "TreePath: parse child node" {
    var path = try TreePath.parse(testing.allocator, "user:123/posts:456");
    defer path.deinit();

    try testing.expectEqual(@as(usize, 2), path.segments.len);
    try testing.expect(path.isNode());
    try testing.expectEqual(@as(NodeId, 456), path.target().node);
}

test "TreePath: parse nested edge" {
    var path = try TreePath.parse(testing.allocator, "user:123/posts:456/comments");
    defer path.deinit();

    try testing.expectEqual(@as(usize, 3), path.segments.len);
    try testing.expect(path.isEdge());
    const tgt = path.target();
    try testing.expectEqual(@as(NodeId, 456), tgt.edge.parent_id);
    try testing.expectEqualStrings("comments", tgt.edge.edge_name);
}

test "TreePath: parse deep path" {
    var path = try TreePath.parse(testing.allocator, "session:1/threads:10/frames:100/variables");
    defer path.deinit();

    try testing.expectEqual(@as(usize, 4), path.segments.len);
    try testing.expectEqualStrings("session", path.rootType());
    try testing.expectEqual(@as(NodeId, 1), path.rootId());
    try testing.expectEqual(@as(usize, 3), path.depth());

    try testing.expect(path.isEdge());
    const tgt = path.target();
    try testing.expectEqual(@as(NodeId, 100), tgt.edge.parent_id);
    try testing.expectEqualStrings("variables", tgt.edge.edge_name);
}

test "TreePath: empty path error" {
    const result = TreePath.parse(testing.allocator, "");
    try testing.expectError(TreePath.ParseError.EmptyPath, result);
}

test "TreePath: missing root id error" {
    const result = TreePath.parse(testing.allocator, "user/posts");
    try testing.expectError(TreePath.ParseError.MissingRootId, result);
}

test "TreePath: invalid node id error" {
    const result = TreePath.parse(testing.allocator, "user:abc");
    try testing.expectError(TreePath.ParseError.InvalidNodeId, result);
}

test "TreePath: parent path" {
    var path = try TreePath.parse(testing.allocator, "user:123/posts:456/comments");
    defer path.deinit();

    var parent_path = path.parent(testing.allocator).?;
    defer parent_path.deinit();

    try testing.expectEqual(@as(usize, 2), parent_path.segments.len);
    try testing.expect(parent_path.isNode());
    try testing.expectEqual(@as(NodeId, 456), parent_path.target().node);
}
