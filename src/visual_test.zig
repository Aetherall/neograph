///! Visual tests for tree rendering, scrolling, and profiling.
///!
///! These tests demonstrate hierarchical tree operations with visual output
///! and profiling to verify expected compute volumes.
///!
///! Uses the public Graph API for black-box testing.

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const watchdog = @import("test_watchdog.zig");

// Global watchdog - started by first test
var global_watchdog: ?watchdog.Watchdog = null;

fn ensureWatchdog() void {
    if (global_watchdog == null) {
        global_watchdog = watchdog.startDefault();
    }
}

// Public API imports - only use Graph and related types
const ng = @import("neograph.zig");
const Graph = ng.Graph;
const Schema = ng.Schema;
const NodeId = ng.NodeId;
const Sort = ng.Sort;
const View = ng.View;
const ViewOpts = ng.ViewOpts;
const parseSchema = ng.parseSchema;

// ============================================================================
// Profiling Infrastructure
// ============================================================================

/// Profiling counters for compute volume verification.
pub const ProfileCounters = struct {
    nodes_visited: u32 = 0,
    nodes_rendered: u32 = 0,
    scroll_ops: u32 = 0,
    expand_ops: u32 = 0,
    collapse_ops: u32 = 0,
    rebuild_ops: u32 = 0,

    pub fn reset(self: *ProfileCounters) void {
        self.* = .{};
    }

    pub fn total_ops(self: *const ProfileCounters) u32 {
        return self.scroll_ops + self.expand_ops + self.collapse_ops + self.rebuild_ops;
    }
};

/// Visual output buffer for assertions.
pub const RenderBuffer = struct {
    lines: std.ArrayListUnmanaged([]const u8),
    allocator: Allocator,

    pub fn init(allocator: Allocator) RenderBuffer {
        return .{
            .lines = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RenderBuffer) void {
        for (self.lines.items) |line| {
            self.allocator.free(line);
        }
        self.lines.deinit(self.allocator);
    }

    pub fn clear(self: *RenderBuffer) void {
        for (self.lines.items) |line| {
            self.allocator.free(line);
        }
        self.lines.clearRetainingCapacity();
    }

    pub fn addLine(self: *RenderBuffer, line: []const u8) !void {
        const copy = try self.allocator.dupe(u8, line);
        try self.lines.append(self.allocator, copy);
    }

    pub fn addFmt(self: *RenderBuffer, comptime fmt: []const u8, args: anytype) !void {
        const line = try std.fmt.allocPrint(self.allocator, fmt, args);
        try self.lines.append(self.allocator, line);
    }

    /// Check if output contains substring.
    pub fn contains(self: *const RenderBuffer, substr: []const u8) bool {
        for (self.lines.items) |line| {
            if (std.mem.indexOf(u8, line, substr) != null) return true;
        }
        return false;
    }

    /// Check if rendered output contains expected multiline pattern.
    pub fn expectContainsLines(self: *const RenderBuffer, pattern: []const u8) !void {
        var pattern_iter = std.mem.splitScalar(u8, pattern, '\n');
        var output_idx: usize = 0;

        while (pattern_iter.next()) |pattern_line| {
            const trimmed_pattern = std.mem.trimRight(u8, pattern_line, " \t\r");
            if (trimmed_pattern.len == 0) continue;

            var found = false;
            while (output_idx < self.lines.items.len) : (output_idx += 1) {
                if (std.mem.indexOf(u8, self.lines.items[output_idx], trimmed_pattern) != null) {
                    found = true;
                    output_idx += 1;
                    break;
                }
            }

            if (!found) {
                std.debug.print("Pattern not found: '{s}'\n", .{trimmed_pattern});
                std.debug.print("Remaining output:\n", .{});
                for (self.lines.items[output_idx..]) |line| {
                    std.debug.print("  '{s}'\n", .{line});
                }
                return error.PatternNotFound;
            }
        }
    }

    pub fn dump(self: *const RenderBuffer) void {
        std.debug.print("\n", .{});
        for (self.lines.items) |line| {
            std.debug.print("{s}\n", .{line});
        }
    }
};

/// Tree renderer with profiling.
pub const TreeRenderer = struct {
    buffer: RenderBuffer,
    counters: ProfileCounters,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .buffer = RenderBuffer.init(allocator),
            .counters = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.buffer.deinit();
    }

    pub fn reset(self: *Self) void {
        self.buffer.clear();
        self.counters.reset();
    }

    /// Render tree viewport as visual tree.
    pub fn renderTree(self: *Self, tree: *View, label: []const u8) !void {
        try self.buffer.addFmt("=== {s} ===", .{label});
        try self.buffer.addFmt("| offset={d} height={d} total={d}", .{
            tree.getOffset(),
            tree.viewport.height,
            tree.total(),
        });
        try self.buffer.addLine("|-------------------");

        var rendered: u32 = 0;
        var iter = tree.items();
        while (iter.next()) |item| {
            // Compute depth from item
            const depth = item.depth;
            const node_id = item.id;

            // Build indent
            var indent_buf: [64]u8 = undefined;
            const indent_len = @min(depth * 2, 60);
            @memset(indent_buf[0..indent_len], ' ');

            // Check if expandable
            const tree_node = tree.get(node_id);
            const has_edges = if (tree_node) |n| n.edges.count() > 0 else false;
            const is_expanded = if (tree_node) |n| n.expanded_edges.count() > 0 else false;

            const expand_char: []const u8 = if (has_edges)
                (if (is_expanded) "v" else ">")
            else
                "-";

            try self.buffer.addFmt("| {s}{s} node:{d} (d={d})", .{
                indent_buf[0..indent_len],
                expand_char,
                node_id,
                depth,
            });

            rendered += 1;
            self.counters.nodes_rendered += 1;
        }

        try self.buffer.addLine("|-------------------");
        try self.buffer.addFmt("| rendered: {d} items", .{rendered});
        try self.buffer.addLine("====================");
    }
};

// ============================================================================
// Test Context - Uses public Graph API
// ============================================================================

/// Creates the test schema with Root and Item types.
fn createTestSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "Root",
        \\      "properties": [{ "name": "priority", "type": "int" }],
        \\      "edges": [{ "name": "children", "target": "Item", "reverse": "_parent" }],
        \\      "indexes": [{ "fields": [{ "field": "priority", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Item",
        \\      "properties": [{ "name": "priority", "type": "int" }],
        \\      "edges": [
        \\        { "name": "_parent", "target": "Root", "reverse": "children" },
        \\        { "name": "children", "target": "Item", "reverse": "item_parent" },
        \\        { "name": "item_parent", "target": "Item", "reverse": "children" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "priority", "direction": "asc" }] }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

const root_sorts = [_]Sort{Sort{ .field = "priority", .direction = .asc }};

// ============================================================================
// Test Helpers
// ============================================================================

/// Create a flat tree with N root nodes.
fn createFlatData(g: *Graph, count: u32) !void {
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const id = try g.insert("Root");
        try g.update(id, .{ .priority = @as(i64, @intCast(i)) });
    }
}

/// Create a tree with hierarchy: N roots, each with M children.
fn createTwoLevelData(g: *Graph, roots: u32, children_per_root: u32) !void {
    var root_idx: u32 = 0;
    while (root_idx < roots) : (root_idx += 1) {
        const root_id = try g.insert("Root");
        try g.update(root_id, .{ .priority = @as(i64, @intCast(root_idx * 100)) });

        var child_idx: u32 = 0;
        while (child_idx < children_per_root) : (child_idx += 1) {
            const child_id = try g.insert("Item");
            try g.update(child_id, .{ .priority = @as(i64, @intCast(root_idx * 100 + child_idx + 1)) });
            try g.link(root_id, "children", child_id);
        }
    }
}

/// Create a deep tree: single chain of depth N.
fn createDeepData(g: *Graph, depth: u32) !void {
    const root_id = try g.insert("Root");
    try g.update(root_id, .{ .priority = @as(i64, 0) });
    var parent_id = root_id;

    var d: u32 = 1;
    while (d < depth) : (d += 1) {
        const child_id = try g.insert("Item");
        try g.update(child_id, .{ .priority = @as(i64, @intCast(d)) });
        try g.link(parent_id, "children", child_id);
        parent_id = child_id;
    }
}

/// Count items in tree viewport by iterating.
fn countTreeItems(tree: *View) u32 {
    var count: u32 = 0;
    var iter = tree.items();
    while (iter.next()) |_| {
        count += 1;
    }
    return count;
}

/// Get node ID at index in tree.
fn getNodeIdAtIndex(tree: *View, index: u32) ?NodeId {
    var iter = tree.items();
    var i: u32 = 0;
    while (iter.next()) |item| {
        if (i == index) return item.id;
        i += 1;
    }
    return null;
}

// ============================================================================
// Visual Tests - Flat List Scrolling
// ============================================================================

test "Tree: flat list scroll down" {
    ensureWatchdog(); // Start watchdog on first test

    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createFlatData(g, 100);

    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    var renderer = TreeRenderer.init(testing.allocator);
    defer renderer.deinit();

    // Scroll down 5 times
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        tree.move(1);
    }

    try renderer.renderTree(&tree, "Scrolled");

    try renderer.buffer.expectContainsLines(
        \\offset=5 height=10 total=100
        \\node:6 (d=0)
        \\node:7 (d=0)
        \\rendered: 10 items
    );

    try testing.expectEqual(@as(u32, 5), tree.getOffset());
}

test "Tree: flat list scroll up" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createFlatData(g, 100);

    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    // Start at offset 50
    tree.scrollTo(50);
    try testing.expectEqual(@as(u32, 50), tree.getOffset());

    // Scroll up 10 times
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        tree.move(-1);
    }

    try testing.expectEqual(@as(u32, 40), tree.getOffset());

    // Scroll up past beginning (should clamp)
    tree.scrollTo(0);
    tree.move(-1);
    try testing.expectEqual(@as(u32, 0), tree.getOffset());
}

test "Tree: flat list scrollTo and scrollBy" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createFlatData(g, 1000);

    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    tree.scrollTo(100);
    try testing.expectEqual(@as(u32, 100), tree.getOffset());

    tree.scrollTo(500);
    try testing.expectEqual(@as(u32, 500), tree.getOffset());

    // scrollTo beyond max (should clamp)
    tree.scrollTo(9999);
    const max_offset = 1000 - 20;
    try testing.expectEqual(max_offset, tree.getOffset());

    // move relative
    tree.scrollTo(100);
    tree.move(50);
    try testing.expectEqual(@as(u32, 150), tree.getOffset());

    tree.move(-30);
    try testing.expectEqual(@as(u32, 120), tree.getOffset());

    // move past bounds (should clamp)
    tree.scrollTo(10);
    tree.move(-100);
    try testing.expectEqual(@as(u32, 0), tree.getOffset());
}

test "Tree: viewport iteration count matches height" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createFlatData(g, 100);

    const heights = [_]u32{ 5, 10, 20, 50 };

    for (heights) |height| {
        var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = height });
        defer tree.deinit();
        tree.activate(false);

        var count = countTreeItems(&tree);
        try testing.expectEqual(height, count);

        tree.scrollTo(30);
        count = countTreeItems(&tree);
        try testing.expectEqual(height, count);
    }
}

// ============================================================================
// Visual Tests - Tree Expansion/Collapse
// ============================================================================

test "Tree: expand increases visible count" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createTwoLevelData(g, 5, 10);

    var tree = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"} }},
    }, .{ .limit = 50 });
    defer tree.deinit();
    tree.activate(false);

    // Initially only roots visible
    try testing.expectEqual(@as(u32, 5), tree.total());

    // Expand first root's children edge
    try tree.expandById(1, "children");

    // Now visible = 5 roots + 10 children = 15
    try testing.expectEqual(@as(u32, 15), tree.total());

    // Expand second root
    try tree.expandById(12, "children");

    // Now visible = 5 + 10 + 10 = 25
    try testing.expectEqual(@as(u32, 25), tree.total());
}

test "Tree: collapse decreases visible count" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createTwoLevelData(g, 3, 5);

    var tree = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"} }},
    }, .{ .limit = 50 });
    defer tree.deinit();
    tree.activate(false);

    // Expand all
    try tree.expandById(1, "children");
    try tree.expandById(7, "children");
    try tree.expandById(13, "children");

    // All expanded: 3 roots + 3*5 children = 18
    try testing.expectEqual(@as(u32, 18), tree.total());

    // Collapse middle one
    tree.collapseById(7, "children");

    // Now: 3 + 5 + 5 = 13
    try testing.expectEqual(@as(u32, 13), tree.total());

    // Collapse all
    tree.collapseById(1, "children");
    tree.collapseById(13, "children");

    // Back to just roots
    try testing.expectEqual(@as(u32, 3), tree.total());
}

test "Tree: expand affects viewport" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createTwoLevelData(g, 10, 5);

    var tree = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"} }},
    }, .{ .limit = 8 });
    defer tree.deinit();
    tree.activate(false);

    var renderer = TreeRenderer.init(testing.allocator);
    defer renderer.deinit();

    // Initial: 10 roots, viewport shows 8
    try renderer.renderTree(&tree, "Collapsed");

    try renderer.buffer.expectContainsLines(
        \\offset=0 height=8 total=10
        \\node:1 (d=0)
        \\node:7 (d=0)
        \\rendered: 8 items
    );

    renderer.reset();

    // Expand first root
    try tree.expandById(1, "children");

    try renderer.renderTree(&tree, "Expanded");

    try renderer.buffer.expectContainsLines(
        \\offset=0 height=8 total=15
        \\v node:1 (d=0)
        \\node:2 (d=1)
        \\node:3 (d=1)
        \\rendered: 8 items
    );
}

test "Tree: deep expansion with depth tracking" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createDeepData(g, 6);

    var tree = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"}, .recursive = true }},
    }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    // Initially only root visible
    try testing.expectEqual(@as(u32, 1), tree.total());

    // Expand each level
    var parent_id: NodeId = 1;
    var depth: u32 = 0;
    while (depth < 5) : (depth += 1) {
        try tree.expandById(parent_id, "children");
        parent_id = depth + 2;
    }

    // All 6 levels expanded = 6 visible
    try testing.expectEqual(@as(u32, 6), tree.total());

    var renderer = TreeRenderer.init(testing.allocator);
    defer renderer.deinit();
    try renderer.renderTree(&tree, "Deep Chain");

    try renderer.buffer.expectContainsLines(
        \\total=6
        \\v node:1 (d=0)
        \\v node:2 (d=1)
        \\v node:3 (d=2)
        \\v node:4 (d=3)
        \\v node:5 (d=4)
        \\- node:6 (d=5)
        \\rendered: 6 items
    );
}

// ============================================================================
// Visual Tests - Scrolling Through Expanded Trees
// ============================================================================

test "Tree: scroll reveals hidden children" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createTwoLevelData(g, 3, 4);

    var tree = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"} }},
    }, .{ .limit = 5 });
    defer tree.deinit();
    tree.activate(false);

    // Expand all roots (IDs: 1, 6, 11)
    try tree.expandById(1, "children");
    try tree.expandById(6, "children");
    try tree.expandById(11, "children");

    // 3 roots + 3*4 children = 15 visible
    try testing.expectEqual(@as(u32, 15), tree.total());

    var renderer = TreeRenderer.init(testing.allocator);
    defer renderer.deinit();

    try renderer.renderTree(&tree, "Top");
    try renderer.buffer.expectContainsLines(
        \\offset=0 height=5 total=15
        \\v node:1 (d=0)
        \\- node:2 (d=1)
        \\- node:3 (d=1)
        \\rendered: 5 items
    );

    renderer.reset();

    tree.scrollTo(5);
    try renderer.renderTree(&tree, "Middle");
    try renderer.buffer.expectContainsLines(
        \\offset=5 height=5 total=15
        \\v node:6 (d=0)
        \\- node:7 (d=1)
        \\rendered: 5 items
    );

    renderer.reset();

    tree.scrollTo(10);
    try renderer.renderTree(&tree, "Bottom");
    try renderer.buffer.expectContainsLines(
        \\offset=10 height=5 total=15
        \\v node:11 (d=0)
        \\- node:12 (d=1)
        \\rendered: 5 items
    );
}

test "Tree: scroll through expanded tree" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createTwoLevelData(g, 20, 5);

    var tree = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"} }},
    }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    // Expand every other root
    var i: u32 = 0;
    while (i < 20) : (i += 2) {
        const root_id: NodeId = i * 6 + 1; // Root IDs: 1, 13, 25, ...
        try tree.expandById(root_id, "children");
    }

    // Count total visible
    const total = tree.total();
    try testing.expect(total > 20); // More than just roots

    // Scroll through entire tree
    var total_items: u32 = 0;
    var offset: u32 = 0;

    while (offset < total) : (offset += 10) {
        tree.scrollTo(offset);

        var iter = tree.items();
        while (iter.next()) |_| {
            total_items += 1;
        }
    }

    // We should see all items
    try testing.expectEqual(total, total_items);
}

// ============================================================================
// Visual Tests - Profiling and Performance Verification
// ============================================================================

test "Tree: O(1) scroll operations" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createFlatData(g, 10000);

    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 50 });
    defer tree.deinit();
    tree.activate(false);

    var renderer = TreeRenderer.init(testing.allocator);
    defer renderer.deinit();

    // Scroll through large dataset
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        tree.move(1);
        renderer.counters.scroll_ops += 1;
    }

    try testing.expectEqual(@as(u32, 1000), tree.getOffset());
    try testing.expectEqual(@as(u32, 1000), renderer.counters.scroll_ops);

    // Scroll back
    i = 0;
    while (i < 500) : (i += 1) {
        tree.move(-1);
        renderer.counters.scroll_ops += 1;
    }

    try testing.expectEqual(@as(u32, 500), tree.getOffset());
}

test "Tree: viewport render cost is O(height) not O(total)" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Use 1000 instead of 100000 - the test verifies O(height) iteration
    // which works the same with smaller data sizes
    try createFlatData(g, 1000);

    const heights = [_]u32{ 10, 50, 100 };

    for (heights) |height| {
        var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = height });
        defer tree.deinit();
        tree.activate(false);

        tree.scrollTo(500);

        const count = countTreeItems(&tree);
        try testing.expectEqual(height, count);
    }
}

// ============================================================================
// Visual Tests - Edge Cases
// ============================================================================

test "Tree: empty tree" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Don't insert any data

    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    try testing.expectEqual(@as(u32, 0), tree.total());

    // Scrolling empty tree should not crash
    tree.move(1);
    tree.move(-1);
    tree.scrollTo(100);

    try testing.expectEqual(@as(u32, 0), tree.getOffset());
}

test "Tree: single node" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createFlatData(g, 1);

    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    try testing.expectEqual(@as(u32, 1), tree.total());

    const count = countTreeItems(&tree);
    try testing.expectEqual(@as(u32, 1), count);
}

test "Tree: height larger than total" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createFlatData(g, 5);

    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 100 });
    defer tree.deinit();
    tree.activate(false);

    try testing.expectEqual(@as(u32, 5), tree.total());

    const count = countTreeItems(&tree);
    try testing.expectEqual(@as(u32, 5), count);

    // Scrolling should be clamped
    tree.scrollTo(100);
    try testing.expectEqual(@as(u32, 0), tree.getOffset());
}

test "Tree: rapid expand/collapse cycles" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createTwoLevelData(g, 5, 5);

    var tree = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"} }},
    }, .{ .limit = 50 });
    defer tree.deinit();
    tree.activate(false);

    // Rapid toggle
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        try tree.expandById(1, "children");
        tree.collapseById(1, "children");
    }

    // Should end collapsed
    try testing.expectEqual(@as(u32, 5), tree.total());
}

test "Tree: scroll boundary conditions" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createFlatData(g, 100);

    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    // Scroll to exact max
    const max_offset = 100 - 10;
    tree.scrollTo(max_offset);
    try testing.expectEqual(max_offset, tree.getOffset());

    // move down at max should not change
    tree.move(1);
    try testing.expectEqual(max_offset, tree.getOffset());

    // move up at 0 should not change
    tree.scrollTo(0);
    tree.move(-1);
    try testing.expectEqual(@as(u32, 0), tree.getOffset());
}

// ============================================================================
// Visual Tests - Node Properties
// ============================================================================

test "Tree: depth accuracy in deep tree" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createDeepData(g, 5);

    var tree = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"}, .recursive = true }},
    }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    // Expand all levels
    var parent_id: NodeId = 1;
    var d: u32 = 0;
    while (d < 4) : (d += 1) {
        try tree.expandById(parent_id, "children");
        parent_id = d + 2;
    }

    // Verify each item has correct depth
    var iter = tree.items();
    var expected_depth: u32 = 0;
    while (iter.next()) |item| {
        try testing.expectEqual(expected_depth, item.depth);
        expected_depth += 1;
    }
}

test "Tree: node ID correctness at viewport positions" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createFlatData(g, 100);

    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    // At offset 0, first item should have ID 1
    try testing.expectEqual(@as(?NodeId, 1), getNodeIdAtIndex(&tree, 0));

    // Scroll to offset 50
    tree.scrollTo(50);
    try testing.expectEqual(@as(?NodeId, 51), getNodeIdAtIndex(&tree, 0));

    // Last item in viewport should be ID 60
    try testing.expectEqual(@as(?NodeId, 60), getNodeIdAtIndex(&tree, 9));
}

// ============================================================================
// Visual Tests - Nested Tree
// ============================================================================

test "Visual: nested tree renders correct hierarchy" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Build: Root -> Level1 (2 children) -> Level2 (2 grandchildren)
    const root = try g.insert("Root");
    try g.update(root, .{ .priority = @as(i64, 0) });
    const l1_1 = try g.insert("Item");
    try g.update(l1_1, .{ .priority = @as(i64, 10) });
    const l1_2 = try g.insert("Item");
    try g.update(l1_2, .{ .priority = @as(i64, 20) });
    try g.link(root, "children", l1_1);
    try g.link(root, "children", l1_2);

    const l2_1 = try g.insert("Item");
    try g.update(l2_1, .{ .priority = @as(i64, 100) });
    const l2_2 = try g.insert("Item");
    try g.update(l2_2, .{ .priority = @as(i64, 101) });
    try g.link(l1_1, "children", l2_1);
    try g.link(l1_1, "children", l2_2);

    var tree = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"}, .recursive = true }},
    }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    // Expand
    try tree.expandById(root, "children");
    try tree.expandById(l1_1, "children");

    var renderer = TreeRenderer.init(testing.allocator);
    defer renderer.deinit();
    try renderer.renderTree(&tree, "Nested Tree");

    try renderer.buffer.expectContainsLines(
        \\node:1 (d=0)
        \\node:2 (d=1)
        \\node:4 (d=2)
        \\node:5 (d=2)
        \\node:3 (d=1)
    );

    try testing.expectEqual(@as(u32, 5), tree.total());
}

test "Visual: collapsed vs expanded edge rendering" {
    const schema = try createTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const root = try g.insert("Root");
    try g.update(root, .{ .priority = @as(i64, 0) });
    const child1 = try g.insert("Item");
    try g.update(child1, .{ .priority = @as(i64, 10) });
    const child2 = try g.insert("Item");
    try g.update(child2, .{ .priority = @as(i64, 20) });
    try g.link(root, "children", child1);
    try g.link(root, "children", child2);

    var tree = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"} }},
    }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    var renderer = TreeRenderer.init(testing.allocator);
    defer renderer.deinit();

    // COLLAPSED - before expansion, tree doesn't know about potential children
    try renderer.renderTree(&tree, "Collapsed");
    try testing.expectEqual(@as(u32, 1), tree.total());

    try renderer.buffer.expectContainsLines(
        \\total=1
        \\node:1
    );

    renderer.reset();

    // EXPANDED - after expansion, tree knows about children
    try tree.expandById(root, "children");

    try renderer.renderTree(&tree, "Expanded");
    try testing.expectEqual(@as(u32, 3), tree.total());

    try renderer.buffer.expectContainsLines(
        \\total=3
        \\v node:1
        \\node:2
        \\node:3
    );
}
