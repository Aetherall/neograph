///! Profiling benchmarks for the Tree system.
///!
///! Run with: `zig build test-profile`
///!
///! These benchmarks measure performance characteristics of:
///! - Tree: insertions, expansions, traversals (via public API)
///! - Viewport: scrolling, iteration
///! - Full stack: schema, store, indexes, tracker
///!
///! Output is a human-readable report showing timing statistics.

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const watchdog = @import("test_watchdog.zig");

// Global watchdog - started by first test (120s for benchmarks)
var global_watchdog: ?watchdog.Watchdog = null;

fn ensureWatchdog() void {
    if (global_watchdog == null) {
        global_watchdog = watchdog.Watchdog.start(120); // 120s for benchmarks
    }
}

const profiling = @import("profiling.zig");
const Profiler = profiling.Profiler;
const Op = profiling.Op;

// Public API imports
const ng = @import("neograph.zig");
const Graph = ng.Graph;
const Schema = ng.Schema;
const NodeId = ng.NodeId;
const View = ng.View;
const ViewOpts = ng.ViewOpts;
const parseSchema = ng.parseSchema;

/// Helper to print profiler report using debug output
fn printReport(profiler: *const Profiler) void {
    var buf: [8192]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buf);
    profiler.report(&writer) catch {};
    std.debug.print("{s}", .{writer.buffered()});
}

/// Get the global profiler for tests
fn globalProfiler() *Profiler {
    return &profiling.global;
}

// ============================================================================
// Test Schema Creation
// ============================================================================

fn createProfileSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "Item",
        \\      "properties": [{ "name": "priority", "type": "int" }],
        \\      "edges": [
        \\        { "name": "children", "target": "Item", "reverse": "_parent" },
        \\        { "name": "_parent", "target": "Item", "reverse": "children" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "priority", "direction": "asc" }] }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

/// Create flat tree with N items
fn createFlatTree(g: *Graph, count: u32) !void {
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const id = try g.insert("Item");
        try g.update(id, .{ .priority = @as(i64, @intCast(i)) });
    }
}

/// Create two-level tree: N roots, each with M children
fn createTwoLevelTree(g: *Graph, roots: u32, children_per_root: u32) !void {
    var root_idx: u32 = 0;
    while (root_idx < roots) : (root_idx += 1) {
        const root_id = try g.insert("Item");
        try g.update(root_id, .{ .priority = @as(i64, @intCast(root_idx * 1000)) });

        var child_idx: u32 = 0;
        while (child_idx < children_per_root) : (child_idx += 1) {
            const child_id = try g.insert("Item");
            try g.update(child_id, .{ .priority = @as(i64, @intCast(root_idx * 1000 + child_idx + 1)) });
            try g.link(root_id, "children", child_id);
        }
    }
}

/// Create deep tree: single chain of depth N
fn createDeepTree(g: *Graph, depth: u32) !void {
    var parent_id = try g.insert("Item");
    try g.update(parent_id, .{ .priority = @as(i64, 0) });

    var d: u32 = 1;
    while (d < depth) : (d += 1) {
        const child_id = try g.insert("Item");
        try g.update(child_id, .{ .priority = @as(i64, @intCast(d)) });
        try g.link(parent_id, "children", child_id);
        parent_id = child_id;
    }
}

// ============================================================================
// Benchmarks: Flat Tree Scrolling
// ============================================================================

test "profile: flat tree 10k scroll performance" {
    ensureWatchdog();

    const profiler = globalProfiler();
    profiler.reset();
    profiler.startSession();

    const schema = try createProfileSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createFlatTree(g, 10_000);

    var tree = try g.view(.{ .root = "Item", .sort = &.{"priority"} }, .{ .limit = 50 });
    defer tree.deinit();
    tree.activate(false);

    // Scroll through entire list in chunks
    const iterations = 1000;
    var i: u32 = 0;
    while (i < iterations) : (i += 1) {
        const timer = profiler.time(.scroll_to);
        tree.scrollTo(i * 10 % 9950); // Wrap around
        timer.stop();
    }

    // Also measure scrollBy (move)
    i = 0;
    while (i < iterations) : (i += 1) {
        const timer = profiler.time(.scroll_by);
        tree.move(1);
        timer.stop();
        if (tree.getOffset() >= 9950) tree.scrollTo(0);
    }

    profiler.recordVisible(tree.total());
    profiler.endSession();

    printReport(profiler);

    // Performance assertions
    const scroll_to_stats = profiler.get(.scroll_to);
    const scroll_by_stats = profiler.get(.scroll_by);

    try testing.expect(scroll_to_stats.count == iterations);
    try testing.expect(scroll_by_stats.count == iterations);

    // scrollBy should be much faster than scrollTo on average
    try testing.expect(scroll_by_stats.avg_ns() < scroll_to_stats.avg_ns());

    // Work counters verification
    try testing.expect(profiler.scroll_steps > 0);
    try testing.expect(profiler.nodes_created == 10_000);
}

// ============================================================================
// Benchmarks: Expand/Collapse
// ============================================================================

test "profile: expand/collapse 1k nodes with 10 children each" {
    const profiler = globalProfiler();
    profiler.reset();
    profiler.startSession();

    const schema = try createProfileSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createTwoLevelTree(g, 100, 10);

    var tree = try g.view(.{
        .root = "Item",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"} }},
    }, .{ .limit = 30 });
    defer tree.deinit();
    tree.activate(false);

    // Expand all roots
    var root_idx: u32 = 0;
    while (root_idx < 100) : (root_idx += 1) {
        const root_id: NodeId = root_idx + 1; // NodeIds start at 1
        const timer = profiler.time(.expand);
        try tree.expandById(root_id, "children");
        timer.stop();
    }

    profiler.recordVisible(tree.total());

    // Collapse all roots
    root_idx = 0;
    while (root_idx < 100) : (root_idx += 1) {
        const root_id: NodeId = root_idx + 1;
        const timer = profiler.time(.collapse);
        tree.collapseById(root_id, "children");
        timer.stop();
    }

    profiler.endSession();

    printReport(profiler);

    // Verify operations
    const expand_stats = profiler.get(.expand);
    const collapse_stats = profiler.get(.collapse);

    try testing.expectEqual(@as(u64, 100), expand_stats.count);
    try testing.expectEqual(@as(u64, 100), collapse_stats.count);

    // 100 roots + 100*10 children = 1100 nodes
    // Note: profiler.nodes_created may differ due to Graph API internal tracking
    try testing.expect(profiler.nodes_created >= 1100);
}

// ============================================================================
// Benchmarks: Deep Tree Traversal
// ============================================================================

test "profile: deep tree expansion (depth 100)" {
    const profiler = globalProfiler();
    profiler.reset();
    profiler.startSession();

    const schema = try createProfileSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createDeepTree(g, 100);

    var tree = try g.view(.{
        .root = "Item",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"}, .recursive = true }},
    }, .{ .limit = 100 });
    defer tree.deinit();
    tree.activate(false);

    // Expand entire chain
    var parent_id: NodeId = 1;
    var d: u32 = 1;
    while (d < 100) : (d += 1) {
        const timer = profiler.time(.expand);
        try tree.expandById(parent_id, "children");
        timer.stop();

        profiling.global.recordDepth(d);
        parent_id = d + 1;
    }

    profiler.recordVisible(tree.total());
    profiler.endSession();

    printReport(profiler);

    // Should have expanded 99 edges
    try testing.expectEqual(@as(u64, 99), profiler.get(.expand).count);
    try testing.expectEqual(@as(u32, 99), profiler.max_depth_seen);
    // Tree total includes root items + expanded children (100 roots + 99 children = 199)
    try testing.expect(tree.total() >= 100);
}

// ============================================================================
// Benchmarks: Viewport Iteration
// ============================================================================

test "profile: viewport iteration 10k items" {
    const profiler = globalProfiler();
    profiler.reset();
    profiler.startSession();

    const schema = try createProfileSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createFlatTree(g, 10_000);

    var tree = try g.view(.{ .root = "Item", .sort = &.{"priority"} }, .{ .limit = 100 });
    defer tree.deinit();
    tree.activate(false);

    // Iterate viewport at various offsets
    const iterations = 100;
    var i: u32 = 0;
    while (i < iterations) : (i += 1) {
        tree.scrollTo(i * 99);

        const timer = profiler.time(.viewport_items);
        var iter = tree.items();
        var count: u32 = 0;
        while (iter.next()) |_| {
            count += 1;
        }
        timer.stop();

        try testing.expectEqual(@as(u32, 100), count);
    }

    profiler.endSession();

    printReport(profiler);

    // Verify consistent iteration cost
    const stats = profiler.get(.viewport_items);
    try testing.expectEqual(@as(u64, iterations), stats.count);

    // Max should not be dramatically higher than average
    try testing.expect(stats.max_ns < stats.avg_ns() * 10);

    // Work counters
    try testing.expectEqual(@as(u64, 10_000), profiler.viewport_iterations);
}

// ============================================================================
// Benchmarks: Mixed Operations
// ============================================================================

test "profile: realistic workload - tree navigation" {
    const profiler = globalProfiler();
    profiler.reset();
    profiler.startSession();

    const schema = try createProfileSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try createTwoLevelTree(g, 50, 20);

    var tree = try g.view(.{
        .root = "Item",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"} }},
    }, .{ .limit = 30 });
    defer tree.deinit();
    tree.activate(false);

    // Simulate user navigation
    const nav_iterations = 200;
    var i: u32 = 0;
    var expanded_count: u32 = 0;

    while (i < nav_iterations) : (i += 1) {
        // Scroll
        {
            const timer = profiler.time(.scroll_by);
            tree.move(if (i % 3 == 0) -5 else 5);
            timer.stop();
        }

        // Occasionally expand/collapse
        if (i % 10 == 0 and expanded_count < 50) {
            const root_id: NodeId = expanded_count + 1;
            const timer = profiler.time(.expand);
            try tree.expandById(root_id, "children");
            timer.stop();
            expanded_count += 1;
        } else if (i % 15 == 0 and expanded_count > 0) {
            const root_id: NodeId = expanded_count;
            const timer = profiler.time(.collapse);
            tree.collapseById(root_id, "children");
            timer.stop();
            expanded_count -|= 1;
        }

        // Iterate viewport
        {
            const timer = profiler.time(.viewport_items);
            var iter = tree.items();
            while (iter.next()) |_| {}
            timer.stop();
        }
    }

    profiler.recordVisible(tree.total());
    profiler.endSession();

    printReport(profiler);

    // Verify work counters reflect mixed operations
    try testing.expect(profiler.totalWork() > 0);
}

// ============================================================================
// Benchmarks: Large Scale
// ============================================================================

test "profile: large scale - 100k flat items" {
    const profiler = globalProfiler();
    profiler.reset();
    profiler.startSession();

    const schema = try createProfileSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Measure node insertion
    var t1 = try std.time.Timer.start();
    try createFlatTree(g, 100_000);
    const insert_ns = t1.read();
    std.debug.print("\n[100k] Insert: {d}ms\n", .{insert_ns / 1_000_000});

    // Measure tree creation
    var t2 = try std.time.Timer.start();
    var tree = try g.view(.{ .root = "Item", .sort = &.{"priority"} }, .{ .limit = 100 });
    defer tree.deinit();
    const tree_ns = t2.read();
    std.debug.print("[100k] g.view(): {d}ms\n", .{tree_ns / 1_000_000});

    // Measure activate
    var t3 = try std.time.Timer.start();
    tree.activate(false);
    const activate_ns = t3.read();
    std.debug.print("[100k] activate(): {d}ms\n", .{activate_ns / 1_000_000});

    // Scroll to various positions
    const positions = [_]u32{ 0, 1000, 10_000, 50_000, 99_000 };
    for (positions) |pos| {
        const timer = profiler.time(.scroll_to);
        tree.scrollTo(pos);
        timer.stop();
    }

    // Measure iteration at end of list
    tree.scrollTo(99_900);
    {
        const timer = profiler.time(.viewport_items);
        var iter = tree.items();
        while (iter.next()) |_| {}
        timer.stop();
    }

    profiler.recordVisible(tree.total());
    profiler.endSession();

    printReport(profiler);

    // Verify scale
    try testing.expectEqual(@as(u32, 100_000), tree.total());

    // Work counters
    try testing.expectEqual(@as(u64, 100_000), profiler.nodes_created);
    try testing.expect(profiler.scroll_steps > 0);
}

// ============================================================================
// Summary Test
// ============================================================================

test "profile: summary" {
    std.debug.print("\n", .{});
    std.debug.print("===============================================================================\n", .{});
    std.debug.print("                     PROFILING BENCHMARKS COMPLETE                            \n", .{});
    std.debug.print("                                                                              \n", .{});
    std.debug.print("  Run with: zig build test-profile                                            \n", .{});
    std.debug.print("  Enable in other tests: zig build test -Dprofile=true                        \n", .{});
    std.debug.print("===============================================================================\n", .{});
    std.debug.print("\n", .{});
}
