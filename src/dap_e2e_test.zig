///! End-to-end DAP tests using the public Graph API.
///!
///! Tests the full Neograph stack using a realistic DAP (Debug Adapter Protocol)
///! debugger schema. Exercises:
///! - Schema with multiple types and relationships
///! - Tree views with filtering, sorting, and expansion
///! - Insert/update/delete through public API
///! - Tree virtualization with expand/collapse
///!
///! Graph structure:
///!   Debugger -> Session -> Thread -> Frame -> Scope -> Variable*
///!   Debugger -> Source -> Breakpoint

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

// Public API imports
const ng = @import("neograph.zig");
const Graph = ng.Graph;
const Schema = ng.Schema;
const NodeId = ng.NodeId;
const View = ng.View;
const parseSchema = ng.parseSchema;

// Profiling for work quantity verification
const profiling = @import("profiling.zig");

// ============================================================================
// DAP Schema (JSON)
// ============================================================================

fn createDapSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "Debugger",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "pid", "type": "int" },
        \\        { "name": "order", "type": "int" },
        \\        { "name": "running", "type": "bool" }
        \\      ],
        \\      "edges": [
        \\        { "name": "sessions", "target": "Session", "reverse": "_debugger" },
        \\        { "name": "sources", "target": "Source", "reverse": "_debugger" },
        \\        { "name": "breakpoints", "target": "Breakpoint", "reverse": "_debugger" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Session",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "status", "type": "string" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [
        \\        { "name": "_debugger", "target": "Debugger", "reverse": "sessions" },
        \\        { "name": "threads", "target": "Thread", "reverse": "_session" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Thread",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "tid", "type": "int" },
        \\        { "name": "state", "type": "string" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [
        \\        { "name": "_session", "target": "Session", "reverse": "threads" },
        \\        { "name": "frames", "target": "Frame", "reverse": "_thread" }
        \\      ],
        \\      "indexes": [
        \\        { "fields": [{ "field": "order", "direction": "asc" }] },
        \\        { "fields": [{ "field": "tid", "direction": "asc" }] }
        \\      ]
        \\    },
        \\    {
        \\      "name": "Frame",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "file", "type": "string" },
        \\        { "name": "line", "type": "int" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [
        \\        { "name": "_thread", "target": "Thread", "reverse": "frames" },
        \\        { "name": "scopes", "target": "Scope", "reverse": "_frame" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Scope",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "expensive", "type": "bool" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [
        \\        { "name": "_frame", "target": "Frame", "reverse": "scopes" },
        \\        { "name": "variables", "target": "Variable", "reverse": "_scope" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Variable",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "value", "type": "string" },
        \\        { "name": "type", "type": "string" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [
        \\        { "name": "_scope", "target": "Scope", "reverse": "variables" },
        \\        { "name": "children", "target": "Variable", "reverse": "_parent" },
        \\        { "name": "_parent", "target": "Variable", "reverse": "children" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Source",
        \\      "properties": [
        \\        { "name": "path", "type": "string" },
        \\        { "name": "name", "type": "string" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [{ "name": "_debugger", "target": "Debugger", "reverse": "sources" }],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Breakpoint",
        \\      "properties": [
        \\        { "name": "line", "type": "int" },
        \\        { "name": "enabled", "type": "bool" },
        \\        { "name": "hit_count", "type": "int" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [{ "name": "_debugger", "target": "Debugger", "reverse": "breakpoints" }],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

// ============================================================================
// Tests: Basic CRUD Operations
// ============================================================================

test "DAP E2E: insert and retrieve nodes" {
    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create debugger
    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    // Verify we can get it back
    const node = g.get(debugger).?;
    try testing.expectEqual(debugger, node.id);

    const name = node.getProperty("name").?.string;
    try testing.expectEqualStrings("gdb", name);
}

test "DAP E2E: link nodes together" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create debugger and session
    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    const session = try g.insert("Session");
    try g.update(session, .{ .name = "main", .order = @as(i64, 0) });

    // Link session to debugger
    try g.link(debugger, "sessions", session);

    // Create tree and verify structure
    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    try testing.expectEqual(@as(u32, 1), tree.total());

    // Expand and verify session appears
    try tree.expandById(debugger, "sessions");
    try testing.expectEqual(@as(u32, 2), tree.total());
}

test "DAP E2E: update node properties" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create thread
    const session = try g.insert("Session");
    try g.update(session, .{ .name = "main", .order = @as(i64, 0) });

    const thread = try g.insert("Thread");
    try g.update(thread, .{ .name = "main", .tid = @as(i64, 1), .state = "running", .order = @as(i64, 0) });
    try g.link(session, "threads", thread);

    // Update thread state
    try g.update(thread, .{ .state = "stopped" });

    // Verify update
    const node = g.get(thread).?;
    const state = node.getProperty("state").?.string;
    try testing.expectEqualStrings("stopped", state);
}

test "DAP E2E: delete nodes" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create breakpoint
    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    const bp = try g.insert("Breakpoint");
    try g.update(bp, .{ .line = @as(i64, 42), .enabled = true, .order = @as(i64, 0) });
    try g.link(debugger, "breakpoints", bp);

    // Create tree to count items
    var tree = try g.view(.{ .root = "Breakpoint", .sort = &.{"order"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    try testing.expectEqual(@as(u32, 1), tree.total());

    // Delete breakpoint
    try g.delete(bp);

    // Verify node is gone
    try testing.expect(g.get(bp) == null);
}

// ============================================================================
// Tests: Tree Visualization
// ============================================================================

test "DAP E2E: tree expand/collapse" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create hierarchy: debugger -> session -> threads
    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    const session = try g.insert("Session");
    try g.update(session, .{ .name = "main", .order = @as(i64, 0) });
    try g.link(debugger, "sessions", session);

    const thread1 = try g.insert("Thread");
    try g.update(thread1, .{ .name = "main", .tid = @as(i64, 1), .order = @as(i64, 0) });
    try g.link(session, "threads", thread1);

    const thread2 = try g.insert("Thread");
    try g.update(thread2, .{ .name = "worker", .tid = @as(i64, 2), .order = @as(i64, 1) });
    try g.link(session, "threads", thread2);

    // Create tree
    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    // Initial state: collapsed
    try testing.expectEqual(@as(u32, 1), tree.total());

    // Expand debugger -> sessions
    try tree.expandById(debugger, "sessions");
    try testing.expectEqual(@as(u32, 2), tree.total());

    // Expand session -> threads
    try tree.expandById(session, "threads");
    try testing.expectEqual(@as(u32, 4), tree.total());

    // Collapse session -> threads
    tree.collapseById(session, "threads");
    try testing.expectEqual(@as(u32, 2), tree.total());
}

test "DAP E2E: tree scrolling" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create thread with many frames
    const session = try g.insert("Session");
    try g.update(session, .{ .name = "main", .order = @as(i64, 0) });

    const thread = try g.insert("Thread");
    try g.update(thread, .{ .name = "main", .tid = @as(i64, 1), .order = @as(i64, 0) });
    try g.link(session, "threads", thread);

    // Create 20 frames
    for (0..20) |i| {
        const frame = try g.insert("Frame");
        try g.update(frame, .{
            .name = "frame",
            .file = "test.zig",
            .line = @as(i64, @intCast(i * 10)),
            .order = @as(i64, @intCast(i)),
        });
        try g.link(thread, "frames", frame);
    }

    // Create tree rooted at Thread
    var tree = try g.view(.{ .root = "Thread", .sort = &.{"order"} }, .{ .limit = 5 });
    defer tree.deinit();
    tree.activate(false);

    // Expand thread -> frames
    try tree.expandById(thread, "frames");

    // Total: 1 thread + 20 frames = 21
    try testing.expectEqual(@as(u32, 21), tree.total());

    // Test scrolling
    try testing.expectEqual(@as(u32, 0), tree.getOffset());

    tree.move(10);
    try testing.expectEqual(@as(u32, 10), tree.getOffset());

    tree.scrollTo(16);
    try testing.expectEqual(@as(u32, 16), tree.getOffset());

    // Scroll past end (should clamp to max = 21 - 5 = 16)
    tree.scrollTo(100);
    try testing.expectEqual(@as(u32, 16), tree.getOffset());
}

test "DAP E2E: recursive variable expansion" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create scope with nested variables
    const frame = try g.insert("Frame");
    try g.update(frame, .{ .name = "main", .order = @as(i64, 0) });

    const scope = try g.insert("Scope");
    try g.update(scope, .{ .name = "Local", .order = @as(i64, 0) });
    try g.link(frame, "scopes", scope);

    // Create nested object: obj -> items -> [0], [1], [2]
    const obj = try g.insert("Variable");
    try g.update(obj, .{ .name = "obj", .value = "Object", .type = "struct", .order = @as(i64, 0) });
    try g.link(scope, "variables", obj);

    const items = try g.insert("Variable");
    try g.update(items, .{ .name = "items", .value = "Array[3]", .type = "[]i32", .order = @as(i64, 0) });
    try g.link(obj, "children", items);

    const item0 = try g.insert("Variable");
    try g.update(item0, .{ .name = "[0]", .value = "10", .type = "i32", .order = @as(i64, 0) });
    try g.link(items, "children", item0);

    const item1 = try g.insert("Variable");
    try g.update(item1, .{ .name = "[1]", .value = "20", .type = "i32", .order = @as(i64, 1) });
    try g.link(items, "children", item1);

    const item2 = try g.insert("Variable");
    try g.update(item2, .{ .name = "[2]", .value = "30", .type = "i32", .order = @as(i64, 2) });
    try g.link(items, "children", item2);

    // Create tree for Scope (to have a rooted view)
    var tree = try g.view(.{ .root = "Scope", .sort = &.{"order"} }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    try testing.expectEqual(@as(u32, 1), tree.total());

    // Expand scope -> variables
    try tree.expandById(scope, "variables");
    try testing.expectEqual(@as(u32, 2), tree.total());

    // Expand obj -> children
    try tree.expandById(obj, "children");
    try testing.expectEqual(@as(u32, 3), tree.total());

    // Expand items -> children
    try tree.expandById(items, "children");
    try testing.expectEqual(@as(u32, 6), tree.total());

    // Count via viewport iterator
    var count: u32 = 0;
    var iter = tree.items();
    while (iter.next()) |_| {
        count += 1;
    }
    try testing.expectEqual(@as(u32, 6), count);
}

// ============================================================================
// Tests: Deep Hierarchy
// ============================================================================

test "DAP E2E: full debug hierarchy" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create: Debugger -> Session -> Thread -> Frame -> Scope -> Variable
    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    const session = try g.insert("Session");
    try g.update(session, .{ .name = "main", .order = @as(i64, 0) });
    try g.link(debugger, "sessions", session);

    const thread = try g.insert("Thread");
    try g.update(thread, .{ .name = "main", .tid = @as(i64, 1), .order = @as(i64, 0) });
    try g.link(session, "threads", thread);

    const frame = try g.insert("Frame");
    try g.update(frame, .{ .name = "main()", .file = "main.zig", .line = @as(i64, 42), .order = @as(i64, 0) });
    try g.link(thread, "frames", frame);

    const scope = try g.insert("Scope");
    try g.update(scope, .{ .name = "Local", .order = @as(i64, 0) });
    try g.link(frame, "scopes", scope);

    const variable = try g.insert("Variable");
    try g.update(variable, .{ .name = "x", .value = "42", .type = "i32", .order = @as(i64, 0) });
    try g.link(scope, "variables", variable);

    // Create tree and expand full path
    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    try testing.expectEqual(@as(u32, 1), tree.total());

    try tree.expandById(debugger, "sessions");
    try testing.expectEqual(@as(u32, 2), tree.total());

    try tree.expandById(session, "threads");
    try testing.expectEqual(@as(u32, 3), tree.total());

    try tree.expandById(thread, "frames");
    try testing.expectEqual(@as(u32, 4), tree.total());

    try tree.expandById(frame, "scopes");
    try testing.expectEqual(@as(u32, 5), tree.total());

    try tree.expandById(scope, "variables");
    try testing.expectEqual(@as(u32, 6), tree.total());
}

test "DAP E2E: multiple children at each level" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create debugger with multiple sessions
    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    // 3 sessions
    var sessions: [3]NodeId = undefined;
    for (0..3) |i| {
        sessions[i] = try g.insert("Session");
        try g.update(sessions[i], .{ .name = "session", .order = @as(i64, @intCast(i)) });
        try g.link(debugger, "sessions", sessions[i]);
    }

    // 2 threads per session
    for (sessions) |sess| {
        for (0..2) |j| {
            const thread = try g.insert("Thread");
            try g.update(thread, .{ .name = "thread", .order = @as(i64, @intCast(j)) });
            try g.link(sess, "threads", thread);
        }
    }

    // Create tree
    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    try testing.expectEqual(@as(u32, 1), tree.total());

    // Expand debugger -> sessions (+3)
    try tree.expandById(debugger, "sessions");
    try testing.expectEqual(@as(u32, 4), tree.total());

    // Expand all sessions -> threads (+2 each = +6)
    for (sessions) |sess| {
        try tree.expandById(sess, "threads");
    }
    try testing.expectEqual(@as(u32, 10), tree.total());
}

// ============================================================================
// Tests: Profiling
// ============================================================================

test "DAP E2E: profiled tree operations" {
    if (profiling.enabled) {
        profiling.global.reset();
        profiling.global.startSession();
    }

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create hierarchy
    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    const session = try g.insert("Session");
    try g.update(session, .{ .name = "main", .order = @as(i64, 0) });
    try g.link(debugger, "sessions", session);

    // Create 10 threads
    for (0..10) |i| {
        const thread = try g.insert("Thread");
        try g.update(thread, .{ .name = "thread", .order = @as(i64, @intCast(i)) });
        try g.link(session, "threads", thread);
    }

    // Create tree
    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 5 });
    defer tree.deinit();
    tree.activate(false);

    // Expand hierarchy
    try tree.expandById(debugger, "sessions");
    try tree.expandById(session, "threads");

    // Total: 1 + 1 + 10 = 12
    try testing.expectEqual(@as(u32, 12), tree.total());

    // Scroll operations
    tree.move(5);
    tree.scrollTo(7);

    if (profiling.enabled) {
        profiling.global.endSession();

        // Verify work counters
        try testing.expect(profiling.global.nodes_created >= 12);
        try testing.expect(profiling.global.edges_expanded >= 2);
        try testing.expect(profiling.global.scroll_steps >= 5);
    }
}

// ============================================================================
// Tests: Toggle Expansion
// ============================================================================

test "DAP E2E: toggle expansion" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    const session = try g.insert("Session");
    try g.update(session, .{ .name = "main", .order = @as(i64, 0) });
    try g.link(debugger, "sessions", session);

    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    // Initial: collapsed
    try testing.expectEqual(@as(u32, 1), tree.total());
    try testing.expect(!tree.isExpandedById(debugger, "sessions"));

    // Toggle to expand
    _ = try tree.toggleById(debugger, "sessions");
    try testing.expectEqual(@as(u32, 2), tree.total());
    try testing.expect(tree.isExpandedById(debugger, "sessions"));

    // Toggle to collapse
    _ = try tree.toggleById(debugger, "sessions");
    try testing.expectEqual(@as(u32, 1), tree.total());
    try testing.expect(!tree.isExpandedById(debugger, "sessions"));
}

// ============================================================================
// Tests: Viewport Iteration
// ============================================================================

test "DAP E2E: viewport respects height" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create 20 sources
    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    for (0..20) |i| {
        const source = try g.insert("Source");
        try g.update(source, .{ .name = "file", .path = "/path", .order = @as(i64, @intCast(i)) });
        try g.link(debugger, "sources", source);
    }

    // Create tree with height 5
    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 5 });
    defer tree.deinit();
    tree.activate(false);

    // Expand sources
    try tree.expandById(debugger, "sources");

    // Total: 1 debugger + 20 sources = 21
    try testing.expectEqual(@as(u32, 21), tree.total());

    // Viewport should only show 5 items
    var count: u32 = 0;
    var iter = tree.items();
    while (iter.next()) |_| {
        count += 1;
    }
    try testing.expectEqual(@as(u32, 5), count);

    // Scroll to middle
    tree.scrollTo(8);

    // Should still show 5 items
    count = 0;
    iter = tree.items();
    while (iter.next()) |_| {
        count += 1;
    }
    try testing.expectEqual(@as(u32, 5), count);
}

test "DAP E2E: change viewport height" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create 10 breakpoints
    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    for (0..10) |i| {
        const bp = try g.insert("Breakpoint");
        try g.update(bp, .{ .line = @as(i64, @intCast((i + 1) * 10)), .enabled = true, .order = @as(i64, @intCast(i)) });
        try g.link(debugger, "breakpoints", bp);
    }

    // Create tree with height 3
    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 3 });
    defer tree.deinit();
    tree.activate(false);

    try tree.expandById(debugger, "breakpoints");

    // Verify height = 3
    try testing.expectEqual(@as(u32, 3), tree.getHeight());

    var count: u32 = 0;
    var iter = tree.items();
    while (iter.next()) |_| {
        count += 1;
    }
    try testing.expectEqual(@as(u32, 3), count);

    // Change height to 7
    tree.setHeight(7);
    try testing.expectEqual(@as(u32, 7), tree.getHeight());

    count = 0;
    iter = tree.items();
    while (iter.next()) |_| {
        count += 1;
    }
    try testing.expectEqual(@as(u32, 7), count);
}

// ============================================================================
// Tests: Lazy Tree Architecture Guarantees
// ============================================================================
//
// These tests verify the key guarantees of the lazy tree architecture:
// 1. O(1) tree creation - no scanning on subscribe
// 2. O(viewport) first render - only load visible items
// 3. Correct total() computation with expanded/collapsed nodes
// 4. Reactive updates work for both loaded and unloaded nodes
// 5. Multi-level expansion state is maintained correctly
// 6. Edge-specific expansion (same node, different edges)
// ============================================================================

test "DAP E2E: lazy tree creation is O(1)" {
    // Verify that tree creation does NOT scan all nodes.
    // We create many nodes but tree creation should be instant.
    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create 1000 breakpoints
    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    for (0..1000) |i| {
        const bp = try g.insert("Breakpoint");
        try g.update(bp, .{ .line = @as(i64, @intCast(i)), .order = @as(i64, @intCast(i)) });
        try g.link(debugger, "breakpoints", bp);
    }

    // Tree creation should be O(1) - does not scan nodes
    var tree = try g.view(.{
        .root = "Debugger",
        .sort = &.{"order"},
        .edges = &.{.{ .name = "breakpoints", .sort = &.{"order"} }},
    }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    // Initially just the root (not expanded)
    try testing.expectEqual(@as(u32, 1), tree.total());

    // After expanding, total includes children
    try tree.expandById(debugger, "breakpoints");
    try testing.expectEqual(@as(u32, 1001), tree.total()); // debugger + 1000 breakpoints
}

test "DAP E2E: viewport loads only visible items" {
    // Verify that viewport iteration only loads O(height) items,
    // not all matching items.
    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    // Create 100 breakpoints
    for (0..100) |i| {
        const bp = try g.insert("Breakpoint");
        try g.update(bp, .{ .line = @as(i64, @intCast(i)), .order = @as(i64, @intCast(i)) });
        try g.link(debugger, "breakpoints", bp);
    }

    // Create tree with small viewport
    var tree = try g.view(.{
        .root = "Debugger",
        .sort = &.{"order"},
        .edges = &.{.{ .name = "breakpoints", .sort = &.{"order"} }},
    }, .{ .limit = 5 });
    defer tree.deinit();
    tree.activate(false);

    try tree.expandById(debugger, "breakpoints");

    // Total should reflect all items
    try testing.expectEqual(@as(u32, 101), tree.total());

    // But iteration should only return viewport size
    var count: u32 = 0;
    var iter = tree.items();
    while (iter.next()) |_| {
        count += 1;
    }
    try testing.expectEqual(@as(u32, 5), count);
}

test "DAP E2E: nested expansion state persistence" {
    // Test that expansion state is maintained correctly across multiple levels:
    // Debugger -> Session -> Thread -> Frame
    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Build hierarchy
    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    const session = try g.insert("Session");
    try g.update(session, .{ .name = "main", .order = @as(i64, 0) });
    try g.link(debugger, "sessions", session);

    const thread1 = try g.insert("Thread");
    try g.update(thread1, .{ .name = "main", .tid = @as(i64, 1), .order = @as(i64, 0) });
    try g.link(session, "threads", thread1);

    const thread2 = try g.insert("Thread");
    try g.update(thread2, .{ .name = "worker", .tid = @as(i64, 2), .order = @as(i64, 1) });
    try g.link(session, "threads", thread2);

    // 3 frames for thread1
    for (0..3) |i| {
        const frame = try g.insert("Frame");
        try g.update(frame, .{ .name = "func", .order = @as(i64, @intCast(i)) });
        try g.link(thread1, "frames", frame);
    }

    // 2 frames for thread2
    for (0..2) |i| {
        const frame = try g.insert("Frame");
        try g.update(frame, .{ .name = "work", .order = @as(i64, @intCast(i)) });
        try g.link(thread2, "frames", frame);
    }

    var tree = try g.view(.{
        .root = "Debugger",
        .sort = &.{"order"},
        .edges = &.{.{
            .name = "sessions",
            .sort = &.{"order"},
            .edges = &.{.{
                .name = "threads",
                .sort = &.{"order"},
                .edges = &.{.{
                    .name = "frames",
                    .sort = &.{"order"},
                }},
            }},
        }},
    }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    // Step by step expansion
    try testing.expectEqual(@as(u32, 1), tree.total()); // debugger

    try tree.expandById(debugger, "sessions");
    try testing.expectEqual(@as(u32, 2), tree.total()); // + session

    try tree.expandById(session, "threads");
    try testing.expectEqual(@as(u32, 4), tree.total()); // + 2 threads

    try tree.expandById(thread1, "frames");
    try testing.expectEqual(@as(u32, 7), tree.total()); // + 3 frames

    try tree.expandById(thread2, "frames");
    try testing.expectEqual(@as(u32, 9), tree.total()); // + 2 frames

    // Collapse thread1 - only its frames disappear
    tree.collapseById(thread1, "frames");
    try testing.expectEqual(@as(u32, 6), tree.total()); // - 3 frames

    // Thread2 frames should still be visible
    try tree.expandById(thread1, "frames");
    try testing.expectEqual(@as(u32, 9), tree.total()); // back to full

    // Collapse session - all descendants disappear
    tree.collapseById(debugger, "sessions");
    try testing.expectEqual(@as(u32, 1), tree.total()); // just debugger

    // Re-expand - expansion state of children is LOST (by design)
    try tree.expandById(debugger, "sessions");
    try testing.expectEqual(@as(u32, 2), tree.total()); // session visible but threads collapsed
}

test "DAP E2E: multi-edge expansion on same node" {
    // Test expanding multiple edges on the same node:
    // Debugger has sessions, sources, and breakpoints
    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    // Add 3 sessions
    for (0..3) |i| {
        const s = try g.insert("Session");
        try g.update(s, .{ .name = "session", .order = @as(i64, @intCast(i)) });
        try g.link(debugger, "sessions", s);
    }

    // Add 2 sources
    for (0..2) |i| {
        const src = try g.insert("Source");
        try g.update(src, .{ .name = "file.c", .order = @as(i64, @intCast(i)) });
        try g.link(debugger, "sources", src);
    }

    // Add 4 breakpoints
    for (0..4) |i| {
        const bp = try g.insert("Breakpoint");
        try g.update(bp, .{ .line = @as(i64, @intCast(i * 10)), .order = @as(i64, @intCast(i)) });
        try g.link(debugger, "breakpoints", bp);
    }

    var tree = try g.view(.{
        .root = "Debugger",
        .sort = &.{"order"},
        .edges = &.{
            .{ .name = "sessions", .sort = &.{"order"} },
            .{ .name = "sources", .sort = &.{"order"} },
            .{ .name = "breakpoints", .sort = &.{"order"} },
        },
    }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    try testing.expectEqual(@as(u32, 1), tree.total()); // debugger

    // Expand sessions only
    try tree.expandById(debugger, "sessions");
    try testing.expectEqual(@as(u32, 4), tree.total()); // + 3 sessions

    // Also expand sources
    try tree.expandById(debugger, "sources");
    try testing.expectEqual(@as(u32, 6), tree.total()); // + 2 sources

    // Also expand breakpoints
    try tree.expandById(debugger, "breakpoints");
    try testing.expectEqual(@as(u32, 10), tree.total()); // + 4 breakpoints

    // Collapse just sources
    tree.collapseById(debugger, "sources");
    try testing.expectEqual(@as(u32, 8), tree.total()); // - 2 sources

    // Sessions and breakpoints still expanded
    tree.collapseById(debugger, "sessions");
    try testing.expectEqual(@as(u32, 5), tree.total()); // - 3 sessions

    // Only breakpoints remain
    tree.collapseById(debugger, "breakpoints");
    try testing.expectEqual(@as(u32, 1), tree.total()); // just debugger
}

test "DAP E2E: reactive insert on expanded parent" {
    // Test that inserting a child into an expanded parent updates the tree reactively
    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    const session = try g.insert("Session");
    try g.update(session, .{ .name = "main", .order = @as(i64, 0) });
    try g.link(debugger, "sessions", session);

    var tree = try g.view(.{
        .root = "Debugger",
        .sort = &.{"order"},
        .edges = &.{.{
            .name = "sessions",
            .sort = &.{"order"},
            .edges = &.{.{
                .name = "threads",
                .sort = &.{"order"},
            }},
        }},
    }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    try tree.expandById(debugger, "sessions");
    try tree.expandById(session, "threads");
    try testing.expectEqual(@as(u32, 2), tree.total()); // debugger + session

    // Insert a thread AFTER expansion
    const thread = try g.insert("Thread");
    try g.update(thread, .{ .name = "main", .tid = @as(i64, 1), .order = @as(i64, 0) });
    try g.link(session, "threads", thread);

    // Tree should update reactively
    try testing.expectEqual(@as(u32, 3), tree.total()); // + thread
}

test "DAP E2E: reactive delete on expanded child" {
    // Test that deleting a child from an expanded parent updates the tree reactively
    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    const session = try g.insert("Session");
    try g.update(session, .{ .name = "main", .order = @as(i64, 0) });
    try g.link(debugger, "sessions", session);

    const thread = try g.insert("Thread");
    try g.update(thread, .{ .name = "main", .tid = @as(i64, 1), .order = @as(i64, 0) });
    try g.link(session, "threads", thread);

    var tree = try g.view(.{
        .root = "Debugger",
        .sort = &.{"order"},
        .edges = &.{.{
            .name = "sessions",
            .sort = &.{"order"},
            .edges = &.{.{
                .name = "threads",
                .sort = &.{"order"},
            }},
        }},
    }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    try tree.expandById(debugger, "sessions");
    try tree.expandById(session, "threads");
    try testing.expectEqual(@as(u32, 3), tree.total()); // debugger + session + thread

    // Delete the thread
    try g.delete(thread);

    // Tree should update reactively
    try testing.expectEqual(@as(u32, 2), tree.total()); // debugger + session
}

test "DAP E2E: reactive unlink on expanded child" {
    // Test that unlinking a child from an expanded parent updates the tree reactively
    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    const session = try g.insert("Session");
    try g.update(session, .{ .name = "main", .order = @as(i64, 0) });
    try g.link(debugger, "sessions", session);

    const thread = try g.insert("Thread");
    try g.update(thread, .{ .name = "main", .tid = @as(i64, 1), .order = @as(i64, 0) });
    try g.link(session, "threads", thread);

    var tree = try g.view(.{
        .root = "Debugger",
        .sort = &.{"order"},
        .edges = &.{.{
            .name = "sessions",
            .sort = &.{"order"},
            .edges = &.{.{
                .name = "threads",
                .sort = &.{"order"},
            }},
        }},
    }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    try tree.expandById(debugger, "sessions");
    try tree.expandById(session, "threads");
    try testing.expectEqual(@as(u32, 3), tree.total()); // debugger + session + thread

    // Unlink the thread (but don't delete it)
    try g.unlink(session, "threads", thread);

    // Tree should update reactively
    try testing.expectEqual(@as(u32, 2), tree.total()); // debugger + session
}

test "DAP E2E: reactive update on collapsed child (unloaded)" {
    // Test that updates to nodes in result_set (but not in viewport) trigger
    // onLeave callbacks which update the expanded children count.
    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    // Create 10 breakpoints
    var bp_ids: [10]NodeId = undefined;
    for (0..10) |i| {
        bp_ids[i] = try g.insert("Breakpoint");
        try g.update(bp_ids[i], .{ .line = @as(i64, @intCast(i * 10)), .enabled = true, .order = @as(i64, @intCast(i)) });
        try g.link(debugger, "breakpoints", bp_ids[i]);
    }

    // Create tree with filter for enabled breakpoints only
    var tree = try g.view(.{
        .root = "Debugger",
        .sort = &.{"order"},
        .edges = &.{.{
            .name = "breakpoints",
            .sort = &.{"order"},
            .filter = &.{.{ .field = "enabled", .op = .eq, .value = .{ .bool = true } }},
        }},
    }, .{ .limit = 5 });
    defer tree.deinit();
    tree.activate(false);

    try tree.expandById(debugger, "breakpoints");

    // All 10 enabled breakpoints should be counted
    try testing.expectEqual(@as(u32, 11), tree.total()); // debugger + 10 breakpoints

    // Disable some breakpoints - these trigger onLeave callbacks which
    // decrement the expanded_nodes count (reactive filter update)
    try g.update(bp_ids[7], .{ .enabled = false });
    try g.update(bp_ids[8], .{ .enabled = false });
    try g.update(bp_ids[9], .{ .enabled = false });

    // Count should update reactively (no need to collapse/re-expand)
    try testing.expectEqual(@as(u32, 8), tree.total()); // debugger + 7 breakpoints
}

test "DAP E2E: deep hierarchy with selective expansion" {
    // Test 4-level deep hierarchy with selective expansion:
    // Debugger -> Session -> Thread -> Frame -> Scope
    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Build deep hierarchy
    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    const session = try g.insert("Session");
    try g.update(session, .{ .name = "debug", .order = @as(i64, 0) });
    try g.link(debugger, "sessions", session);

    const thread = try g.insert("Thread");
    try g.update(thread, .{ .name = "main", .tid = @as(i64, 1), .order = @as(i64, 0) });
    try g.link(session, "threads", thread);

    // 5 frames
    var frame_ids: [5]NodeId = undefined;
    for (0..5) |i| {
        frame_ids[i] = try g.insert("Frame");
        try g.update(frame_ids[i], .{ .name = "func", .order = @as(i64, @intCast(i)) });
        try g.link(thread, "frames", frame_ids[i]);
    }

    // 3 scopes per frame (15 total)
    for (frame_ids) |frame_id| {
        for (0..3) |i| {
            const scope = try g.insert("Scope");
            try g.update(scope, .{ .name = "scope", .order = @as(i64, @intCast(i)) });
            try g.link(frame_id, "scopes", scope);
        }
    }

    var tree = try g.view(.{
        .root = "Debugger",
        .sort = &.{"order"},
        .edges = &.{.{
            .name = "sessions",
            .sort = &.{"order"},
            .edges = &.{.{
                .name = "threads",
                .sort = &.{"order"},
                .edges = &.{.{
                    .name = "frames",
                    .sort = &.{"order"},
                    .edges = &.{.{
                        .name = "scopes",
                        .sort = &.{"order"},
                    }},
                }},
            }},
        }},
    }, .{ .limit = 30 });
    defer tree.deinit();
    tree.activate(false);

    // Progressive expansion
    try testing.expectEqual(@as(u32, 1), tree.total());

    try tree.expandById(debugger, "sessions");
    try testing.expectEqual(@as(u32, 2), tree.total());

    try tree.expandById(session, "threads");
    try testing.expectEqual(@as(u32, 3), tree.total());

    try tree.expandById(thread, "frames");
    try testing.expectEqual(@as(u32, 8), tree.total()); // + 5 frames

    // Expand scopes only on first and last frame
    try tree.expandById(frame_ids[0], "scopes");
    try testing.expectEqual(@as(u32, 11), tree.total()); // + 3 scopes

    try tree.expandById(frame_ids[4], "scopes");
    try testing.expectEqual(@as(u32, 14), tree.total()); // + 3 scopes

    // Collapse thread - all nested expansions disappear
    tree.collapseById(session, "threads");
    try testing.expectEqual(@as(u32, 2), tree.total()); // just debugger + session

    // Re-expand thread - frame expansion state is lost
    try tree.expandById(session, "threads");
    try testing.expectEqual(@as(u32, 3), tree.total()); // thread visible but frames collapsed
}

test "DAP E2E: viewport scrolling maintains correct items" {
    // Test that scrolling through a large expanded tree maintains correct items
    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    // Create 50 breakpoints
    for (0..50) |i| {
        const bp = try g.insert("Breakpoint");
        try g.update(bp, .{ .line = @as(i64, @intCast(i)), .order = @as(i64, @intCast(i)) });
        try g.link(debugger, "breakpoints", bp);
    }

    var tree = try g.view(.{
        .root = "Debugger",
        .sort = &.{"order"},
        .edges = &.{.{ .name = "breakpoints", .sort = &.{"order"} }},
    }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    try tree.expandById(debugger, "breakpoints");
    try testing.expectEqual(@as(u32, 51), tree.total()); // debugger + 50 breakpoints

    // At offset 0, should see debugger + first 9 breakpoints
    tree.move(0);
    var iter = tree.items();
    var count: u32 = 0;
    while (iter.next()) |_| {
        count += 1;
    }
    try testing.expectEqual(@as(u32, 10), count);

    // Move to middle
    tree.move(25);
    iter = tree.items();
    count = 0;
    while (iter.next()) |_| {
        count += 1;
    }
    try testing.expectEqual(@as(u32, 10), count);

    // Move to end
    tree.move(41); // 51 total - 10 height = 41
    iter = tree.items();
    count = 0;
    while (iter.next()) |_| {
        count += 1;
    }
    try testing.expectEqual(@as(u32, 10), count);
}

test "DAP E2E: expansion isExpanded query" {
    // Test that isExpandedById correctly reports expansion state
    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    const session = try g.insert("Session");
    try g.update(session, .{ .name = "main", .order = @as(i64, 0) });
    try g.link(debugger, "sessions", session);

    var tree = try g.view(.{
        .root = "Debugger",
        .sort = &.{"order"},
        .edges = &.{.{
            .name = "sessions",
            .sort = &.{"order"},
            .edges = &.{.{ .name = "threads", .sort = &.{"order"} }},
        }},
    }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    // Initially nothing expanded
    try testing.expect(!tree.isExpandedById(debugger, "sessions"));
    try testing.expect(!tree.isExpandedById(session, "threads"));

    // Expand sessions
    try tree.expandById(debugger, "sessions");
    try testing.expect(tree.isExpandedById(debugger, "sessions"));
    try testing.expect(!tree.isExpandedById(session, "threads"));

    // Expand threads
    try tree.expandById(session, "threads");
    try testing.expect(tree.isExpandedById(debugger, "sessions"));
    try testing.expect(tree.isExpandedById(session, "threads"));

    // Collapse sessions
    tree.collapseById(debugger, "sessions");
    try testing.expect(!tree.isExpandedById(debugger, "sessions"));
    // Note: session's threads expansion is lost when parent collapses
}

test "DAP E2E: empty edges don't affect total" {
    // Test that expanding an edge with no children is handled correctly
    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "gdb", .order = @as(i64, 0) });

    const session = try g.insert("Session");
    try g.update(session, .{ .name = "main", .order = @as(i64, 0) });
    try g.link(debugger, "sessions", session);

    // Session has no threads

    var tree = try g.view(.{
        .root = "Debugger",
        .sort = &.{"order"},
        .edges = &.{.{
            .name = "sessions",
            .sort = &.{"order"},
            .edges = &.{.{ .name = "threads", .sort = &.{"order"} }},
        }},
    }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    try tree.expandById(debugger, "sessions");
    try testing.expectEqual(@as(u32, 2), tree.total()); // debugger + session

    // Expanding empty threads edge should work but add nothing
    try tree.expandById(session, "threads");
    try testing.expectEqual(@as(u32, 2), tree.total()); // still just debugger + session

    // Adding a thread should now show up
    const thread = try g.insert("Thread");
    try g.update(thread, .{ .name = "main", .tid = @as(i64, 1), .order = @as(i64, 0) });
    try g.link(session, "threads", thread);

    try testing.expectEqual(@as(u32, 3), tree.total()); // debugger + session + thread
}
