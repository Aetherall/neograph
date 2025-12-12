///! Tests for the reactive system that verify bugs are FIXED.
///!
///! These tests document issues that were identified in the reactive system audit
///! and verify they are now fixed through the public Graph API.
///!
///! See docs/reactive-system-audit.md for full details on each issue.

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

// Import the public API only
const ng = @import("neograph.zig");
const Graph = ng.Graph;
const Schema = ng.Schema;
const NodeId = ng.NodeId;
const View = ng.View;
const ViewOpts = ng.ViewOpts;
const parseSchema = ng.parseSchema;
const Callbacks = ng.Callbacks;
const Item = ng.Item;

// ============================================================================
// Schema Definitions (JSON)
// ============================================================================

fn createTreeTestSchema(allocator: Allocator) !Schema {
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
        \\        { "name": "children", "target": "Item", "reverse": "_parent_item" },
        \\        { "name": "_parent_item", "target": "Item", "reverse": "children" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "priority", "direction": "asc" }] }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

fn createHierarchySchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "Session",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "priority", "type": "int" }
        \\      ],
        \\      "edges": [{ "name": "threads", "target": "Thread", "reverse": "session" }],
        \\      "indexes": [{ "fields": [{ "field": "priority", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Thread",
        \\      "properties": [{ "name": "tid", "type": "int" }],
        \\      "edges": [
        \\        { "name": "session", "target": "Session", "reverse": "threads" },
        \\        { "name": "frames", "target": "Frame", "reverse": "thread" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "tid", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Frame",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "index", "type": "int" }
        \\      ],
        \\      "edges": [{ "name": "thread", "target": "Thread", "reverse": "frames" }],
        \\      "indexes": [{ "fields": [{ "field": "index", "direction": "asc" }] }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

fn createDapSchema(allocator: Allocator) !Schema {
    // Matches the DAP demo schema exactly
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "Debugger",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [{ "name": "threads", "target": "Thread", "reverse": "debugger" }],
        \\      "indexes": [{ "fields": [{ "field": "name" }] }]
        \\    },
        \\    {
        \\      "name": "Thread",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "state", "type": "string" }
        \\      ],
        \\      "edges": [
        \\        { "name": "debugger", "target": "Debugger", "reverse": "threads" },
        \\        { "name": "frames", "target": "Frame", "reverse": "thread" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "name" }] }]
        \\    },
        \\    {
        \\      "name": "Frame",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "line", "type": "int" }
        \\      ],
        \\      "edges": [
        \\        { "name": "thread", "target": "Thread", "reverse": "frames" },
        \\        { "name": "scopes", "target": "Scope", "reverse": "frame" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "line" }] }]
        \\    },
        \\    {
        \\      "name": "Scope",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [
        \\        { "name": "frame", "target": "Frame", "reverse": "scopes" },
        \\        { "name": "variables", "target": "Variable", "reverse": "scope" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "name" }] }]
        \\    },
        \\    {
        \\      "name": "Variable",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "value", "type": "string" }
        \\      ],
        \\      "edges": [{ "name": "scope", "target": "Scope", "reverse": "variables" }],
        \\      "indexes": [{ "fields": [{ "field": "name" }] }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

// ============================================================================
// FIXED: Tree API Core Tests (Black-Box)
// ============================================================================
// These tests verify the core bugs are fixed via the public Graph API.

test "FIXED: Nested expansion updates total visible count" {
    ensureWatchdog();

    // BUG #1: CountTree visible count not cascaded
    // When nested edge expands, total visible count was not updated correctly.
    // Tree API fixes this with proper visibility propagation.

    const schema = try createTreeTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create: Root -> Items (sessions) -> Items (threads)
    const root = try g.insert("Root");
    try g.update(root, .{ .priority = @as(i64, 1) });

    const session1 = try g.insert("Item");
    try g.update(session1, .{ .priority = @as(i64, 10) });
    const session2 = try g.insert("Item");
    try g.update(session2, .{ .priority = @as(i64, 20) });
    try g.link(root, "children", session1);
    try g.link(root, "children", session2);

    const thread1 = try g.insert("Item");
    try g.update(thread1, .{ .priority = @as(i64, 100) });
    const thread2 = try g.insert("Item");
    try g.update(thread2, .{ .priority = @as(i64, 101) });
    const thread3 = try g.insert("Item");
    try g.update(thread3, .{ .priority = @as(i64, 102) });
    try g.link(session1, "children", thread1);
    try g.link(session1, "children", thread2);
    try g.link(session1, "children", thread3);

    // Create tree (only root visible initially) - edges must be in query to be expandable
    var tree = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"}, .recursive = true }},
    }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    // Initially: only root visible (query returns only Root type)
    try testing.expectEqual(@as(u32, 1), tree.total());

    // Expand root -> children (shows sessions)
    try tree.expandById(root, "children");
    try testing.expectEqual(@as(u32, 3), tree.total()); // root + 2 sessions

    // Expand session1 -> children (shows threads)
    try tree.expandById(session1, "children");

    // FIXED: Total visible = root + session1 + 3 threads + session2 = 6
    try testing.expectEqual(@as(u32, 6), tree.total());
}

test "FIXED: Expansion state is fresh after delete and re-insert" {
    // BUG #5: Expansion state persists after delete
    // When item was deleted and re-added with same ID, old expansion state persisted.
    // Tree API fixes this by tying expansion state to node lifecycle.

    const schema = try createTreeTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create root with children
    const root = try g.insert("Root");
    try g.update(root, .{ .priority = @as(i64, 1) });

    const child1 = try g.insert("Item");
    try g.update(child1, .{ .priority = @as(i64, 10) });
    const child2 = try g.insert("Item");
    try g.update(child2, .{ .priority = @as(i64, 20) });
    try g.link(root, "children", child1);
    try g.link(root, "children", child2);

    // Create tree - edges must be in query to be expandable
    var tree = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"} }},
    }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    // Expand and verify
    try tree.expandById(root, "children");
    try testing.expectEqual(@as(u32, 3), tree.total()); // root + 2 children
    try testing.expect(tree.isExpandedById(root, "children"));

    // Delete root (triggers onLeave callback)
    try g.delete(root);

    // FIXED: Tree should be empty (root was deleted)
    try testing.expectEqual(@as(u32, 0), tree.total());

    // Re-insert a new root with different children
    const new_root = try g.insert("Root");
    try g.update(new_root, .{ .priority = @as(i64, 1) });
    const new_child = try g.insert("Item");
    try g.update(new_child, .{ .priority = @as(i64, 30) });
    try g.link(new_root, "children", new_child);

    // FIXED: New root starts collapsed (no stale state)
    // Note: Tree receives onEnter callback for new root
    try testing.expectEqual(@as(u32, 1), tree.total());
    try testing.expect(!tree.isExpandedById(new_root, "children"));
}

test "FIXED: Index lookup works correctly for large trees" {
    // BUG #3: indexOf is O(n)
    // Every indexOf call walked the linked list.
    // Tree API provides efficient index lookup.

    const schema = try createTreeTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create many Root items with ascending priorities
    const COUNT = 100;
    var ids: [COUNT]NodeId = undefined;
    for (0..COUNT) |i| {
        ids[i] = try g.insert("Root");
        try g.update(ids[i], .{ .priority = @as(i64, @intCast(i)) });
    }

    // Create tree
    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 50 });
    defer tree.deinit();
    tree.activate(false);

    try testing.expectEqual(@as(u32, COUNT), tree.total());

    // Verify indices are correct for items in viewport (lazy loading only loads viewport)
    // With lazy loading, only the first 'height' items are loaded
    const viewport_size: usize = 50;
    for (0..viewport_size) |i| {
        const idx = tree.indexOfId(ids[i]);
        try testing.expect(idx != null);
        try testing.expectEqual(@as(u32, @intCast(i)), idx.?);
    }

    // Items outside viewport won't be in reactive_tree (this is expected with lazy loading)
    // They'll be loaded when scrolling brings them into viewport
}

test "FIXED: Deep hierarchy renders all nodes" {
    // BUG #12: Path buffer overflow (silent truncation)
    // Fixed 32-element path buffer with silent truncation.
    // Tree API doesn't use path buffers - direct NodeId lookup.

    const schema = try createTreeTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Build a deep tree (old system truncated at 32)
    const DEPTH = 50;
    var ids: [DEPTH]NodeId = undefined;

    // First node is Root, rest are Items
    ids[0] = try g.insert("Root");
    try g.update(ids[0], .{ .priority = @as(i64, 0) });

    for (1..DEPTH) |i| {
        ids[i] = try g.insert("Item");
        try g.update(ids[i], .{ .priority = @as(i64, @intCast(i)) });
    }

    // Link them into a chain: 0 -> 1 -> 2 -> ... -> 49
    for (0..DEPTH - 1) |i| {
        try g.link(ids[i], "children", ids[i + 1]);
    }

    // Create tree with root node - recursive edges for deep hierarchy
    var tree = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"}, .recursive = true }},
    }, .{ .limit = 100 });
    defer tree.deinit();
    tree.activate(false);

    // Expand entire chain
    for (0..DEPTH - 1) |i| {
        try tree.expandById(ids[i], "children");
    }

    // FIXED: All nodes visible
    try testing.expectEqual(@as(u32, DEPTH), tree.total());

    // FIXED: Can get index of deepest node
    const deepest_idx = tree.indexOfId(ids[DEPTH - 1]);
    try testing.expect(deepest_idx != null);
    try testing.expectEqual(@as(u32, DEPTH - 1), deepest_idx.?);
}

test "FIXED: Scroll consistency between small and large scrolls" {
    // BUG #4: Window scroll path inconsistency
    // Small scroll used linked list walk, large scroll rebuilt.
    // Tree API has consistent behavior for all scrolls.

    const schema = try createTreeTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create 100 Root items
    for (0..100) |i| {
        const id = try g.insert("Root");
        try g.update(id, .{ .priority = @as(i64, @intCast(i)) });
    }

    // Create two trees with same content
    var tree1 = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 10 });
    defer tree1.deinit();
    tree1.activate(false);

    var tree2 = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 10 });
    defer tree2.deinit();
    tree2.activate(false);

    // Tree 1: Small scrolls (50 single-step moves)
    for (0..50) |_| {
        tree1.move(1);
    }

    // Tree 2: Direct scrollTo
    tree2.scrollTo(50);

    // FIXED: Both at same position
    try testing.expectEqual(tree1.getOffset(), tree2.getOffset());
    try testing.expectEqual(@as(u32, 50), tree1.getOffset());
}

test "FIXED: Items maintain sorted order after inserts" {
    // BUG #9: insertRootAt sort key mismatch
    // Window insertion used index but nodeAtIndex walked linked list.
    // Tree API maintains consistent sorted order.

    const schema = try createTreeTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Insert Root items with gaps to allow insertions between
    const id100 = try g.insert("Root");
    try g.update(id100, .{ .priority = @as(i64, 100) });
    const id80 = try g.insert("Root");
    try g.update(id80, .{ .priority = @as(i64, 80) });
    const id60 = try g.insert("Root");
    try g.update(id60, .{ .priority = @as(i64, 60) });
    const id40 = try g.insert("Root");
    try g.update(id40, .{ .priority = @as(i64, 40) });
    const id20 = try g.insert("Root");
    try g.update(id20, .{ .priority = @as(i64, 20) });

    // Create tree
    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    // Verify order (sorted by priority ascending): 20, 40, 60, 80, 100
    try testing.expectEqual(@as(?u32, 0), tree.indexOfId(id20));
    try testing.expectEqual(@as(?u32, 1), tree.indexOfId(id40));
    try testing.expectEqual(@as(?u32, 2), tree.indexOfId(id60));
    try testing.expectEqual(@as(?u32, 3), tree.indexOfId(id80));
    try testing.expectEqual(@as(?u32, 4), tree.indexOfId(id100));

    // Insert with priority 90 (should go between 80 and 100)
    const id90 = try g.insert("Root");
    try g.update(id90, .{ .priority = @as(i64, 90) });

    // FIXED: Correct position (index 4, between 80 and 100)
    try testing.expectEqual(@as(?u32, 4), tree.indexOfId(id90));
    try testing.expectEqual(@as(?u32, 5), tree.indexOfId(id100));

    // Verify full order: 20, 40, 60, 80, 90, 100
    try testing.expectEqual(@as(u32, 6), tree.total());
}

test "FIXED: Sort order updated after property change" {
    // Tests that changing a node's sort key updates its position

    const schema = try createTreeTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create Root items with priorities: 10, 20, 30, 40
    const id1 = try g.insert("Root");
    try g.update(id1, .{ .priority = @as(i64, 10) });
    const id2 = try g.insert("Root");
    try g.update(id2, .{ .priority = @as(i64, 20) });
    const id3 = try g.insert("Root");
    try g.update(id3, .{ .priority = @as(i64, 30) });
    const id4 = try g.insert("Root");
    try g.update(id4, .{ .priority = @as(i64, 40) });

    // Create tree
    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    // Verify initial order: id1(10), id2(20), id3(30), id4(40)
    try testing.expectEqual(@as(?u32, 0), tree.indexOfId(id1));
    try testing.expectEqual(@as(?u32, 1), tree.indexOfId(id2));
    try testing.expectEqual(@as(?u32, 2), tree.indexOfId(id3));
    try testing.expectEqual(@as(?u32, 3), tree.indexOfId(id4));

    // Change id1's priority to 35 (should move between 30 and 40)
    try g.update(id1, .{ .priority = @as(i64, 35) });

    // FIXED: Order updated: id2(20), id3(30), id1(35), id4(40)
    try testing.expectEqual(@as(?u32, 0), tree.indexOfId(id2));
    try testing.expectEqual(@as(?u32, 1), tree.indexOfId(id3));
    try testing.expectEqual(@as(?u32, 2), tree.indexOfId(id1));
    try testing.expectEqual(@as(?u32, 3), tree.indexOfId(id4));
}

// ============================================================================
// Hierarchy/Nested Edge Tests
// ============================================================================

test "Nested: expand and collapse hierarchy" {
    // Test that hierarchy expansion and collapse works correctly through Graph API

    const schema = try createHierarchySchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create hierarchy: Session -> Thread -> Frame
    const session = try g.insert("Session");
    try g.update(session, .{ .name = "debug", .priority = @as(i64, 1) });

    const thread = try g.insert("Thread");
    try g.update(thread, .{ .tid = @as(i64, 1) });
    try g.link(session, "threads", thread);

    const frame1 = try g.insert("Frame");
    try g.update(frame1, .{ .name = "main", .index = @as(i64, 0) });
    try g.link(thread, "frames", frame1);

    const frame2 = try g.insert("Frame");
    try g.update(frame2, .{ .name = "init", .index = @as(i64, 1) });
    try g.link(thread, "frames", frame2);

    // Create tree - edges must be in query to be expandable
    var tree = try g.view(.{
        .root = "Session",
        .sort = &.{"priority"},
        .edges = &.{.{
            .name = "threads",
            .sort = &.{"tid"},
            .edges = &.{.{ .name = "frames", .sort = &.{"index"} }},
        }},
    }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    // Initially: only session visible
    try testing.expectEqual(@as(u32, 1), tree.total());

    // Expand session -> threads
    try tree.expandById(session, "threads");
    try testing.expectEqual(@as(u32, 2), tree.total()); // session + thread

    // Expand thread -> frames
    try tree.expandById(thread, "frames");
    try testing.expectEqual(@as(u32, 4), tree.total()); // session + thread + 2 frames

    // Collapse thread -> frames
    tree.collapseById(thread, "frames");
    try testing.expectEqual(@as(u32, 2), tree.total()); // back to session + thread

    // Collapse session -> threads
    tree.collapseById(session, "threads");
    try testing.expectEqual(@as(u32, 1), tree.total()); // back to just session
}

test "Nested: new child appears after link when expanded" {
    // Test that linking a new child makes it visible in an expanded tree

    const schema = try createHierarchySchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create session with thread
    const session = try g.insert("Session");
    try g.update(session, .{ .name = "debug", .priority = @as(i64, 1) });

    const thread = try g.insert("Thread");
    try g.update(thread, .{ .tid = @as(i64, 1) });
    try g.link(session, "threads", thread);

    // Create tree and expand - edges must be in query to be expandable
    var tree = try g.view(.{
        .root = "Session",
        .sort = &.{"priority"},
        .edges = &.{.{
            .name = "threads",
            .sort = &.{"tid"},
            .edges = &.{.{ .name = "frames", .sort = &.{"index"} }},
        }},
    }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    try tree.expandById(session, "threads");
    try tree.expandById(thread, "frames");
    try testing.expectEqual(@as(u32, 2), tree.total()); // session + thread (no frames yet)

    // Link a new frame
    const frame = try g.insert("Frame");
    try g.update(frame, .{ .name = "main", .index = @as(i64, 0) });
    try g.link(thread, "frames", frame);

    // The tree should show the new frame after a reload
    // Note: The Tree API may need expansion refresh - let's check
    // Since the edge was already expanded, we need to verify behavior

    // For now, test that we can expand again and see the frame
    tree.collapseById(thread, "frames");
    try tree.expandById(thread, "frames");
    try testing.expectEqual(@as(u32, 3), tree.total()); // session + thread + frame
}

test "Nested: reactive unlink with edge selections" {
    // Test that unlinking a child removes it reactively from an expanded tree
    // when edge selections are in the query.
    //
    // Flow: Create tree with edge selections -> link children -> expand -> unlink -> verify removed

    const schema = try createHierarchySchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create root session first
    const session = try g.insert("Session");
    try g.update(session, .{ .name = "debug", .priority = @as(i64, 1) });

    // Create tree WITH edge selections (enables reactive child tracking)
    var tree = try g.view(.{
        .root = "Session",
        .sort = &.{"priority"},
        .edges = &.{.{
            .name = "threads",
            .edges = &.{.{
                .name = "frames",
                .sort = &.{"index"},
            }},
        }},
    }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    // Initially just the session
    try testing.expectEqual(@as(u32, 1), tree.total());

    // Link children AFTER tree creation - onLink fires and registers with tracker
    const thread = try g.insert("Thread");
    try g.update(thread, .{ .tid = @as(i64, 1) });
    try g.link(session, "threads", thread);

    const frame = try g.insert("Frame");
    try g.update(frame, .{ .name = "main", .index = @as(i64, 0) });
    try g.link(thread, "frames", frame);

    // Expand to see children (loaded from result_set)
    try tree.expandById(session, "threads");
    try tree.expandById(thread, "frames");
    try testing.expectEqual(@as(u32, 3), tree.total()); // session + thread + frame

    // Unlink the frame - should trigger onLeave and remove from tree
    try g.unlink(thread, "frames", frame);

    // Tree should update reactively
    try testing.expectEqual(@as(u32, 2), tree.total()); // session + thread (no frame)
}

test "Viewport iteration returns correct items" {
    // Test that viewport iteration returns the correct visible items

    const schema = try createTreeTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create 20 Root items
    var ids: [20]NodeId = undefined;
    for (0..20) |i| {
        ids[i] = try g.insert("Root");
        try g.update(ids[i], .{ .priority = @as(i64, @intCast(i * 10)) });
    }

    // Create tree with height 5
    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 5 });
    defer tree.deinit();
    tree.activate(false);

    // Count visible items
    var iter = tree.items();
    var count: u32 = 0;
    while (iter.next()) |_| {
        count += 1;
    }
    try testing.expectEqual(@as(u32, 5), count);

    // Scroll and count again
    tree.scrollTo(10);
    iter = tree.items();
    count = 0;
    while (iter.next()) |_| {
        count += 1;
    }
    try testing.expectEqual(@as(u32, 5), count);
}

test "Multiple trees on same graph are independent" {
    // Test that multiple trees on the same graph work independently

    const schema = try createTreeTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create root with children
    const root = try g.insert("Root");
    try g.update(root, .{ .priority = @as(i64, 1) });

    const child = try g.insert("Item");
    try g.update(child, .{ .priority = @as(i64, 10) });
    try g.link(root, "children", child);

    // Create two trees - edges must be in query to be expandable
    var tree1 = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"} }},
    }, .{ .limit = 10 });
    defer tree1.deinit();
    tree1.activate(false);

    var tree2 = try g.view(.{
        .root = "Root",
        .sort = &.{"priority"},
        .edges = &.{.{ .name = "children", .sort = &.{"priority"} }},
    }, .{ .limit = 10 });
    defer tree2.deinit();
    tree2.activate(false);

    // Both start with 1 item
    try testing.expectEqual(@as(u32, 1), tree1.total());
    try testing.expectEqual(@as(u32, 1), tree2.total());

    // Expand tree1 only
    try tree1.expandById(root, "children");

    // Tree1 shows children, tree2 does not
    try testing.expectEqual(@as(u32, 2), tree1.total());
    try testing.expectEqual(@as(u32, 1), tree2.total());

    // Expand tree2
    try tree2.expandById(root, "children");
    try testing.expectEqual(@as(u32, 2), tree2.total());
}

test "Reactive: new root appears in tree" {
    // Test that inserting a new root node makes it appear in the tree

    const schema = try createTreeTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create initial root
    const root1 = try g.insert("Root");
    try g.update(root1, .{ .priority = @as(i64, 10) });

    // Create tree
    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    try testing.expectEqual(@as(u32, 1), tree.total());
    try testing.expectEqual(@as(?u32, 0), tree.indexOfId(root1));

    // Insert new root with lower priority (should appear first)
    const root2 = try g.insert("Root");
    try g.update(root2, .{ .priority = @as(i64, 5) });

    try testing.expectEqual(@as(u32, 2), tree.total());
    try testing.expectEqual(@as(?u32, 0), tree.indexOfId(root2)); // root2 first (priority 5)
    try testing.expectEqual(@as(?u32, 1), tree.indexOfId(root1)); // root1 second (priority 10)
}

test "Reactive: deleted root disappears from tree" {
    // Test that deleting a root node removes it from the tree

    const schema = try createTreeTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create two roots
    const root1 = try g.insert("Root");
    try g.update(root1, .{ .priority = @as(i64, 10) });
    const root2 = try g.insert("Root");
    try g.update(root2, .{ .priority = @as(i64, 20) });

    // Create tree
    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    try testing.expectEqual(@as(u32, 2), tree.total());

    // Delete root1
    try g.delete(root1);

    try testing.expectEqual(@as(u32, 1), tree.total());
    try testing.expectEqual(@as(?u32, null), tree.indexOfId(root1));
    try testing.expectEqual(@as(?u32, 0), tree.indexOfId(root2));
}

test "Reactive: property update moves node in sorted tree" {
    // Test that updating a sort property moves the node

    const schema = try createTreeTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create roots with specific priorities
    const root1 = try g.insert("Root");
    try g.update(root1, .{ .priority = @as(i64, 10) });
    const root2 = try g.insert("Root");
    try g.update(root2, .{ .priority = @as(i64, 20) });
    const root3 = try g.insert("Root");
    try g.update(root3, .{ .priority = @as(i64, 30) });

    // Create tree
    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    // Initial order: root1, root2, root3
    try testing.expectEqual(@as(?u32, 0), tree.indexOfId(root1));
    try testing.expectEqual(@as(?u32, 1), tree.indexOfId(root2));
    try testing.expectEqual(@as(?u32, 2), tree.indexOfId(root3));

    // Update root1's priority to 25 (should move between root2 and root3)
    try g.update(root1, .{ .priority = @as(i64, 25) });

    // New order: root2, root1, root3
    try testing.expectEqual(@as(?u32, 0), tree.indexOfId(root2));
    try testing.expectEqual(@as(?u32, 1), tree.indexOfId(root1));
    try testing.expectEqual(@as(?u32, 2), tree.indexOfId(root3));
}

test "Reactive: delete AFTER items() - verifies loaded viewport delete works" {
    // This test matches exactly what the Lua binding does:
    // 1. Create nodes
    // 2. Create view
    // 3. Call items() which loads the viewport (like Lua's get_visible())
    // 4. Delete a node
    // 5. Verify the tree updates correctly
    //
    // If this test passes but Lua crashes, the bug is Lua-specific.

    const schema = try createTreeTestSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create two roots
    const root1 = try g.insert("Root");
    try g.update(root1, .{ .priority = @as(i64, 10) });
    const root2 = try g.insert("Root");
    try g.update(root2, .{ .priority = @as(i64, 20) });

    // Create tree
    var tree = try g.view(.{ .root = "Root", .sort = &.{"priority"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    // KEY: Call items() to load the viewport - this is what Lua's get_visible() does
    var count: u32 = 0;
    var iter = tree.items();
    while (iter.next()) |_| {
        count += 1;
    }
    try testing.expectEqual(@as(u32, 2), count);

    // NOW delete while viewport is loaded - this is where Lua crashes
    try g.delete(root1);

    // Verify tree updates correctly
    try testing.expectEqual(@as(u32, 1), tree.total());
    try testing.expectEqual(@as(?u32, null), tree.indexOfId(root1));
    try testing.expectEqual(@as(?u32, 0), tree.indexOfId(root2));
}

// ============================================================================
// BUG: onEnter not firing for linked children on expanded edge
// ============================================================================

/// Callback tracker for reactive event testing
const CallbackTracker = struct {
    enters: std.ArrayListUnmanaged(EventRecord),
    leaves: std.ArrayListUnmanaged(EventRecord),
    allocator: Allocator,

    const EventRecord = struct {
        node_id: NodeId,
        index: u32,
    };

    fn init(allocator: Allocator) CallbackTracker {
        return .{
            .enters = .{},
            .leaves = .{},
            .allocator = allocator,
        };
    }

    fn deinit(self: *CallbackTracker) void {
        self.enters.deinit(self.allocator);
        self.leaves.deinit(self.allocator);
    }

    fn onEnter(ctx: ?*anyopaque, item: Item, index: u32) void {
        const self: *CallbackTracker = @ptrCast(@alignCast(ctx));
        self.enters.append(self.allocator, .{ .node_id = item.id, .index = index }) catch {};
    }

    fn onLeave(ctx: ?*anyopaque, item: Item, index: u32) void {
        const self: *CallbackTracker = @ptrCast(@alignCast(ctx));
        self.leaves.append(self.allocator, .{ .node_id = item.id, .index = index }) catch {};
    }

    fn getCallbacks(self: *CallbackTracker) Callbacks {
        return .{
            .on_enter = onEnter,
            .on_leave = onLeave,
            .context = self,
        };
    }

    fn hasEntered(self: *const CallbackTracker, node_id: NodeId) bool {
        for (self.enters.items) |record| {
            if (record.node_id == node_id) return true;
        }
        return false;
    }
};

test "BUG: onEnter fires when linking new node to already-expanded edge" {
    // BUG: On a reactive view, when a children edge is expanded, linking new nodes
    // to that edge does NOT trigger onEnter callbacks on the view.
    //
    // This reproduces the bug from dap_interactive.lua:
    // 1. Create a scope with EXISTING variables (important!)
    // 2. Create view with edge selections and activate (immediate = true)
    // 3. Expand the scope's edge (which shows existing children)
    // 4. Add a NEW child to the scope
    // 5. Expected: onEnter fires for the new child, it appears in get_visible()
    // 6. Actual: onEnter does NOT fire, child doesn't appear
    //
    // The workaround is to collapse and re-expand the edge.

    ensureWatchdog();

    const schema = try createHierarchySchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create initial data with EXISTING children (critical for reproducing bug)
    // Session -> Thread (with existing frames)
    const session = try g.insert("Session");
    try g.update(session, .{ .name = "debug", .priority = @as(i64, 1) });

    const thread1 = try g.insert("Thread");
    try g.update(thread1, .{ .tid = @as(i64, 1) });
    try g.link(session, "threads", thread1);

    // Create EXISTING frames (these will be visible when we expand)
    const existing_frame1 = try g.insert("Frame");
    try g.update(existing_frame1, .{ .name = "init", .index = @as(i64, 0) });
    try g.link(thread1, "frames", existing_frame1);

    const existing_frame2 = try g.insert("Frame");
    try g.update(existing_frame2, .{ .name = "setup", .index = @as(i64, 1) });
    try g.link(thread1, "frames", existing_frame2);

    // Create callback tracker to monitor onEnter events
    var tracker = CallbackTracker.init(testing.allocator);
    defer tracker.deinit();

    // Create view with nested edge selections (like dap_interactive.lua)
    var view = try g.view(.{
        .root = "Session",
        .sort = &.{"priority"},
        .edges = &.{.{
            .name = "threads",
            .sort = &.{"tid"},
            .edges = &.{.{
                .name = "frames",
                .sort = &.{"index"},
            }},
        }},
    }, .{ .limit = 100 });
    defer view.deinit();

    // Set callbacks and activate with FALSE (non-reactive mode, like most tree tests)
    view.setCallbacks(tracker.getCallbacks());
    view.activate(false);

    // Load the viewport first (like a UI's render() does with get_visible())
    var iter = view.items();
    while (iter.next()) |_| {}

    // Verify initial state: just session visible
    try testing.expectEqual(@as(u32, 1), view.total());

    // Expand session -> threads to see thread1
    try view.expandById(session, "threads");
    try testing.expectEqual(@as(u32, 2), view.total()); // session + thread1

    // Expand thread1 -> frames (shows existing frames!)
    try view.expandById(thread1, "frames");
    try testing.expectEqual(@as(u32, 4), view.total()); // session + thread1 + 2 existing frames

    // Load the viewport again after expansion (like UI's render() would)
    iter = view.items();
    while (iter.next()) |_| {}

    // Record state before linking new frame
    const enters_before_link = tracker.enters.items.len;
    const total_before_link = view.total();

    // Now create and link a NEW frame to the ALREADY-EXPANDED "frames" edge
    const new_frame = try g.insert("Frame");
    try g.update(new_frame, .{ .name = "main", .index = @as(i64, 2) });
    try g.link(thread1, "frames", new_frame);

    // BUG: The new frame should now be visible in the view (since edge is expanded)
    // Expected: total = 5 (session + thread1 + 2 existing frames + 1 new frame)
    // Actual: total stays at 4 (new frame not counted)
    try testing.expectEqual(total_before_link + 1, view.total());

    // BUG: onEnter should have fired for the newly linked frame
    try testing.expect(tracker.hasEntered(new_frame));
    try testing.expect(tracker.enters.items.len > enters_before_link);
}

test "rapid links to expanded edge should all appear (regression test)" {
    // This test verifies that rapid linking to an expanded edge works correctly.
    //
    // NOTE: A bug exists in the interactive nvim demo (dap_interactive.lua):
    // When pressing 'v' 20 times rapidly, only ~6 new variables appear.
    // The rest are "lost" until collapse/re-expand.
    //
    // This test passes in synchronous execution but serves as a regression test.

    ensureWatchdog();

    const schema = try createHierarchySchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create Session and Thread with expanded frames edge
    const session = try g.insert("Session");
    try g.update(session, .{ .name = "debug", .priority = @as(i64, 1) });

    const thread1 = try g.insert("Thread");
    try g.update(thread1, .{ .tid = @as(i64, 1) });
    try g.link(session, "threads", thread1);

    // Create initial frames
    const frame1 = try g.insert("Frame");
    try g.update(frame1, .{ .name = "init", .index = @as(i64, 0) });
    try g.link(thread1, "frames", frame1);

    // Create view with nested edge selections
    var view = try g.view(.{
        .root = "Session",
        .sort = &.{"priority"},
        .edges = &.{.{
            .name = "threads",
            .sort = &.{"tid"},
            .edges = &.{.{
                .name = "frames",
                .sort = &.{"index"},
            }},
        }},
    }, .{ .limit = 100 });
    defer view.deinit();

    // Activate and load
    view.activate(false);
    var iter = view.items();
    while (iter.next()) |_| {}

    // Expand session -> threads -> frames
    try view.expandById(session, "threads");
    try view.expandById(thread1, "frames");

    // Load viewport (like render() does)
    iter = view.items();
    while (iter.next()) |_| {}

    const total_before = view.total();

    // Now rapidly link 20 new frames (like pressing 'v' 20 times)
    const num_new_frames = 20;
    var new_frame_ids: [num_new_frames]NodeId = undefined;
    for (0..num_new_frames) |i| {
        const new_frame = try g.insert("Frame");
        try g.update(new_frame, .{ .name = "new_frame", .index = @as(i64, @intCast(i + 10)) });
        try g.link(thread1, "frames", new_frame);
        new_frame_ids[i] = new_frame;

        // Simulate render() - load viewport after each link
        iter = view.items();
        while (iter.next()) |_| {}
    }

    // All 20 new frames should be visible
    const expected_total = total_before + num_new_frames;
    try testing.expectEqual(expected_total, view.total());

    // Check that all new frames are actually in the view
    for (new_frame_ids) |frame_id| {
        try testing.expect(view.indexOfId(frame_id) != null);
    }
}

test "BUG: link after expand but BEFORE items() reload should still appear" {
    // This test attempts to reproduce the bug from dap_interactive.lua
    //
    // The bug hypothesis:
    // 1. expandById sets viewport_dirty=true but may not mark tree_node.expanded_edges
    // 2. Link happens which triggers handleEnter
    // 3. handleEnter checks tree_node.isExpanded() which may return false
    // 4. The new node is NOT added to reactive_tree
    // 5. When items() is finally called, the node is missing
    //
    // To reproduce: expand -> link -> items() (NOT expand -> items() -> link -> items())

    ensureWatchdog();

    const schema = try createHierarchySchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create Session -> Thread
    const session = try g.insert("Session");
    try g.update(session, .{ .name = "debug", .priority = @as(i64, 1) });

    const thread1 = try g.insert("Thread");
    try g.update(thread1, .{ .tid = @as(i64, 1) });
    try g.link(session, "threads", thread1);

    // Create initial frame
    const frame1 = try g.insert("Frame");
    try g.update(frame1, .{ .name = "init", .index = @as(i64, 0) });
    try g.link(thread1, "frames", frame1);

    // Create view
    var view = try g.view(.{
        .root = "Session",
        .sort = &.{"priority"},
        .edges = &.{.{
            .name = "threads",
            .sort = &.{"tid"},
            .edges = &.{.{
                .name = "frames",
                .sort = &.{"index"},
            }},
        }},
    }, .{ .limit = 100 });
    defer view.deinit();

    // Activate and do initial load
    view.activate(false);
    var iter = view.items();
    while (iter.next()) |_| {}

    // Expand session -> threads first and load
    try view.expandById(session, "threads");
    iter = view.items();
    while (iter.next()) |_| {}

    // Now expand thread1 -> frames but DON'T call items() yet
    try view.expandById(thread1, "frames");
    // viewport_dirty is now true, tree_node for thread1 should have "frames" expanded

    // Link a new frame BEFORE calling items()
    const new_frame = try g.insert("Frame");
    try g.update(new_frame, .{ .name = "new_frame", .index = @as(i64, 10) });
    try g.link(thread1, "frames", new_frame);

    // NOW call items() to reload the viewport
    iter = view.items();
    var count: u32 = 0;
    while (iter.next()) |_| {
        count += 1;
    }

    // The new frame should be visible
    // Expected: session + thread1 + frame1 + new_frame = 4
    // If bug exists: session + thread1 + frame1 = 3 (new_frame missing)
    try testing.expectEqual(@as(u32, 4), view.total());
    try testing.expect(view.indexOfId(new_frame) != null);
}

test "BUG: link stops appearing after 3 links with items() between each" {
    // Reproduces bug from dap_interactive.lua with EXACT same order of operations:
    // 1. Create debugger
    // 2. Create view
    // 3. Expand debugger→threads
    // 4. THEN create thread/frames/scopes/variables via simulated keypresses

    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Step 1: Create debugger (like line 134 in dap_interactive.lua)
    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "main" });

    // Step 2: Create view (like line 661)
    var view = try g.view(.{
        .root = "Debugger",
        .sort = &.{"name"},
        .edges = &.{.{
            .name = "threads",
            .sort = &.{"name"},
            .edges = &.{.{
                .name = "frames",
                .sort = &.{"line"},
                .edges = &.{.{
                    .name = "scopes",
                    .sort = &.{"name"},
                    .edges = &.{.{
                        .name = "variables",
                        .sort = &.{"name"},
                    }},
                }},
            }},
        }},
    }, .{ .limit = 100 });
    defer view.deinit();

    view.activate(false);

    // Step 3: Initial render + expand debugger→threads (like line 664)
    var iter = view.items();
    while (iter.next()) |_| {}
    try view.expandById(debugger, "threads");
    iter = view.items();
    while (iter.next()) |_| {}

    // Step 4: Simulate 't' keypress - create thread and link
    const thread = try g.insert("Thread");
    try g.update(thread, .{ .name = "Thread 1", .state = "running" });
    try g.link(debugger, "threads", thread);
    iter = view.items();
    while (iter.next()) |_| {}

    // Simulate 's' keypress - stop thread, create frames/scopes/variables
    try g.update(thread, .{ .state = "stopped" });

    const frame1 = try g.insert("Frame");
    try g.update(frame1, .{ .name = "main", .line = @as(i64, 10) });
    try g.link(thread, "frames", frame1);

    const frame2 = try g.insert("Frame");
    try g.update(frame2, .{ .name = "helper", .line = @as(i64, 20) });
    try g.link(thread, "frames", frame2);

    const scope_locals = try g.insert("Scope");
    try g.update(scope_locals, .{ .name = "Locals" });
    try g.link(frame1, "scopes", scope_locals);

    const scope_globals = try g.insert("Scope");
    try g.update(scope_globals, .{ .name = "Globals" });
    try g.link(frame1, "scopes", scope_globals);

    // 3 existing variables
    const var1 = try g.insert("Variable");
    try g.update(var1, .{ .name = "x", .value = "1" });
    try g.link(scope_locals, "variables", var1);

    const var2 = try g.insert("Variable");
    try g.update(var2, .{ .name = "y", .value = "2" });
    try g.link(scope_locals, "variables", var2);

    const var3 = try g.insert("Variable");
    try g.update(var3, .{ .name = "z", .value = "3" });
    try g.link(scope_locals, "variables", var3);

    iter = view.items();
    while (iter.next()) |_| {}

    // Simulate 'o' keypresses to expand: thread→frames, frame1→scopes, scope_locals→variables
    try view.expandById(thread, "frames");
    iter = view.items();
    while (iter.next()) |_| {}

    try view.expandById(frame1, "scopes");
    iter = view.items();
    while (iter.next()) |_| {}

    try view.expandById(scope_locals, "variables");
    iter = view.items();
    while (iter.next()) |_| {}

    // Should have: debugger(1) + thread(1) + frame1(1) + scope_locals(1) + vars(3) + scope_globals(1) + frame2(1) = 9
    const initial_count = view.total();
    std.debug.print("\nInitial count: {} (expected 9)\n", .{initial_count});
    try testing.expectEqual(@as(u32, 9), initial_count);

    // Now simulate 'v' keypresses
    const NUM_LINKS = 5;
    var new_var_ids: [NUM_LINKS]NodeId = undefined;

    for (0..NUM_LINKS) |i| {
        iter = view.items();
        while (iter.next()) |_| {}

        const new_var = try g.insert("Variable");
        try g.update(new_var, .{ .name = "new_var", .value = "42" });
        try g.link(scope_locals, "variables", new_var);
        new_var_ids[i] = new_var;

        iter = view.items();
        while (iter.next()) |_| {}
        iter = view.items();
        while (iter.next()) |_| {}

        const expected_total = initial_count + @as(u32, @intCast(i + 1));
        const actual_total = view.total();

        std.debug.print("Link #{}: expected {}, got {}\n", .{ i + 1, expected_total, actual_total });

        if (actual_total != expected_total) {
            std.debug.print("=== BUG REPRODUCED ===\n", .{});
        }

        try testing.expectEqual(expected_total, actual_total);
    }
}

// ============================================================================
// BUG: Node Duplication - newly linked nodes appear twice in get_visible()
// ============================================================================

fn createParentChildSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "Parent",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [{ "name": "children", "target": "Child", "reverse": "parent" }],
        \\      "indexes": [{ "fields": [{ "field": "name" }] }]
        \\    },
        \\    {
        \\      "name": "Child",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [{ "name": "parent", "target": "Parent", "reverse": "children" }],
        \\      "indexes": [{ "fields": [{ "field": "name" }] }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

test "FIXED: virtual root with linked children - no duplication" {
    // Verifies fix for duplication bug found via Lua bisection.
    // The bug was: newly linked nodes appeared TWICE in items().
    //
    // Root cause: When query.virtual=true, tree.zig's loadSubtreeInViewport
    // was adding the root to node_to_subs even though tracker.zig's loadDirectNode
    // already added it to virtual_to_subs. This caused handleLinkForSubscription
    // to be called twice when linking children.
    //
    // Fix: loadSubtreeInViewport now respects query.virtual and skips adding
    // virtual roots to reactive_tree, result_set, and node_to_subs.
    //
    // See: docs/BUG-BISECT-RESULTS.md, test/raw_sync_test.lua

    ensureWatchdog();

    const schema = try createParentChildSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create parent
    const parent = try g.insert("Parent");
    try g.update(parent, .{ .name = "root" });

    // Create view using JSON like Lua does
    var json_buf: [256]u8 = undefined;
    const json_query = std.fmt.bufPrint(&json_buf,
        \\{{"root": "Parent", "id": {d}, "virtual": true, "edges": [{{"name": "children"}}]}}
    , .{parent}) catch unreachable;

    var view = try g.viewFromJson(json_query, .{ .limit = 100 });
    defer view.deinit();

    // Activate with immediate=true like Lua does
    view.activate(true);

    // Initial items() call (like Lua's render)
    var iter = view.items();
    while (iter.next()) |_| {}

    // Expand parent->children
    try view.expandById(parent, "children");

    // Load viewport after expand
    iter = view.items();
    while (iter.next()) |_| {}

    // Add 3 initial children (with items() after each, like Lua)
    for (0..3) |i| {
        const child = try g.insert("Child");
        var buf: [32]u8 = undefined;
        const name = std.fmt.bufPrint(&buf, "child_{}", .{i}) catch "child";
        try g.update(child, .{ .name = name });
        try g.link(parent, "children", child);

        // Load viewport after each link (like Lua's render)
        iter = view.items();
        while (iter.next()) |_| {}
    }

    // Check initial state
    // With virtual=true, only children are visible (not the parent)
    iter = view.items();
    var count: u32 = 0;
    while (iter.next()) |_| {
        count += 1;
    }
    std.debug.print("\nInitial: {} items (expected 3: children only, parent is virtual)\n", .{count});
    try testing.expectEqual(@as(u32, 3), count);

    // Now add 5 NEW children and check for duplicates after each
    for (0..5) |i| {
        const new_child = try g.insert("Child");
        var buf: [32]u8 = undefined;
        const name = std.fmt.bufPrint(&buf, "new_child_{}", .{i}) catch "new";
        try g.update(new_child, .{ .name = name });
        try g.link(parent, "children", new_child);

        // Count items and check for duplicates
        iter = view.items();
        count = 0;
        var seen_ids = std.AutoHashMap(NodeId, u32).init(testing.allocator);
        defer seen_ids.deinit();

        while (iter.next()) |item| {
            count += 1;
            const entry = try seen_ids.getOrPut(item.id);
            if (entry.found_existing) {
                entry.value_ptr.* += 1;
                std.debug.print("DUPLICATE: id={} seen {} times\n", .{ item.id, entry.value_ptr.* + 1 });
            } else {
                entry.value_ptr.* = 0;
            }
        }

        const expected: u32 = 3 + @as(u32, @intCast(i + 1)); // 3 initial children + (i+1) new
        std.debug.print("Link #{}: expected {}, got {}\n", .{ i + 1, expected, count });

        if (count != expected) {
            std.debug.print("=== BUG: DUPLICATION DETECTED ===\n", .{});
        }

        try testing.expectEqual(expected, count);
    }
}

test "BUG: onEnter callback must fire when linking to expanded edge WITHOUT items()" {
    // This is the definitive test for the bug observed in dap_interactive.lua.
    //
    // THE BUG:
    // After expandById() is called, linking new nodes to that edge does NOT
    // trigger the onEnter callback.
    //
    // CRITICAL: This test does NOT call items() after expand, because items()
    // triggers reloadViewport() which syncs state and hides the bug.
    //
    // This matches the Lua test: test/bug_link_expanded_edge.lua

    ensureWatchdog();

    const schema = try createHierarchySchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create hierarchy: Session -> Thread -> Frame -> (will add new frame)
    const session = try g.insert("Session");
    try g.update(session, .{ .name = "debug", .priority = @as(i64, 1) });

    const thread = try g.insert("Thread");
    try g.update(thread, .{ .tid = @as(i64, 1) });
    try g.link(session, "threads", thread);

    const existing_frame = try g.insert("Frame");
    try g.update(existing_frame, .{ .name = "init", .index = @as(i64, 0) });
    try g.link(thread, "frames", existing_frame);

    // Create callback tracker
    var tracker = CallbackTracker.init(testing.allocator);
    defer tracker.deinit();

    // Create view with nested edge selections
    var view = try g.view(.{
        .root = "Session",
        .sort = &.{"priority"},
        .edges = &.{.{
            .name = "threads",
            .sort = &.{"tid"},
            .edges = &.{.{
                .name = "frames",
                .sort = &.{"index"},
            }},
        }},
    }, .{ .limit = 100 });
    defer view.deinit();

    view.setCallbacks(tracker.getCallbacks());
    view.activate(false);

    // Expand all edges WITHOUT calling items()
    try view.expandById(session, "threads");
    try view.expandById(thread, "frames");

    // Clear tracker to only track new events
    tracker.enters.clearRetainingCapacity();

    // Link a NEW frame - onEnter SHOULD fire
    const new_frame = try g.insert("Frame");
    try g.update(new_frame, .{ .name = "new_frame", .index = @as(i64, 10) });
    try g.link(thread, "frames", new_frame);

    // Debug output
    if (!tracker.hasEntered(new_frame)) {
        std.debug.print("\n=== BUG REPRODUCED ===\n", .{});
        std.debug.print("onEnter was NOT called for new_frame (id={})\n", .{new_frame});
        std.debug.print("Entered IDs after link: ", .{});
        for (tracker.enters.items) |record| {
            std.debug.print("{} ", .{record.node_id});
        }
        std.debug.print("\n", .{});
    }

    // This assertion will FAIL if the bug exists
    try testing.expect(tracker.hasEntered(new_frame));
}

test "BUG #2: Thread disappears after expanding Thread->frames (virtual root regression)" {
    // This test reproduces the exact scenario from dap_interactive.lua where:
    // 1. Debugger is a virtual root (not shown in items)
    // 2. Thread is child of virtual root (appears as root in items)
    // 3. After expanding Thread->frames, Thread DISAPPEARS from items
    //
    // This is a regression from the Bug #1 fix (3b72235).
    // Bug #1 was: linked children appear twice
    // Bug #2 is: parent disappears after child expand
    //
    // KEY: The bug only reproduces when items() is called MULTIPLE times
    // between operations (simulating interactive UI with multiple renders).

    ensureWatchdog();

    const schema = try createHierarchySchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create Session (will be virtual root)
    const session = try g.insert("Session");
    try g.update(session, .{ .name = "debug", .priority = @as(i64, 1) });

    // Create view with Session as virtual root
    var json_buf: [512]u8 = undefined;
    const json_query = std.fmt.bufPrint(&json_buf,
        \\{{"root": "Session", "id": {d}, "virtual": true, "edges": [{{"name": "threads", "edges": [{{"name": "frames"}}]}}]}}
    , .{session}) catch unreachable;

    var view = try g.viewFromJson(json_query, .{ .limit = 100 });
    defer view.deinit();

    view.activate(true);

    // Expand session->threads (like demo's setup)
    try view.expandById(session, "threads");

    // Initial items() - 0 items (no threads yet)
    var iter = view.items();
    var count: u32 = 0;
    while (iter.next()) |_| count += 1;
    std.debug.print("\nInitial (before thread): {} items\n", .{count});
    try testing.expectEqual(@as(u32, 0), count);

    // === SIMULATE 't' KEYPRESS: Create thread ===
    const thread = try g.insert("Thread");
    try g.update(thread, .{ .tid = @as(i64, 1) });
    try g.link(session, "threads", thread);

    // Multiple items() calls (like interactive UI renders)
    iter = view.items();
    count = 0;
    while (iter.next()) |_| count += 1;
    std.debug.print("After create thread (render 1): {} items\n", .{count});

    iter = view.items();
    count = 0;
    while (iter.next()) |_| count += 1;
    std.debug.print("After create thread (render 2): {} items\n", .{count});
    try testing.expectEqual(@as(u32, 1), count);

    // === SIMULATE 's' KEYPRESS: Stop thread and create frames ===
    // First, a render happens (from reactive callback)
    iter = view.items();
    count = 0;
    while (iter.next()) |_| count += 1;
    std.debug.print("During 's' action (render from callback): {} items\n", .{count});

    // Create frames and link them
    const frame1 = try g.insert("Frame");
    try g.update(frame1, .{ .name = "main", .index = @as(i64, 0) });
    try g.link(thread, "frames", frame1);

    const frame2 = try g.insert("Frame");
    try g.update(frame2, .{ .name = "init", .index = @as(i64, 1) });
    try g.link(thread, "frames", frame2);

    // Another render at end of 's' action
    iter = view.items();
    count = 0;
    while (iter.next()) |_| count += 1;
    std.debug.print("After 's' action: {} items\n", .{count});
    try testing.expectEqual(@as(u32, 1), count);

    // === SIMULATE 'o' KEYPRESS: Toggle Thread->frames ===
    // First get_visible (like action_toggle's items_before)
    iter = view.items();
    count = 0;
    var thread_seen = false;
    while (iter.next()) |item| {
        count += 1;
        if (item.id == thread) thread_seen = true;
    }
    std.debug.print("Before toggle: {} items, thread_seen={}\n", .{ count, thread_seen });
    try testing.expectEqual(@as(u32, 1), count);
    try testing.expect(thread_seen);

    // Second get_visible (like get_selected_item)
    iter = view.items();
    count = 0;
    while (iter.next()) |_| count += 1;
    std.debug.print("get_selected_item: {} items\n", .{count});

    // NOW toggle (expand) Thread->frames
    _ = try view.toggleById(thread, "frames");

    // Get items after toggle (before render)
    iter = view.items();
    count = 0;
    thread_seen = false;
    var frame1_seen = false;
    var frame2_seen = false;
    while (iter.next()) |item| {
        count += 1;
        std.debug.print("  item: id={}\n", .{item.id});
        if (item.id == thread) thread_seen = true;
        if (item.id == frame1) frame1_seen = true;
        if (item.id == frame2) frame2_seen = true;
    }

    std.debug.print("After toggle: {} items\n", .{count});
    std.debug.print("  thread_seen={}, frame1_seen={}, frame2_seen={}\n", .{ thread_seen, frame1_seen, frame2_seen });

    // BUG #2: Thread should still be visible, but it disappears!
    // Expected: 3 items (Thread + Frame1 + Frame2)
    // Bug behavior: 2 items (only Frame1 + Frame2)
    if (!thread_seen) {
        std.debug.print("\n=== BUG #2 REPRODUCED: Thread disappeared after toggle! ===\n", .{});
    }

    try testing.expect(thread_seen);
    try testing.expect(frame1_seen);
    try testing.expect(frame2_seen);
    try testing.expectEqual(@as(u32, 3), count);
}

test "BUG: Reactive link maintains sort order (virtual root multi-level)" {
    // This test reproduces the sort order bug observed in dap_interactive.lua:
    // 1. Create deep hierarchy: Debugger(virtual) -> Thread -> Frame -> Scope -> Variables
    // 2. Lazy-load variables - they appear in correct order (by node ID ascending)
    // 3. Add NEW variable via link
    // 4. BUG: New variable appears at WRONG position (at top instead of at end)
    // 5. After collapse/re-expand, variables are in CORRECT order
    //
    // Root cause: handleLinkForSubscription builds different sort key than loadChildrenLazy
    // due to nextQueryLevel returning null for virtual root (no edge targets Debugger type).

    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create debugger (will be virtual root)
    const debugger = try g.insert("Debugger");
    try g.update(debugger, .{ .name = "main" });

    // Create thread
    const thread = try g.insert("Thread");
    try g.update(thread, .{ .name = "main", .state = "stopped" });
    try g.link(debugger, "threads", thread);

    // Create frame
    const frame = try g.insert("Frame");
    try g.update(frame, .{ .name = "main", .line = @as(i64, 10) });
    try g.link(thread, "frames", frame);

    // Create scope
    const scope = try g.insert("Scope");
    try g.update(scope, .{ .name = "Locals" });
    try g.link(frame, "scopes", scope);

    // Create initial variables (IDs will be sequential: 5, 6, 7)
    const var1 = try g.insert("Variable");
    try g.update(var1, .{ .name = "counter", .value = "0" });
    try g.link(scope, "variables", var1);

    const var2 = try g.insert("Variable");
    try g.update(var2, .{ .name = "name", .value = "test" });
    try g.link(scope, "variables", var2);

    const var3 = try g.insert("Variable");
    try g.update(var3, .{ .name = "user", .value = "null" });
    try g.link(scope, "variables", var3);

    // Create view with Debugger as virtual root (exactly like dap_interactive.lua)
    var json_buf: [1024]u8 = undefined;
    const json_query = std.fmt.bufPrint(&json_buf,
        \\{{"root": "Debugger", "id": {d}, "virtual": true, "edges": [{{"name": "threads", "edges": [{{"name": "frames", "edges": [{{"name": "scopes", "edges": [{{"name": "variables"}}]}}]}}]}}]}}
    , .{debugger}) catch unreachable;

    var view = try g.viewFromJson(json_query, .{ .limit = 100 });
    defer view.deinit();

    view.activate(true);

    // Expand all the way down to variables
    try view.expandById(debugger, "threads");
    _ = view.items(); // Load viewport
    try view.expandById(thread, "frames");
    _ = view.items();
    try view.expandById(frame, "scopes");
    _ = view.items();
    try view.expandById(scope, "variables");

    // Get initial order of variables
    var iter = view.items();
    var count: u32 = 0;
    var initial_order: [10]NodeId = undefined;
    while (iter.next()) |item| {
        if (item.depth == 4) { // Variables are at depth 4
            initial_order[count] = item.id;
            count += 1;
        }
    }
    std.debug.print("\nInitial variable order ({} vars): ", .{count});
    for (initial_order[0..count]) |id| {
        std.debug.print("{} ", .{id});
    }
    std.debug.print("\n", .{});
    try testing.expectEqual(@as(u32, 3), count);

    // Verify initial order is correct (ascending by node ID since no sorts specified)
    try testing.expect(initial_order[0] < initial_order[1]);
    try testing.expect(initial_order[1] < initial_order[2]);

    // === ADD NEW VARIABLE (higher ID) ===
    const new_var = try g.insert("Variable");
    try g.update(new_var, .{ .name = "new_var", .value = "42" });
    try g.link(scope, "variables", new_var);

    std.debug.print("Added new variable with ID: {}\n", .{new_var});

    // Get order after reactive link
    iter = view.items();
    count = 0;
    var after_link_order: [10]NodeId = undefined;
    while (iter.next()) |item| {
        if (item.depth == 4) {
            after_link_order[count] = item.id;
            count += 1;
        }
    }
    std.debug.print("After reactive link ({} vars): ", .{count});
    for (after_link_order[0..count]) |id| {
        std.debug.print("{} ", .{id});
    }
    std.debug.print("\n", .{});
    try testing.expectEqual(@as(u32, 4), count);

    // === COLLAPSE AND RE-EXPAND ===
    view.collapseById(scope, "variables");
    _ = view.items();
    try view.expandById(scope, "variables");

    // Get order after re-expand
    iter = view.items();
    count = 0;
    var after_reexpand_order: [10]NodeId = undefined;
    while (iter.next()) |item| {
        if (item.depth == 4) {
            after_reexpand_order[count] = item.id;
            count += 1;
        }
    }
    std.debug.print("After collapse/re-expand ({} vars): ", .{count});
    for (after_reexpand_order[0..count]) |id| {
        std.debug.print("{} ", .{id});
    }
    std.debug.print("\n", .{});
    try testing.expectEqual(@as(u32, 4), count);

    // === KEY ASSERTION: Order should be the same ===
    // After reactive link should have same order as after re-expand
    // BUG: They are different - new_var appears at wrong position
    var orders_match = true;
    for (0..count) |i| {
        if (after_link_order[i] != after_reexpand_order[i]) {
            orders_match = false;
            break;
        }
    }

    if (!orders_match) {
        std.debug.print("\n=== BUG: Sort order mismatch! ===\n", .{});
        std.debug.print("After link:     ", .{});
        for (after_link_order[0..count]) |id| std.debug.print("{} ", .{id});
        std.debug.print("\nAfter re-expand: ", .{});
        for (after_reexpand_order[0..count]) |id| std.debug.print("{} ", .{id});
        std.debug.print("\n", .{});
    }

    // This will FAIL if the bug exists
    for (0..count) |i| {
        try testing.expectEqual(after_reexpand_order[i], after_link_order[i]);
    }
}

// ============================================================================
// BUG: View shows unrelated nodes through reverse edge
// ============================================================================

test "BUG: View rooted at Child should not show sibling children of same parent" {
    // This test reproduces the bug from repro_view_bug.lua:
    //
    // SCENARIO:
    // 1. Create Parent "TheParent"
    // 2. Create Child1, link Child1 -> TheParent via "parent" edge
    // 3. Create view rooted at Child1 with "parent" edge expanded
    // 4. View correctly shows: Child1, TheParent (2 items)
    // 5. Create Child2, link Child2 -> TheParent via "parent" edge
    // 6. BUG: View now shows: Child1, TheParent, Child2 (3 items!)
    //
    // EXPECTED: View should still show only Child1 and TheParent
    // The view is for entities reachable FROM Child1, not all entities
    // that share a common parent.
    //
    // ROOT CAUSE HYPOTHESIS:
    // When Child2 links to TheParent, the reactive system sees the link
    // on the reverse edge ("children") of TheParent. Since TheParent is
    // in the view's subscription, it incorrectly adds Child2 to the view.

    ensureWatchdog();

    const schema = try createParentChildSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Step 1: Create parent
    const parent_id = try g.insert("Parent");
    try g.update(parent_id, .{ .name = "TheParent" });

    // Step 2: Create Child1 and link to parent
    const child1_id = try g.insert("Child");
    try g.update(child1_id, .{ .name = "Child1" });
    try g.link(child1_id, "parent", parent_id);

    // Step 3: Create view rooted at Child1 with "parent" edge
    var json_buf: [256]u8 = undefined;
    const json_query = std.fmt.bufPrint(&json_buf,
        \\{{"root": "Child", "id": {d}, "edges": [{{"name": "parent"}}]}}
    , .{child1_id}) catch unreachable;

    var view = try g.viewFromJson(json_query, .{ .limit = 100 });
    defer view.deinit();

    // Activate with immediate=true (like Lua's { immediate = true })
    view.activate(true);

    // Expand Child1's "parent" edge
    try view.expandById(child1_id, "parent");

    // Load viewport
    var iter = view.items();
    while (iter.next()) |_| {}

    // Step 4: Verify initial state - should have 2 items (Child1 + TheParent)
    const count_before = view.total();
    std.debug.print("\n=== After creating Child1 ===\n", .{});
    std.debug.print("View items: {}\n", .{count_before});

    iter = view.items();
    while (iter.next()) |item| {
        std.debug.print("  id: {}\n", .{item.id});
    }

    try testing.expectEqual(@as(u32, 2), count_before);

    // Step 5: Create Child2 and link to SAME parent
    const child2_id = try g.insert("Child");
    try g.update(child2_id, .{ .name = "Child2" });
    try g.link(child2_id, "parent", parent_id);

    std.debug.print("\n=== After creating Child2 (linked to same parent) ===\n", .{});

    // Step 6: Check view - BUG: Child2 appears incorrectly
    iter = view.items();
    while (iter.next()) |_| {}

    const count_after = view.total();
    std.debug.print("View items: {}\n", .{count_after});

    iter = view.items();
    var child2_found = false;
    while (iter.next()) |item| {
        std.debug.print("  id: {}\n", .{item.id});
        if (item.id == child2_id) {
            child2_found = true;
        }
    }

    std.debug.print("\n=== EXPECTED ===\n", .{});
    std.debug.print("View for Child1.parent should contain only:\n", .{});
    std.debug.print("  - Child1 (the root)\n", .{});
    std.debug.print("  - TheParent (linked via 'parent' edge)\n", .{});
    std.debug.print("Total: 2 items\n", .{});

    std.debug.print("\n=== RESULT ===\n", .{});
    if (count_after > 2) {
        std.debug.print("BUG: View contains {} items instead of 2\n", .{count_after});
        std.debug.print("Child2 incorrectly appears in Child1's parent edge view!\n", .{});
        std.debug.print("The view seems to include ALL children of TheParent,\n", .{});
        std.debug.print("not just entities reachable FROM Child1.\n", .{});
    } else {
        std.debug.print("OK: View contains {} items\n", .{count_after});
    }

    // These assertions will FAIL if the bug exists
    try testing.expect(!child2_found);
    try testing.expectEqual(@as(u32, 2), count_after);
}
