///! Black-box tests for view enter/leave events with expanded edges.
///!
///! These tests document the bug where view:on("enter") doesn't fire when
///! linking to an already-expanded edge. The item correctly appears in
///! view.items() but the enter callback never fires.
///!
///! Bug reproduction:
///!   1. Create parent with child, link them
///!   2. Create view with edge definition, expand the edge
///!   3. Register on_enter callback
///!   4. Link NEW child to the already-expanded edge
///!   5. Expected: on_enter fires for new child
///!   6. Actual: on_enter never fires (but new child IS in items)

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

const ng = @import("neograph.zig");
const Graph = ng.Graph;
const Schema = ng.Schema;
const NodeId = ng.NodeId;
const View = ng.View;
const Item = ng.Item;
const parseSchema = ng.parseSchema;

// ============================================================================
// Test Schema: Parent/Child relationship
// ============================================================================

fn createParentChildSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "Parent",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [{ "name": "children", "target": "Child", "reverse": "parent" }],
        \\      "indexes": [{ "fields": [{ "field": "name", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Child",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [{ "name": "parent", "target": "Parent", "reverse": "children" }],
        \\      "indexes": [{ "fields": [{ "field": "name", "direction": "asc" }] }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

// ============================================================================
// Callback Tracker
// ============================================================================

const EventTracker = struct {
    enters: std.ArrayListUnmanaged(NodeId),
    leaves: std.ArrayListUnmanaged(NodeId),
    allocator: Allocator,

    fn init(allocator: Allocator) EventTracker {
        return .{
            .enters = .{},
            .leaves = .{},
            .allocator = allocator,
        };
    }

    fn deinit(self: *EventTracker) void {
        self.enters.deinit(self.allocator);
        self.leaves.deinit(self.allocator);
    }

    fn onEnter(ctx: ?*anyopaque, item: Item, _: u32) void {
        const self: *EventTracker = @ptrCast(@alignCast(ctx));
        self.enters.append(self.allocator, item.id) catch {};
    }

    fn onLeave(ctx: ?*anyopaque, item: Item, _: u32) void {
        const self: *EventTracker = @ptrCast(@alignCast(ctx));
        self.leaves.append(self.allocator, item.id) catch {};
    }

    fn getCallbacks(self: *EventTracker) ng.Callbacks {
        return .{
            .on_enter = onEnter,
            .on_leave = onLeave,
            .context = self,
        };
    }

    fn hasEntered(self: *const EventTracker, id: NodeId) bool {
        for (self.enters.items) |entered_id| {
            if (entered_id == id) return true;
        }
        return false;
    }

    fn hasLeft(self: *const EventTracker, id: NodeId) bool {
        for (self.leaves.items) |left_id| {
            if (left_id == id) return true;
        }
        return false;
    }
};

/// Check if a node ID is present in the view's items
fn viewContains(view: *View, id: NodeId) bool {
    var iter = view.items();
    while (iter.next()) |item| {
        if (item.id == id) return true;
    }
    return false;
}

// ============================================================================
// Tests: Enter events for expanded edges
// ============================================================================

test "on_enter fires when linking to already-expanded edge" {
    // This is the main bug reproduction test.
    // When a new child is linked to an already-expanded edge,
    // the on_enter callback should fire.

    const schema = try createParentChildSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create parent with one child
    const parent = try g.insert("Parent");
    try g.update(parent, .{ .name = "p" });

    const child1 = try g.insert("Child");
    try g.update(child1, .{ .name = "c1" });
    try g.link(parent, "children", child1);

    // Create view rooted at Parent type with children edge defined
    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{.{ .name = "children", .sort = &.{"name"} }},
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial items
    var iter = view.items();
    while (iter.next()) |_| {}

    // Register callback BEFORE expand
    var tracker = EventTracker.init(testing.allocator);
    defer tracker.deinit();
    view.setCallbacks(tracker.getCallbacks());

    // Expand the children edge - this should fire enter for existing child1
    try view.expandById(parent, "children");

    // Clear the tracker to only track NEW events
    // (child1 would have triggered enter on expand)
    tracker.enters.clearRetainingCapacity();

    // CRITICAL: Do NOT call items() after expand - that would sync state
    // and hide the bug. This matches the dap_interactive.lua scenario.

    // Link NEW child to already-expanded edge
    const child2 = try g.insert("Child");
    try g.update(child2, .{ .name = "c2" });
    try g.link(parent, "children", child2);

    // BUG: on_enter should have fired for child2 immediately
    // The callback should fire when the link happens, not only after items()
    try testing.expectEqual(@as(usize, 1), tracker.enters.items.len);
    try testing.expect(tracker.hasEntered(child2));
}

test "on_enter fires for multiple links to expanded edge" {
    const schema = try createParentChildSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const parent = try g.insert("Parent");
    try g.update(parent, .{ .name = "p" });

    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{.{ .name = "children", .sort = &.{"name"} }},
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial items
    var iter = view.items();
    while (iter.next()) |_| {}

    var tracker = EventTracker.init(testing.allocator);
    defer tracker.deinit();
    view.setCallbacks(tracker.getCallbacks());

    // Expand (no existing children, so no enter events)
    try view.expandById(parent, "children");

    // CRITICAL: Do NOT call items() after expand - that hides the bug

    // Link 5 children
    var children: [5]NodeId = undefined;
    for (0..5) |i| {
        children[i] = try g.insert("Child");
        try g.update(children[i], .{ .name = "c" });
        try g.link(parent, "children", children[i]);
    }

    // BUG: on_enter should fire for each linked child immediately
    try testing.expectEqual(@as(usize, 5), tracker.enters.items.len);
}

// ============================================================================
// Tests: Leave events for expanded edges (these should pass - leave works)
// ============================================================================

test "on_leave fires when unlinking from already-expanded edge" {
    const schema = try createParentChildSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const parent = try g.insert("Parent");
    try g.update(parent, .{ .name = "p" });

    const child1 = try g.insert("Child");
    try g.update(child1, .{ .name = "c1" });
    try g.link(parent, "children", child1);

    const child2 = try g.insert("Child");
    try g.update(child2, .{ .name = "c2" });
    try g.link(parent, "children", child2);

    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{.{ .name = "children", .sort = &.{"name"} }},
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial items
    var iter = view.items();
    while (iter.next()) |_| {}

    try view.expandById(parent, "children");

    // Load items after expand
    iter = view.items();
    while (iter.next()) |_| {}

    var tracker = EventTracker.init(testing.allocator);
    defer tracker.deinit();
    view.setCallbacks(tracker.getCallbacks());

    // Unlink child2
    try g.unlink(parent, "children", child2);

    // on_leave should fire
    try testing.expectEqual(@as(usize, 1), tracker.leaves.items.len);
    try testing.expect(tracker.hasLeft(child2));
}

test "on_leave fires when deleting linked child" {
    const schema = try createParentChildSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const parent = try g.insert("Parent");
    try g.update(parent, .{ .name = "p" });

    const child = try g.insert("Child");
    try g.update(child, .{ .name = "c1" });
    try g.link(parent, "children", child);

    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{.{ .name = "children", .sort = &.{"name"} }},
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial items
    var iter = view.items();
    while (iter.next()) |_| {}

    try view.expandById(parent, "children");

    // Load items after expand
    iter = view.items();
    while (iter.next()) |_| {}

    var tracker = EventTracker.init(testing.allocator);
    defer tracker.deinit();
    view.setCallbacks(tracker.getCallbacks());

    // Delete child
    try g.delete(child);

    // on_leave should fire
    try testing.expectEqual(@as(usize, 1), tracker.leaves.items.len);
    try testing.expect(tracker.hasLeft(child));
}

// ============================================================================
// Tests: Edge cases
// ============================================================================

test "on_enter should NOT fire when edge is collapsed" {
    // Expected behavior: enter events should only fire for EXPANDED edges.
    // If an edge is defined but not expanded, linking children should NOT
    // trigger enter events because the children are not visible in the view.

    const schema = try createParentChildSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const parent = try g.insert("Parent");
    try g.update(parent, .{ .name = "p" });

    // Edge is defined but NOT expanded
    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{.{ .name = "children", .sort = &.{"name"} }},
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial items
    var iter = view.items();
    while (iter.next()) |_| {}

    // Do NOT expand - edge remains collapsed

    var tracker = EventTracker.init(testing.allocator);
    defer tracker.deinit();
    view.setCallbacks(tracker.getCallbacks());

    const child = try g.insert("Child");
    try g.update(child, .{ .name = "c1" });
    try g.link(parent, "children", child);

    // Enter should NOT fire because the edge is collapsed
    // The child is not visible in the view until the edge is expanded
    try testing.expectEqual(@as(usize, 0), tracker.enters.items.len);
}

test "on_enter fires when expanding with existing children" {
    // When you expand an edge that already has linked children,
    // those children should trigger on_enter events.

    const schema = try createParentChildSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const parent = try g.insert("Parent");
    try g.update(parent, .{ .name = "p" });

    const child = try g.insert("Child");
    try g.update(child, .{ .name = "c1" });
    try g.link(parent, "children", child);

    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{.{ .name = "children", .sort = &.{"name"} }},
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial items
    var iter = view.items();
    while (iter.next()) |_| {}

    var tracker = EventTracker.init(testing.allocator);
    defer tracker.deinit();
    view.setCallbacks(tracker.getCallbacks());

    // Expand AFTER registering callback - should fire for existing child
    try view.expandById(parent, "children");

    try testing.expectEqual(@as(usize, 1), tracker.enters.items.len);
    try testing.expect(tracker.hasEntered(child));
}
