///! Black-box tests for nested edge expansion reactivity.
///!
///! BUG: When expanding a nested edge (child->items after parent->children),
///! new links to that nested edge are not tracked - they don't appear in
///! view.items() and don't fire enter events.
///!
///! This is the real neodap scenario: dap-tree:@debugger view expands a Session,
///! then Session->outputs edge, but new outputs don't appear live.
///!
///! Bug reproduction:
///!   1. Create Parent -> Child hierarchy
///!   2. Create view with edges [children, items]
///!   3. Expand parent->children (first level)
///!   4. Expand child->items (nested, second level)
///!   5. Link new Item to child->items
///!   6. Expected: Item appears in items(), on_enter fires
///!   7. Actual: Item NOT in items(), on_enter does NOT fire

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
// Test Schema: 3-level hierarchy Parent -> Child -> Item
// ============================================================================

fn createNestedSchema(allocator: Allocator) !Schema {
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
        \\      "edges": [
        \\        { "name": "parent", "target": "Parent", "reverse": "children" },
        \\        { "name": "items", "target": "Item", "reverse": "child" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "name", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Item",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [{ "name": "child", "target": "Child", "reverse": "items" }],
        \\      "indexes": [{ "fields": [{ "field": "name", "direction": "asc" }] }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

// 4-level deep schema for testing deeply nested edges
fn createDeepSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "Root",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [{ "name": "level1", "target": "L1", "reverse": "root" }],
        \\      "indexes": [{ "fields": [{ "field": "name", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "L1",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [
        \\        { "name": "root", "target": "Root", "reverse": "level1" },
        \\        { "name": "level2", "target": "L2", "reverse": "l1" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "name", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "L2",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [
        \\        { "name": "l1", "target": "L1", "reverse": "level2" },
        \\        { "name": "level3", "target": "L3", "reverse": "l2" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "name", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "L3",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [{ "name": "l2", "target": "L2", "reverse": "level3" }],
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

    fn clear(self: *EventTracker) void {
        self.enters.clearRetainingCapacity();
        self.leaves.clearRetainingCapacity();
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

/// Count items in view
fn viewCount(view: *View) usize {
    var count: usize = 0;
    var iter = view.items();
    while (iter.next()) |_| {
        count += 1;
    }
    return count;
}

// ============================================================================
// Tests: Basic nested expansion
// ============================================================================

test "1.1 expanding nested edge shows existing items" {
    const schema = try createNestedSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create Parent -> Child -> Item
    const parent = try g.insert("Parent");
    try g.update(parent, .{ .name = "p" });

    const child = try g.insert("Child");
    try g.update(child, .{ .name = "c" });
    try g.link(parent, "children", child);

    const item = try g.insert("Item");
    try g.update(item, .{ .name = "i" });
    try g.link(child, "items", item);

    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{
            .{ .name = "children", .sort = &.{"name"}, .edges = &.{
                .{ .name = "items", .sort = &.{"name"} },
            } },
        },
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial items
    _ = viewCount(&view);

    // Expand first level
    try view.expandById(parent, "children");
    try testing.expectEqual(@as(usize, 2), viewCount(&view)); // parent + child

    // Expand nested level
    try view.expandById(child, "items");
    try testing.expectEqual(@as(usize, 3), viewCount(&view)); // parent + child + item
    try testing.expect(viewContains(&view, item));
}

// ============================================================================
// Tests: Reactivity on nested expanded edges
// ============================================================================

test "2.1 linking to nested expanded edge adds item to view" {
    const schema = try createNestedSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const parent = try g.insert("Parent");
    try g.update(parent, .{ .name = "p" });

    const child = try g.insert("Child");
    try g.update(child, .{ .name = "c" });
    try g.link(parent, "children", child);

    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{
            .{ .name = "children", .sort = &.{"name"}, .edges = &.{
                .{ .name = "items", .sort = &.{"name"} },
            } },
        },
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial items
    _ = viewCount(&view);

    // Expand both levels
    try view.expandById(parent, "children");
    try view.expandById(child, "items");
    try testing.expectEqual(@as(usize, 2), viewCount(&view)); // parent + child

    // Link new item to nested expanded edge
    const item = try g.insert("Item");
    try g.update(item, .{ .name = "i1" });
    try g.link(child, "items", item);

    // BUG: Item should appear in view
    try testing.expectEqual(@as(usize, 3), viewCount(&view)); // parent + child + item
    try testing.expect(viewContains(&view, item));
}

test "2.2 on_enter fires when linking to nested expanded edge" {
    const schema = try createNestedSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const parent = try g.insert("Parent");
    try g.update(parent, .{ .name = "p" });

    const child = try g.insert("Child");
    try g.update(child, .{ .name = "c" });
    try g.link(parent, "children", child);

    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{
            .{ .name = "children", .sort = &.{"name"}, .edges = &.{
                .{ .name = "items", .sort = &.{"name"} },
            } },
        },
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial items
    _ = viewCount(&view);

    var tracker = EventTracker.init(testing.allocator);
    defer tracker.deinit();
    view.setCallbacks(tracker.getCallbacks());

    // Expand both levels
    try view.expandById(parent, "children");
    try view.expandById(child, "items");
    tracker.clear(); // Clear events from expand

    // Link new item
    const item = try g.insert("Item");
    try g.update(item, .{ .name = "i1" });
    try g.link(child, "items", item);

    // BUG: on_enter should fire
    try testing.expectEqual(@as(usize, 1), tracker.enters.items.len);
    try testing.expect(tracker.hasEntered(item));
}

test "2.3 on_leave fires when unlinking from nested expanded edge" {
    const schema = try createNestedSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const parent = try g.insert("Parent");
    try g.update(parent, .{ .name = "p" });

    const child = try g.insert("Child");
    try g.update(child, .{ .name = "c" });
    try g.link(parent, "children", child);

    const item = try g.insert("Item");
    try g.update(item, .{ .name = "i1" });
    try g.link(child, "items", item);

    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{
            .{ .name = "children", .sort = &.{"name"}, .edges = &.{
                .{ .name = "items", .sort = &.{"name"} },
            } },
        },
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial items
    _ = viewCount(&view);

    // Expand both levels
    try view.expandById(parent, "children");
    try view.expandById(child, "items");
    try testing.expectEqual(@as(usize, 3), viewCount(&view)); // parent + child + item

    var tracker = EventTracker.init(testing.allocator);
    defer tracker.deinit();
    view.setCallbacks(tracker.getCallbacks());

    // Unlink item
    try g.unlink(child, "items", item);

    // on_leave should fire exactly once
    try testing.expectEqual(@as(usize, 1), tracker.leaves.items.len);
    try testing.expect(tracker.hasLeft(item));
    try testing.expectEqual(@as(usize, 2), viewCount(&view)); // parent + child
}

// ============================================================================
// Tests: Multiple nested levels (3+ deep)
// ============================================================================

test "3.1 linking to deeply nested expanded edge (3+ levels)" {
    const schema = try createDeepSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const root = try g.insert("Root");
    try g.update(root, .{ .name = "r" });

    const l1 = try g.insert("L1");
    try g.update(l1, .{ .name = "l1" });
    try g.link(root, "level1", l1);

    const l2 = try g.insert("L2");
    try g.update(l2, .{ .name = "l2" });
    try g.link(l1, "level2", l2);

    var view = try g.view(.{
        .root = "Root",
        .sort = &.{"name"},
        .edges = &.{
            .{ .name = "level1", .sort = &.{"name"}, .edges = &.{
                .{ .name = "level2", .sort = &.{"name"}, .edges = &.{
                    .{ .name = "level3", .sort = &.{"name"} },
                } },
            } },
        },
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial items
    _ = viewCount(&view);

    // Expand all levels
    try view.expandById(root, "level1");
    try view.expandById(l1, "level2");
    try view.expandById(l2, "level3");
    try testing.expectEqual(@as(usize, 3), viewCount(&view)); // root + l1 + l2

    var tracker = EventTracker.init(testing.allocator);
    defer tracker.deinit();
    view.setCallbacks(tracker.getCallbacks());

    // Link at deepest level
    const l3 = try g.insert("L3");
    try g.update(l3, .{ .name = "l3" });
    try g.link(l2, "level3", l3);

    // BUG: Should have 4 items and enter should fire
    try testing.expectEqual(@as(usize, 4), viewCount(&view)); // root + l1 + l2 + l3
    try testing.expect(viewContains(&view, l3));
    try testing.expectEqual(@as(usize, 1), tracker.enters.items.len);
}

// ============================================================================
// Tests: Edge cases
// ============================================================================

test "4.1 no enter when nested edge is collapsed" {
    const schema = try createNestedSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const parent = try g.insert("Parent");
    try g.update(parent, .{ .name = "p" });

    const child = try g.insert("Child");
    try g.update(child, .{ .name = "c" });
    try g.link(parent, "children", child);

    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{
            .{ .name = "children", .sort = &.{"name"}, .edges = &.{
                .{ .name = "items", .sort = &.{"name"} },
            } },
        },
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial items
    _ = viewCount(&view);

    // Expand only first level, keep items collapsed
    try view.expandById(parent, "children");

    var tracker = EventTracker.init(testing.allocator);
    defer tracker.deinit();
    view.setCallbacks(tracker.getCallbacks());
    tracker.clear();

    // Link item - should NOT trigger enter since items edge is collapsed
    const item = try g.insert("Item");
    try g.update(item, .{ .name = "i1" });
    try g.link(child, "items", item);

    // No enter should fire when edge is collapsed
    try testing.expectEqual(@as(usize, 0), tracker.enters.items.len);
    try testing.expectEqual(@as(usize, 2), viewCount(&view)); // parent + child only
}

test "4.2 multiple items linked to same nested edge" {
    const schema = try createNestedSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const parent = try g.insert("Parent");
    try g.update(parent, .{ .name = "p" });

    const child = try g.insert("Child");
    try g.update(child, .{ .name = "c" });
    try g.link(parent, "children", child);

    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{
            .{ .name = "children", .sort = &.{"name"}, .edges = &.{
                .{ .name = "items", .sort = &.{"name"} },
            } },
        },
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial items
    _ = viewCount(&view);

    // Expand both levels
    try view.expandById(parent, "children");
    try view.expandById(child, "items");

    var tracker = EventTracker.init(testing.allocator);
    defer tracker.deinit();
    view.setCallbacks(tracker.getCallbacks());
    tracker.clear();

    // Link multiple items
    for (0..5) |i| {
        const item = try g.insert("Item");
        var buf: [8]u8 = undefined;
        const name = std.fmt.bufPrint(&buf, "i{d}", .{i}) catch "i";
        try g.update(item, .{ .name = name });
        try g.link(child, "items", item);
    }

    // BUG: enter should fire for each linked item
    try testing.expectEqual(@as(usize, 5), tracker.enters.items.len);
    try testing.expectEqual(@as(usize, 7), viewCount(&view)); // parent + child + 5 items
}

test "4.3 items appear after collapse and re-expand of nested edge" {
    const schema = try createNestedSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const parent = try g.insert("Parent");
    try g.update(parent, .{ .name = "p" });

    const child = try g.insert("Child");
    try g.update(child, .{ .name = "c" });
    try g.link(parent, "children", child);

    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{
            .{ .name = "children", .sort = &.{"name"}, .edges = &.{
                .{ .name = "items", .sort = &.{"name"} },
            } },
        },
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial items
    _ = viewCount(&view);

    // Expand both levels
    try view.expandById(parent, "children");
    try view.expandById(child, "items");

    // Link item while expanded
    const item1 = try g.insert("Item");
    try g.update(item1, .{ .name = "i1" });
    try g.link(child, "items", item1);

    // BUG: Should have 3 items now
    try testing.expectEqual(@as(usize, 3), viewCount(&view));

    // Collapse and re-expand
    view.collapseById(child, "items");
    try testing.expectEqual(@as(usize, 2), viewCount(&view)); // parent + child

    try view.expandById(child, "items");
    try testing.expectEqual(@as(usize, 3), viewCount(&view)); // parent + child + item1
    try testing.expect(viewContains(&view, item1));
}

// ============================================================================
// Tests: Virtual edge hop (intermediate node hidden)
// ============================================================================

test "5.1 virtual edge expansion shows nested children" {
    // Schema: Container -> Session -> Item
    // When session edge is virtual, Items should appear as children of Container
    const schema = try createNestedSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Using Parent as Container, Child as Session, Item as Item
    const container = try g.insert("Parent");
    try g.update(container, .{ .name = "container" });

    const session = try g.insert("Child");
    try g.update(session, .{ .name = "session" });
    try g.link(container, "children", session);

    const item1 = try g.insert("Item");
    try g.update(item1, .{ .name = "i1" });
    try g.link(session, "items", item1);

    const item2 = try g.insert("Item");
    try g.update(item2, .{ .name = "i2" });
    try g.link(session, "items", item2);

    // Query with virtual hop: children edge is virtual
    // Items should appear as children of Container, skipping Session
    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{
            .{ .name = "children", .virtual = true, .edges = &.{
                .{ .name = "items", .sort = &.{"name"} },
            } },
        },
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial - only container visible (session is virtual)
    try testing.expectEqual(@as(usize, 1), viewCount(&view));

    // Expand the virtual edge
    try view.expandById(container, "children");

    // Items should now be visible (session is hidden)
    try testing.expectEqual(@as(usize, 3), viewCount(&view)); // container + 2 items
    try testing.expect(viewContains(&view, item1));
    try testing.expect(viewContains(&view, item2));
    try testing.expect(!viewContains(&view, session)); // Session should NOT be visible
}

test "5.2 virtual edge hop fires enter events for nested children" {
    const schema = try createNestedSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const container = try g.insert("Parent");
    try g.update(container, .{ .name = "container" });

    const session = try g.insert("Child");
    try g.update(session, .{ .name = "session" });
    try g.link(container, "children", session);

    const item1 = try g.insert("Item");
    try g.update(item1, .{ .name = "i1" });
    try g.link(session, "items", item1);

    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{
            .{ .name = "children", .virtual = true, .edges = &.{
                .{ .name = "items", .sort = &.{"name"} },
            } },
        },
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    var tracker = EventTracker.init(testing.allocator);
    defer tracker.deinit();
    view.setCallbacks(tracker.getCallbacks());

    // Load initial
    _ = viewCount(&view);
    tracker.clear();

    // Expand the virtual edge
    try view.expandById(container, "children");

    // Enter should fire for item1 (not for session - it's virtual)
    try testing.expect(tracker.hasEntered(item1));
    try testing.expect(!tracker.hasEntered(session));
}

test "5.3 items linked before expand appear after virtual edge expansion" {
    // Test that items linked before expand appear when virtual edge is expanded
    const schema = try createNestedSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const container = try g.insert("Parent");
    try g.update(container, .{ .name = "container" });

    const session = try g.insert("Child");
    try g.update(session, .{ .name = "session" });
    try g.link(container, "children", session);

    // Link item BEFORE creating view/expanding
    const item = try g.insert("Item");
    try g.update(item, .{ .name = "item" });
    try g.link(session, "items", item);

    var view = try g.view(.{
        .root = "Parent",
        .sort = &.{"name"},
        .edges = &.{
            .{ .name = "children", .virtual = true, .edges = &.{
                .{ .name = "items", .sort = &.{"name"} },
            } },
        },
    }, .{ .limit = 100 });
    defer view.deinit();
    view.activate(false);

    // Load initial - only container visible
    try testing.expectEqual(@as(usize, 1), viewCount(&view));

    // Expand the virtual edge
    try view.expandById(container, "children");

    // Item should appear (session is hidden)
    try testing.expectEqual(@as(usize, 2), viewCount(&view)); // container + item
    try testing.expect(viewContains(&view, item));
    try testing.expect(!viewContains(&view, session)); // Session should NOT be visible
}
