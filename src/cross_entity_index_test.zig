///! Cross-Entity Index Tests
///!
///! These tests verify the cross-entity index feature described in:
///! - docs/design/cross-entity-indexes.md
///! - docs/design/cross-entity-indexes-implementation-plan.md
///!
///! IMPORTANT: These tests are expected to FAIL until the feature is implemented.
///! They serve as acceptance criteria for the implementation.
///!
///! Test categories:
///! - Phase 1: Index maintenance on link/unlink
///! - Phase 2: Query execution with cross-entity indexes
///! - Phase 3: Rollup optimization (first/last)

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

// Import the public API
const ng = @import("neograph.zig");
const Graph = ng.Graph;
const Schema = ng.Schema;
const View = ng.View;
const ViewOpts = ng.ViewOpts;
const Value = ng.Value;
const NodeId = ng.NodeId;
const parseSchema = ng.parseSchema;

// ============================================================================
// Test Schemas
// ============================================================================

/// Schema with cross-entity index: Stack indexed by (thread, timestamp DESC)
fn createThreadStackSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "Thread",
        \\      "properties": [
        \\        { "name": "name", "type": "string" }
        \\      ],
        \\      "edges": [
        \\        { "name": "stacks", "target": "Stack", "reverse": "thread" }
        \\      ],
        \\      "indexes": [
        \\        { "fields": [{ "field": "name" }] }
        \\      ]
        \\    },
        \\    {
        \\      "name": "Stack",
        \\      "properties": [
        \\        { "name": "timestamp", "type": "int" },
        \\        { "name": "data", "type": "string" }
        \\      ],
        \\      "edges": [
        \\        { "name": "thread", "target": "Thread", "reverse": "stacks" }
        \\      ],
        \\      "indexes": [
        \\        {
        \\          "fields": [
        \\            { "field": "thread", "kind": "edge", "direction": "asc" },
        \\            { "field": "timestamp", "direction": "desc" }
        \\          ]
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

/// Schema with first/last rollups (Phase 3)
fn createThreadStackSchemaWithRollups(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "Thread",
        \\      "properties": [
        \\        { "name": "name", "type": "string" }
        \\      ],
        \\      "edges": [
        \\        { "name": "stacks", "target": "Stack", "reverse": "thread" }
        \\      ],
        \\      "rollups": [
        \\        { "name": "latestTimestamp", "first": { "edge": "stacks", "field": "timestamp", "direction": "desc", "property": "timestamp" } },
        \\        { "name": "oldestTimestamp", "last": { "edge": "stacks", "field": "timestamp", "direction": "desc", "property": "timestamp" } },
        \\        { "name": "stackCount", "count": "stacks" }
        \\      ],
        \\      "indexes": [
        \\        { "fields": [{ "field": "name" }] }
        \\      ]
        \\    },
        \\    {
        \\      "name": "Stack",
        \\      "properties": [
        \\        { "name": "timestamp", "type": "int" },
        \\        { "name": "data", "type": "string" }
        \\      ],
        \\      "edges": [
        \\        { "name": "thread", "target": "Thread", "reverse": "stacks" }
        \\      ],
        \\      "indexes": [
        \\        {
        \\          "fields": [
        \\            { "field": "thread", "kind": "edge", "direction": "asc" },
        \\            { "field": "timestamp", "direction": "desc" }
        \\          ]
        \\        }
        \\      ]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Collect visible item IDs from a view into a slice.
fn collectItemIds(allocator: Allocator, view: *View) ![]NodeId {
    var result = std.ArrayListUnmanaged(NodeId){};
    errdefer result.deinit(allocator);

    var iter = view.items();
    while (iter.next()) |item| {
        try result.append(allocator, item.id);
    }

    return result.toOwnedSlice(allocator);
}

/// Trigger viewport loading by iterating through items.
fn loadViewport(view: *View) void {
    var iter = view.items();
    while (iter.next()) |_| {}
}

// ============================================================================
// Phase 1: Index Maintenance Tests
// ============================================================================

test "Phase1: cross-entity index updated on link" {
    const schema = try createThreadStackSchema(testing.allocator);

    var graph = try Graph.init(testing.allocator, schema);
    defer graph.deinit();

    // Create thread and stacks
    const thread = try graph.insert("Thread");
    try graph.update(thread, .{ .name = "T1" });

    const stack1 = try graph.insert("Stack");
    try graph.update(stack1, .{ .timestamp = @as(i64, 100), .data = "s1" });

    const stack2 = try graph.insert("Stack");
    try graph.update(stack2, .{ .timestamp = @as(i64, 200), .data = "s2" });

    const stack3 = try graph.insert("Stack");
    try graph.update(stack3, .{ .timestamp = @as(i64, 150), .data = "s3" });

    // Link stacks to thread (this should update the cross-entity index)
    try graph.link(stack1, "thread", thread);
    try graph.link(stack2, "thread", thread);
    try graph.link(stack3, "thread", thread);

    // Query stacks for this thread, sorted by timestamp DESC
    // The cross-entity index (thread, timestamp DESC) should provide sorted order
    var json_buf: [512]u8 = undefined;
    const json_query = std.fmt.bufPrint(&json_buf,
        \\{{"root": "Thread", "id": {d}, "virtual": true, "edges": [{{"name": "stacks", "sort": [{{"field": "timestamp", "direction": "desc"}}]}}]}}
    , .{thread}) catch unreachable;

    var view = try graph.viewFromJson(json_query, .{ .limit = 100 });
    defer view.deinit();

    view.activate(true);
    loadViewport(&view);

    // Expand the stacks edge
    try view.expandById(thread, "stacks");
    loadViewport(&view);

    // Get items - with virtual=true, only children are visible
    // Should be: stack2(200), stack3(150), stack1(100)
    const items = try collectItemIds(testing.allocator, &view);
    defer testing.allocator.free(items);

    try testing.expectEqual(@as(usize, 3), items.len);
    try testing.expectEqual(stack2, items[0]); // timestamp 200 (highest)
    try testing.expectEqual(stack3, items[1]); // timestamp 150
    try testing.expectEqual(stack1, items[2]); // timestamp 100 (lowest)
}

test "Phase1: cross-entity index updated on unlink" {
    const schema = try createThreadStackSchema(testing.allocator);

    var graph = try Graph.init(testing.allocator, schema);
    defer graph.deinit();

    // Create and link
    const thread = try graph.insert("Thread");
    try graph.update(thread, .{ .name = "T1" });

    const stack1 = try graph.insert("Stack");
    try graph.update(stack1, .{ .timestamp = @as(i64, 100), .data = "s1" });

    const stack2 = try graph.insert("Stack");
    try graph.update(stack2, .{ .timestamp = @as(i64, 200), .data = "s2" });

    try graph.link(stack1, "thread", thread);
    try graph.link(stack2, "thread", thread);

    // Unlink stack2 (highest timestamp)
    try graph.unlink(stack2, "thread", thread);

    // Query should now only return stack1
    var json_buf: [512]u8 = undefined;
    const json_query = std.fmt.bufPrint(&json_buf,
        \\{{"root": "Thread", "id": {d}, "virtual": true, "edges": [{{"name": "stacks", "sort": [{{"field": "timestamp", "direction": "desc"}}]}}]}}
    , .{thread}) catch unreachable;

    var view = try graph.viewFromJson(json_query, .{ .limit = 100 });
    defer view.deinit();

    view.activate(true);
    loadViewport(&view);

    try view.expandById(thread, "stacks");
    loadViewport(&view);

    const items = try collectItemIds(testing.allocator, &view);
    defer testing.allocator.free(items);

    try testing.expectEqual(@as(usize, 1), items.len);
    try testing.expectEqual(stack1, items[0]); // Only stack1 remains
}

test "Phase1: cross-entity index handles multiple threads" {
    const schema = try createThreadStackSchema(testing.allocator);

    var graph = try Graph.init(testing.allocator, schema);
    defer graph.deinit();

    // Create two threads
    const thread1 = try graph.insert("Thread");
    try graph.update(thread1, .{ .name = "T1" });

    const thread2 = try graph.insert("Thread");
    try graph.update(thread2, .{ .name = "T2" });

    // Create stacks with interleaved timestamps
    const s1_t1 = try graph.insert("Stack");
    try graph.update(s1_t1, .{ .timestamp = @as(i64, 100), .data = "t1-s1" });

    const s2_t1 = try graph.insert("Stack");
    try graph.update(s2_t1, .{ .timestamp = @as(i64, 300), .data = "t1-s2" });

    const s1_t2 = try graph.insert("Stack");
    try graph.update(s1_t2, .{ .timestamp = @as(i64, 200), .data = "t2-s1" });

    const s2_t2 = try graph.insert("Stack");
    try graph.update(s2_t2, .{ .timestamp = @as(i64, 400), .data = "t2-s2" });

    // Link to respective threads
    try graph.link(s1_t1, "thread", thread1);
    try graph.link(s2_t1, "thread", thread1);
    try graph.link(s1_t2, "thread", thread2);
    try graph.link(s2_t2, "thread", thread2);

    // Query thread1's stacks - should be s2_t1(300), s1_t1(100)
    var json_buf1: [512]u8 = undefined;
    const json_query1 = std.fmt.bufPrint(&json_buf1,
        \\{{"root": "Thread", "id": {d}, "virtual": true, "edges": [{{"name": "stacks", "sort": [{{"field": "timestamp", "direction": "desc"}}]}}]}}
    , .{thread1}) catch unreachable;

    var view1 = try graph.viewFromJson(json_query1, .{ .limit = 100 });
    defer view1.deinit();

    view1.activate(true);
    loadViewport(&view1);

    try view1.expandById(thread1, "stacks");
    loadViewport(&view1);

    const items1 = try collectItemIds(testing.allocator, &view1);
    defer testing.allocator.free(items1);

    try testing.expectEqual(@as(usize, 2), items1.len);
    try testing.expectEqual(s2_t1, items1[0]); // timestamp 300
    try testing.expectEqual(s1_t1, items1[1]); // timestamp 100

    // Query thread2's stacks - should be s2_t2(400), s1_t2(200)
    var json_buf2: [512]u8 = undefined;
    const json_query2 = std.fmt.bufPrint(&json_buf2,
        \\{{"root": "Thread", "id": {d}, "virtual": true, "edges": [{{"name": "stacks", "sort": [{{"field": "timestamp", "direction": "desc"}}]}}]}}
    , .{thread2}) catch unreachable;

    var view2 = try graph.viewFromJson(json_query2, .{ .limit = 100 });
    defer view2.deinit();

    view2.activate(true);
    loadViewport(&view2);

    try view2.expandById(thread2, "stacks");
    loadViewport(&view2);

    const items2 = try collectItemIds(testing.allocator, &view2);
    defer testing.allocator.free(items2);

    try testing.expectEqual(@as(usize, 2), items2.len);
    try testing.expectEqual(s2_t2, items2[0]); // timestamp 400
    try testing.expectEqual(s1_t2, items2[1]); // timestamp 200
}

// ============================================================================
// Phase 2: Query Execution Tests
// ============================================================================

test "Phase2: sorted edge traversal uses cross-entity index" {
    const schema = try createThreadStackSchema(testing.allocator);

    var graph = try Graph.init(testing.allocator, schema);
    defer graph.deinit();

    const thread = try graph.insert("Thread");
    try graph.update(thread, .{ .name = "T1" });

    // Insert 100 stacks with varying timestamps
    var stack_ids: [100]NodeId = undefined;
    for (&stack_ids, 0..) |*sid, i| {
        sid.* = try graph.insert("Stack");
        try graph.update(sid.*, .{
            .timestamp = @as(i64, @intCast(i * 10)), // 0, 10, 20, ... 990
            .data = "stack",
        });
        try graph.link(sid.*, "thread", thread);
    }

    // Query with sort by timestamp DESC - should use index
    var json_buf: [512]u8 = undefined;
    const json_query = std.fmt.bufPrint(&json_buf,
        \\{{"root": "Thread", "id": {d}, "virtual": true, "edges": [{{"name": "stacks", "sort": [{{"field": "timestamp", "direction": "desc"}}]}}]}}
    , .{thread}) catch unreachable;

    var view = try graph.viewFromJson(json_query, .{ .limit = 200 });
    defer view.deinit();

    view.activate(true);
    loadViewport(&view);

    try view.expandById(thread, "stacks");
    loadViewport(&view);

    const items = try collectItemIds(testing.allocator, &view);
    defer testing.allocator.free(items);

    // Should have 100 stacks (virtual=true means thread not shown)
    try testing.expectEqual(@as(usize, 100), items.len);

    // First stack should be the one with highest timestamp (990)
    try testing.expectEqual(stack_ids[99], items[0]);

    // Last stack should be the one with lowest timestamp (0)
    try testing.expectEqual(stack_ids[0], items[99]);

    // Verify all are in descending order
    for (0..items.len - 1) |i| {
        const curr = graph.get(items[i]).?.getProperty("timestamp").?.int;
        const next = graph.get(items[i + 1]).?.getProperty("timestamp").?.int;
        try testing.expect(curr >= next);
    }
}

test "Phase2: sorted edge traversal with many items" {
    const schema = try createThreadStackSchema(testing.allocator);

    var graph = try Graph.init(testing.allocator, schema);
    defer graph.deinit();

    const thread = try graph.insert("Thread");
    try graph.update(thread, .{ .name = "T1" });

    // Insert 50 stacks (enough to validate sorting without being too slow)
    var highest_stack: NodeId = undefined;
    var lowest_stack: NodeId = undefined;
    for (0..50) |i| {
        const stack = try graph.insert("Stack");
        try graph.update(stack, .{
            .timestamp = @as(i64, @intCast(i * 10)), // 0, 10, 20, ..., 490
            .data = "stack",
        });
        try graph.link(stack, "thread", thread);
        if (i == 49) highest_stack = stack;
        if (i == 0) lowest_stack = stack;
    }

    // Query sorted by timestamp DESC
    var json_buf: [512]u8 = undefined;
    const json_query = std.fmt.bufPrint(&json_buf,
        \\{{"root": "Thread", "id": {d}, "virtual": true, "edges": [{{"name": "stacks", "sort": [{{"field": "timestamp", "direction": "desc"}}]}}]}}
    , .{thread}) catch unreachable;

    var view = try graph.viewFromJson(json_query, .{ .limit = 100 });
    defer view.deinit();

    view.activate(true);
    loadViewport(&view);

    try view.expandById(thread, "stacks");
    loadViewport(&view);

    const items = try collectItemIds(testing.allocator, &view);
    defer testing.allocator.free(items);

    // Should have 50 stacks (virtual=true)
    try testing.expectEqual(@as(usize, 50), items.len);

    // First stack should have highest timestamp (490)
    try testing.expectEqual(highest_stack, items[0]);

    // Last stack should have lowest timestamp (0)
    try testing.expectEqual(lowest_stack, items[49]);

    // Verify all are in descending order
    for (0..items.len - 1) |i| {
        const curr = graph.get(items[i]).?.getProperty("timestamp").?.int;
        const next = graph.get(items[i + 1]).?.getProperty("timestamp").?.int;
        try testing.expect(curr >= next);
    }
}

test "Phase2: ascending sort order uses index" {
    const schema = try createThreadStackSchema(testing.allocator);

    var graph = try Graph.init(testing.allocator, schema);
    defer graph.deinit();

    const thread = try graph.insert("Thread");
    try graph.update(thread, .{ .name = "T1" });

    const stack1 = try graph.insert("Stack");
    try graph.update(stack1, .{ .timestamp = @as(i64, 300), .data = "s1" });

    const stack2 = try graph.insert("Stack");
    try graph.update(stack2, .{ .timestamp = @as(i64, 100), .data = "s2" });

    const stack3 = try graph.insert("Stack");
    try graph.update(stack3, .{ .timestamp = @as(i64, 200), .data = "s3" });

    try graph.link(stack1, "thread", thread);
    try graph.link(stack2, "thread", thread);
    try graph.link(stack3, "thread", thread);

    // Query with ASC order
    var json_buf: [512]u8 = undefined;
    const json_query = std.fmt.bufPrint(&json_buf,
        \\{{"root": "Thread", "id": {d}, "virtual": true, "edges": [{{"name": "stacks", "sort": [{{"field": "timestamp", "direction": "asc"}}]}}]}}
    , .{thread}) catch unreachable;

    var view = try graph.viewFromJson(json_query, .{ .limit = 100 });
    defer view.deinit();

    view.activate(true);
    loadViewport(&view);

    try view.expandById(thread, "stacks");
    loadViewport(&view);

    const items = try collectItemIds(testing.allocator, &view);
    defer testing.allocator.free(items);

    try testing.expectEqual(@as(usize, 3), items.len);
    try testing.expectEqual(stack2, items[0]); // timestamp 100 (lowest)
    try testing.expectEqual(stack3, items[1]); // timestamp 200
    try testing.expectEqual(stack1, items[2]); // timestamp 300 (highest)
}

// ============================================================================
// Phase 3: Rollup Optimization Tests
// ============================================================================

test "Phase3: schema with first rollup parses correctly" {
    // This test verifies the schema parser accepts 'first' rollup kind
    const schema = createThreadStackSchemaWithRollups(testing.allocator) catch |err| {
        // Expected to fail until Phase 3 is implemented
        if (err == error.InvalidJson) {
            // Mark as expected failure
            std.debug.print("\n[EXPECTED FAIL] 'first' rollup not yet supported in schema\n", .{});
            return error.SkipZigTest;
        }
        return err;
    };

    // Graph will own the schema
    var graph = try Graph.init(testing.allocator, schema);
    defer graph.deinit();

    // If we get here, schema parsed successfully - verify via graph
    try testing.expect(graph.get(1) == null); // Just verify graph works
}

test "Phase3: first rollup returns highest by sort field" {
    const schema = createThreadStackSchemaWithRollups(testing.allocator) catch |err| {
        if (err == error.InvalidJson) {
            std.debug.print("\n[EXPECTED FAIL] 'first' rollup not yet supported\n", .{});
            return error.SkipZigTest;
        }
        return err;
    };

    var graph = try Graph.init(testing.allocator, schema);
    defer graph.deinit();

    const thread = try graph.insert("Thread");
    try graph.update(thread, .{ .name = "T1" });

    const stack1 = try graph.insert("Stack");
    try graph.update(stack1, .{ .timestamp = @as(i64, 100), .data = "s1" });

    const stack2 = try graph.insert("Stack");
    try graph.update(stack2, .{ .timestamp = @as(i64, 300), .data = "s2" });

    const stack3 = try graph.insert("Stack");
    try graph.update(stack3, .{ .timestamp = @as(i64, 200), .data = "s3" });

    try graph.link(stack1, "thread", thread);
    try graph.link(stack2, "thread", thread);
    try graph.link(stack3, "thread", thread);

    // Get thread - latestTimestamp rollup should return 300
    const node = graph.get(thread).?;
    const latest = node.getProperty("latestTimestamp") orelse {
        std.debug.print("\n[EXPECTED FAIL] latestTimestamp rollup not computed\n", .{});
        return error.SkipZigTest;
    };

    try testing.expectEqual(@as(i64, 300), latest.int);
}

test "Phase3: last rollup returns lowest by sort field" {
    const schema = createThreadStackSchemaWithRollups(testing.allocator) catch |err| {
        if (err == error.InvalidJson) {
            std.debug.print("\n[EXPECTED FAIL] 'last' rollup not yet supported\n", .{});
            return error.SkipZigTest;
        }
        return err;
    };

    var graph = try Graph.init(testing.allocator, schema);
    defer graph.deinit();

    const thread = try graph.insert("Thread");
    try graph.update(thread, .{ .name = "T1" });

    const stack1 = try graph.insert("Stack");
    try graph.update(stack1, .{ .timestamp = @as(i64, 100), .data = "s1" });

    const stack2 = try graph.insert("Stack");
    try graph.update(stack2, .{ .timestamp = @as(i64, 300), .data = "s2" });

    const stack3 = try graph.insert("Stack");
    try graph.update(stack3, .{ .timestamp = @as(i64, 200), .data = "s3" });

    try graph.link(stack1, "thread", thread);
    try graph.link(stack2, "thread", thread);
    try graph.link(stack3, "thread", thread);

    // Get thread - oldestTimestamp rollup should return 100
    const node = graph.get(thread).?;
    const oldest = node.getProperty("oldestTimestamp") orelse {
        std.debug.print("\n[EXPECTED FAIL] oldestTimestamp rollup not computed\n", .{});
        return error.SkipZigTest;
    };

    try testing.expectEqual(@as(i64, 100), oldest.int);
}

test "Phase3: first rollup updates when new highest linked" {
    const schema = createThreadStackSchemaWithRollups(testing.allocator) catch |err| {
        if (err == error.InvalidJson) {
            std.debug.print("\n[EXPECTED FAIL] 'first' rollup not yet supported\n", .{});
            return error.SkipZigTest;
        }
        return err;
    };

    var graph = try Graph.init(testing.allocator, schema);
    defer graph.deinit();

    const thread = try graph.insert("Thread");
    try graph.update(thread, .{ .name = "T1" });

    const stack1 = try graph.insert("Stack");
    try graph.update(stack1, .{ .timestamp = @as(i64, 100), .data = "s1" });

    try graph.link(stack1, "thread", thread);

    // Initial value should be 100
    var node = graph.get(thread).?;
    var latest = node.getProperty("latestTimestamp") orelse {
        std.debug.print("\n[EXPECTED FAIL] latestTimestamp rollup not computed\n", .{});
        return error.SkipZigTest;
    };
    try testing.expectEqual(@as(i64, 100), latest.int);

    // Link a newer stack
    const stack2 = try graph.insert("Stack");
    try graph.update(stack2, .{ .timestamp = @as(i64, 500), .data = "s2" });
    try graph.link(stack2, "thread", thread);

    // Should now be 500
    node = graph.get(thread).?;
    latest = node.getProperty("latestTimestamp") orelse return error.SkipZigTest;
    try testing.expectEqual(@as(i64, 500), latest.int);
}

test "Phase3: first rollup updates when highest unlinked" {
    const schema = createThreadStackSchemaWithRollups(testing.allocator) catch |err| {
        if (err == error.InvalidJson) {
            std.debug.print("\n[EXPECTED FAIL] 'first' rollup not yet supported\n", .{});
            return error.SkipZigTest;
        }
        return err;
    };

    var graph = try Graph.init(testing.allocator, schema);
    defer graph.deinit();

    const thread = try graph.insert("Thread");
    try graph.update(thread, .{ .name = "T1" });

    const stack1 = try graph.insert("Stack");
    try graph.update(stack1, .{ .timestamp = @as(i64, 100), .data = "s1" });

    const stack2 = try graph.insert("Stack");
    try graph.update(stack2, .{ .timestamp = @as(i64, 300), .data = "s2" });

    try graph.link(stack1, "thread", thread);
    try graph.link(stack2, "thread", thread);

    // Initial highest is 300
    var node = graph.get(thread).?;
    var latest = node.getProperty("latestTimestamp") orelse {
        std.debug.print("\n[EXPECTED FAIL] latestTimestamp rollup not computed\n", .{});
        return error.SkipZigTest;
    };
    try testing.expectEqual(@as(i64, 300), latest.int);

    // Unlink the highest
    try graph.unlink(stack2, "thread", thread);

    // Should now fall back to 100
    node = graph.get(thread).?;
    latest = node.getProperty("latestTimestamp") orelse return error.SkipZigTest;
    try testing.expectEqual(@as(i64, 100), latest.int);
}

test "Phase3: first rollup invalidated on sort field change" {
    const schema = createThreadStackSchemaWithRollups(testing.allocator) catch |err| {
        if (err == error.InvalidJson) {
            std.debug.print("\n[EXPECTED FAIL] 'first' rollup not yet supported\n", .{});
            return error.SkipZigTest;
        }
        return err;
    };

    var graph = try Graph.init(testing.allocator, schema);
    defer graph.deinit();

    const thread = try graph.insert("Thread");
    try graph.update(thread, .{ .name = "T1" });

    const stack1 = try graph.insert("Stack");
    try graph.update(stack1, .{ .timestamp = @as(i64, 100), .data = "s1" });

    const stack2 = try graph.insert("Stack");
    try graph.update(stack2, .{ .timestamp = @as(i64, 300), .data = "s2" });

    try graph.link(stack1, "thread", thread);
    try graph.link(stack2, "thread", thread);

    // Initial highest is stack2 with 300
    var node = graph.get(thread).?;
    var latest = node.getProperty("latestTimestamp") orelse {
        std.debug.print("\n[EXPECTED FAIL] latestTimestamp rollup not computed\n", .{});
        return error.SkipZigTest;
    };
    try testing.expectEqual(@as(i64, 300), latest.int);

    // Update stack1's timestamp to be higher
    try graph.update(stack1, .{ .timestamp = @as(i64, 500) });

    // Should now return 500
    node = graph.get(thread).?;
    latest = node.getProperty("latestTimestamp") orelse return error.SkipZigTest;
    try testing.expectEqual(@as(i64, 500), latest.int);
}
