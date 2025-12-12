///! Black-box integration tests for Neograph.
///!
///! Tests verify behavior through the public API only, as specified in TEST.md.
///! Each test category covers one dimension of behavior with orthogonal coverage.

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

// Import the public API
const ng = @import("neograph.zig");
const Graph = ng.Graph;
const Schema = ng.Schema;
const View = ng.View;
const ViewOpts = ng.ViewOpts;
const Value = ng.Value;
const NodeId = ng.NodeId;
const Node = ng.Node;
const Callbacks = ng.Callbacks;
const Item = ng.Item;
const NodeStoreError = ng.NodeStoreError;
const SortDir = ng.SortDir;
const ParseError = ng.ParseError;
const parseSchema = ng.parseSchema;

// ============================================================================
// Test Helpers
// ============================================================================

/// Collect item IDs from a View into a slice.
fn collectItems(allocator: Allocator, view: *View) ![]NodeId {
    var result = std.ArrayListUnmanaged(NodeId){};
    errdefer result.deinit(allocator);

    var iter = view.items();
    while (iter.next()) |item_view| {
        try result.append(allocator, item_view.id);
    }

    return try result.toOwnedSlice(allocator);
}

// ============================================================================
// Test Helper - Get property value from Graph
// ============================================================================

/// Get a node's property value (convenience wrapper).
fn getProperty(g: *Graph, id: NodeId, prop: []const u8) ?Value {
    const node = g.get(id) orelse return null;
    return node.getProperty(prop);
}

// ============================================================================
// Schema Builders for Tests
// ============================================================================

/// Create a basic User/Post schema for CRUD tests.
fn createUserPostSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "User",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "age", "type": "int" },
        \\        { "name": "score", "type": "number" },
        \\        { "name": "active", "type": "bool" }
        \\      ],
        \\      "edges": [{ "name": "posts", "target": "Post", "reverse": "author" }],
        \\      "indexes": [
        \\        { "fields": [{ "field": "name", "direction": "asc" }] },
        \\        { "fields": [{ "field": "active", "direction": "asc" }] }
        \\      ]
        \\    },
        \\    {
        \\      "name": "Post",
        \\      "properties": [
        \\        { "name": "title", "type": "string" },
        \\        { "name": "views", "type": "int" },
        \\        { "name": "published", "type": "bool" }
        \\      ],
        \\      "edges": [
        \\        { "name": "author", "target": "User", "reverse": "posts" },
        \\        { "name": "comments", "target": "Comment", "reverse": "post" }
        \\      ],
        \\      "rollups": [
        \\        { "name": "author_name", "traverse": { "edge": "author", "property": "name" } },
        \\        { "name": "comment_count", "count": "comments" }
        \\      ],
        \\      "indexes": [
        \\        { "fields": [{ "field": "views", "direction": "desc" }] },
        \\        { "fields": [{ "field": "published", "direction": "asc" }, { "field": "views", "direction": "desc" }] }
        \\      ]
        \\    },
        \\    {
        \\      "name": "Comment",
        \\      "properties": [
        \\        { "name": "text", "type": "string" },
        \\        { "name": "approved", "type": "bool" }
        \\      ],
        \\      "edges": [{ "name": "post", "target": "Post", "reverse": "comments" }],
        \\      "indexes": [{ "fields": [{ "field": "approved", "direction": "asc" }] }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

// ============================================================================
// Section 1: Schema Validation Tests
// ============================================================================

test "1.1 Valid Schemas - Empty type" {
    ensureWatchdog();

    var schema = try parseSchema(testing.allocator,
        \\{ "types": [{ "name": "User" }] }
    );
    defer schema.deinit();

    try testing.expect(schema.getType("User") != null);
}

test "1.1 Valid Schemas - Property types" {
    var schema = try parseSchema(testing.allocator,
        \\{
        \\  "types": [{
        \\    "name": "User",
        \\    "properties": [
        \\      { "name": "name", "type": "string" },
        \\      { "name": "age", "type": "int" },
        \\      { "name": "score", "type": "number" },
        \\      { "name": "active", "type": "bool" }
        \\    ]
        \\  }]
        \\}
    );
    defer schema.deinit();

    const user = schema.getType("User").?;
    try testing.expect(user.getProperty("name") == .string);
    try testing.expect(user.getProperty("age") == .int);
    try testing.expect(user.getProperty("score") == .number);
    try testing.expect(user.getProperty("active") == .bool);
}

test "1.1 Valid Schemas - Bidirectional edge" {
    var schema = try parseSchema(testing.allocator,
        \\{
        \\  "types": [
        \\    { "name": "User", "edges": [{ "name": "posts", "target": "Post", "reverse": "author" }] },
        \\    { "name": "Post", "edges": [{ "name": "author", "target": "User", "reverse": "posts" }] }
        \\  ]
        \\}
    );
    defer schema.deinit();

    const user = schema.getType("User").?;
    const post = schema.getType("Post").?;

    try testing.expectEqualStrings("Post", user.getEdge("posts").?.target_type_name);
    try testing.expectEqualStrings("User", post.getEdge("author").?.target_type_name);
}

test "1.1 Valid Schemas - Self-referential edge" {
    var schema = try parseSchema(testing.allocator,
        \\{
        \\  "types": [{
        \\    "name": "User",
        \\    "edges": [{ "name": "friends", "target": "User", "reverse": "friends" }]
        \\  }]
        \\}
    );
    defer schema.deinit();

    const user = schema.getType("User").?;
    const friends_edge = user.getEdge("friends").?;
    try testing.expectEqualStrings("User", friends_edge.target_type_name);
    try testing.expectEqualStrings("friends", friends_edge.reverse_name);
}

test "1.1 Valid Schemas - Single-field index" {
    var schema = try parseSchema(testing.allocator,
        \\{
        \\  "types": [{
        \\    "name": "User",
        \\    "properties": [{ "name": "name", "type": "string" }],
        \\    "indexes": [{ "fields": [{ "field": "name", "direction": "asc" }] }]
        \\  }]
        \\}
    );
    defer schema.deinit();

    const user = schema.getType("User").?;
    try testing.expectEqual(@as(usize, 1), user.indexes.len);
    try testing.expectEqualStrings("name", user.indexes[0].fields[0].name);
}

test "1.1 Valid Schemas - Compound index" {
    var schema = try parseSchema(testing.allocator,
        \\{
        \\  "types": [{
        \\    "name": "Post",
        \\    "properties": [
        \\      { "name": "status", "type": "string" },
        \\      { "name": "views", "type": "int" }
        \\    ],
        \\    "indexes": [{ "fields": [
        \\      { "field": "status", "direction": "asc" },
        \\      { "field": "views", "direction": "desc" }
        \\    ]}]
        \\  }]
        \\}
    );
    defer schema.deinit();

    const post = schema.getType("Post").?;
    try testing.expectEqual(@as(usize, 2), post.indexes[0].fields.len);
    try testing.expectEqual(SortDir.asc, post.indexes[0].fields[0].direction);
    try testing.expectEqual(SortDir.desc, post.indexes[0].fields[1].direction);
}

test "1.1 Valid Schemas - Traverse rollup" {
    var schema = try parseSchema(testing.allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "User",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [{ "name": "posts", "target": "Post", "reverse": "author" }]
        \\    },
        \\    {
        \\      "name": "Post",
        \\      "edges": [{ "name": "author", "target": "User", "reverse": "posts" }],
        \\      "rollups": [{ "name": "author_name", "traverse": { "edge": "author", "property": "name" } }]
        \\    }
        \\  ]
        \\}
    );
    defer schema.deinit();

    const post = schema.getType("Post").?;
    const rollup = post.getRollup("author_name").?;
    try testing.expectEqualStrings("author", rollup.kind.traverse.edge);
    try testing.expectEqualStrings("name", rollup.kind.traverse.property);
}

test "1.1 Valid Schemas - Count rollup" {
    var schema = try parseSchema(testing.allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "Post",
        \\      "edges": [{ "name": "comments", "target": "Comment", "reverse": "post" }],
        \\      "rollups": [{ "name": "comment_count", "count": "comments" }]
        \\    },
        \\    {
        \\      "name": "Comment",
        \\      "edges": [{ "name": "post", "target": "Post", "reverse": "comments" }]
        \\    }
        \\  ]
        \\}
    );
    defer schema.deinit();

    const post = schema.getType("Post").?;
    const rollup = post.getRollup("comment_count").?;
    try testing.expectEqualStrings("comments", rollup.kind.count);
}

test "1.2 Invalid Schemas - Missing reverse edge" {
    // Post.author references User.posts, but User doesn't have posts edge
    const result = parseSchema(testing.allocator,
        \\{
        \\  "types": [
        \\    { "name": "Post", "edges": [{ "name": "author", "target": "User", "reverse": "posts" }] },
        \\    { "name": "User", "properties": [{ "name": "name", "type": "string" }] }
        \\  ]
        \\}
    );
    try testing.expectError(ParseError.MissingReverseEdge, result);
}

// ============================================================================
// Section 2: CRUD Operations Tests
// ============================================================================

test "2.1 Insert - Returns id > 0" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id = try g.insert("User");
    try testing.expect(id > 0);
}

test "2.1 Insert - Creates retrievable node" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id = try g.insert("User");
    try g.update(id, .{ .name = "Alice" });

    const name = getProperty(g,id, "name");
    try testing.expect(name != null);
    try testing.expectEqualStrings("Alice", name.?.string);
}

test "2.1 Insert - All property types" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id = try g.insert("User");
    try g.update(id, .{
        .name = "Test",
        .age = @as(i64, 42),
        .score = @as(f64, 3.14),
        .active = true,
    });

    try testing.expectEqualStrings("Test", getProperty(g,id, "name").?.string);
    try testing.expectEqual(@as(i64, 42), getProperty(g,id, "age").?.int);
    try testing.expectApproxEqAbs(@as(f64, 3.14), getProperty(g,id, "score").?.number, 0.001);
    try testing.expect(getProperty(g,id, "active").?.bool == true);
}

test "2.1 Insert - Partial properties returns nil for unset" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id = try g.insert("User");
    try g.update(id, .{ .name = "Alice" });

    try testing.expect(getProperty(g,id, "age") == null);
}

test "2.1 Insert - Unknown type fails" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const result = g.insert("Unknown");
    try testing.expectError(NodeStoreError.UnknownType, result);
}

test "2.1 Insert - Sequential ids" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id1 = try g.insert("User");
    const id2 = try g.insert("User");
    try testing.expectEqual(id1 + 1, id2);
}

test "2.2 Get - Existing node" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id = try g.insert("User");
    try g.update(id, .{ .name = "Alice" });

    const node = g.get(id);
    try testing.expect(node != null);
}

test "2.2 Get - Nonexistent node" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const node = g.get(99999);
    try testing.expect(node == null);
}

test "2.2 Get - Deleted node returns nil" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id = try g.insert("User");
    try g.delete(id);

    try testing.expect(g.get(id) == null);
}

test "2.3 Update - Single property" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id = try g.insert("User");
    try g.update(id, .{ .name = "Alice" });
    try g.update(id, .{ .name = "Bob" });

    try testing.expectEqualStrings("Bob", getProperty(g,id, "name").?.string);
}

test "2.3 Update - Preserves other properties" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id = try g.insert("User");
    try g.update(id, .{ .name = "Alice", .age = @as(i64, 30) });
    try g.update(id, .{ .name = "Bob" });

    try testing.expectEqualStrings("Bob", getProperty(g,id, "name").?.string);
    try testing.expectEqual(@as(i64, 30), getProperty(g,id, "age").?.int);
}

test "2.3 Update - Nonexistent node fails" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const result = g.update(99999, .{ .name = "Test" });
    try testing.expectError(NodeStoreError.NodeNotFound, result);
}

test "2.4 Delete - Removes node" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id = try g.insert("User");
    try g.delete(id);

    try testing.expect(g.get(id) == null);
}

test "2.4 Delete - Nonexistent fails" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const result = g.delete(99999);
    try testing.expectError(NodeStoreError.NodeNotFound, result);
}

// ============================================================================
// Section 3: Edge Operations Tests
// ============================================================================

test "3.1 Link - Creates forward edge" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const user_id = try g.insert("User");
    const post_id = try g.insert("Post");

    try g.link(post_id, "author", user_id);

    // Verify forward edge exists
    const targets = g.getEdgeTargets(post_id, "author").?;
    try testing.expectEqual(@as(usize, 1), targets.len);
    try testing.expectEqual(user_id, targets[0]);
}

test "3.1 Link - Creates reverse edge" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const user_id = try g.insert("User");
    const post_id = try g.insert("Post");

    try g.link(post_id, "author", user_id);

    // Verify reverse edge exists
    const targets = g.getEdgeTargets(user_id, "posts").?;
    try testing.expectEqual(@as(usize, 1), targets.len);
    try testing.expectEqual(post_id, targets[0]);
}

/// Create a User schema with self-referential friends edge.
fn createSelfRefSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [{
        \\    "name": "User",
        \\    "properties": [{ "name": "name", "type": "string" }],
        \\    "edges": [{ "name": "friends", "target": "User", "reverse": "friends" }],
        \\    "indexes": [{ "fields": [{ "field": "name", "direction": "asc" }] }]
        \\  }]
        \\}
    ) catch return error.InvalidJson;
}

test "3.1 Link - Self reference" {
    const schema = try createSelfRefSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const user_id = try g.insert("User");
    try g.link(user_id, "friends", user_id);

    // Verify self-referential edge
    const targets = g.getEdgeTargets(user_id, "friends").?;
    try testing.expectEqual(@as(usize, 1), targets.len);
    try testing.expectEqual(user_id, targets[0]);
}

test "3.1 Link - Unknown edge fails" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const user_id = try g.insert("User");
    const post_id = try g.insert("Post");

    const result = g.link(user_id, "unknown", post_id);
    try testing.expectError(NodeStoreError.UnknownEdge, result);
}

test "3.1 Link - To nonexistent node fails" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const post_id = try g.insert("Post");

    const result = g.link(post_id, "author", 99999);
    try testing.expectError(NodeStoreError.EdgeTargetNotFound, result);
}

test "3.1 Link - Multi-target edge" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const user_id = try g.insert("User");
    const post1_id = try g.insert("Post");
    const post2_id = try g.insert("Post");

    try g.link(post1_id, "author", user_id);
    try g.link(post2_id, "author", user_id);

    // Verify user has both posts
    const targets = g.getEdgeTargets(user_id, "posts").?;
    try testing.expectEqual(@as(usize, 2), targets.len);
}

test "3.2 Unlink - Removes forward edge" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const user_id = try g.insert("User");
    const post_id = try g.insert("Post");

    try g.link(post_id, "author", user_id);
    try g.unlink(post_id, "author", user_id);

    const targets = g.getEdgeTargets(post_id, "author").?;
    try testing.expectEqual(@as(usize, 0), targets.len);
}

test "3.2 Unlink - Removes reverse edge" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const user_id = try g.insert("User");
    const post_id = try g.insert("Post");

    try g.link(post_id, "author", user_id);
    try g.unlink(post_id, "author", user_id);

    const targets = g.getEdgeTargets(user_id, "posts").?;
    try testing.expectEqual(@as(usize, 0), targets.len);
}

test "3.2 Unlink - Preserves other targets" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const user_id = try g.insert("User");
    const post1_id = try g.insert("Post");
    const post2_id = try g.insert("Post");

    try g.link(post1_id, "author", user_id);
    try g.link(post2_id, "author", user_id);
    try g.unlink(post1_id, "author", user_id);

    const targets = g.getEdgeTargets(user_id, "posts").?;
    try testing.expectEqual(@as(usize, 1), targets.len);
    try testing.expectEqual(post2_id, targets[0]);
}

// ============================================================================
// Section 10: Edge Cases Tests
// ============================================================================

test "10.1 Empty States - Node count starts at zero" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    try testing.expectEqual(@as(usize, 0), g.count());
}

test "10.1 Empty States - Delete all items" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id1 = try g.insert("User");
    const id2 = try g.insert("User");

    try g.delete(id1);
    try g.delete(id2);

    try testing.expectEqual(@as(usize, 0), g.count());
}

test "10.2 Large Scale - Insert many nodes" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const count = 1000;
    var last_id: NodeId = 0;
    for (0..count) |_| {
        last_id = try g.insert("User");
    }

    try testing.expectEqual(@as(usize, count), g.count());
    try testing.expectEqual(@as(NodeId, count), last_id);
}

test "10.3 Cycles - Self reference doesn't infinite loop" {
    const schema = try createSelfRefSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const user_id = try g.insert("User");
    try g.update(user_id, .{ .name = "Alice" });
    try g.link(user_id, "friends", user_id);

    // Query should not loop infinitely
    const targets = g.getEdgeTargets(user_id, "friends").?;
    try testing.expectEqual(@as(usize, 1), targets.len);
}

// ============================================================================
// Section 14: Strengthened Verifications
// ============================================================================

test "14.6 Boundary Values - Empty string" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id = try g.insert("User");
    try g.update(id, .{ .name = "" });

    const name = getProperty(g,id, "name");
    try testing.expect(name != null);
    try testing.expectEqualStrings("", name.?.string);
}

test "14.6 Boundary Values - Zero integer" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id = try g.insert("Post");
    try g.update(id, .{ .views = @as(i64, 0) });

    const views = getProperty(g,id, "views");
    try testing.expect(views != null);
    try testing.expectEqual(@as(i64, 0), views.?.int);
}

test "14.6 Boundary Values - Negative integer" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id = try g.insert("User");
    try g.update(id, .{ .age = @as(i64, -100) });

    const age = getProperty(g,id, "age");
    try testing.expect(age != null);
    try testing.expectEqual(@as(i64, -100), age.?.int);
}

test "14.6 Boundary Values - Large integer" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const large: i64 = 1 << 62;
    const id = try g.insert("User");
    try g.update(id, .{ .age = large });

    const age = getProperty(g,id, "age");
    try testing.expect(age != null);
    try testing.expectEqual(large, age.?.int);
}

test "14.7 State Consistency - Deleted not in edge" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const user_id = try g.insert("User");
    const post_id = try g.insert("Post");

    try g.link(post_id, "author", user_id);
    try g.delete(user_id);

    // Post should no longer have author edge
    const targets = g.getEdgeTargets(post_id, "author").?;
    try testing.expectEqual(@as(usize, 0), targets.len);
}

test "14.7 State Consistency - Unlink restores state" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const user_id = try g.insert("User");
    const post_id = try g.insert("Post");

    // Link then unlink
    try g.link(post_id, "author", user_id);
    try g.unlink(post_id, "author", user_id);

    // Should be same as never linked
    const post_targets = g.getEdgeTargets(post_id, "author").?;
    try testing.expectEqual(@as(usize, 0), post_targets.len);

    const user_targets = g.getEdgeTargets(user_id, "posts").?;
    try testing.expectEqual(@as(usize, 0), user_targets.len);
}

test "14.12 Error Recovery - Failed insert has no side effects" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const count_before = g.count();

    const result = g.insert("NonexistentType");
    try testing.expectError(NodeStoreError.UnknownType, result);

    const count_after = g.count();
    try testing.expectEqual(count_before, count_after);
}

test "14.12 Error Recovery - Failed update preserves value" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const id = try g.insert("User");
    try g.update(id, .{ .name = "Original" });

    // Try to update non-existent node
    const result = g.update(99999, .{ .name = "New" });
    try testing.expectError(NodeStoreError.NodeNotFound, result);

    // Original should be preserved
    try testing.expectEqualStrings("Original", getProperty(g,id, "name").?.string);
}

test "14.12 Error Recovery - Failed link has no partial edge" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const post_id = try g.insert("Post");

    const result = g.link(post_id, "author", 99999);
    try testing.expectError(NodeStoreError.EdgeTargetNotFound, result);

    // Post should have no edge
    const targets = g.getEdgeTargets(post_id, "author").?;
    try testing.expectEqual(@as(usize, 0), targets.len);
}

// ============================================================================
// Section 15: Reactive Callback Tests
// ============================================================================

/// Test context for tracking reactive callbacks
const CallbackTracker = struct {
    enters: std.ArrayListUnmanaged(EventRecord),
    leaves: std.ArrayListUnmanaged(EventRecord),
    changes: std.ArrayListUnmanaged(ChangeRecord),
    moves: std.ArrayListUnmanaged(MoveRecord),
    allocator: Allocator,

    const EventRecord = struct {
        node_id: NodeId,
        index: u32,
    };

    const ChangeRecord = struct {
        node_id: NodeId,
        index: u32,
    };

    const MoveRecord = struct {
        node_id: NodeId,
        from: u32,
        to: u32,
    };

    fn init(allocator: Allocator) CallbackTracker {
        return .{
            .enters = .{},
            .leaves = .{},
            .changes = .{},
            .moves = .{},
            .allocator = allocator,
        };
    }

    fn deinit(self: *CallbackTracker) void {
        self.enters.deinit(self.allocator);
        self.leaves.deinit(self.allocator);
        self.changes.deinit(self.allocator);
        self.moves.deinit(self.allocator);
    }

    fn onEnter(ctx: ?*anyopaque, item: Item, index: u32) void {
        const self: *CallbackTracker = @ptrCast(@alignCast(ctx));
        self.enters.append(self.allocator, .{ .node_id = item.id, .index = index }) catch {};
    }

    fn onLeave(ctx: ?*anyopaque, item: Item, index: u32) void {
        const self: *CallbackTracker = @ptrCast(@alignCast(ctx));
        self.leaves.append(self.allocator, .{ .node_id = item.id, .index = index }) catch {};
    }

    fn onChange(ctx: ?*anyopaque, item: Item, index: u32, _: Item) void {
        const self: *CallbackTracker = @ptrCast(@alignCast(ctx));
        self.changes.append(self.allocator, .{ .node_id = item.id, .index = index }) catch {};
    }

    fn onMove(ctx: ?*anyopaque, item: Item, from: u32, to: u32) void {
        const self: *CallbackTracker = @ptrCast(@alignCast(ctx));
        self.moves.append(self.allocator, .{ .node_id = item.id, .from = from, .to = to }) catch {};
    }

    fn getCallbacks(self: *CallbackTracker) ng.Callbacks {
        return .{
            .on_enter = onEnter,
            .on_leave = onLeave,
            .on_change = onChange,
            .on_move = onMove,
            .context = self,
        };
    }
};

test "15.1 Reactive - on_enter fires when node inserted matches subscription" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    var tracker = CallbackTracker.init(testing.allocator);
    defer tracker.deinit();

    // Create view for active users
    var view = try g.view(.{
        .root = "User",
        .filter = &.{.{ .field = "active", .value = .{ .bool = true } }},
    }, .{});
    defer view.deinit();
    view.setCallbacks(tracker.getCallbacks());
    view.activate(true);

    // Insert matching node
    const user_id = try g.insert("User");
    try g.update(user_id, .{ .name = "Alice", .active = true });

    // Verify on_enter was called
    try testing.expectEqual(@as(usize, 1), tracker.enters.items.len);
    try testing.expectEqual(user_id, tracker.enters.items[0].node_id);
}

test "15.1 Reactive - on_enter does not fire for non-matching node" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    var tracker = CallbackTracker.init(testing.allocator);
    defer tracker.deinit();

    // Create view for active users
    var view = try g.view(.{
        .root = "User",
        .filter = &.{.{ .field = "active", .value = .{ .bool = true } }},
    }, .{});
    defer view.deinit();
    view.setCallbacks(tracker.getCallbacks());
    view.activate(true);

    // Insert non-matching node (active = false)
    const user_id = try g.insert("User");
    try g.update(user_id, .{ .name = "Bob", .active = false });

    // Verify on_enter was NOT called
    try testing.expectEqual(@as(usize, 0), tracker.enters.items.len);
}

test "15.2 Reactive - on_leave fires when node deleted" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Insert node first
    const user_id = try g.insert("User");
    try g.update(user_id, .{ .name = "Alice", .active = true });

    var tracker = CallbackTracker.init(testing.allocator);
    defer tracker.deinit();

    // Create view for active users (node already exists)
    var view = try g.view(.{
        .root = "User",
        .filter = &.{.{ .field = "active", .value = .{ .bool = true } }},
    }, .{});
    defer view.deinit();
    view.setCallbacks(tracker.getCallbacks());
    view.activate(true);

    // Delete the node
    try g.delete(user_id);

    // Verify on_leave was called
    try testing.expectEqual(@as(usize, 1), tracker.leaves.items.len);
    try testing.expectEqual(user_id, tracker.leaves.items[0].node_id);
}

test "15.2 Reactive - on_leave fires when node no longer matches filter" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Insert matching node first
    const user_id = try g.insert("User");
    try g.update(user_id, .{ .name = "Alice", .active = true });

    var tracker = CallbackTracker.init(testing.allocator);
    defer tracker.deinit();

    // Create view for active users
    var view = try g.view(.{
        .root = "User",
        .filter = &.{.{ .field = "active", .value = .{ .bool = true } }},
    }, .{});
    defer view.deinit();
    view.setCallbacks(tracker.getCallbacks());
    view.activate(true);

    // Update to no longer match (active = false)
    try g.update(user_id, .{ .active = false });

    // Verify on_leave was called
    try testing.expectEqual(@as(usize, 1), tracker.leaves.items.len);
    try testing.expectEqual(user_id, tracker.leaves.items[0].node_id);
}

test "15.3 Reactive - on_enter fires when update makes node match" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Insert non-matching node first
    const user_id = try g.insert("User");
    try g.update(user_id, .{ .name = "Alice", .active = false });

    var tracker = CallbackTracker.init(testing.allocator);
    defer tracker.deinit();

    // Create view for active users
    var view = try g.view(.{
        .root = "User",
        .filter = &.{.{ .field = "active", .value = .{ .bool = true } }},
    }, .{});
    defer view.deinit();
    view.setCallbacks(tracker.getCallbacks());
    view.activate(true);

    // Update to match (active = true)
    try g.update(user_id, .{ .active = true });

    // Verify on_enter was called
    try testing.expectEqual(@as(usize, 1), tracker.enters.items.len);
    try testing.expectEqual(user_id, tracker.enters.items[0].node_id);
}

test "15.4 Reactive - update non-filter field keeps node in view" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Insert matching node first
    const user_id = try g.insert("User");
    try g.update(user_id, .{ .name = "Alice", .active = true });

    var tracker = CallbackTracker.init(testing.allocator);
    defer tracker.deinit();

    // Create view for active users
    var view = try g.view(.{
        .root = "User",
        .filter = &.{.{ .field = "active", .value = .{ .bool = true } }},
    }, .{});
    defer view.deinit();
    view.setCallbacks(tracker.getCallbacks());
    view.activate(true);

    // Verify node is in view
    try testing.expectEqual(@as(u32, 1), view.total());

    // Update non-filter field (name) - node should stay in view
    try g.update(user_id, .{ .name = "Bob" });

    // Node still in view
    try testing.expectEqual(@as(u32, 1), view.total());

    // No leave callback (node still matches)
    try testing.expectEqual(@as(usize, 0), tracker.leaves.items.len);
}

test "15.5 Reactive - deinit stops callbacks" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    var tracker = CallbackTracker.init(testing.allocator);
    defer tracker.deinit();

    // Create and activate view for active users
    var view = try g.view(.{
        .root = "User",
        .filter = &.{.{ .field = "active", .value = .{ .bool = true } }},
    }, .{});
    view.setCallbacks(tracker.getCallbacks());
    view.activate(true);

    // Deinit (unsubscribe)
    view.deinit();

    // Insert node after deinit
    const user_id = try g.insert("User");
    try g.update(user_id, .{ .name = "Alice", .active = true });

    // Verify no callbacks were called
    try testing.expectEqual(@as(usize, 0), tracker.enters.items.len);
}

test "15.6 Reactive - multiple views independent" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    var tracker1 = CallbackTracker.init(testing.allocator);
    defer tracker1.deinit();

    var tracker2 = CallbackTracker.init(testing.allocator);
    defer tracker2.deinit();

    // View for active users
    var view1 = try g.view(.{
        .root = "User",
        .filter = &.{.{ .field = "active", .value = .{ .bool = true } }},
    }, .{});
    defer view1.deinit();
    view1.setCallbacks(tracker1.getCallbacks());
    view1.activate(true);

    // View for inactive users
    var view2 = try g.view(.{
        .root = "User",
        .filter = &.{.{ .field = "active", .value = .{ .bool = false } }},
    }, .{});
    defer view2.deinit();
    view2.setCallbacks(tracker2.getCallbacks());
    view2.activate(true);

    // Insert active user
    const user1 = try g.insert("User");
    try g.update(user1, .{ .name = "Alice", .active = true });

    // Insert inactive user
    const user2 = try g.insert("User");
    try g.update(user2, .{ .name = "Bob", .active = false });

    // Verify each view got its own events
    try testing.expectEqual(@as(usize, 1), tracker1.enters.items.len);
    try testing.expectEqual(user1, tracker1.enters.items[0].node_id);

    try testing.expectEqual(@as(usize, 1), tracker2.enters.items.len);
    try testing.expectEqual(user2, tracker2.enters.items[0].node_id);
}

// ============================================================================
// Section 16: View Query Tests (using public Graph.view API)
// ============================================================================

test "16.1 View - returns matching items" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const user1 = try g.insert("User");
    try g.update(user1, .{ .name = "Alice", .active = true });

    const user2 = try g.insert("User");
    try g.update(user2, .{ .name = "Bob", .active = false });

    // Query for active users
    var view = try g.view(.{
        .root = "User",
        .filter = &.{.{ .field = "active", .value = .{ .bool = true } }},
    }, .{});
    defer view.deinit();
    view.activate(true);

    // Should only have Alice
    try testing.expectEqual(@as(u32, 1), view.total());
    const items = try collectItems(testing.allocator, &view);
    defer testing.allocator.free(items);
    try testing.expectEqual(user1, items[0]);
}

test "16.2 View - sorting by indexed field" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Insert posts with different views
    const post1 = try g.insert("Post");
    try g.update(post1, .{ .title = "Low", .views = @as(i64, 10) });

    const post2 = try g.insert("Post");
    try g.update(post2, .{ .title = "High", .views = @as(i64, 1000) });

    const post3 = try g.insert("Post");
    try g.update(post3, .{ .title = "Medium", .views = @as(i64, 100) });

    // Query posts sorted by views desc (uses views index)
    var view = try g.view(.{
        .root = "Post",
        .sort = &.{"-views"}, // desc
    }, .{});
    defer view.deinit();
    view.activate(true);

    // Should be sorted: High (1000), Medium (100), Low (10)
    const items = try collectItems(testing.allocator, &view);
    defer testing.allocator.free(items);
    try testing.expectEqual(@as(usize, 3), items.len);
    try testing.expectEqual(post2, items[0]); // High - 1000
    try testing.expectEqual(post3, items[1]); // Medium - 100
    try testing.expectEqual(post1, items[2]); // Low - 10
}

test "16.3 View - reactive insert updates view" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    var view = try g.view(.{
        .root = "User",
        .filter = &.{.{ .field = "active", .value = .{ .bool = true } }},
    }, .{});
    defer view.deinit();
    view.activate(true);

    try testing.expectEqual(@as(u32, 0), view.total());

    // Insert matching user
    const user1 = try g.insert("User");
    try g.update(user1, .{ .name = "Alice", .active = true });

    try testing.expectEqual(@as(u32, 1), view.total());
}

test "16.4 View - reactive delete updates view" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const user1 = try g.insert("User");
    try g.update(user1, .{ .name = "Alice", .active = true });

    var view = try g.view(.{
        .root = "User",
        .filter = &.{.{ .field = "active", .value = .{ .bool = true } }},
    }, .{});
    defer view.deinit();
    view.activate(true);

    try testing.expectEqual(@as(u32, 1), view.total());

    // Delete user
    try g.delete(user1);

    try testing.expectEqual(@as(u32, 0), view.total());
}

test "16.5 View - viewport limits visible items" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Insert 5 users
    for (0..5) |i| {
        const user = try g.insert("User");
        try g.update(user, .{ .name = "User", .age = @as(i64, @intCast(i)), .active = true });
    }

    // Create view with viewport limit of 3
    var view = try g.view(.{
        .root = "User",
        .filter = &.{.{ .field = "active", .value = .{ .bool = true } }},
    }, .{ .limit = 3 });
    defer view.deinit();
    view.activate(false);

    // Total is 5, but viewport shows 3
    try testing.expectEqual(@as(u32, 5), view.total());

    var count: u32 = 0;
    var iter = view.items();
    while (iter.next()) |_| {
        count += 1;
    }
    try testing.expectEqual(@as(u32, 3), count);
}

// ============================================================================
// Section 17: View Compound Query Tests
// ============================================================================

test "17.1 View - compound filter" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Post 1: published=true, views=500 (matches: published AND views > 100)
    const post1 = try g.insert("Post");
    try g.update(post1, .{ .title = "Good", .published = true, .views = @as(i64, 500) });

    // Post 2: published=true, views=50 (no match - views too low)
    _ = try g.insert("Post");
    // post2 has views=0 by default, doesn't match views > 100

    // Post 3: published=false, views=500 (no match - not published)
    const post3 = try g.insert("Post");
    try g.update(post3, .{ .title = "Draft", .published = false, .views = @as(i64, 500) });

    // Query: published=true (uses existing index)
    var view = try g.view(.{
        .root = "Post",
        .filter = &.{
            .{ .field = "published", .value = .{ .bool = true } },
        },
    }, .{});
    defer view.deinit();
    view.activate(true);

    // Only post1 matches
    try testing.expectEqual(@as(u32, 1), view.total());
    const items = try collectItems(testing.allocator, &view);
    defer testing.allocator.free(items);
    try testing.expectEqual(post1, items[0]);
}

test "17.2 View - on_move fires when sort key changes" {
    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Insert posts with different views
    const post1 = try g.insert("Post");
    try g.update(post1, .{ .title = "First", .published = true, .views = @as(i64, 100) });

    const post2 = try g.insert("Post");
    try g.update(post2, .{ .title = "Second", .published = true, .views = @as(i64, 200) });

    var tracker = CallbackTracker.init(testing.allocator);
    defer tracker.deinit();

    // Query published posts sorted by views desc (uses published,views index)
    var view = try g.view(.{
        .root = "Post",
        .filter = &.{
            .{ .field = "published", .value = .{ .bool = true } },
        },
        .sort = &.{"-views"}, // desc
    }, .{});
    defer view.deinit();
    view.setCallbacks(tracker.getCallbacks());
    view.activate(true);

    // Initial order: post2 (200), post1 (100) by views desc
    var items = try collectItems(testing.allocator, &view);
    try testing.expectEqual(post2, items[0]);
    try testing.expectEqual(post1, items[1]);
    testing.allocator.free(items);

    // Update post1's views to be highest
    try g.update(post1, .{ .views = @as(i64, 300) });

    // on_move should have been called
    try testing.expectEqual(@as(usize, 1), tracker.moves.items.len);

    // New order: post1 (300), post2 (200)
    items = try collectItems(testing.allocator, &view);
    defer testing.allocator.free(items);
    try testing.expectEqual(post1, items[0]);
    try testing.expectEqual(post2, items[1]);
}

// ============================================================================
// 17. Edge Unlink on Delete
// ============================================================================

test "17.1 Edge unlink callback fires when linked node is deleted" {
    // This tests the fix for: EdgeCollection cleanup callbacks not triggered
    // when target entity is deleted.
    ensureWatchdog();

    const schema = try createUserPostSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    // Create a User and a Post, link them
    const user = try g.insert("User");
    try g.update(user, .{ .name = "Alice" });

    const post = try g.insert("Post");
    try g.update(post, .{ .title = "Hello" });

    try g.link(user, "posts", post);

    // Track unlink events on the User node
    const UnlinkTracker = struct {
        unlink_count: usize = 0,
        last_edge: ?[]const u8 = null,
        last_target: ?NodeId = null,

        fn onUnlink(ctx: ?*anyopaque, _: NodeId, edge_name: []const u8, target: NodeId) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.unlink_count += 1;
            self.last_edge = edge_name;
            self.last_target = target;
        }
    };

    var tracker: UnlinkTracker = .{};

    try g.watchNode(user, .{
        .on_unlink = UnlinkTracker.onUnlink,
        .context = &tracker,
    });

    // Delete the post - this should trigger on_unlink on the User
    try g.delete(post);

    // Verify unlink callback was fired
    try testing.expectEqual(@as(usize, 1), tracker.unlink_count);
    try testing.expectEqualStrings("posts", tracker.last_edge.?);
    try testing.expectEqual(post, tracker.last_target.?);

    // Clean up
    g.unwatchNode(user);
}
