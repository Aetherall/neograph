///! Node storage and management for the graph database.
///!
///! The NodeStore is the central repository for all nodes, providing
///! CRUD operations and edge management.

const std = @import("std");
const Allocator = std.mem.Allocator;
const node_mod = @import("node.zig");
const Node = node_mod.Node;
const NodeId = node_mod.NodeId;
const TypeId = node_mod.TypeId;
const EdgeId = node_mod.EdgeId;
const EdgeSortContext = node_mod.EdgeSortContext;
const Value = @import("value.zig").Value;
const schema_mod = @import("schema.zig");
const Schema = schema_mod.Schema;
const EdgeSortDef = schema_mod.EdgeSortDef;

/// Errors that can occur during node operations.
pub const NodeStoreError = error{
    NodeNotFound,
    EdgeTargetNotFound,
    UnknownType,
    UnknownEdge,
    InvalidPropertyType,
    OutOfMemory,
};

/// Central storage for all nodes in the database.
pub const NodeStore = struct {
    nodes: std.AutoHashMapUnmanaged(NodeId, *Node),
    next_id: NodeId,
    schema: *const Schema,
    allocator: Allocator,

    const Self = @This();

    /// Type-erased node getter for EdgeSortContext.
    /// This function is used as the get_node_fn in EdgeSortContext.
    fn getNodeForSort(store_ptr: *const anyopaque, id: NodeId) ?*const Node {
        const self: *const Self = @ptrCast(@alignCast(store_ptr));
        return self.nodes.get(id);
    }

    /// Create an EdgeSortContext for the given sort definition.
    pub fn createSortContext(self: *const Self, sort_def: EdgeSortDef) EdgeSortContext {
        return .{
            .store_ptr = self,
            .get_node_fn = getNodeForSort,
            .property = sort_def.property,
            .direction = sort_def.direction,
        };
    }

    /// Initialize an empty node store.
    pub fn init(allocator: Allocator, schema: *const Schema) Self {
        return .{
            .nodes = .{},
            .next_id = 1, // Start at 1, 0 can be used as "no node"
            .schema = schema,
            .allocator = allocator,
        };
    }

    /// Free all nodes and the store itself.
    pub fn deinit(self: *Self) void {
        var iter = self.nodes.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.nodes.deinit(self.allocator);
    }

    /// Insert a new node with the given type and properties.
    /// Returns the new node's id.
    pub fn insert(self: *Self, type_name: []const u8) NodeStoreError!NodeId {
        const type_def = self.schema.getType(type_name) orelse return NodeStoreError.UnknownType;

        const id = self.next_id;
        self.next_id += 1;

        const node = self.allocator.create(Node) catch return NodeStoreError.OutOfMemory;
        node.* = Node.init(self.allocator, id, type_def.id);

        self.nodes.put(self.allocator, id, node) catch {
            node.deinit();
            self.allocator.destroy(node);
            return NodeStoreError.OutOfMemory;
        };

        return id;
    }

    /// Get a node by id.
    pub fn get(self: *const Self, id: NodeId) ?*Node {
        return self.nodes.get(id);
    }

    /// Update a node's properties (merge semantics).
    /// Properties set to null are removed.
    pub fn update(self: *Self, id: NodeId, props: anytype) NodeStoreError!void {
        const node = self.nodes.get(id) orelse return NodeStoreError.NodeNotFound;

        const fields = @typeInfo(@TypeOf(props)).@"struct".fields;
        inline for (fields) |field| {
            const value = @field(props, field.name);
            const actual_value = if (@typeInfo(field.type) == .optional)
                value
            else
                @as(?@TypeOf(value), value);

            node.setProperty(field.name, if (actual_value) |v| valueFromAny(v) else null) catch return NodeStoreError.OutOfMemory;
        }
    }

    /// Delete a node and all its edges.
    pub fn delete(self: *Self, id: NodeId) NodeStoreError!void {
        const node = self.nodes.get(id) orelse return NodeStoreError.NodeNotFound;

        // Remove all edges pointing to this node
        self.unlinkAll(id) catch {};

        // Remove from store and free
        _ = self.nodes.remove(id);
        node.deinit();
        self.allocator.destroy(node);
    }

    /// Create a link between two nodes.
    /// Creates both forward and reverse edges.
    /// If the edge has a sort specification, targets are stored in property-sorted order.
    pub fn link(self: *Self, source_id: NodeId, edge_name: []const u8, target_id: NodeId) NodeStoreError!void {
        const source = self.nodes.get(source_id) orelse return NodeStoreError.NodeNotFound;
        const target = self.nodes.get(target_id) orelse return NodeStoreError.EdgeTargetNotFound;

        const edge_def = self.schema.getEdgeDef(source.type_id, edge_name) orelse return NodeStoreError.UnknownEdge;

        // Build sort context for forward edge if edge has sort specification
        const forward_sort_ctx: ?EdgeSortContext = if (edge_def.sort) |sort| self.createSortContext(sort) else null;

        // Add forward edge with sort context
        source.addEdgeWithSort(edge_def.id, target_id, forward_sort_ctx) catch return NodeStoreError.OutOfMemory;

        // Get reverse edge definition to check for its sort specification
        const target_type = self.schema.getTypeById(edge_def.target_type_id) orelse return NodeStoreError.UnknownEdge;
        var reverse_sort_ctx: ?EdgeSortContext = null;
        for (target_type.edges) |e| {
            if (e.id == edge_def.reverse_edge_id) {
                if (e.sort) |sort| {
                    reverse_sort_ctx = self.createSortContext(sort);
                }
                break;
            }
        }

        // Add reverse edge with sort context
        target.addEdgeWithSort(edge_def.reverse_edge_id, source_id, reverse_sort_ctx) catch return NodeStoreError.OutOfMemory;
    }

    /// Remove a link between two nodes.
    /// Removes both forward and reverse edges.
    pub fn unlink(self: *Self, source_id: NodeId, edge_name: []const u8, target_id: NodeId) NodeStoreError!void {
        const source = self.nodes.get(source_id) orelse return NodeStoreError.NodeNotFound;
        const target = self.nodes.get(target_id) orelse return;

        const edge_def = self.schema.getEdgeDef(source.type_id, edge_name) orelse return NodeStoreError.UnknownEdge;

        // Remove forward edge
        _ = source.removeEdge(edge_def.id, target_id);

        // Remove reverse edge
        _ = target.removeEdge(edge_def.reverse_edge_id, source_id);
    }

    /// Remove all edges to and from a node.
    pub fn unlinkAll(self: *Self, id: NodeId) NodeStoreError!void {
        const node = self.nodes.get(id) orelse return NodeStoreError.NodeNotFound;

        // For each edge in this node
        var edge_iter = node.edges.iterator();
        while (edge_iter.next()) |entry| {
            const edge_id = entry.key_ptr.*;
            const targets = entry.value_ptr;

            // Find the reverse edge id
            // We need to look up the edge definition to get the reverse edge
            const type_def = self.schema.getTypeById(node.type_id) orelse continue;
            for (type_def.edges) |edge_def| {
                if (edge_def.id == edge_id) {
                    // Remove reverse edges from all targets
                    for (targets.items()) |target_id| {
                        if (self.nodes.get(target_id)) |target_node| {
                            _ = target_node.removeEdge(edge_def.reverse_edge_id, id);
                        }
                    }
                    break;
                }
            }
        }
    }

    /// Get the count of all nodes.
    pub fn count(self: *const Self) usize {
        return self.nodes.count();
    }

    /// Convert any type to a Value.
    fn valueFromAny(v: anytype) Value {
        const T = @TypeOf(v);
        if (T == Value) return v;
        if (T == []const u8) return .{ .string = v };
        if (T == i64) return .{ .int = v };
        if (T == f64) return .{ .number = v };
        if (T == bool) return .{ .bool = v };
        if (@typeInfo(T) == .comptime_int) return .{ .int = @intCast(v) };
        if (@typeInfo(T) == .comptime_float) return .{ .number = @floatCast(v) };
        // Handle string literals (*const [N:0]u8)
        if (@typeInfo(T) == .pointer) {
            const child = @typeInfo(T).pointer.child;
            if (@typeInfo(child) == .array) {
                const arr_info = @typeInfo(child).array;
                if (arr_info.child == u8) {
                    return .{ .string = v };
                }
            }
        }
        @compileError("Unsupported value type: " ++ @typeName(T));
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;
const parseSchema = @import("json.zig").parseSchema;

fn createTestSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "User",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "age", "type": "int" }
        \\      ],
        \\      "edges": [{ "name": "posts", "target": "Post", "reverse": "author" }]
        \\    },
        \\    {
        \\      "name": "Post",
        \\      "properties": [{ "name": "title", "type": "string" }],
        \\      "edges": [{ "name": "author", "target": "User", "reverse": "posts" }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

test "NodeStore insert and get" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    defer store.deinit();

    const id1 = try store.insert("User");
    const id2 = try store.insert("User");
    const id3 = try store.insert("Post");

    try testing.expectEqual(@as(NodeId, 1), id1);
    try testing.expectEqual(@as(NodeId, 2), id2);
    try testing.expectEqual(@as(NodeId, 3), id3);

    try testing.expect(store.get(id1) != null);
    try testing.expect(store.get(id2) != null);
    try testing.expect(store.get(99) == null);
}

test "NodeStore insert unknown type fails" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    defer store.deinit();

    const insert_result = store.insert("Unknown");
    try testing.expectError(NodeStoreError.UnknownType, insert_result);
}

test "NodeStore update" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    defer store.deinit();

    const id = try store.insert("User");
    try store.update(id, .{ .name = "Alice", .age = @as(i64, 30) });

    const node = store.get(id).?;
    try testing.expectEqualStrings("Alice", node.getProperty("name").?.string);
    try testing.expectEqual(@as(i64, 30), node.getProperty("age").?.int);
}

test "NodeStore delete" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    defer store.deinit();

    const id = try store.insert("User");
    try testing.expect(store.get(id) != null);

    try store.delete(id);
    try testing.expect(store.get(id) == null);
}

test "NodeStore link creates bidirectional edges" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    defer store.deinit();

    const user_id = try store.insert("User");
    const post_id = try store.insert("Post");

    try store.link(post_id, "author", user_id);

    // Check forward edge: post -> author -> user
    const post = store.get(post_id).?;
    const author_edge_def = schema.getEdgeDef(post.type_id, "author").?;
    const post_targets = post.getEdgeTargets(author_edge_def.id);
    try testing.expectEqual(@as(usize, 1), post_targets.len);
    try testing.expectEqual(user_id, post_targets[0]);

    // Check reverse edge: user -> posts -> post
    const user = store.get(user_id).?;
    const posts_edge_def = schema.getEdgeDef(user.type_id, "posts").?;
    const user_targets = user.getEdgeTargets(posts_edge_def.id);
    try testing.expectEqual(@as(usize, 1), user_targets.len);
    try testing.expectEqual(post_id, user_targets[0]);
}

test "NodeStore unlink removes bidirectional edges" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    defer store.deinit();

    const user_id = try store.insert("User");
    const post_id = try store.insert("Post");

    try store.link(post_id, "author", user_id);
    try store.unlink(post_id, "author", user_id);

    // Check forward edge removed
    const post = store.get(post_id).?;
    const author_edge_def = schema.getEdgeDef(post.type_id, "author").?;
    try testing.expectEqual(@as(usize, 0), post.getEdgeTargets(author_edge_def.id).len);

    // Check reverse edge removed
    const user = store.get(user_id).?;
    const posts_edge_def = schema.getEdgeDef(user.type_id, "posts").?;
    try testing.expectEqual(@as(usize, 0), user.getEdgeTargets(posts_edge_def.id).len);
}

test "NodeStore delete cascades edges" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    defer store.deinit();

    const user_id = try store.insert("User");
    const post_id = try store.insert("Post");

    try store.link(post_id, "author", user_id);

    // Delete user - should remove edge from post
    try store.delete(user_id);

    // Check post no longer has author edge
    const post = store.get(post_id).?;
    const author_edge_def = schema.getEdgeDef(post.type_id, "author").?;
    try testing.expectEqual(@as(usize, 0), post.getEdgeTargets(author_edge_def.id).len);
}

test "NodeStore count" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    defer store.deinit();

    try testing.expectEqual(@as(usize, 0), store.count());

    _ = try store.insert("User");
    _ = try store.insert("User");
    try testing.expectEqual(@as(usize, 2), store.count());

    try store.delete(1);
    try testing.expectEqual(@as(usize, 1), store.count());
}
