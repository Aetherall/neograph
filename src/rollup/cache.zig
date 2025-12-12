///! Rollup cache for computed fields.
///!
///! The cache stores computed rollup values (traverse and count)
///! with invalidation triggered by edge and property changes.
///!
///! Performance optimizations:
///! - Uses InvertedEdgeIndex for O(S) reverse lookups instead of O(N) scans
///! - Where S = sources pointing to a target (typically small)
///! - And N = total cached nodes (can be very large)

const std = @import("std");
const Allocator = std.mem.Allocator;

const Value = @import("../value.zig").Value;
const Schema = @import("../schema.zig").Schema;
const RollupDef = @import("../schema.zig").RollupDef;
const RollupKind = @import("../schema.zig").RollupKind;
const FirstLastDef = @import("../schema.zig").FirstLastDef;
const Node = @import("../node.zig").Node;
const NodeId = @import("../node.zig").NodeId;
const TypeId = @import("../node.zig").TypeId;
const EdgeId = @import("../node.zig").EdgeId;
const NodeStore = @import("../node_store.zig").NodeStore;
const InvertedEdgeIndex = @import("inverted_index.zig").InvertedEdgeIndex;
const IndexManager = @import("../index/index.zig").IndexManager;
const SortDir = @import("../schema.zig").SortDir;
const Sort = @import("../query/builder.zig").Sort;

/// A cached rollup value.
const CachedValue = struct {
    value: Value,
    valid: bool,
};

/// Rollup values for a single node.
const NodeRollups = std.StringHashMapUnmanaged(CachedValue);

/// Cache for computed rollup fields.
pub const RollupCache = struct {
    /// Cache storage: node_id -> (rollup_name -> cached_value)
    cache: std.AutoHashMapUnmanaged(NodeId, NodeRollups),
    /// Inverted edge index for O(S) reverse lookups
    inverted_index: InvertedEdgeIndex,
    schema: *const Schema,
    store: *const NodeStore,
    indexes: *const IndexManager,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, schema: *const Schema, store: *const NodeStore, indexes: *const IndexManager) Self {
        return .{
            .cache = .{},
            .inverted_index = InvertedEdgeIndex.init(allocator),
            .schema = schema,
            .store = store,
            .indexes = indexes,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        var iter = self.cache.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.cache.deinit(self.allocator);
        self.inverted_index.deinit();
    }

    /// Called when an edge is created. Updates the inverted index.
    pub fn onLink(self: *Self, source_id: NodeId, source_type_id: TypeId, edge_id: EdgeId, target_id: NodeId) void {
        self.inverted_index.onLink(source_id, source_type_id, edge_id, target_id);
    }

    /// Called when an edge is removed. Updates the inverted index.
    pub fn onUnlink(self: *Self, source_id: NodeId, source_type_id: TypeId, edge_id: EdgeId, target_id: NodeId) void {
        self.inverted_index.onUnlink(source_id, source_type_id, edge_id, target_id);
    }

    /// Get a rollup value, computing if not cached or invalid.
    pub fn get(self: *Self, node: *const Node, rollup_name: []const u8) !Value {
        // Check cache first
        if (self.cache.getPtr(node.id)) |node_rollups| {
            if (node_rollups.get(rollup_name)) |cached| {
                if (cached.valid) {
                    return cached.value;
                }
            }
        }

        // Compute and cache
        const type_def = self.schema.getTypeById(node.type_id) orelse return Value{ .null = {} };
        const rollup_def = type_def.getRollup(rollup_name) orelse return Value{ .null = {} };

        const value = try self.compute(node, rollup_def);

        // Store in cache
        const result = try self.cache.getOrPut(self.allocator, node.id);
        if (!result.found_existing) {
            result.value_ptr.* = NodeRollups{};
        }
        try result.value_ptr.put(self.allocator, rollup_name, .{ .value = value, .valid = true });

        return value;
    }

    /// Check if a rollup value is currently valid in cache.
    pub fn isValid(self: *const Self, node_id: NodeId, rollup_name: []const u8) bool {
        if (self.cache.get(node_id)) |node_rollups| {
            if (node_rollups.get(rollup_name)) |cached| {
                return cached.valid;
            }
        }
        return false;
    }

    /// Invalidate rollups for a node that depend on a specific edge.
    pub fn invalidate(self: *Self, node_id: NodeId, edge_name: []const u8) void {
        const node = self.store.get(node_id) orelse return;
        const type_def = self.schema.getTypeById(node.type_id) orelse return;

        const node_rollups = self.cache.getPtr(node_id) orelse return;

        // Find and invalidate rollups that depend on this edge
        for (type_def.rollups) |rollup| {
            const depends_on_edge = switch (rollup.kind) {
                .traverse => |t| std.mem.eql(u8, t.edge, edge_name),
                .count => |e| std.mem.eql(u8, e, edge_name),
                .first => |f| std.mem.eql(u8, f.edge, edge_name),
                .last => |f| std.mem.eql(u8, f.edge, edge_name),
            };

            if (depends_on_edge) {
                if (node_rollups.getPtr(rollup.name)) |cached| {
                    cached.valid = false;
                }
            }
        }
    }

    /// Invalidate all rollups for a node.
    pub fn invalidateAll(self: *Self, node_id: NodeId) void {
        if (self.cache.getPtr(node_id)) |node_rollups| {
            var iter = node_rollups.iterator();
            while (iter.next()) |entry| {
                entry.value_ptr.valid = false;
            }
        }
    }

    /// Invalidate traverse rollups that depend on a target node's field (property or rollup).
    /// Called when a target node's property changes, or when a rollup becomes invalid.
    ///
    /// Supports cascading invalidation for recursive rollups:
    /// If Post.author_dept_name depends on User.dept_name, and User.dept_name depends on Dept.name,
    /// then changing Dept.name will cascade: Dept.name -> User.dept_name -> Post.author_dept_name
    ///
    /// Performance: O(S × R) per level, where S = sources pointing to target, R = rollups per type
    pub fn invalidateTraverseDeps(self: *Self, target_id: NodeId, field_name: []const u8) void {
        // Use inverted index for O(S) lookup instead of O(N) scan
        const sources = self.inverted_index.getSourcesFor(target_id);

        for (sources) |ref| {
            // Get the source type's rollups
            const source_type = self.schema.getTypeById(ref.source_type_id) orelse continue;

            // Get edge name from edge_id
            const edge_name = self.schema.getEdgeNameById(ref.source_type_id, ref.edge_id) orelse continue;

            // Find rollups that traverse this edge to the changed field
            for (source_type.rollups) |rollup| {
                const should_invalidate = switch (rollup.kind) {
                    .traverse => |t| std.mem.eql(u8, t.edge, edge_name) and
                        std.mem.eql(u8, t.property, field_name),
                    .first => |f| std.mem.eql(u8, f.edge, edge_name) and
                        (std.mem.eql(u8, f.sort, field_name) or
                        (f.property != null and std.mem.eql(u8, f.property.?, field_name))),
                    .last => |f| std.mem.eql(u8, f.edge, edge_name) and
                        (std.mem.eql(u8, f.sort, field_name) or
                        (f.property != null and std.mem.eql(u8, f.property.?, field_name))),
                    .count => false,
                };

                if (should_invalidate) {
                    // Invalidate this specific cache entry
                    if (self.cache.getPtr(ref.source_id)) |node_rollups| {
                        if (node_rollups.getPtr(rollup.name)) |cached| {
                            cached.valid = false;
                        }
                    }

                    // CASCADE: This rollup just became invalid, so any rollups
                    // that depend on IT also need to be invalidated
                    self.invalidateTraverseDeps(ref.source_id, rollup.name);
                }
            }
        }
    }

    // ========================================================================
    // Eager Computation (Write-time rollups)
    // ========================================================================

    /// Eagerly compute and store all rollups for a node that depend on a specific edge.
    /// Called when an edge is linked/unlinked.
    pub fn recomputeForEdge(self: *Self, node_id: NodeId, edge_name: []const u8) !void {
        const node = self.store.get(node_id) orelse return;
        const type_def = self.schema.getTypeById(node.type_id) orelse return;

        for (type_def.rollups) |*rollup| {
            const depends_on_edge = switch (rollup.kind) {
                .traverse => |t| std.mem.eql(u8, t.edge, edge_name),
                .count => |e| std.mem.eql(u8, e, edge_name),
                .first => |f| std.mem.eql(u8, f.edge, edge_name),
                .last => |f| std.mem.eql(u8, f.edge, edge_name),
            };

            if (depends_on_edge) {
                const value = try self.compute(node, rollup);
                try node.setRollup(rollup.name, value);
            }
        }
    }

    /// Eagerly recompute rollups on source nodes that traverse to a target's field.
    /// Called when a target node's property or rollup value changes.
    /// Cascades to rollups that depend on the recomputed rollups.
    pub fn recomputeTraverseDeps(self: *Self, target_id: NodeId, field_name: []const u8) !void {
        const sources = self.inverted_index.getSourcesFor(target_id);

        for (sources) |ref| {
            const source_type = self.schema.getTypeById(ref.source_type_id) orelse continue;
            const edge_name = self.schema.getEdgeNameById(ref.source_type_id, ref.edge_id) orelse continue;
            const source_node = self.store.get(ref.source_id) orelse continue;

            for (source_type.rollups) |*rollup| {
                const depends_on_field = switch (rollup.kind) {
                    .traverse => |t| std.mem.eql(u8, t.edge, edge_name) and
                        std.mem.eql(u8, t.property, field_name),
                    .first => |f| std.mem.eql(u8, f.edge, edge_name) and
                        (std.mem.eql(u8, f.sort, field_name) or
                        (f.property != null and std.mem.eql(u8, f.property.?, field_name))),
                    .last => |f| std.mem.eql(u8, f.edge, edge_name) and
                        (std.mem.eql(u8, f.sort, field_name) or
                        (f.property != null and std.mem.eql(u8, f.property.?, field_name))),
                    .count => false,
                };

                if (depends_on_field) {
                    const value = try self.compute(source_node, rollup);
                    try source_node.setRollup(rollup.name, value);

                    // CASCADE: This rollup just changed, so recompute rollups that depend on IT
                    try self.recomputeTraverseDeps(ref.source_id, rollup.name);
                }
            }
        }
    }

    /// Initialize all rollups for a newly inserted node.
    /// Called right after a node is inserted.
    pub fn initializeRollups(self: *Self, node_id: NodeId) !void {
        const node = self.store.get(node_id) orelse return;
        const type_def = self.schema.getTypeById(node.type_id) orelse return;

        for (type_def.rollups) |*rollup| {
            const value = try self.compute(node, rollup);
            try node.setRollup(rollup.name, value);
        }
    }

    /// Legacy O(N) implementation - kept for comparison/testing.
    /// Use invalidateTraverseDeps instead.
    pub fn invalidateTraverseDepsSlow(self: *Self, target_id: NodeId, property_name: []const u8) void {
        const target = self.store.get(target_id) orelse return;
        _ = self.schema.getTypeById(target.type_id) orelse return;

        // For each type
        for (self.schema.types) |*source_type| {
            // For each edge in that type
            for (source_type.edges) |edge| {
                if (edge.target_type_id != target.type_id) continue;

                // For each rollup in source type that traverses this edge
                for (source_type.rollups) |rollup| {
                    switch (rollup.kind) {
                        .traverse => |t| {
                            if (std.mem.eql(u8, t.edge, edge.name) and
                                std.mem.eql(u8, t.property, property_name))
                            {
                                // Find nodes with this edge pointing to target
                                self.invalidateNodesWithEdgeTo(source_type.id, edge.id, target_id, rollup.name);
                            }
                        },
                        .count => {},
                    }
                }
            }
        }
    }

    /// Remove all cached values for a node (when deleted).
    /// Also cleans up the inverted index.
    pub fn removeNode(self: *Self, node_id: NodeId) void {
        if (self.cache.fetchRemove(node_id)) |entry| {
            var node_rollups = entry.value;
            node_rollups.deinit(self.allocator);
        }
        // Clean up inverted index - remove as both source and target
        self.inverted_index.removeSource(node_id);
        self.inverted_index.removeTarget(node_id);
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    fn compute(self: *Self, node: *const Node, rollup_def: *const RollupDef) Allocator.Error!Value {
        switch (rollup_def.kind) {
            .traverse => |t| {
                // Get edge definition
                const edge_def = self.schema.getEdgeDef(node.type_id, t.edge) orelse return Value{ .null = {} };

                // Get first target
                const targets = node.getEdgeTargets(edge_def.id);
                if (targets.len == 0) return Value{ .null = {} };

                // Get target node
                const target = self.store.get(targets[0]) orelse return Value{ .null = {} };

                // First try as a property on the target
                if (target.getProperty(t.property)) |val| {
                    return val;
                }

                // Then try as a rollup on the target type (recursive rollup)
                const target_type = self.schema.getTypeById(target.type_id) orelse return Value{ .null = {} };
                if (target_type.getRollup(t.property) != null) {
                    return try self.get(target, t.property);
                }

                return Value{ .null = {} };
            },
            .count => |edge_name| {
                const edge_def = self.schema.getEdgeDef(node.type_id, edge_name) orelse return Value{ .int = 0 };
                const targets = node.getEdgeTargets(edge_def.id);
                return Value{ .int = @intCast(targets.len) };
            },
            .first => |f| return self.computeFirstLast(node, f, true),
            .last => |f| return self.computeFirstLast(node, f, false),
        }
    }

    /// Compute first or last rollup using cross-entity index.
    /// Returns null if no applicable index exists (requires index coverage).
    fn computeFirstLast(self: *Self, node: *const Node, def: FirstLastDef, is_first: bool) Allocator.Error!Value {
        const edge_def = self.schema.getEdgeDef(node.type_id, def.edge) orelse return Value{ .null = {} };

        // Get reverse edge name to find cross-entity index on target type
        const reverse_edge = edge_def.reverse_name;

        // Build sort specification matching the rollup definition
        const sort_dir: SortDir = if (def.descending) .desc else .asc;
        const sorts = [_]Sort{.{ .field = def.sort, .direction = sort_dir }};

        // Find cross-entity index: (reverse_edge, sort_field)
        var coverage = self.indexes.selectNestedIndex(
            edge_def.target_type_id,
            reverse_edge,
            &.{}, // no filters
            &sorts,
        ) orelse return Value{ .null = {} }; // No index = no result
        defer coverage.deinit();

        // Scan index with edge prefix (parent_id)
        var iter = self.indexes.scanWithEdgePrefix(coverage, node.id, &.{});

        // For 'first', take the first entry
        // For 'last', skip to the last entry
        if (!is_first) {
            const count = iter.countRemaining();
            if (count == 0) return Value{ .null = {} };
            iter.skip(count - 1);
        }

        // Get the target node
        const target_id = iter.next() orelse return Value{ .null = {} };
        const target = self.store.get(target_id) orelse return Value{ .null = {} };

        // Return the specified property, or the ID if no property specified
        if (def.property) |prop| {
            return target.getProperty(prop) orelse Value{ .null = {} };
        } else {
            return Value{ .int = @intCast(target.id) };
        }
    }

    fn invalidateNodesWithEdgeTo(
        self: *Self,
        source_type_id: u16,
        edge_id: u16,
        target_id: NodeId,
        rollup_name: []const u8,
    ) void {
        // Scan all cached nodes of this type
        // This is O(n) - in production, maintain an inverted edge index
        var cache_iter = self.cache.iterator();
        while (cache_iter.next()) |entry| {
            const node = self.store.get(entry.key_ptr.*) orelse continue;
            if (node.type_id != source_type_id) continue;

            // Check if this node has edge to target
            const targets = node.getEdgeTargets(edge_id);
            for (targets) |t| {
                if (t == target_id) {
                    if (entry.value_ptr.getPtr(rollup_name)) |cached| {
                        cached.valid = false;
                    }
                    break;
                }
            }
        }
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;
const parseSchema = @import("../json.zig").parseSchema;

fn createTestSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "User",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [{ "name": "posts", "target": "Post", "reverse": "author" }]
        \\    },
        \\    {
        \\      "name": "Post",
        \\      "properties": [{ "name": "title", "type": "string" }],
        \\      "edges": [
        \\        { "name": "author", "target": "User", "reverse": "posts" },
        \\        { "name": "comments", "target": "Comment", "reverse": "post" }
        \\      ],
        \\      "rollups": [
        \\        { "name": "author_name", "traverse": { "edge": "author", "property": "name" } },
        \\        { "name": "comment_count", "count": "comments" }
        \\      ]
        \\    },
        \\    {
        \\      "name": "Comment",
        \\      "properties": [{ "name": "text", "type": "string" }],
        \\      "edges": [{ "name": "post", "target": "Post", "reverse": "comments" }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

test "RollupCache count rollup" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();
    defer store.deinit();

    var cache = RollupCache.init(testing.allocator, &schema, &store, &indexes);
    defer cache.deinit();

    // Create post with no comments
    const post_id = try store.insert("Post");
    const post = store.get(post_id).?;
    try store.update(post_id, .{ .title = "Hello" });

    // Count should be 0
    const count = try cache.get(post, "comment_count");
    try testing.expectEqual(@as(i64, 0), count.int);

    // Add comments
    const c1 = try store.insert("Comment");
    const c2 = try store.insert("Comment");
    try store.link(c1, "post", post_id);
    try store.link(c2, "post", post_id);

    // Invalidate cache
    cache.invalidate(post_id, "comments");

    // Count should be 2
    const count2 = try cache.get(post, "comment_count");
    try testing.expectEqual(@as(i64, 2), count2.int);
}

test "RollupCache traverse rollup" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();
    defer store.deinit();

    var cache = RollupCache.init(testing.allocator, &schema, &store, &indexes);
    defer cache.deinit();

    // Create user
    const user_id = try store.insert("User");
    try store.update(user_id, .{ .name = "Alice" });

    // Create post
    const post_id = try store.insert("Post");
    const post = store.get(post_id).?;

    // No author yet
    const name1 = try cache.get(post, "author_name");
    try testing.expect(name1.isNull());

    // Link author
    try store.link(post_id, "author", user_id);
    cache.invalidate(post_id, "author");

    // Should have author name
    const name2 = try cache.get(post, "author_name");
    try testing.expectEqualStrings("Alice", name2.string);
}

test "RollupCache invalidation" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();
    defer store.deinit();

    var cache = RollupCache.init(testing.allocator, &schema, &store, &indexes);
    defer cache.deinit();

    const post_id = try store.insert("Post");
    const post = store.get(post_id).?;

    // Compute and cache
    _ = try cache.get(post, "comment_count");
    try testing.expect(cache.isValid(post_id, "comment_count"));

    // Invalidate
    cache.invalidate(post_id, "comments");
    try testing.expect(!cache.isValid(post_id, "comment_count"));
}

test "RollupCache invalidateTraverseDeps with inverted index" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();
    defer store.deinit();

    var cache = RollupCache.init(testing.allocator, &schema, &store, &indexes);
    defer cache.deinit();

    // Create user
    const user_id = try store.insert("User");
    try store.update(user_id, .{ .name = "Alice" });

    // Get Post type info for onLink calls
    const post_type_id = schema.getType("Post").?.id;
    const author_edge_id = schema.getEdgeDef(post_type_id, "author").?.id;

    // Create multiple posts pointing to the same user
    var post_ids: [10]NodeId = undefined;
    for (&post_ids) |*post_id| {
        post_id.* = try store.insert("Post");
        try store.update(post_id.*, .{ .title = "Post" });
        try store.link(post_id.*, "author", user_id);

        // Register in inverted index
        cache.onLink(post_id.*, post_type_id, author_edge_id, user_id);
    }

    // Cache author_name for all posts
    for (post_ids) |post_id| {
        const post = store.get(post_id).?;
        _ = try cache.get(post, "author_name");
        try testing.expect(cache.isValid(post_id, "author_name"));
    }

    // Now change user's name - this should invalidate all posts' author_name
    try store.update(user_id, .{ .name = "Bob" });
    cache.invalidateTraverseDeps(user_id, "name");

    // All posts should have invalid author_name cache
    for (post_ids) |post_id| {
        try testing.expect(!cache.isValid(post_id, "author_name"));
    }

    // Re-fetch should get new value
    const post = store.get(post_ids[0]).?;
    const name = try cache.get(post, "author_name");
    try testing.expectEqualStrings("Bob", name.string);

    // Verify inverted index has correct count
    try testing.expectEqual(@as(usize, 10), cache.inverted_index.countSourcesFor(user_id));
}

test "RollupCache onLink and onUnlink maintain inverted index" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();
    defer store.deinit();

    var cache = RollupCache.init(testing.allocator, &schema, &store, &indexes);
    defer cache.deinit();

    // Create user and post
    const user_id = try store.insert("User");
    const user = store.get(user_id).?;
    _ = user;
    try store.update(user_id, .{ .name = "Alice" });

    const post_id = try store.insert("Post");
    const post = store.get(post_id).?;

    // Initially no sources
    try testing.expectEqual(@as(usize, 0), cache.inverted_index.countSourcesFor(user_id));

    // Link post -> user
    try store.link(post_id, "author", user_id);
    const edge_def = schema.getEdgeDef(post.type_id, "author").?;
    cache.onLink(post_id, post.type_id, edge_def.id, user_id);

    // Now should have 1 source
    try testing.expectEqual(@as(usize, 1), cache.inverted_index.countSourcesFor(user_id));

    // Unlink
    try store.unlink(post_id, "author", user_id);
    cache.onUnlink(post_id, post.type_id, edge_def.id, user_id);

    // Back to 0 sources
    try testing.expectEqual(@as(usize, 0), cache.inverted_index.countSourcesFor(user_id));
}

test "RollupCache removeNode cleans up inverted index" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();
    defer store.deinit();

    var cache = RollupCache.init(testing.allocator, &schema, &store, &indexes);
    defer cache.deinit();

    // Create user and posts
    const user_id = try store.insert("User");
    try store.update(user_id, .{ .name = "Alice" });

    const post1_id = try store.insert("Post");
    const post1 = store.get(post1_id).?;
    try store.link(post1_id, "author", user_id);
    const edge_def = schema.getEdgeDef(post1.type_id, "author").?;
    cache.onLink(post1_id, post1.type_id, edge_def.id, user_id);

    const post2_id = try store.insert("Post");
    try store.link(post2_id, "author", user_id);
    cache.onLink(post2_id, post1.type_id, edge_def.id, user_id);

    try testing.expectEqual(@as(usize, 2), cache.inverted_index.countSourcesFor(user_id));

    // Remove post1
    cache.removeNode(post1_id);

    // Should only have 1 source now
    try testing.expectEqual(@as(usize, 1), cache.inverted_index.countSourcesFor(user_id));

    // Remove user (target)
    cache.removeNode(user_id);

    // All references should be cleaned up
    try testing.expectEqual(@as(usize, 0), cache.inverted_index.countSourcesFor(user_id));
}

test "RollupCache performance: invalidateTraverseDeps O(S) vs O(N)" {
    // This test verifies the optimized path is used correctly
    // by checking that invalidation works with the inverted index

    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();
    defer store.deinit();

    var cache = RollupCache.init(testing.allocator, &schema, &store, &indexes);
    defer cache.deinit();

    // Create 1000 users (N = 1000)
    var user_ids: [1000]NodeId = undefined;
    for (&user_ids, 0..) |*uid, i| {
        uid.* = try store.insert("User");
        try store.update(uid.*, .{ .name = "User" });
        _ = i;
    }

    // Create 100 posts pointing to user_ids[0] (S = 100)
    const target_user = user_ids[0];
    var post_ids: [100]NodeId = undefined;
    for (&post_ids) |*pid| {
        pid.* = try store.insert("Post");
        try store.update(pid.*, .{ .title = "Post" });
        try store.link(pid.*, "author", target_user);

        const post = store.get(pid.*).?;
        const edge_def = schema.getEdgeDef(post.type_id, "author").?;
        cache.onLink(pid.*, post.type_id, edge_def.id, target_user);
    }

    // Cache author_name for all posts
    for (post_ids) |pid| {
        const post = store.get(pid).?;
        _ = try cache.get(post, "author_name");
    }

    // The optimized path should only look at S=100 entries, not N=1000
    // We can't easily measure this in a unit test, but we verify correctness
    try store.update(target_user, .{ .name = "NewName" });
    cache.invalidateTraverseDeps(target_user, "name");

    // All 100 posts should be invalidated
    for (post_ids) |pid| {
        try testing.expect(!cache.isValid(pid, "author_name"));
    }

    // Verify total edges tracked
    try testing.expectEqual(@as(usize, 100), cache.inverted_index.totalEdges());
}

// ============================================================================
// Multi-hop Rollup Tests
// ============================================================================

fn createMultiHopSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "Department",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [{ "name": "members", "target": "User", "reverse": "department" }]
        \\    },
        \\    {
        \\      "name": "User",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [
        \\        { "name": "department", "target": "Department", "reverse": "members" },
        \\        { "name": "posts", "target": "Post", "reverse": "author" }
        \\      ],
        \\      "rollups": [{ "name": "dept_name", "traverse": { "edge": "department", "property": "name" } }]
        \\    },
        \\    {
        \\      "name": "Post",
        \\      "properties": [{ "name": "title", "type": "string" }],
        \\      "edges": [{ "name": "author", "target": "User", "reverse": "posts" }],
        \\      "rollups": [
        \\        { "name": "author_name", "traverse": { "edge": "author", "property": "name" } },
        \\        { "name": "author_dept_name", "traverse": { "edge": "author", "property": "dept_name" } }
        \\      ]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

test "Multi-hop rollup computation" {
    var schema = try createMultiHopSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();
    defer store.deinit();

    var cache = RollupCache.init(testing.allocator, &schema, &store, &indexes);
    defer cache.deinit();

    // Create department
    const dept_id = try store.insert("Department");
    try store.update(dept_id, .{ .name = "Engineering" });

    // Create user in department
    const user_id = try store.insert("User");
    try store.update(user_id, .{ .name = "Alice" });
    try store.link(user_id, "department", dept_id);

    // Register edge in inverted index
    const user = store.get(user_id).?;
    const dept_edge = schema.getEdgeDef(user.type_id, "department").?;
    cache.onLink(user_id, user.type_id, dept_edge.id, dept_id);

    // Create post by user
    const post_id = try store.insert("Post");
    try store.update(post_id, .{ .title = "Hello World" });
    try store.link(post_id, "author", user_id);

    // Register edge in inverted index
    const post = store.get(post_id).?;
    const author_edge = schema.getEdgeDef(post.type_id, "author").?;
    cache.onLink(post_id, post.type_id, author_edge.id, user_id);

    // Test single-hop rollup: user.dept_name should be "Engineering"
    const dept_name = try cache.get(user, "dept_name");
    try testing.expectEqualStrings("Engineering", dept_name.string);

    // Test recursive rollup: post.author_dept_name should also be "Engineering"
    const author_dept_name = try cache.get(post, "author_dept_name");
    try testing.expectEqualStrings("Engineering", author_dept_name.string);

    // Also verify the direct property rollup still works
    const author_name = try cache.get(post, "author_name");
    try testing.expectEqualStrings("Alice", author_name.string);
}

test "Multi-hop rollup cascading invalidation" {
    var schema = try createMultiHopSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();
    defer store.deinit();

    var cache = RollupCache.init(testing.allocator, &schema, &store, &indexes);
    defer cache.deinit();

    // Create department
    const dept_id = try store.insert("Department");
    try store.update(dept_id, .{ .name = "Engineering" });

    // Create user in department
    const user_id = try store.insert("User");
    try store.update(user_id, .{ .name = "Alice" });
    try store.link(user_id, "department", dept_id);

    const user = store.get(user_id).?;
    const dept_edge = schema.getEdgeDef(user.type_id, "department").?;
    cache.onLink(user_id, user.type_id, dept_edge.id, dept_id);

    // Create post by user
    const post_id = try store.insert("Post");
    try store.update(post_id, .{ .title = "Hello World" });
    try store.link(post_id, "author", user_id);

    const post = store.get(post_id).?;
    const author_edge = schema.getEdgeDef(post.type_id, "author").?;
    cache.onLink(post_id, post.type_id, author_edge.id, user_id);

    // Cache all rollups
    _ = try cache.get(user, "dept_name");
    _ = try cache.get(post, "author_dept_name");

    // Verify both are cached and valid
    try testing.expect(cache.isValid(user_id, "dept_name"));
    try testing.expect(cache.isValid(post_id, "author_dept_name"));

    // Now change the department name - this should cascade!
    try store.update(dept_id, .{ .name = "Product" });
    cache.invalidateTraverseDeps(dept_id, "name");

    // Both should be invalidated:
    // 1. user.dept_name (directly depends on dept.name)
    // 2. post.author_dept_name (depends on user.dept_name via cascade)
    try testing.expect(!cache.isValid(user_id, "dept_name"));
    try testing.expect(!cache.isValid(post_id, "author_dept_name"));

    // Re-fetch should get new values
    const new_dept_name = try cache.get(user, "dept_name");
    try testing.expectEqualStrings("Product", new_dept_name.string);

    const new_author_dept_name = try cache.get(post, "author_dept_name");
    try testing.expectEqualStrings("Product", new_author_dept_name.string);
}

test "Multi-hop rollup with multiple posts" {
    var schema = try createMultiHopSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();
    defer store.deinit();

    var cache = RollupCache.init(testing.allocator, &schema, &store, &indexes);
    defer cache.deinit();

    // Create department
    const dept_id = try store.insert("Department");
    try store.update(dept_id, .{ .name = "Engineering" });

    // Create user in department
    const user_id = try store.insert("User");
    try store.update(user_id, .{ .name = "Alice" });
    try store.link(user_id, "department", dept_id);

    const user = store.get(user_id).?;
    const dept_edge = schema.getEdgeDef(user.type_id, "department").?;
    cache.onLink(user_id, user.type_id, dept_edge.id, dept_id);

    // Create multiple posts by this user
    var post_ids: [5]NodeId = undefined;
    for (&post_ids) |*pid| {
        pid.* = try store.insert("Post");
        try store.update(pid.*, .{ .title = "Post" });
        try store.link(pid.*, "author", user_id);

        const p = store.get(pid.*).?;
        const author_edge = schema.getEdgeDef(p.type_id, "author").?;
        cache.onLink(pid.*, p.type_id, author_edge.id, user_id);
    }

    // Cache author_dept_name for all posts
    for (post_ids) |pid| {
        const p = store.get(pid).?;
        const val = try cache.get(p, "author_dept_name");
        try testing.expectEqualStrings("Engineering", val.string);
    }

    // Change department name
    try store.update(dept_id, .{ .name = "Sales" });
    cache.invalidateTraverseDeps(dept_id, "name");

    // All posts should be invalidated via cascade
    for (post_ids) |pid| {
        try testing.expect(!cache.isValid(pid, "author_dept_name"));
    }

    // Re-fetch should all get new value
    for (post_ids) |pid| {
        const p = store.get(pid).?;
        const val = try cache.get(p, "author_dept_name");
        try testing.expectEqualStrings("Sales", val.string);
    }
}
