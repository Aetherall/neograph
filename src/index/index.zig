///! Index management for query optimization.
///!
///! Indexes use B+ trees with compound keys to support:
///! - Equality filters on prefix fields
///! - Range filters on one field after equality prefix
///! - Sorted iteration matching index field order

const std = @import("std");
const Allocator = std.mem.Allocator;

const BPlusTree = @import("btree.zig").BPlusTree;
const key_mod = @import("key.zig");
const CompoundKey = key_mod.CompoundKey;
const MAX_KEY_SIZE = key_mod.MAX_KEY_SIZE;
const Schema = @import("../schema.zig").Schema;
const IndexDef = @import("../schema.zig").IndexDef;
const IndexField = @import("../schema.zig").IndexField;
const SortDir = @import("../schema.zig").SortDir;
const FieldKind = @import("../schema.zig").FieldKind;
const TypeId = @import("../node.zig").TypeId;
const NodeId = @import("../node.zig").NodeId;
const Node = @import("../node.zig").Node;
const Value = @import("../value.zig").Value;
const GroupedMap = @import("../ds.zig").GroupedMap;

// Import types from query builder
const builder = @import("../query/builder.zig");
pub const Filter = builder.Filter;
pub const FilterOp = builder.FilterOp;
pub const Sort = builder.Sort;

/// A single index backed by a B+ tree.
pub const Index = struct {
    def: *const IndexDef,
    tree: BPlusTree(CompoundKey, NodeId),
    type_id: TypeId,

    const Self = @This();

    pub fn init(allocator: Allocator, def: *const IndexDef, type_id: TypeId) Self {
        return .{
            .def = def,
            .tree = BPlusTree(CompoundKey, NodeId).init(allocator),
            .type_id = type_id,
        };
    }

    pub fn deinit(self: *Self) void {
        self.tree.deinit();
    }

    /// Insert a node into the index.
    pub fn insert(self: *Self, schema: *const Schema, node: *const Node) !void {
        const key = CompoundKey.encode(schema, self.type_id, self.def.fields, node);
        try self.tree.insert(key, node.id);
    }

    /// Remove a node from the index.
    pub fn remove(self: *Self, schema: *const Schema, node: *const Node) void {
        const key = CompoundKey.encode(schema, self.type_id, self.def.fields, node);
        _ = self.tree.remove(key);
    }

    /// Update a node in the index (remove old key, insert new).
    pub fn update(
        self: *Self,
        schema: *const Schema,
        node: *const Node,
        old_node: *const Node,
    ) !void {
        self.remove(schema, old_node);
        try self.insert(schema, node);
    }
};

/// Analysis of how well an index covers a query.
pub const IndexCoverage = struct {
    index: *const Index,
    equality_prefix: u8, // fields consumed by = filters
    range_field: ?u8, // field used by >, <, >=, <=
    sort_suffix: u8, // fields matching query sort
    score: u32,
    post_filters: []const Filter, // filters not covered by index
    allocator: ?Allocator = null, // set if post_filters was allocated

    /// Free any allocated memory. Call this when done with the coverage.
    pub fn deinit(self: *IndexCoverage) void {
        if (self.allocator) |alloc| {
            if (self.post_filters.len > 0) {
                alloc.free(@constCast(self.post_filters));
            }
        }
    }
};

/// Manages all indexes for the database.
pub const IndexManager = struct {
    indexes_by_type: GroupedMap(TypeId, Index),
    schema: *const Schema,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, schema: *const Schema) !Self {
        var self = Self{
            .indexes_by_type = GroupedMap(TypeId, Index).init(allocator),
            .schema = schema,
            .allocator = allocator,
        };
        errdefer self.deinitIndexes();

        // Create indexes from schema definitions
        for (schema.types, 0..) |*type_def, type_id| {
            for (type_def.indexes) |*idx_def| {
                const index = Index.init(allocator, idx_def, @intCast(type_id));
                try self.indexes_by_type.add(@intCast(type_id), index);
            }
        }

        return self;
    }

    fn deinitIndexes(self: *Self) void {
        var iter = self.indexes_by_type.iterator();
        while (iter.next()) |entry| {
            for (entry.values) |*idx| {
                @constCast(idx).deinit();
            }
        }
    }

    pub fn deinit(self: *Self) void {
        self.deinitIndexes();
        self.indexes_by_type.deinit();
    }

    /// Called when a node is inserted.
    pub fn onInsert(self: *Self, node: *const Node) !void {
        for (self.indexes_by_type.getForKeyMut(node.type_id)) |*idx| {
            try idx.insert(self.schema, node);
        }
    }

    /// Called when a node is updated.
    pub fn onUpdate(self: *Self, node: *const Node, old_node: *const Node) !void {
        for (self.indexes_by_type.getForKeyMut(node.type_id)) |*idx| {
            // Check if any indexed fields changed
            if (self.indexedFieldsChanged(idx.def, node, old_node)) {
                try idx.update(self.schema, node, old_node);
            }
        }
    }

    /// Called when a node is deleted.
    pub fn onDelete(self: *Self, node: *const Node) void {
        for (self.indexes_by_type.getForKeyMut(node.type_id)) |*idx| {
            idx.remove(self.schema, node);
        }
    }

    /// Called when an edge is linked.
    /// Updates indexes that have this edge as a field.
    pub fn onLink(
        self: *Self,
        node: *const Node,
        old_node: *const Node,
        edge_name: []const u8,
    ) !void {
        for (self.indexes_by_type.getForKeyMut(node.type_id)) |*idx| {
            if (self.indexHasEdgeField(idx.def, edge_name)) {
                // Re-index: edge field value may have changed
                try idx.update(self.schema, node, old_node);
            }
        }
    }

    /// Called when an edge is unlinked.
    /// Updates indexes that have this edge as a field.
    pub fn onUnlink(
        self: *Self,
        node: *const Node,
        old_node: *const Node,
        edge_name: []const u8,
    ) !void {
        for (self.indexes_by_type.getForKeyMut(node.type_id)) |*idx| {
            if (self.indexHasEdgeField(idx.def, edge_name)) {
                // Re-index: edge field value may have changed
                try idx.update(self.schema, node, old_node);
            }
        }
    }

    /// Check if an index definition has an edge field with the given name.
    fn indexHasEdgeField(self: *const Self, def: *const IndexDef, edge_name: []const u8) bool {
        _ = self;
        for (def.fields) |field| {
            if (field.kind == .edge and std.mem.eql(u8, field.name, edge_name)) {
                return true;
            }
        }
        return false;
    }

    /// Get any index for a type (for direct node ID lookups).
    /// Returns a minimal coverage that allows scanning all nodes.
    pub fn getAnyIndex(self: *const Self, type_id: TypeId) ?IndexCoverage {
        const indexes = self.indexes_by_type.getForKey(type_id);
        if (indexes.len == 0) return null;

        // Return coverage for first index with no filter/sort optimization
        return IndexCoverage{
            .index = &indexes[0],
            .equality_prefix = 0,
            .range_field = null,
            .sort_suffix = 0,
            .score = 0,
            .post_filters = &.{},
        };
    }

    /// Select the best index for a query.
    pub fn selectIndex(
        self: *const Self,
        type_id: TypeId,
        filters: []const Filter,
        sorts: []const Sort,
    ) ?IndexCoverage {
        const indexes = self.indexes_by_type.getForKey(type_id);
        if (indexes.len == 0) return null;

        var best: ?IndexCoverage = null;

        for (indexes) |*idx| {
            if (self.computeCoverage(idx, filters, sorts)) |coverage| {
                if (best == null or coverage.score > best.?.score) {
                    // Deinit old best if it had allocated memory
                    if (best) |*old| old.deinit();
                    best = coverage;
                } else {
                    // Deinit the new coverage since we're not using it
                    var cov = coverage;
                    cov.deinit();
                }
            }
        }

        return best;
    }

    /// Select index for nested traversal (edge field as first filter).
    pub fn selectNestedIndex(
        self: *const Self,
        target_type_id: TypeId,
        reverse_edge: []const u8,
        filters: []const Filter,
        sorts: []const Sort,
    ) ?IndexCoverage {
        const indexes = self.indexes_by_type.getForKey(target_type_id);
        if (indexes.len == 0) return null;

        var best: ?IndexCoverage = null;

        for (indexes) |*idx| {
            // Index must start with the reverse edge field
            if (idx.def.fields.len == 0) continue;
            const first_field = idx.def.fields[0];
            if (first_field.kind != .edge) continue;
            if (!std.mem.eql(u8, first_field.name, reverse_edge)) continue;

            // Compute coverage for remaining fields
            if (self.computeCoverageFromOffset(idx, 1, filters, sorts)) |coverage| {
                if (best == null or coverage.score > best.?.score) {
                    // Deinit old best if it had allocated memory
                    if (best) |*old| old.deinit();
                    best = coverage;
                } else {
                    // Deinit the new coverage since we're not using it
                    var cov = coverage;
                    cov.deinit();
                }
            }
        }

        return best;
    }

    /// Create an iterator for scanning an index with given bounds.
    pub fn scan(
        self: *const Self,
        coverage: IndexCoverage,
        filters: []const Filter,
    ) ScanIterator {
        const low = self.computeLowBound(coverage, filters);
        const high = self.computeHighBound(coverage, filters);
        return ScanIterator.init(coverage.index, low, high);
    }

    /// Create an iterator with edge prefix for nested scans.
    pub fn scanWithEdgePrefix(
        self: *const Self,
        coverage: IndexCoverage,
        parent_id: NodeId,
        filters: []const Filter,
    ) ScanIterator {
        _ = self;
        _ = filters;
        // Build key starting with edge prefix
        const prefix = CompoundKey.encodeEdgePrefix(parent_id, coverage.index.def.fields[0].direction);
        return ScanIterator.initPrefixScan(coverage.index, prefix);
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    fn indexedFieldsChanged(self: *const Self, def: *const IndexDef, node: *const Node, old_node: *const Node) bool {
        _ = self;
        for (def.fields) |field| {
            const new_val = node.getProperty(field.name);
            const old_val = old_node.getProperty(field.name);

            const vals_equal = blk: {
                if (new_val == null and old_val == null) break :blk true;
                if (new_val == null or old_val == null) break :blk false;
                break :blk new_val.?.eql(old_val.?);
            };

            if (!vals_equal) return true;
        }
        return false;
    }

    fn computeCoverage(
        self: *const Self,
        index: *const Index,
        filters: []const Filter,
        sorts: []const Sort,
    ) ?IndexCoverage {
        return self.computeCoverageFromOffset(index, 0, filters, sorts);
    }

    fn computeCoverageFromOffset(
        self: *const Self,
        index: *const Index,
        start_offset: usize,
        filters: []const Filter,
        sorts: []const Sort,
    ) ?IndexCoverage {
        const fields = index.def.fields[start_offset..];
        if (fields.len == 0) return null;

        var equality_prefix: u8 = 0;
        var range_field: ?u8 = null;
        var sort_suffix: u8 = 0;

        // Track which filters are covered by the index
        var covered_mask: u64 = 0; // Bitmap of covered filter indices (max 64 filters)

        // Count equality prefix
        for (fields, 0..) |field, i| {
            var found_eq = false;
            for (filters, 0..) |filter, fi| {
                if (filter.op == .eq and std.mem.eql(u8, filter.fieldName(), field.name)) {
                    found_eq = true;
                    if (fi < 64) covered_mask |= (@as(u64, 1) << @intCast(fi));
                    break;
                }
            }
            if (found_eq) {
                equality_prefix = @intCast(i + 1);
            } else {
                // Check for range filter
                for (filters, 0..) |filter, fi| {
                    if ((filter.op == .gt or filter.op == .gte or
                        filter.op == .lt or filter.op == .lte) and
                        std.mem.eql(u8, filter.fieldName(), field.name))
                    {
                        range_field = @intCast(i);
                        if (fi < 64) covered_mask |= (@as(u64, 1) << @intCast(fi));
                        break;
                    }
                }
                break;
            }
        }

        // Count matching sort suffix after equality/range
        const sort_start = equality_prefix + (if (range_field != null) @as(u8, 1) else 0);
        if (sort_start < fields.len) {
            var sort_idx: usize = 0;
            for (fields[sort_start..], 0..) |field, i| {
                if (sort_idx < sorts.len and
                    std.mem.eql(u8, sorts[sort_idx].field, field.name) and
                    sorts[sort_idx].direction == field.direction)
                {
                    sort_suffix = @intCast(i + 1);
                    sort_idx += 1;
                } else {
                    break;
                }
            }
        }

        // Score: 100×equality + 50×range + 10×sort
        const score = @as(u32, equality_prefix) * 100 +
            (if (range_field != null) @as(u32, 50) else 0) +
            @as(u32, sort_suffix) * 10;

        if (score == 0) return null;

        // Compute uncovered filters
        const uncovered = self.computeUncoveredFilters(filters, covered_mask);

        return IndexCoverage{
            .index = index,
            .equality_prefix = equality_prefix,
            .range_field = range_field,
            .sort_suffix = sort_suffix,
            .score = score,
            .post_filters = uncovered.filters,
            .allocator = if (uncovered.allocated) self.allocator else null,
        };
    }

    const UncoveredFiltersResult = struct {
        filters: []const Filter,
        allocated: bool,
    };

    /// Compute filters not covered by the index scan.
    fn computeUncoveredFilters(self: *const Self, filters: []const Filter, covered_mask: u64) UncoveredFiltersResult {
        if (filters.len == 0 or covered_mask == 0) return .{ .filters = filters, .allocated = false };

        // Count uncovered filters
        var uncovered_count: usize = 0;
        for (0..filters.len) |i| {
            if (i >= 64 or (covered_mask & (@as(u64, 1) << @intCast(i))) == 0) {
                uncovered_count += 1;
            }
        }

        // If all filters are covered, return empty slice
        if (uncovered_count == 0) return .{ .filters = &.{}, .allocated = false };

        // If no filters are covered, return original slice
        if (uncovered_count == filters.len) return .{ .filters = filters, .allocated = false };

        // Allocate and populate uncovered filters array
        const post_filters = self.allocator.alloc(Filter, uncovered_count) catch return .{ .filters = filters, .allocated = false };
        var idx: usize = 0;
        for (filters, 0..) |filter, i| {
            if (i >= 64 or (covered_mask & (@as(u64, 1) << @intCast(i))) == 0) {
                post_filters[idx] = filter;
                idx += 1;
            }
        }

        return .{ .filters = post_filters, .allocated = true };
    }

    fn computeLowBound(self: *const Self, coverage: IndexCoverage, filters: []const Filter) CompoundKey {
        _ = self;
        _ = coverage;
        _ = filters;
        // TODO: Build low bound from equality prefix and range lower bound
        // For now, return min key to scan entire index (correct but not optimized)
        return CompoundKey.minKey();
    }

    fn computeHighBound(self: *const Self, coverage: IndexCoverage, filters: []const Filter) CompoundKey {
        _ = self;
        _ = coverage;
        _ = filters;
        // TODO: Build high bound from equality prefix and range upper bound
        // For now, return max key to scan entire index (correct but not optimized)
        return CompoundKey.maxKey();
    }
};

/// Iterator for scanning index entries.
pub const ScanIterator = struct {
    inner: BPlusTree(CompoundKey, NodeId).Iterator,

    pub fn init(index: *const Index, low: CompoundKey, high: CompoundKey) ScanIterator {
        return .{
            .inner = index.tree.range(low, high),
        };
    }

    pub fn initPrefixScan(index: *const Index, prefix: CompoundKey) ScanIterator {
        return .{
            .inner = index.tree.prefixScan(prefix),
        };
    }

    pub fn next(self: *ScanIterator) ?NodeId {
        return self.inner.next();
    }

    pub fn skip(self: *ScanIterator, count: u32) void {
        self.inner.skip(count);
    }

    pub fn countRemaining(self: *ScanIterator) u32 {
        return self.inner.countRemaining();
    }

    /// O(log n) skip to absolute position using B-tree subtree counts.
    /// Position 0 is the first matching element.
    pub fn skipToPosition(self: *ScanIterator, pos: u64) void {
        self.inner.skipToPosition(pos);
    }

    /// O(1) total count from tree root.
    /// Note: This returns the total entries in the underlying B-tree,
    /// which may include entries outside the query range for range scans.
    /// For accurate query counts, use countRemaining() or iterate.
    pub fn totalCount(self: *const ScanIterator) u64 {
        return self.inner.totalCount();
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
    ) catch return error.InvalidJson;
}

test "IndexManager basic operations" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var manager = try IndexManager.init(testing.allocator, &schema);
    defer manager.deinit();

    // Create a node
    var node = Node.init(testing.allocator, 1, 0);
    defer node.deinit();
    try node.setProperty("status", .{ .string = "active" });
    try node.setProperty("views", .{ .int = 100 });

    // Insert into index
    try manager.onInsert(&node);

    // Verify index selection works
    const coverage = manager.selectIndex(0, &.{
        Filter{ .path = &.{"status"}, .op = .eq, .value = .{ .string = "active" } },
    }, &.{});

    try testing.expect(coverage != null);
    try testing.expectEqual(@as(u8, 1), coverage.?.equality_prefix);
}

test "IndexManager update" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var manager = try IndexManager.init(testing.allocator, &schema);
    defer manager.deinit();

    var node = Node.init(testing.allocator, 1, 0);
    defer node.deinit();
    try node.setProperty("status", .{ .string = "draft" });
    try node.setProperty("views", .{ .int = 50 });

    try manager.onInsert(&node);

    // Clone for old state
    var old_node = try node.clone();
    defer old_node.deinit();

    // Update node
    try node.setProperty("status", .{ .string = "active" });
    try manager.onUpdate(&node, &old_node);
}

test "IndexManager coverage scoring" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var manager = try IndexManager.init(testing.allocator, &schema);
    defer manager.deinit();

    // Equality on first field
    const cov1 = manager.selectIndex(0, &.{
        Filter{ .path = &.{"status"}, .op = .eq, .value = .{ .string = "active" } },
    }, &.{});
    try testing.expect(cov1 != null);
    try testing.expectEqual(@as(u32, 100), cov1.?.score);

    // Equality + range
    const cov2 = manager.selectIndex(0, &.{
        Filter{ .path = &.{"status"}, .op = .eq, .value = .{ .string = "active" } },
        Filter{ .path = &.{"views"}, .op = .gt, .value = .{ .int = 100 } },
    }, &.{});
    try testing.expect(cov2 != null);
    try testing.expectEqual(@as(u32, 150), cov2.?.score);

    // Equality + sort
    const cov3 = manager.selectIndex(0, &.{
        Filter{ .path = &.{"status"}, .op = .eq, .value = .{ .string = "active" } },
    }, &.{
        Sort{ .field = "views", .direction = .desc },
    });
    try testing.expect(cov3 != null);
    try testing.expectEqual(@as(u32, 110), cov3.?.score);
}
