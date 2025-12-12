///! Query Builder - constructs Query from structured data (JSON/Lua tables).
///!
///! JSON format:
///! {
///!   "root": "Thread",
///!   "sort": ["tid", "-name"],  // "-" prefix = descending
///!   "edges": {
///!     "frames": {
///!       "sort": ["line"],
///!       "edges": { ... }
///!     }
///!   }
///! }

const std = @import("std");
const Allocator = std.mem.Allocator;
const Value = @import("../value.zig").Value;
const SortDir = @import("../schema.zig").SortDir;
const FieldKind = @import("../schema.zig").FieldKind;
const TypeId = @import("../node.zig").TypeId;

// ============================================================================
// Query Types
// ============================================================================

/// Filter operation.
pub const FilterOp = enum {
    eq,
    neq,
    gt,
    gte,
    lt,
    lte,
    in,
};

/// A filter condition.
pub const Filter = struct {
    path: []const []const u8, // ["field"] or ["edge", "field"]
    op: FilterOp,
    value: Value,
    values: ?[]const Value = null, // for 'in' op
    kind: FieldKind = .property,

    pub fn fieldName(self: *const Filter) []const u8 {
        return self.path[self.path.len - 1];
    }
};

/// Sort specification.
pub const Sort = struct {
    field: []const u8,
    direction: SortDir,
};

/// Selection of an edge with nested query.
pub const EdgeSelection = struct {
    name: []const u8,
    recursive: bool = false,
    virtual: bool = false, // If true, nodes at this level are inlined (not shown as items)
    filters: []const Filter = &.{},
    sorts: []const Sort = &.{},
    selections: []const EdgeSelection = &.{},
};

/// A complete query.
pub const Query = struct {
    root_type: []const u8,
    root_type_id: TypeId = 0, // resolved during validation
    root_id: ?u64 = null, // Direct node ID lookup (bypasses index/filters)
    virtual: bool = false, // If true, root nodes are inlined (not shown as items)
    filters: []const Filter = &.{},
    sorts: []const Sort = &.{},
    selections: []const EdgeSelection = &.{},
    owns_strings: bool = false, // True if this query owns its string allocations (from parseQuery)

    /// Free all query allocations.
    /// Only frees strings if owns_strings is true (queries from parseQuery).
    pub fn deinit(self: *Query, allocator: Allocator) void {
        if (self.owns_strings) {
            // Free root_type string (duplicated from JSON)
            allocator.free(self.root_type);

            // Free filters and their path strings
            freeFiltersWithStrings(allocator, self.filters);
            allocator.free(self.filters);

            // Free sort field names
            for (self.sorts) |s| allocator.free(s.field);
            allocator.free(self.sorts);

            // Free edge selections recursively (with strings)
            freeEdgeSelectionsWithStrings(allocator, self.selections);
            allocator.free(self.selections);
        } else {
            // Just free the slices, not the string contents
            freeFilters(allocator, self.filters);
            allocator.free(self.filters);
            allocator.free(self.sorts);
            freeEdgeSelections(allocator, self.selections);
            allocator.free(self.selections);
        }
    }
};

/// Free filters without freeing string contents (for QueryBuilder-created queries)
fn freeFilters(allocator: Allocator, filters: []const Filter) void {
    for (filters) |f| {
        allocator.free(f.path);
        if (f.values) |vals| {
            allocator.free(vals);
        }
    }
}

/// Free filters including string contents (for parseQuery-created queries)
fn freeFiltersWithStrings(allocator: Allocator, filters: []const Filter) void {
    for (filters) |f| {
        for (f.path) |p| allocator.free(p);
        allocator.free(f.path);
        if (f.values) |vals| {
            allocator.free(vals);
        }
    }
}

/// Free edge selections without freeing string contents (for QueryBuilder-created queries)
fn freeEdgeSelections(allocator: Allocator, selections: []const EdgeSelection) void {
    for (selections) |e| {
        freeFilters(allocator, e.filters);
        allocator.free(e.filters);
        allocator.free(e.sorts);
        freeEdgeSelections(allocator, e.selections);
        allocator.free(e.selections);
    }
}

/// Free edge selections including string contents (for parseQuery-created queries)
fn freeEdgeSelectionsWithStrings(allocator: Allocator, selections: []const EdgeSelection) void {
    for (selections) |e| {
        freeFiltersWithStrings(allocator, e.filters);
        allocator.free(e.filters);

        for (e.sorts) |s| allocator.free(s.field);
        allocator.free(e.sorts);

        freeEdgeSelectionsWithStrings(allocator, e.selections);
        allocator.free(e.selections);

        allocator.free(e.name);
    }
}

// ============================================================================
// Builder
// ============================================================================

pub const BuildError = error{
    MissingRoot,
    InvalidSort,
    InvalidEdge,
    InvalidFilter,
    OutOfMemory,
};

/// Builder for constructing Query objects programmatically.
pub const QueryBuilder = struct {
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{ .allocator = allocator };
    }

    /// Build a Query from structured input.
    /// Caller owns the returned Query and must call deinit on it.
    pub fn build(self: *Self, input: QueryInput) BuildError!Query {
        const filters = try self.buildFilters(input.filter);
        errdefer {
            self.freeFilters(filters);
            self.allocator.free(filters);
        }

        const sorts = try self.buildSorts(input.sort);
        errdefer self.allocator.free(sorts);

        const selections = try self.buildEdgeSelections(input.edges);
        errdefer {
            for (selections) |sel| {
                self.freeEdgeSelection(sel);
            }
            self.allocator.free(selections);
        }

        return Query{
            .root_type = input.root,
            .virtual = input.virtual,
            .filters = filters,
            .sorts = sorts,
            .selections = selections,
        };
    }

    fn buildSorts(self: *Self, sort_fields: []const []const u8) BuildError![]const Sort {
        if (sort_fields.len == 0) return &.{};

        const sorts = self.allocator.alloc(Sort, sort_fields.len) catch return BuildError.OutOfMemory;
        errdefer self.allocator.free(sorts);

        for (sort_fields, 0..) |field, i| {
            if (field.len == 0) return BuildError.InvalidSort;

            if (field[0] == '-') {
                if (field.len < 2) return BuildError.InvalidSort;
                sorts[i] = .{
                    .field = field[1..],
                    .direction = .desc,
                };
            } else {
                sorts[i] = .{
                    .field = field,
                    .direction = .asc,
                };
            }
        }

        return sorts;
    }

    fn buildFilters(self: *Self, filter_inputs: []const FilterInput) BuildError![]const Filter {
        if (filter_inputs.len == 0) return &.{};

        const filters = self.allocator.alloc(Filter, filter_inputs.len) catch return BuildError.OutOfMemory;
        errdefer self.allocator.free(filters);

        for (filter_inputs, 0..) |input, i| {
            filters[i] = try self.buildFilter(input);
        }

        return filters;
    }

    fn buildFilter(self: *Self, input: FilterInput) BuildError!Filter {
        // Build path array
        const path = self.allocator.alloc([]const u8, 1) catch return BuildError.OutOfMemory;
        path[0] = input.field;

        // Build values array for 'in' operator
        var values: ?[]const Value = null;
        if (input.op == .in) {
            if (input.values) |vals| {
                values = self.allocator.dupe(Value, vals) catch return BuildError.OutOfMemory;
            }
        }

        return Filter{
            .path = path,
            .op = input.op,
            .value = input.value,
            .values = values,
        };
    }

    fn freeFilters(self: *Self, filters: []const Filter) void {
        for (filters) |f| {
            self.allocator.free(f.path);
            if (f.values) |vals| {
                self.allocator.free(vals);
            }
        }
    }

    fn buildEdgeSelections(self: *Self, edges: []const EdgeInput) BuildError![]const EdgeSelection {
        if (edges.len == 0) return &.{};

        const selections = self.allocator.alloc(EdgeSelection, edges.len) catch return BuildError.OutOfMemory;
        errdefer self.allocator.free(selections);

        for (edges, 0..) |edge, i| {
            selections[i] = try self.buildEdgeSelection(edge);
        }

        return selections;
    }

    fn buildEdgeSelection(self: *Self, input: EdgeInput) BuildError!EdgeSelection {
        const filters = try self.buildFilters(input.filter);
        errdefer {
            self.freeFilters(filters);
            self.allocator.free(filters);
        }

        const sorts = try self.buildSorts(input.sort);
        errdefer self.allocator.free(sorts);

        const selections = try self.buildEdgeSelections(input.edges);
        errdefer {
            for (selections) |sel| {
                self.freeEdgeSelection(sel);
            }
            self.allocator.free(selections);
        }

        return EdgeSelection{
            .name = input.name,
            .virtual = input.virtual,
            .recursive = input.recursive,
            .filters = filters,
            .sorts = sorts,
            .selections = selections,
        };
    }

    fn freeEdgeSelection(self: *Self, sel: EdgeSelection) void {
        self.freeFilters(sel.filters);
        self.allocator.free(sel.filters);
        self.allocator.free(sel.sorts);
        for (sel.selections) |s| {
            self.freeEdgeSelection(s);
        }
        self.allocator.free(sel.selections);
    }
};

/// Input structure for a filter condition.
pub const FilterInput = struct {
    field: []const u8,
    op: FilterOp = .eq,
    value: Value = .{ .null = {} },
    values: ?[]const Value = null, // for 'in' op
};

/// Input structure for building a query.
pub const QueryInput = struct {
    root: []const u8,
    virtual: bool = false,
    filter: []const FilterInput = &.{},
    sort: []const []const u8 = &.{},
    edges: []const EdgeInput = &.{},
};

/// Input structure for an edge in the query.
pub const EdgeInput = struct {
    name: []const u8,
    virtual: bool = false,
    recursive: bool = false,
    filter: []const FilterInput = &.{},
    sort: []const []const u8 = &.{},
    edges: []const EdgeInput = &.{},
};

// ============================================================================
// Tests
// ============================================================================

test "QueryBuilder: simple query" {
    var builder = QueryBuilder.init(std.testing.allocator);

    const query = try builder.build(.{
        .root = "User",
        .sort = &.{"name"},
    });
    defer {
        var q = query;
        q.deinit(std.testing.allocator);
    }

    try std.testing.expectEqualStrings("User", query.root_type);
    try std.testing.expectEqual(@as(usize, 1), query.sorts.len);
    try std.testing.expectEqualStrings("name", query.sorts[0].field);
    try std.testing.expectEqual(SortDir.asc, query.sorts[0].direction);
    try std.testing.expectEqual(@as(usize, 0), query.selections.len); // No edges = no selections
}

test "QueryBuilder: descending sort" {
    var builder = QueryBuilder.init(std.testing.allocator);

    const query = try builder.build(.{
        .root = "User",
        .sort = &.{"-created_at"},
    });
    defer {
        var q = query;
        q.deinit(std.testing.allocator);
    }

    try std.testing.expectEqual(SortDir.desc, query.sorts[0].direction);
    try std.testing.expectEqualStrings("created_at", query.sorts[0].field);
}

test "QueryBuilder: nested edges" {
    var builder = QueryBuilder.init(std.testing.allocator);

    const query = try builder.build(.{
        .root = "Thread",
        .sort = &.{"tid"},
        .edges = &.{
            .{
                .name = "frames",
                .sort = &.{"line"},
                .edges = &.{
                    .{
                        .name = "scopes",
                        .sort = &.{"name"},
                    },
                },
            },
        },
    });
    defer {
        var q = query;
        q.deinit(std.testing.allocator);
    }

    try std.testing.expectEqualStrings("Thread", query.root_type);
    try std.testing.expectEqual(@as(usize, 1), query.selections.len);

    // Check frames edge
    const frames = query.selections[0];
    try std.testing.expectEqualStrings("frames", frames.name);
    try std.testing.expectEqual(@as(usize, 1), frames.selections.len);

    // Check scopes edge
    const scopes = frames.selections[0];
    try std.testing.expectEqualStrings("scopes", scopes.name);
}
