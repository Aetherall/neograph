//! JSON parsing for schemas and queries.
//!
//! This module provides JSON-based APIs for defining schemas and queries,
//! making it easier to integrate with other languages (Lua, etc.) by passing
//! simple JSON strings instead of complex nested structures.
//!
//! The JSON format is designed to match our internal structs for direct deserialization.
//!
//! ## Schema JSON Format
//!
//! ```json
//! {
//!   "types": [
//!     {
//!       "name": "User",
//!       "properties": [
//!         { "name": "name", "type": "string" },
//!         { "name": "age", "type": "int" }
//!       ],
//!       "edges": [
//!         {
//!           "name": "posts",
//!           "target": "Post",
//!           "reverse": "author",
//!           "sort": { "property": "createdAt", "direction": "desc" }
//!         }
//!       ],
//!       "indexes": [
//!         { "fields": [{ "field": "age", "direction": "desc" }] }
//!       ],
//!       "rollups": [
//!         { "name": "post_count", "count": "posts" },
//!         { "name": "author_name", "traverse": { "edge": "author", "property": "name" } }
//!       ]
//!     },
//!     {
//!       "name": "Post",
//!       "properties": [
//!         { "name": "title", "type": "string" },
//!         { "name": "createdAt", "type": "int" }
//!       ],
//!       "edges": [{ "name": "author", "target": "User", "reverse": "posts" }]
//!     }
//!   ]
//! }
//! ```
//!
//! ## Query JSON Format
//!
//! ```json
//! {
//!   "root": "User",
//!   "sort": [{ "field": "name", "direction": "asc" }],
//!   "filter": [{ "field": "age", "op": "gt", "value": 18 }],
//!   "edges": [
//!     {
//!       "name": "posts",
//!       "sort": [{ "field": "title", "direction": "asc" }],
//!       "edges": []
//!     }
//!   ]
//! }
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const schema_mod = @import("schema.zig");
const Schema = schema_mod.Schema;
const TypeDef = schema_mod.TypeDef;
const PropertyDef = schema_mod.PropertyDef;
const PropertyType = schema_mod.PropertyType;
const EdgeDef = schema_mod.EdgeDef;
const EdgeSortDef = schema_mod.EdgeSortDef;
const RollupDef = schema_mod.RollupDef;
const RollupKind = schema_mod.RollupKind;
const FirstLastDef = schema_mod.FirstLastDef;
const IndexDef = schema_mod.IndexDef;
const IndexField = schema_mod.IndexField;
const SortDir = schema_mod.SortDir;
const FieldKind = schema_mod.FieldKind;
const StringInterner = @import("string_interner.zig").StringInterner;
const query_mod = @import("query/builder.zig");
const QueryBuilder = query_mod.QueryBuilder;
const Query = query_mod.Query;
const Filter = query_mod.Filter;
const FilterOp = query_mod.FilterOp;
const Sort = query_mod.Sort;
const EdgeSelection = query_mod.EdgeSelection;
const Value = @import("value.zig").Value;

pub const ParseError = error{
    InvalidJson,
    MissingField,
    InvalidType,
    InvalidPropertyType,
    InvalidFilterOp,
    InvalidSortDirection,
    InvalidEdgeDefinition,
    InvalidRollupDefinition,
    InvalidIndexDefinition,
    MissingReverseEdge,
    OutOfMemory,
};

// =============================================================================
// JSON Schema Types (for deserialization)
// =============================================================================

/// JSON-compatible property definition
const JsonPropertyDef = struct {
    name: []const u8,
    type: []const u8,
};

/// JSON-compatible edge sort specification
const JsonEdgeSortDef = struct {
    property: []const u8,
    direction: []const u8 = "asc",
};

/// JSON-compatible edge definition
const JsonEdgeDef = struct {
    name: []const u8,
    target: []const u8,
    reverse: []const u8,
    sort: ?JsonEdgeSortDef = null,
};

/// JSON-compatible traverse rollup
const JsonTraverse = struct {
    edge: []const u8,
    property: []const u8,
};

/// JSON-compatible first/last rollup
const JsonFirstLast = struct {
    edge: []const u8,
    field: []const u8,
    direction: []const u8 = "asc",
    property: ?[]const u8 = null,
};

/// JSON-compatible rollup definition
const JsonRollupDef = struct {
    name: []const u8,
    count: ?[]const u8 = null,
    traverse: ?JsonTraverse = null,
    first: ?JsonFirstLast = null,
    last: ?JsonFirstLast = null,
};

/// JSON-compatible index field
const JsonIndexField = struct {
    field: []const u8,
    direction: []const u8 = "asc",
    kind: []const u8 = "property",
};

/// JSON-compatible index definition
const JsonIndexDef = struct {
    fields: []const JsonIndexField,
};

/// JSON-compatible type definition
const JsonTypeDef = struct {
    name: []const u8,
    properties: []const JsonPropertyDef = &.{},
    edges: []const JsonEdgeDef = &.{},
    indexes: []const JsonIndexDef = &.{},
    rollups: []const JsonRollupDef = &.{},
};

/// JSON-compatible schema
const JsonSchema = struct {
    types: []const JsonTypeDef,
};

// =============================================================================
// JSON Query Types (for deserialization)
// =============================================================================

/// JSON-compatible sort definition
const JsonSort = struct {
    field: []const u8,
    direction: []const u8 = "asc",
};

/// JSON-compatible filter definition
const JsonFilter = struct {
    field: []const u8,
    op: []const u8 = "eq",
    value: ?std.json.Value = null,
};

/// JSON-compatible edge selection (recursive)
const JsonEdgeSelection = struct {
    name: []const u8,
    recursive: bool = false,
    virtual: bool = false,
    sort: []const JsonSort = &.{},
    filter: []const JsonFilter = &.{},
    edges: []const JsonEdgeSelection = &.{},
};

/// JSON-compatible query
const JsonQuery = struct {
    root: []const u8,
    id: ?u64 = null, // Direct node ID lookup (bypasses index)
    virtual: bool = false,
    sort: []const JsonSort = &.{},
    filter: []const JsonFilter = &.{},
    edges: []const JsonEdgeSelection = &.{},
};

// =============================================================================
// Schema Parsing
// =============================================================================

/// Parse a JSON string into a Schema.
/// Caller owns the returned Schema and must call deinit().
pub fn parseSchema(allocator: Allocator, json_str: []const u8) ParseError!Schema {
    const parsed = std.json.parseFromSlice(JsonSchema, allocator, json_str, .{}) catch {
        return ParseError.InvalidJson;
    };
    defer parsed.deinit();

    return convertSchema(allocator, parsed.value);
}

/// Convert a JsonSchema to a Schema.
fn convertSchema(allocator: Allocator, json_schema: JsonSchema) ParseError!Schema {
    var interner = StringInterner.init(allocator);
    errdefer interner.deinit();

    // Allocate types array
    const types = allocator.alloc(TypeDef, json_schema.types.len) catch return ParseError.OutOfMemory;
    errdefer {
        for (types) |t| {
            allocator.free(t.properties);
            allocator.free(t.edges);
            allocator.free(t.rollups);
            for (t.indexes) |idx| {
                allocator.free(idx.fields);
            }
            allocator.free(t.indexes);
        }
        allocator.free(types);
    }

    // First pass: convert all types
    for (json_schema.types, 0..) |jt, i| {
        types[i] = try convertTypeDef(allocator, &interner, jt, @intCast(i));
    }

    // Second pass: resolve edge target types and reverse edges
    for (types) |*t| {
        for (t.edges) |*e| {
            const edge_ptr: *EdgeDef = @constCast(e);

            // Find target type
            var target_found = false;
            for (types, 0..) |*target_type, target_id| {
                if (std.mem.eql(u8, target_type.name, e.target_type_name)) {
                    edge_ptr.target_type_id = @intCast(target_id);

                    // Validate sort property exists on target type
                    if (e.sort) |sort| {
                        if (target_type.getProperty(sort.property) == null) {
                            return ParseError.InvalidEdgeDefinition;
                        }
                    }

                    // Find reverse edge
                    for (target_type.edges) |*rev| {
                        if (std.mem.eql(u8, rev.name, e.reverse_name)) {
                            edge_ptr.reverse_edge_id = rev.id;
                            target_found = true;
                            break;
                        }
                    }
                    break;
                }
            }
            if (!target_found) {
                return ParseError.MissingReverseEdge;
            }
        }
    }

    return Schema{
        .types = types,
        .interner = interner,
        .allocator = allocator,
    };
}

/// Parse a first/last rollup definition.
fn parseFirstLastDef(interner: *StringInterner, fl: JsonFirstLast) ParseError!FirstLastDef {
    const descending = std.mem.eql(u8, fl.direction, "desc");

    return .{
        .edge = interner.intern(fl.edge) catch return ParseError.OutOfMemory,
        .sort = interner.intern(fl.field) catch return ParseError.OutOfMemory,
        .descending = descending,
        .property = if (fl.property) |p| interner.intern(p) catch return ParseError.OutOfMemory else null,
    };
}

fn convertTypeDef(allocator: Allocator, interner: *StringInterner, jt: JsonTypeDef, id: u16) ParseError!TypeDef {
    // Convert properties
    const properties = allocator.alloc(PropertyDef, jt.properties.len) catch return ParseError.OutOfMemory;
    errdefer allocator.free(properties);

    for (jt.properties, 0..) |jp, i| {
        const prop_type = PropertyType.fromString(jp.type) orelse return ParseError.InvalidPropertyType;
        properties[i] = .{
            .name = interner.intern(jp.name) catch return ParseError.OutOfMemory,
            .type = prop_type,
        };
    }

    // Convert edges
    const edges = allocator.alloc(EdgeDef, jt.edges.len) catch return ParseError.OutOfMemory;
    errdefer allocator.free(edges);

    for (jt.edges, 0..) |je, i| {
        // Parse optional sort specification
        const sort_def: ?EdgeSortDef = if (je.sort) |s| .{
            .property = interner.intern(s.property) catch return ParseError.OutOfMemory,
            .direction = if (std.mem.eql(u8, s.direction, "desc")) .desc else .asc,
        } else null;

        edges[i] = .{
            .id = @intCast(i),
            .name = interner.intern(je.name) catch return ParseError.OutOfMemory,
            .target_type_name = interner.intern(je.target) catch return ParseError.OutOfMemory,
            .target_type_id = 0, // Resolved in second pass
            .reverse_name = interner.intern(je.reverse) catch return ParseError.OutOfMemory,
            .reverse_edge_id = 0, // Resolved in second pass
            .sort = sort_def,
        };
    }

    // Convert rollups
    const rollups = allocator.alloc(RollupDef, jt.rollups.len) catch return ParseError.OutOfMemory;
    errdefer allocator.free(rollups);

    for (jt.rollups, 0..) |jr, i| {
        const kind: RollupKind = if (jr.count) |count_edge|
            .{ .count = interner.intern(count_edge) catch return ParseError.OutOfMemory }
        else if (jr.traverse) |trav|
            .{ .traverse = .{
                .edge = interner.intern(trav.edge) catch return ParseError.OutOfMemory,
                .property = interner.intern(trav.property) catch return ParseError.OutOfMemory,
            } }
        else if (jr.first) |fl|
            .{ .first = try parseFirstLastDef(interner, fl) }
        else if (jr.last) |fl|
            .{ .last = try parseFirstLastDef(interner, fl) }
        else
            return ParseError.InvalidRollupDefinition;

        rollups[i] = .{
            .name = interner.intern(jr.name) catch return ParseError.OutOfMemory,
            .kind = kind,
        };
    }

    // Convert indexes
    const indexes = allocator.alloc(IndexDef, jt.indexes.len) catch return ParseError.OutOfMemory;
    errdefer {
        for (indexes) |idx| {
            allocator.free(idx.fields);
        }
        allocator.free(indexes);
    }

    for (jt.indexes, 0..) |ji, i| {
        const fields = allocator.alloc(IndexField, ji.fields.len) catch return ParseError.OutOfMemory;
        for (ji.fields, 0..) |jf, fi| {
            const direction: SortDir = if (std.mem.eql(u8, jf.direction, "desc")) .desc else .asc;
            const kind: FieldKind = if (std.mem.eql(u8, jf.kind, "edge")) .edge else .property;
            fields[fi] = .{
                .name = interner.intern(jf.field) catch return ParseError.OutOfMemory,
                .direction = direction,
                .kind = kind,
            };
        }
        indexes[i] = .{ .fields = fields };
    }

    return .{
        .id = id,
        .name = interner.intern(jt.name) catch return ParseError.OutOfMemory,
        .properties = properties,
        .edges = edges,
        .rollups = rollups,
        .indexes = indexes,
    };
}

// =============================================================================
// Query Parsing
// =============================================================================

/// Parse a JSON string into a Query.
/// Caller owns the returned Query and must call deinit().
pub fn parseQuery(allocator: Allocator, json_str: []const u8) ParseError!Query {
    const parsed = std.json.parseFromSlice(JsonQuery, allocator, json_str, .{}) catch {
        return ParseError.InvalidJson;
    };
    defer parsed.deinit();

    return convertQuery(allocator, parsed.value);
}

/// Helper to duplicate a string for long-term storage.
fn dupeStr(allocator: Allocator, s: []const u8) ParseError![]const u8 {
    return allocator.dupe(u8, s) catch return ParseError.OutOfMemory;
}

fn convertQuery(allocator: Allocator, jq: JsonQuery) ParseError!Query {
    // Duplicate root_type string for long-term storage
    const root_type = try dupeStr(allocator, jq.root);
    errdefer allocator.free(root_type);

    // Convert sorts (with duplicated field names)
    const sorts = allocator.alloc(Sort, jq.sort.len) catch return ParseError.OutOfMemory;
    errdefer {
        for (sorts) |s| allocator.free(s.field);
        allocator.free(sorts);
    }

    for (jq.sort, 0..) |js, i| {
        const direction: SortDir = if (std.mem.eql(u8, js.direction, "desc")) .desc else .asc;
        sorts[i] = .{
            .field = try dupeStr(allocator, js.field),
            .direction = direction,
        };
    }

    // Convert filters
    const filters = allocator.alloc(Filter, jq.filter.len) catch return ParseError.OutOfMemory;
    errdefer allocator.free(filters);

    for (jq.filter, 0..) |jf, i| {
        filters[i] = try convertFilter(allocator, jf);
    }

    // Convert edge selections
    const selections = allocator.alloc(EdgeSelection, jq.edges.len) catch return ParseError.OutOfMemory;
    errdefer {
        for (selections) |s| {
            freeEdgeSelection(allocator, s);
        }
        allocator.free(selections);
    }

    for (jq.edges, 0..) |je, i| {
        selections[i] = try convertEdgeSelection(allocator, je);
    }

    return Query{
        .root_type = root_type,
        .root_type_id = 0, // Resolved when executing against schema
        .root_id = jq.id, // Direct node lookup (bypasses index)
        .virtual = jq.virtual,
        .filters = filters,
        .sorts = sorts,
        .selections = selections,
        .owns_strings = true, // This query owns its string allocations
    };
}

fn convertFilter(allocator: Allocator, jf: JsonFilter) ParseError!Filter {
    const op: FilterOp = if (std.mem.eql(u8, jf.op, "eq"))
        .eq
    else if (std.mem.eql(u8, jf.op, "neq"))
        .neq
    else if (std.mem.eql(u8, jf.op, "gt"))
        .gt
    else if (std.mem.eql(u8, jf.op, "gte"))
        .gte
    else if (std.mem.eql(u8, jf.op, "lt"))
        .lt
    else if (std.mem.eql(u8, jf.op, "lte"))
        .lte
    else if (std.mem.eql(u8, jf.op, "in"))
        .in
    else
        return ParseError.InvalidFilterOp;

    const value = if (jf.value) |v| jsonToValue(allocator, v) else Value{ .null = {} };

    // Create a single-element path (allocated) with duplicated field name
    const path = allocator.alloc([]const u8, 1) catch return ParseError.OutOfMemory;
    errdefer allocator.free(path);
    path[0] = try dupeStr(allocator, jf.field);

    return Filter{
        .path = path,
        .op = op,
        .value = value,
    };
}

fn convertEdgeSelection(allocator: Allocator, je: JsonEdgeSelection) ParseError!EdgeSelection {
    // Duplicate the edge name for long-term storage
    const name = try dupeStr(allocator, je.name);
    errdefer allocator.free(name);

    // Convert sorts (with duplicated field names)
    const sorts = allocator.alloc(Sort, je.sort.len) catch return ParseError.OutOfMemory;
    errdefer {
        for (sorts) |s| allocator.free(s.field);
        allocator.free(sorts);
    }

    for (je.sort, 0..) |js, i| {
        const direction: SortDir = if (std.mem.eql(u8, js.direction, "desc")) .desc else .asc;
        sorts[i] = .{
            .field = try dupeStr(allocator, js.field),
            .direction = direction,
        };
    }

    // Convert filters
    const filters = allocator.alloc(Filter, je.filter.len) catch return ParseError.OutOfMemory;
    errdefer allocator.free(filters);

    for (je.filter, 0..) |jf, i| {
        filters[i] = try convertFilter(allocator, jf);
    }

    // Convert nested edges recursively
    const nested = allocator.alloc(EdgeSelection, je.edges.len) catch return ParseError.OutOfMemory;
    errdefer {
        for (nested) |n| {
            freeEdgeSelection(allocator, n);
        }
        allocator.free(nested);
    }

    for (je.edges, 0..) |ne, i| {
        nested[i] = try convertEdgeSelection(allocator, ne);
    }

    return EdgeSelection{
        .name = name,
        .recursive = je.recursive,
        .virtual = je.virtual,
        .filters = filters,
        .sorts = sorts,
        .selections = nested,
    };
}

fn freeEdgeSelection(allocator: Allocator, sel: EdgeSelection) void {
    // Free nested selections recursively
    for (sel.selections) |s| {
        freeEdgeSelection(allocator, s);
    }
    allocator.free(sel.selections);

    // Free filters and their path strings
    for (sel.filters) |f| {
        for (f.path) |p| allocator.free(p);
        allocator.free(f.path);
    }
    allocator.free(sel.filters);

    // Free sort field names
    for (sel.sorts) |s| allocator.free(s.field);
    allocator.free(sel.sorts);

    // Free the edge name
    allocator.free(sel.name);
}

fn jsonToValue(allocator: Allocator, json_val: std.json.Value) Value {
    return switch (json_val) {
        .null => .{ .null = {} },
        .bool => |b| .{ .bool = b },
        .integer => |i| .{ .int = i },
        .float => |f| .{ .number = f },
        .string => |s| .{ .string = allocator.dupe(u8, s) catch s }, // Try to dupe, fallback to original
        else => .{ .null = {} },
    };
}

// =============================================================================
// Tests
// =============================================================================

test "parse simple schema" {
    const allocator = std.testing.allocator;

    const json =
        \\{
        \\  "types": [
        \\    {
        \\      "name": "User",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "age", "type": "int" }
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    var schema = try parseSchema(allocator, json);
    defer schema.deinit();

    try std.testing.expectEqual(@as(usize, 1), schema.types.len);
    const user_type = schema.getType("User").?;
    try std.testing.expectEqualStrings("User", user_type.name);
    try std.testing.expect(user_type.getProperty("name") == .string);
    try std.testing.expect(user_type.getProperty("age") == .int);
}

test "parse schema with edges" {
    const allocator = std.testing.allocator;

    const json =
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
        \\      "edges": [{ "name": "author", "target": "User", "reverse": "posts" }]
        \\    }
        \\  ]
        \\}
    ;

    var schema = try parseSchema(allocator, json);
    defer schema.deinit();

    try std.testing.expectEqual(@as(usize, 2), schema.types.len);
    const user_type = schema.getType("User").?;
    const post_edge = user_type.getEdge("posts").?;
    try std.testing.expectEqualStrings("Post", post_edge.target_type_name);
    try std.testing.expectEqualStrings("author", post_edge.reverse_name);
    try std.testing.expectEqual(@as(u16, 1), post_edge.target_type_id);
}

test "parse schema with indexes" {
    const allocator = std.testing.allocator;

    const json =
        \\{
        \\  "types": [
        \\    {
        \\      "name": "User",
        \\      "properties": [{ "name": "age", "type": "int" }],
        \\      "indexes": [
        \\        { "fields": [{ "field": "age", "direction": "desc" }] }
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    var schema = try parseSchema(allocator, json);
    defer schema.deinit();

    const user_type = schema.getType("User").?;
    try std.testing.expectEqual(@as(usize, 1), user_type.indexes.len);
    try std.testing.expectEqualStrings("age", user_type.indexes[0].fields[0].name);
    try std.testing.expect(user_type.indexes[0].fields[0].direction == .desc);
}

test "parse schema with rollups" {
    const allocator = std.testing.allocator;

    const json =
        \\{
        \\  "types": [
        \\    {
        \\      "name": "User",
        \\      "properties": [{ "name": "name", "type": "string" }],
        \\      "edges": [{ "name": "posts", "target": "Post", "reverse": "author" }],
        \\      "rollups": [{ "name": "post_count", "count": "posts" }]
        \\    },
        \\    {
        \\      "name": "Post",
        \\      "properties": [{ "name": "title", "type": "string" }],
        \\      "edges": [{ "name": "author", "target": "User", "reverse": "posts" }],
        \\      "rollups": [{ "name": "author_name", "traverse": { "edge": "author", "property": "name" } }]
        \\    }
        \\  ]
        \\}
    ;

    var schema = try parseSchema(allocator, json);
    defer schema.deinit();

    const user_type = schema.getType("User").?;
    try std.testing.expectEqual(@as(usize, 1), user_type.rollups.len);
    try std.testing.expectEqualStrings("post_count", user_type.rollups[0].name);
    try std.testing.expectEqualStrings("posts", user_type.rollups[0].kind.count);

    const post_type = schema.getType("Post").?;
    try std.testing.expectEqualStrings("author_name", post_type.rollups[0].name);
    try std.testing.expectEqualStrings("author", post_type.rollups[0].kind.traverse.edge);
    try std.testing.expectEqualStrings("name", post_type.rollups[0].kind.traverse.property);
}

test "parse simple query" {
    const allocator = std.testing.allocator;

    const json =
        \\{
        \\  "root": "User",
        \\  "sort": [
        \\    { "field": "name", "direction": "asc" },
        \\    { "field": "age", "direction": "desc" }
        \\  ]
        \\}
    ;

    var query = try parseQuery(allocator, json);
    defer query.deinit(allocator);

    try std.testing.expectEqualStrings("User", query.root_type);
    try std.testing.expectEqual(@as(usize, 2), query.sorts.len);
    try std.testing.expectEqualStrings("name", query.sorts[0].field);
    try std.testing.expect(query.sorts[0].direction == .asc);
    try std.testing.expectEqualStrings("age", query.sorts[1].field);
    try std.testing.expect(query.sorts[1].direction == .desc);
}

test "parse query with filters" {
    const allocator = std.testing.allocator;

    const json =
        \\{
        \\  "root": "User",
        \\  "filter": [
        \\    { "field": "age", "op": "gte", "value": 18 }
        \\  ]
        \\}
    ;

    var query = try parseQuery(allocator, json);
    defer query.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), query.filters.len);
    try std.testing.expect(query.filters[0].op == .gte);
    try std.testing.expectEqual(@as(i64, 18), query.filters[0].value.int);
}

test "parse query with edges" {
    const allocator = std.testing.allocator;

    const json =
        \\{
        \\  "root": "User",
        \\  "edges": [
        \\    {
        \\      "name": "posts",
        \\      "sort": [{ "field": "title", "direction": "asc" }]
        \\    }
        \\  ]
        \\}
    ;

    var query = try parseQuery(allocator, json);
    defer query.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), query.selections.len);
    try std.testing.expectEqualStrings("posts", query.selections[0].name);
    try std.testing.expectEqual(@as(usize, 1), query.selections[0].sorts.len);
}
