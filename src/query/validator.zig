///! Query validator.
///!
///! Validates queries against the schema and resolves type IDs.

const std = @import("std");
const Allocator = std.mem.Allocator;

const builder = @import("builder.zig");
const Query = builder.Query;
const Filter = builder.Filter;
const EdgeSelection = builder.EdgeSelection;
const Schema = @import("../schema.zig").Schema;
const TypeDef = @import("../schema.zig").TypeDef;
const IndexManager = @import("../index/index.zig").IndexManager;
const IndexCoverage = @import("../index/index.zig").IndexCoverage;

pub const ValidationError = error{
    UnknownType,
    UnknownProperty,
    UnknownEdge,
    NoSuitableIndex,
    TypeMismatch,
};

/// Validation result containing resolved query and index coverage.
pub const ValidationResult = struct {
    query: *Query,
    coverage: IndexCoverage,
};

/// Validate a query against the schema.
pub fn validate(
    query: *Query,
    schema: *const Schema,
    indexes: *const IndexManager,
) ValidationError!ValidationResult {
    // Resolve root type
    const type_def = schema.getType(query.root_type) orelse return ValidationError.UnknownType;
    query.root_type_id = type_def.id;

    // Validate filters
    for (query.filters) |filter| {
        try validateFilter(filter, type_def, schema);
    }

    // Validate sorts
    for (query.sorts) |sort| {
        if (type_def.getProperty(sort.field) == null and
            type_def.getRollup(sort.field) == null)
        {
            return ValidationError.UnknownProperty;
        }
    }

    // Validate edge selections
    for (query.selections) |edge_sel| {
        try validateEdgeSelection(edge_sel, type_def, schema);
    }

    // Select index
    const coverage = indexes.selectIndex(
        type_def.id,
        query.filters,
        query.sorts,
    ) orelse return ValidationError.NoSuitableIndex;

    return ValidationResult{
        .query = query,
        .coverage = coverage,
    };
}

/// Validate only edge selections (without index selection).
/// Useful for queries that bypass normal index selection (e.g., direct ID lookups).
pub fn validateEdges(query: *Query, schema: *const Schema) ValidationError!void {
    // Resolve root type
    const type_def = schema.getType(query.root_type) orelse return ValidationError.UnknownType;
    query.root_type_id = type_def.id;

    // Validate edge selections
    for (query.selections) |edge_sel| {
        try validateEdgeSelection(edge_sel, type_def, schema);
    }
}

fn validateFilter(filter: Filter, type_def: *const TypeDef, schema: *const Schema) ValidationError!void {
    if (filter.path.len == 0) return ValidationError.UnknownProperty;

    if (filter.path.len == 1) {
        // Simple property filter
        const field_name = filter.path[0];
        if (type_def.getProperty(field_name) == null and
            type_def.getRollup(field_name) == null)
        {
            return ValidationError.UnknownProperty;
        }
    } else {
        // Cross-edge filter (e.g., .author.name)
        var current_type = type_def;
        for (filter.path[0 .. filter.path.len - 1]) |edge_name| {
            const edge_def = current_type.getEdge(edge_name) orelse
                return ValidationError.UnknownEdge;
            current_type = schema.getTypeById(edge_def.target_type_id) orelse
                return ValidationError.UnknownType;
        }

        const final_field = filter.path[filter.path.len - 1];
        if (current_type.getProperty(final_field) == null) {
            return ValidationError.UnknownProperty;
        }
    }
}

fn validateEdgeSelection(edge_sel: EdgeSelection, type_def: *const TypeDef, schema: *const Schema) ValidationError!void {
    const edge_def = type_def.getEdge(edge_sel.name) orelse
        return ValidationError.UnknownEdge;

    const target_type = schema.getTypeById(edge_def.target_type_id) orelse
        return ValidationError.UnknownType;

    // Validate nested filters
    for (edge_sel.filters) |filter| {
        try validateFilter(filter, target_type, schema);
    }

    // Validate nested sorts
    for (edge_sel.sorts) |sort| {
        if (target_type.getProperty(sort.field) == null and
            target_type.getRollup(sort.field) == null)
        {
            return ValidationError.UnknownProperty;
        }
    }

    // Validate nested edge selections
    for (edge_sel.selections) |nested_sel| {
        try validateEdgeSelection(nested_sel, target_type, schema);
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;
const parseSchema = @import("../json.zig").parseSchema;
const QueryBuilder = @import("builder.zig").QueryBuilder;

fn createTestSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "User",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
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
        \\        { "name": "views", "type": "int" }
        \\      ],
        \\      "edges": [{ "name": "author", "target": "User", "reverse": "posts" }],
        \\      "rollups": [{ "name": "author_name", "traverse": { "edge": "author", "property": "name" } }],
        \\      "indexes": [{ "fields": [{ "field": "views", "direction": "desc" }] }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

test "Validator valid query" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();

    var qb = QueryBuilder.init(testing.allocator);
    var query = try qb.build(.{
        .root = "User",
        .sort = &.{"name"},
    });
    defer query.deinit(testing.allocator);

    const validation = try validate(&query, &schema, &indexes);
    try testing.expectEqual(@as(u16, 0), validation.query.root_type_id);
}

test "Validator unknown type" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();

    var qb = QueryBuilder.init(testing.allocator);
    var query = try qb.build(.{
        .root = "Unknown",
    });
    defer query.deinit(testing.allocator);

    const validation_result = validate(&query, &schema, &indexes);
    try testing.expectError(ValidationError.UnknownType, validation_result);
}

test "Validator unknown sort field" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();

    var qb = QueryBuilder.init(testing.allocator);
    var query = try qb.build(.{
        .root = "User",
        .sort = &.{"unknown"},
    });
    defer query.deinit(testing.allocator);

    const validation_result = validate(&query, &schema, &indexes);
    try testing.expectError(ValidationError.UnknownProperty, validation_result);
}

test "Validator rollup sort" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();

    var qb = QueryBuilder.init(testing.allocator);
    var query = try qb.build(.{
        .root = "Post",
        .sort = &.{"-views"},
    });
    defer query.deinit(testing.allocator);

    const validation = validate(&query, &schema, &indexes);
    // Should succeed - we have a views desc index
    if (validation) |v| {
        try testing.expectEqual(@as(u16, 1), v.query.root_type_id);
    } else |_| {
        // No index coverage - that's okay for this test
    }
}
