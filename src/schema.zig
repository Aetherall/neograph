///! Schema definition and validation for the graph database.
///!
///! The schema defines types, their properties, edges between types,
///! computed rollup fields, and indexes for query optimization.

const std = @import("std");
const Allocator = std.mem.Allocator;
const StringInterner = @import("string_interner.zig").StringInterner;

/// Property types supported in the schema.
pub const PropertyType = enum {
    string,
    int,
    number,
    bool,

    /// Parse a property type from a string.
    pub fn fromString(s: []const u8) ?PropertyType {
        const map = std.StaticStringMap(PropertyType).initComptime(.{
            .{ "string", .string },
            .{ "int", .int },
            .{ "number", .number },
            .{ "bool", .bool },
        });
        return map.get(s);
    }
};

/// A property definition within a type.
pub const PropertyDef = struct {
    name: []const u8,
    type: PropertyType,
};

/// Sort direction for index fields.
pub const SortDir = enum {
    asc,
    desc,
};

/// Kind of field in an index.
pub const FieldKind = enum {
    property,
    edge,
};

/// A field within an index definition.
pub const IndexField = struct {
    name: []const u8,
    direction: SortDir,
    kind: FieldKind,
};

/// An index definition for a type.
pub const IndexDef = struct {
    fields: []const IndexField,

    /// Check if this index has a field by name.
    pub fn hasField(self: *const IndexDef, name: []const u8) bool {
        for (self.fields) |f| {
            if (std.mem.eql(u8, f.name, name)) return true;
        }
        return false;
    }
};

/// Definition for first/last rollup kinds.
pub const FirstLastDef = struct {
    edge: []const u8,
    sort: []const u8,
    descending: bool,
    property: ?[]const u8,
};

/// Kind of rollup computation.
pub const RollupKind = union(enum) {
    /// Traverse to edge target and copy a property value.
    traverse: struct {
        edge: []const u8,
        property: []const u8,
    },
    /// Count the number of edge targets.
    count: []const u8,
    /// Get the first (highest by sort order) edge target's property.
    first: FirstLastDef,
    /// Get the last (lowest by sort order) edge target's property.
    last: FirstLastDef,
};

/// A computed rollup field definition.
pub const RollupDef = struct {
    name: []const u8,
    kind: RollupKind,
};

/// Sort specification for edge targets.
/// When defined, edge targets are stored sorted by the target's property value.
pub const EdgeSortDef = struct {
    property: []const u8,
    direction: SortDir,
};

/// An edge definition between types.
pub const EdgeDef = struct {
    id: u16,
    name: []const u8,
    target_type_name: []const u8,
    target_type_id: u16,
    reverse_name: []const u8,
    reverse_edge_id: u16,
    sort: ?EdgeSortDef,
};

/// A type definition in the schema.
pub const TypeDef = struct {
    id: u16,
    name: []const u8,
    properties: []const PropertyDef,
    edges: []const EdgeDef,
    rollups: []const RollupDef,
    indexes: []const IndexDef,

    /// Get a property definition by name.
    pub fn getProperty(self: *const TypeDef, name: []const u8) ?PropertyType {
        for (self.properties) |p| {
            if (std.mem.eql(u8, p.name, name)) return p.type;
        }
        return null;
    }

    /// Get an edge definition by name.
    pub fn getEdge(self: *const TypeDef, name: []const u8) ?*const EdgeDef {
        for (self.edges) |*e| {
            if (std.mem.eql(u8, e.name, name)) return e;
        }
        return null;
    }

    /// Get a rollup definition by name.
    pub fn getRollup(self: *const TypeDef, name: []const u8) ?*const RollupDef {
        for (self.rollups) |*r| {
            if (std.mem.eql(u8, r.name, name)) return r;
        }
        return null;
    }

    /// Check if a field name (property, edge, or rollup) exists.
    pub fn hasField(self: *const TypeDef, name: []const u8) bool {
        return self.getProperty(name) != null or
            self.getEdge(name) != null or
            self.getRollup(name) != null;
    }
};

/// Errors that can occur during schema parsing and validation.
pub const SchemaError = error{
    UnknownType,
    UnknownProperty,
    UnknownEdge,
    MissingReverseEdge,
    InvalidPropertyType,
    InvalidEdgeDefinition,
    InvalidRollupDefinition,
    InvalidIndexDefinition,
    DuplicateTypeName,
    DuplicateFieldName,
    OutOfMemory,
};

/// The complete schema for the database.
pub const Schema = struct {
    types: []const TypeDef,
    interner: StringInterner,
    allocator: Allocator,

    const Self = @This();

    /// Initialize an empty schema.
    pub fn init(allocator: Allocator) Self {
        return .{
            .types = &.{},
            .interner = StringInterner.init(allocator),
            .allocator = allocator,
        };
    }

    /// Free all schema resources.
    pub fn deinit(self: *Self) void {
        // Free allocated slices
        for (self.types) |t| {
            self.allocator.free(t.properties);
            self.allocator.free(t.edges);
            self.allocator.free(t.rollups);
            for (t.indexes) |idx| {
                self.allocator.free(idx.fields);
            }
            self.allocator.free(t.indexes);
        }
        self.allocator.free(self.types);
        self.interner.deinit();
    }

    /// Get a type definition by name.
    pub fn getType(self: *const Self, name: []const u8) ?*const TypeDef {
        for (self.types) |*t| {
            if (std.mem.eql(u8, t.name, name)) return t;
        }
        return null;
    }

    /// Get a type definition by id.
    pub fn getTypeById(self: *const Self, id: u16) ?*const TypeDef {
        if (id >= self.types.len) return null;
        return &self.types[id];
    }

    /// Get the type name for a type id.
    pub fn getTypeName(self: *const Self, id: u16) ?[]const u8 {
        if (self.getTypeById(id)) |t| {
            return t.name;
        }
        return null;
    }

    /// Get an edge definition by type id and edge name.
    pub fn getEdgeDef(self: *const Self, type_id: u16, edge_name: []const u8) ?*const EdgeDef {
        if (self.getTypeById(type_id)) |t| {
            return t.getEdge(edge_name);
        }
        return null;
    }

    /// Get the edge name for an edge id within a type.
    pub fn getEdgeNameById(self: *const Self, type_id: u16, edge_id: u16) ?[]const u8 {
        if (self.getTypeById(type_id)) |t| {
            for (t.edges) |e| {
                if (e.id == edge_id) return e.name;
            }
        }
        return null;
    }

    /// Get the target type ID for an edge by name.
    /// Searches all types to find any edge with this name and returns its target type.
    /// Returns null if no edge with this name exists.
    pub fn getEdgeTargetType(self: *const Self, edge_name: []const u8) ?u16 {
        for (self.types) |t| {
            if (t.getEdge(edge_name)) |edge| {
                return edge.target_type_id;
            }
        }
        return null;
    }

};

// ============================================================================
// Unit Tests
// ============================================================================

const parseSchema = @import("json.zig").parseSchema;
const ParseError = @import("json.zig").ParseError;

test "Schema basic type creation" {
    var schema = try parseSchema(std.testing.allocator,
        \\{
        \\  "types": [{
        \\    "name": "User",
        \\    "properties": [
        \\      { "name": "name", "type": "string" },
        \\      { "name": "age", "type": "int" }
        \\    ]
        \\  }]
        \\}
    );
    defer schema.deinit();

    try std.testing.expectEqual(@as(usize, 1), schema.types.len);

    const user = schema.getType("User").?;
    try std.testing.expectEqualStrings("User", user.name);
    try std.testing.expectEqual(@as(usize, 2), user.properties.len);
    try std.testing.expectEqual(PropertyType.string, user.getProperty("name").?);
    try std.testing.expectEqual(PropertyType.int, user.getProperty("age").?);
}

test "Schema bidirectional edges" {
    var schema = try parseSchema(std.testing.allocator,
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
    );
    defer schema.deinit();

    const user = schema.getType("User").?;
    const post = schema.getType("Post").?;

    const posts_edge = user.getEdge("posts").?;
    try std.testing.expectEqualStrings("Post", posts_edge.target_type_name);
    try std.testing.expectEqualStrings("author", posts_edge.reverse_name);

    const author_edge = post.getEdge("author").?;
    try std.testing.expectEqualStrings("User", author_edge.target_type_name);
    try std.testing.expectEqualStrings("posts", author_edge.reverse_name);
}

test "Schema self-referential edge" {
    var schema = try parseSchema(std.testing.allocator,
        \\{
        \\  "types": [{
        \\    "name": "User",
        \\    "properties": [{ "name": "name", "type": "string" }],
        \\    "edges": [{ "name": "friends", "target": "User", "reverse": "friends" }]
        \\  }]
        \\}
    );
    defer schema.deinit();

    const user = schema.getType("User").?;
    const friends_edge = user.getEdge("friends").?;
    try std.testing.expectEqualStrings("User", friends_edge.target_type_name);
    try std.testing.expectEqualStrings("friends", friends_edge.reverse_name);
}

test "Schema rollups" {
    var schema = try parseSchema(std.testing.allocator,
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
    );
    defer schema.deinit();

    const post = schema.getType("Post").?;
    try std.testing.expectEqual(@as(usize, 2), post.rollups.len);

    const author_name = post.getRollup("author_name").?;
    try std.testing.expectEqualStrings("author", author_name.kind.traverse.edge);
    try std.testing.expectEqualStrings("name", author_name.kind.traverse.property);

    const comment_count = post.getRollup("comment_count").?;
    try std.testing.expectEqualStrings("comments", comment_count.kind.count);
}

test "Schema indexes" {
    var schema = try parseSchema(std.testing.allocator,
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
    try std.testing.expectEqual(@as(usize, 1), post.indexes.len);

    const idx = post.indexes[0];
    try std.testing.expectEqual(@as(usize, 2), idx.fields.len);
    try std.testing.expectEqualStrings("status", idx.fields[0].name);
    try std.testing.expectEqual(SortDir.asc, idx.fields[0].direction);
    try std.testing.expectEqualStrings("views", idx.fields[1].name);
    try std.testing.expectEqual(SortDir.desc, idx.fields[1].direction);
}

test "Schema missing reverse edge fails" {
    // Post.author references User.posts, but User doesn't have posts edge
    const result = parseSchema(std.testing.allocator,
        \\{
        \\  "types": [
        \\    { "name": "Post", "edges": [{ "name": "author", "target": "User", "reverse": "posts" }] },
        \\    { "name": "User", "properties": [{ "name": "name", "type": "string" }] }
        \\  ]
        \\}
    );
    try std.testing.expectError(ParseError.MissingReverseEdge, result);
}

test "PropertyType fromString" {
    try std.testing.expectEqual(PropertyType.string, PropertyType.fromString("string").?);
    try std.testing.expectEqual(PropertyType.int, PropertyType.fromString("int").?);
    try std.testing.expectEqual(PropertyType.number, PropertyType.fromString("number").?);
    try std.testing.expectEqual(PropertyType.bool, PropertyType.fromString("bool").?);
    try std.testing.expect(PropertyType.fromString("unknown") == null);
}
