///! Compound key encoding for B+ tree indexes.
///!
///! Keys are encoded as byte-comparable sequences supporting:
///! - Property values (null, bool, int, number, string)
///! - Edge targets (NodeId)
///! - Sort direction (ascending/descending via bit inversion)

const std = @import("std");
const Order = std.math.Order;
const Value = @import("../value.zig").Value;
const Schema = @import("../schema.zig").Schema;
const SortDir = @import("../schema.zig").SortDir;
const IndexField = @import("../schema.zig").IndexField;
const FieldKind = @import("../schema.zig").FieldKind;
const Node = @import("../node.zig").Node;
const NodeId = @import("../node.zig").NodeId;

/// Maximum size of an encoded compound key
pub const MAX_KEY_SIZE: usize = 255;

/// A compound key encoded as bytes for lexicographic comparison.
/// Encoding ensures that byte-wise comparison yields correct ordering.
pub const CompoundKey = struct {
    buffer: [MAX_KEY_SIZE]u8 = undefined,
    len: u8 = 0,

    const Self = @This();

    /// Compare two compound keys lexicographically.
    pub fn order(self: Self, other: Self) Order {
        const a = self.buffer[0..self.len];
        const b = other.buffer[0..other.len];
        return std.mem.order(u8, a, b);
    }

    /// Check equality between two keys.
    pub fn eql(self: Self, other: Self) bool {
        return self.order(other) == .eq;
    }

    /// Check if this key starts with the given prefix.
    pub fn hasPrefix(self: Self, prefix: Self) bool {
        if (prefix.len > self.len) return false;
        return std.mem.eql(u8, self.buffer[0..prefix.len], prefix.buffer[0..prefix.len]);
    }

    /// Get the encoded bytes as a slice.
    pub fn bytes(self: *const Self) []const u8 {
        return self.buffer[0..self.len];
    }

    // ========================================================================
    // Encoding methods
    // ========================================================================

    /// Encode a complete key from a node using index field definitions.
    pub fn encode(
        schema: *const Schema,
        type_id: u16,
        fields: []const IndexField,
        node: *const Node,
    ) Self {
        var key = Self{};

        for (fields) |field| {
            switch (field.kind) {
                .property => {
                    const value = node.getProperty(field.name) orelse Value{ .null = {} };
                    appendValue(&key, value, field.direction);
                },
                .edge => {
                    // Edge field: encode the first target's NodeId
                    const edge_def = schema.getEdgeDef(type_id, field.name);
                    if (edge_def) |def| {
                        const targets = node.getEdgeTargets(def.id);
                        const target_id: NodeId = if (targets.len > 0) targets[0] else 0;
                        appendNodeId(&key, target_id, field.direction);
                    } else {
                        appendNodeId(&key, 0, field.direction);
                    }
                },
            }
        }

        // Append node ID to ensure key uniqueness
        // Always ascending so nodes with same property values are still findable
        appendNodeId(&key, node.id, .asc);

        return key;
    }

    /// Encode a partial key from values for range bounds.
    pub fn encodePartial(values: []const Value, directions: []const SortDir) Self {
        std.debug.assert(values.len == directions.len);

        var key = Self{};
        for (values, directions) |val, dir| {
            appendValue(&key, val, dir);
        }
        return key;
    }

    /// Encode a key starting with edge target (for nested traversal scans).
    pub fn encodeEdgePrefix(target_id: NodeId, direction: SortDir) Self {
        var key = Self{};
        appendNodeId(&key, target_id, direction);
        return key;
    }

    /// Create a minimum key (empty).
    pub fn minKey() Self {
        return Self{};
    }

    /// Create a maximum key (all 0xFF).
    pub fn maxKey() Self {
        var key = Self{};
        @memset(&key.buffer, 0xFF);
        key.len = MAX_KEY_SIZE;
        return key;
    }

    // ========================================================================
    // Encoding helpers (public for IndexManager bounds computation)
    // ========================================================================

    /// Append a value to the key buffer with sort direction handling.
    pub fn appendValue(key: *Self, value: Value, direction: SortDir) void {
        // Type tag byte (for type ordering: null < bool < int < number < string)
        const tag_byte = value.tagByte();
        appendByte(key, tag_byte, direction);

        switch (value) {
            .null => {}, // No additional bytes
            .bool => |b| appendByte(key, if (b) 1 else 0, direction),
            .int => |i| appendInt64(key, i, direction),
            .number => |n| appendFloat64(key, n, direction),
            .string => |s| appendString(key, s, direction),
        }
    }

    /// Append a single byte with direction handling.
    pub fn appendByte(key: *Self, byte: u8, direction: SortDir) void {
        if (key.len >= MAX_KEY_SIZE) return;
        const encoded = if (direction == .desc) ~byte else byte;
        key.buffer[key.len] = encoded;
        key.len += 1;
    }

    /// Append a NodeId (u64) in big-endian order.
    fn appendNodeId(key: *Self, id: NodeId, direction: SortDir) void {
        appendU64(key, id, direction);
    }

    /// Append a u64 in big-endian order for lexicographic comparison.
    fn appendU64(key: *Self, value: u64, direction: SortDir) void {
        if (key.len + 8 > MAX_KEY_SIZE) return;

        var be_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &be_bytes, value, .big);

        for (be_bytes) |b| {
            appendByte(key, b, direction);
        }
    }

    /// Append i64 with sign handling for correct ordering.
    /// Converts signed to unsigned by flipping the sign bit.
    fn appendInt64(key: *Self, value: i64, direction: SortDir) void {
        // Flip sign bit so negative numbers sort before positive
        const unsigned: u64 = @bitCast(value);
        const flipped = unsigned ^ (1 << 63);
        appendU64(key, flipped, direction);
    }

    /// Append f64 with IEEE 754 encoding for correct ordering.
    fn appendFloat64(key: *Self, value: f64, direction: SortDir) void {
        const bits: u64 = @bitCast(value);

        // For positive floats (including +0): flip sign bit
        // For negative floats: flip all bits
        const encoded = if (bits & (1 << 63) != 0)
            ~bits // negative: flip all bits
        else
            bits ^ (1 << 63); // positive: flip sign bit

        appendU64(key, encoded, direction);
    }

    /// Append a string with null terminator.
    fn appendString(key: *Self, s: []const u8, direction: SortDir) void {
        for (s) |c| {
            if (key.len >= MAX_KEY_SIZE - 1) break;

            // Escape null bytes (use 0x00 0x01 for null, 0x00 0x02 for end)
            if (c == 0) {
                appendByte(key, 0, direction);
                appendByte(key, 1, direction);
            } else {
                appendByte(key, c, direction);
            }
        }
        // String terminator
        appendByte(key, 0, direction);
        appendByte(key, 0, direction);
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "CompoundKey value ordering - same types" {
    // Integers
    const int_neg = CompoundKey.encodePartial(&.{Value{ .int = -100 }}, &.{.asc});
    const int_zero = CompoundKey.encodePartial(&.{Value{ .int = 0 }}, &.{.asc});
    const int_pos = CompoundKey.encodePartial(&.{Value{ .int = 100 }}, &.{.asc});

    try std.testing.expect(int_neg.order(int_zero) == .lt);
    try std.testing.expect(int_zero.order(int_pos) == .lt);
    try std.testing.expect(int_neg.order(int_pos) == .lt);
}

test "CompoundKey value ordering - descending" {
    const int_low = CompoundKey.encodePartial(&.{Value{ .int = 10 }}, &.{.desc});
    const int_high = CompoundKey.encodePartial(&.{Value{ .int = 100 }}, &.{.desc});

    // With descending, higher values should sort first (lower in byte order)
    try std.testing.expect(int_high.order(int_low) == .lt);
}

test "CompoundKey value ordering - cross types" {
    const null_val = CompoundKey.encodePartial(&.{Value{ .null = {} }}, &.{.asc});
    const bool_val = CompoundKey.encodePartial(&.{Value{ .bool = false }}, &.{.asc});
    const int_val = CompoundKey.encodePartial(&.{Value{ .int = 0 }}, &.{.asc});
    const num_val = CompoundKey.encodePartial(&.{Value{ .number = 0.0 }}, &.{.asc});
    const str_val = CompoundKey.encodePartial(&.{Value{ .string = "" }}, &.{.asc});

    try std.testing.expect(null_val.order(bool_val) == .lt);
    try std.testing.expect(bool_val.order(int_val) == .lt);
    try std.testing.expect(int_val.order(num_val) == .lt);
    try std.testing.expect(num_val.order(str_val) == .lt);
}

test "CompoundKey string ordering" {
    const alice = CompoundKey.encodePartial(&.{Value{ .string = "alice" }}, &.{.asc});
    const bob = CompoundKey.encodePartial(&.{Value{ .string = "bob" }}, &.{.asc});
    const empty = CompoundKey.encodePartial(&.{Value{ .string = "" }}, &.{.asc});

    try std.testing.expect(empty.order(alice) == .lt);
    try std.testing.expect(alice.order(bob) == .lt);
}

test "CompoundKey float ordering" {
    const neg_inf = CompoundKey.encodePartial(&.{Value{ .number = -std.math.inf(f64) }}, &.{.asc});
    const neg = CompoundKey.encodePartial(&.{Value{ .number = -1.0 }}, &.{.asc});
    const zero = CompoundKey.encodePartial(&.{Value{ .number = 0.0 }}, &.{.asc});
    const pos = CompoundKey.encodePartial(&.{Value{ .number = 1.0 }}, &.{.asc});
    const pos_inf = CompoundKey.encodePartial(&.{Value{ .number = std.math.inf(f64) }}, &.{.asc});

    try std.testing.expect(neg_inf.order(neg) == .lt);
    try std.testing.expect(neg.order(zero) == .lt);
    try std.testing.expect(zero.order(pos) == .lt);
    try std.testing.expect(pos.order(pos_inf) == .lt);
}

test "CompoundKey compound keys" {
    // Two-field key: (status, views)
    const key1 = CompoundKey.encodePartial(
        &.{ Value{ .string = "active" }, Value{ .int = 100 } },
        &.{ .asc, .desc },
    );
    const key2 = CompoundKey.encodePartial(
        &.{ Value{ .string = "active" }, Value{ .int = 50 } },
        &.{ .asc, .desc },
    );
    const key3 = CompoundKey.encodePartial(
        &.{ Value{ .string = "draft" }, Value{ .int = 200 } },
        &.{ .asc, .desc },
    );

    // Same status, higher views should sort first (desc)
    try std.testing.expect(key1.order(key2) == .lt);
    // Different status
    try std.testing.expect(key1.order(key3) == .lt);
}

test "CompoundKey prefix matching" {
    const full = CompoundKey.encodePartial(
        &.{ Value{ .string = "active" }, Value{ .int = 100 } },
        &.{ .asc, .asc },
    );
    const prefix = CompoundKey.encodePartial(
        &.{Value{ .string = "active" }},
        &.{.asc},
    );

    try std.testing.expect(full.hasPrefix(prefix));
}

test "CompoundKey edge prefix" {
    const prefix = CompoundKey.encodeEdgePrefix(42, .asc);
    try std.testing.expectEqual(@as(u8, 8), prefix.len);
}

test "CompoundKey equality" {
    const key1 = CompoundKey.encodePartial(&.{Value{ .int = 42 }}, &.{.asc});
    const key2 = CompoundKey.encodePartial(&.{Value{ .int = 42 }}, &.{.asc});
    const key3 = CompoundKey.encodePartial(&.{Value{ .int = 43 }}, &.{.asc});

    try std.testing.expect(key1.eql(key2));
    try std.testing.expect(!key1.eql(key3));
}
