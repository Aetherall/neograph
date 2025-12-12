///! Value type representing property values in the graph database.
///!
///! Values can be strings, integers, floats, booleans, or null.
///! They support comparison and ordering for use in indexes.

const std = @import("std");
const math = std.math;
const mem = std.mem;
const Order = math.Order;

/// A property value in the database.
/// Tagged union supporting the core scalar types.
pub const Value = union(Tag) {
    null: void,
    bool: bool,
    int: i64,
    number: f64,
    string: []const u8,

    pub const Tag = enum(u8) {
        null = 0,
        bool = 1,
        int = 2,
        number = 3,
        string = 4,
    };

    /// Compare two values for ordering.
    /// Values of different types are ordered by type tag: null < bool < int < number < string
    pub fn order(a: Value, b: Value) Order {
        const a_tag = @intFromEnum(std.meta.activeTag(a));
        const b_tag = @intFromEnum(std.meta.activeTag(b));

        if (a_tag != b_tag) {
            return math.order(a_tag, b_tag);
        }

        return switch (a) {
            .null => .eq,
            .bool => |av| math.order(@intFromBool(av), @intFromBool(b.bool)),
            .int => |av| math.order(av, b.int),
            .number => |av| orderFloat(av, b.number),
            .string => |av| mem.order(u8, av, b.string),
        };
    }

    /// Check equality between two values.
    pub fn eql(a: Value, b: Value) bool {
        return a.order(b) == .eq;
    }

    /// Format value for display.
    pub fn format(
        self: Value,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        switch (self) {
            .null => try writer.writeAll("null"),
            .bool => |v| try writer.writeAll(if (v) "true" else "false"),
            .int => |v| try writer.print("{d}", .{v}),
            .number => |v| try writer.print("{d}", .{v}),
            .string => |v| try writer.print("\"{s}\"", .{v}),
        }
    }

    /// Get the type tag as an integer for encoding.
    pub fn tagByte(self: Value) u8 {
        return @intFromEnum(std.meta.activeTag(self));
    }

    /// Check if value is null.
    pub fn isNull(self: Value) bool {
        return self == .null;
    }

    /// Try to get as string, returns null if not a string.
    pub fn asString(self: Value) ?[]const u8 {
        return switch (self) {
            .string => |s| s,
            else => null,
        };
    }

    /// Try to get as int, returns null if not an int.
    pub fn asInt(self: Value) ?i64 {
        return switch (self) {
            .int => |i| i,
            else => null,
        };
    }

    /// Try to get as number, returns null if not a number.
    pub fn asNumber(self: Value) ?f64 {
        return switch (self) {
            .number => |n| n,
            else => null,
        };
    }

    /// Try to get as bool, returns null if not a bool.
    pub fn asBool(self: Value) ?bool {
        return switch (self) {
            .bool => |b| b,
            else => null,
        };
    }
};

/// Compare two floats with proper handling of NaN and infinities.
fn orderFloat(a: f64, b: f64) Order {
    // Handle NaN - NaN is considered greater than everything
    const a_nan = math.isNan(a);
    const b_nan = math.isNan(b);

    if (a_nan and b_nan) return .eq;
    if (a_nan) return .gt;
    if (b_nan) return .lt;

    return math.order(a, b);
}

// ============================================================================
// Unit Tests
// ============================================================================

test "Value type ordering across types" {
    const null_val = Value{ .null = {} };
    const bool_val = Value{ .bool = false };
    const int_val = Value{ .int = 0 };
    const num_val = Value{ .number = 0.0 };
    const str_val = Value{ .string = "" };

    // null < bool < int < number < string
    try std.testing.expect(null_val.order(bool_val) == .lt);
    try std.testing.expect(bool_val.order(int_val) == .lt);
    try std.testing.expect(int_val.order(num_val) == .lt);
    try std.testing.expect(num_val.order(str_val) == .lt);

    // Reverse
    try std.testing.expect(str_val.order(num_val) == .gt);
    try std.testing.expect(num_val.order(int_val) == .gt);
}

test "Value bool ordering" {
    const false_val = Value{ .bool = false };
    const true_val = Value{ .bool = true };

    try std.testing.expect(false_val.order(true_val) == .lt);
    try std.testing.expect(true_val.order(false_val) == .gt);
    try std.testing.expect(true_val.order(true_val) == .eq);
}

test "Value int ordering with negative numbers" {
    const neg = Value{ .int = -100 };
    const zero = Value{ .int = 0 };
    const pos = Value{ .int = 100 };

    try std.testing.expect(neg.order(zero) == .lt);
    try std.testing.expect(zero.order(pos) == .lt);
    try std.testing.expect(neg.order(pos) == .lt);
    try std.testing.expect(pos.order(neg) == .gt);
}

test "Value number ordering with special values" {
    const neg_inf = Value{ .number = -math.inf(f64) };
    const neg = Value{ .number = -1.0 };
    const zero = Value{ .number = 0.0 };
    const pos = Value{ .number = 1.0 };
    const pos_inf = Value{ .number = math.inf(f64) };

    try std.testing.expect(neg_inf.order(neg) == .lt);
    try std.testing.expect(neg.order(zero) == .lt);
    try std.testing.expect(zero.order(pos) == .lt);
    try std.testing.expect(pos.order(pos_inf) == .lt);
}

test "Value string ordering" {
    const a = Value{ .string = "alice" };
    const b = Value{ .string = "bob" };
    const empty = Value{ .string = "" };

    try std.testing.expect(empty.order(a) == .lt);
    try std.testing.expect(a.order(b) == .lt);
    try std.testing.expect(b.order(a) == .gt);
    try std.testing.expect(a.order(a) == .eq);
}

test "Value equality" {
    const int1 = Value{ .int = 42 };
    const int2 = Value{ .int = 42 };
    const int3 = Value{ .int = 43 };
    const str = Value{ .string = "42" };

    try std.testing.expect(int1.eql(int2));
    try std.testing.expect(!int1.eql(int3));
    try std.testing.expect(!int1.eql(str)); // Different types
}

test "Value accessors" {
    const str = Value{ .string = "hello" };
    const int = Value{ .int = 42 };
    const num = Value{ .number = 3.14 };
    const b = Value{ .bool = true };
    const n = Value{ .null = {} };

    try std.testing.expectEqualStrings("hello", str.asString().?);
    try std.testing.expect(str.asInt() == null);

    try std.testing.expectEqual(@as(i64, 42), int.asInt().?);
    try std.testing.expect(int.asString() == null);

    try std.testing.expectEqual(@as(f64, 3.14), num.asNumber().?);
    try std.testing.expectEqual(true, b.asBool().?);
    try std.testing.expect(n.isNull());
}

test "Value tag byte" {
    try std.testing.expectEqual(@as(u8, 0), (Value{ .null = {} }).tagByte());
    try std.testing.expectEqual(@as(u8, 1), (Value{ .bool = true }).tagByte());
    try std.testing.expectEqual(@as(u8, 2), (Value{ .int = 0 }).tagByte());
    try std.testing.expectEqual(@as(u8, 3), (Value{ .number = 0 }).tagByte());
    try std.testing.expectEqual(@as(u8, 4), (Value{ .string = "" }).tagByte());
}
