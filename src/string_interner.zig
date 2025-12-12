///! String interner for deduplicating string allocations.
///!
///! The interner stores unique copies of strings and returns stable pointers.
///! This is used for field names, type names, and other frequently-used strings
///! to reduce memory usage and enable pointer equality comparisons.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// A string interner that deduplicates string allocations.
/// All interned strings remain valid until the interner is deinitialized.
pub const StringInterner = struct {
    /// Map from string content to the interned slice
    strings: std.StringHashMapUnmanaged(void),
    /// Arena allocator for string storage
    arena: std.heap.ArenaAllocator,

    const Self = @This();

    /// Initialize a new string interner.
    pub fn init(allocator: Allocator) Self {
        return .{
            .strings = .{},
            .arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    /// Free all interned strings and the interner itself.
    pub fn deinit(self: *Self) void {
        self.strings.deinit(self.arena.child_allocator);
        self.arena.deinit();
    }

    /// Intern a string, returning a stable pointer to the interned copy.
    /// If the string was already interned, returns the existing copy.
    /// The returned slice is valid until the interner is deinitialized.
    pub fn intern(self: *Self, str: []const u8) Allocator.Error![]const u8 {
        // Check if already interned
        if (self.strings.getKey(str)) |existing| {
            return existing;
        }

        // Allocate new copy in arena
        const arena_alloc = self.arena.allocator();
        const copy = try arena_alloc.dupe(u8, str);

        // Insert into map
        try self.strings.put(self.arena.child_allocator, copy, {});

        return copy;
    }

    /// Check if a string has been interned.
    pub fn contains(self: *const Self, str: []const u8) bool {
        return self.strings.contains(str);
    }

    /// Get the interned version of a string, or null if not interned.
    pub fn get(self: *const Self, str: []const u8) ?[]const u8 {
        return self.strings.getKey(str);
    }

    /// Return the number of unique strings interned.
    pub fn count(self: *const Self) usize {
        return self.strings.count();
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "StringInterner basic interning" {
    var interner = StringInterner.init(std.testing.allocator);
    defer interner.deinit();

    const s1 = try interner.intern("hello");
    const s2 = try interner.intern("hello");
    const s3 = try interner.intern("world");

    // Same content returns same pointer
    try std.testing.expectEqual(s1.ptr, s2.ptr);
    try std.testing.expectEqualStrings("hello", s1);
    try std.testing.expectEqualStrings("hello", s2);

    // Different content returns different pointer
    try std.testing.expect(s1.ptr != s3.ptr);
    try std.testing.expectEqualStrings("world", s3);
}

test "StringInterner count" {
    var interner = StringInterner.init(std.testing.allocator);
    defer interner.deinit();

    try std.testing.expectEqual(@as(usize, 0), interner.count());

    _ = try interner.intern("one");
    try std.testing.expectEqual(@as(usize, 1), interner.count());

    _ = try interner.intern("two");
    try std.testing.expectEqual(@as(usize, 2), interner.count());

    // Duplicate doesn't increase count
    _ = try interner.intern("one");
    try std.testing.expectEqual(@as(usize, 2), interner.count());
}

test "StringInterner contains and get" {
    var interner = StringInterner.init(std.testing.allocator);
    defer interner.deinit();

    try std.testing.expect(!interner.contains("hello"));
    try std.testing.expect(interner.get("hello") == null);

    const interned = try interner.intern("hello");

    try std.testing.expect(interner.contains("hello"));
    try std.testing.expectEqual(interned.ptr, interner.get("hello").?.ptr);
}

test "StringInterner empty string" {
    var interner = StringInterner.init(std.testing.allocator);
    defer interner.deinit();

    const empty1 = try interner.intern("");
    const empty2 = try interner.intern("");

    try std.testing.expectEqual(empty1.ptr, empty2.ptr);
    try std.testing.expectEqual(@as(usize, 0), empty1.len);
}

test "StringInterner unicode strings" {
    var interner = StringInterner.init(std.testing.allocator);
    defer interner.deinit();

    const jp1 = try interner.intern("日本語");
    const jp2 = try interner.intern("日本語");
    const emoji = try interner.intern("🎉");

    try std.testing.expectEqual(jp1.ptr, jp2.ptr);
    try std.testing.expectEqualStrings("日本語", jp1);
    try std.testing.expectEqualStrings("🎉", emoji);
}
