///! Map that groups values by a key type (typically TypeId).
///!
///! Useful for type-indexed dispatch where you need to efficiently
///! access all values associated with a particular type.
///!
///! Usage:
///! ```
///! var groups = TypeGroupedMap(*Subscription).init(allocator);
///! defer groups.deinit();
///!
///! try groups.add(user_type_id, &user_subscription);
///! try groups.add(post_type_id, &post_subscription);
///!
///! for (groups.getForKey(user_type_id)) |sub| {
///!     // Process all subscriptions for User type
///! }
///! ```

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Map that groups values by a key type.
///
/// - K: Key type (e.g., TypeId)
/// - V: Value type (e.g., *Subscription)
pub fn GroupedMap(comptime K: type, comptime V: type) type {
    return struct {
        groups: std.AutoHashMapUnmanaged(K, std.ArrayListUnmanaged(V)),
        allocator: Allocator,

        const Self = @This();
        const ValueList = std.ArrayListUnmanaged(V);

        pub fn init(allocator: Allocator) Self {
            return .{
                .groups = .{},
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            var iter = self.groups.iterator();
            while (iter.next()) |entry| {
                entry.value_ptr.deinit(self.allocator);
            }
            self.groups.deinit(self.allocator);
        }

        /// Add a value to a group. O(1) amortized.
        pub fn add(self: *Self, key: K, value: V) !void {
            const result = try self.groups.getOrPut(self.allocator, key);
            if (!result.found_existing) {
                result.value_ptr.* = ValueList{};
            }
            try result.value_ptr.append(self.allocator, value);
        }

        /// Remove a value from a group. O(n) where n is group size.
        /// Returns true if the value was found and removed.
        pub fn remove(self: *Self, key: K, value: V) bool {
            const list = self.groups.getPtr(key) orelse return false;

            for (list.items, 0..) |item, i| {
                if (valuesEqual(item, value)) {
                    _ = list.swapRemove(i);

                    // Clean up empty groups
                    if (list.items.len == 0) {
                        list.deinit(self.allocator);
                        _ = self.groups.remove(key);
                    }
                    return true;
                }
            }
            return false;
        }

        /// Get all values for a key. O(1).
        pub fn getForKey(self: *const Self, key: K) []const V {
            if (self.groups.get(key)) |list| {
                return list.items;
            }
            return &.{};
        }

        /// Get mutable access to values for a key. O(1).
        pub fn getForKeyMut(self: *Self, key: K) []V {
            if (self.groups.getPtr(key)) |list| {
                return list.items;
            }
            return &.{};
        }

        /// Check if a key has any values. O(1).
        pub fn hasKey(self: *const Self, key: K) bool {
            return self.groups.contains(key);
        }

        /// Get count of values for a key. O(1).
        pub fn countForKey(self: *const Self, key: K) usize {
            if (self.groups.get(key)) |list| {
                return list.items.len;
            }
            return 0;
        }

        /// Get total count of all values across all groups. O(k) where k is number of keys.
        pub fn totalCount(self: *const Self) usize {
            var total: usize = 0;
            var iter = self.groups.iterator();
            while (iter.next()) |entry| {
                total += entry.value_ptr.items.len;
            }
            return total;
        }

        /// Get number of groups (distinct keys). O(1).
        pub fn keyCount(self: *const Self) usize {
            return self.groups.count();
        }

        /// Remove all values for a key. O(1).
        pub fn removeKey(self: *Self, key: K) void {
            if (self.groups.fetchRemove(key)) |entry| {
                var list = entry.value;
                list.deinit(self.allocator);
            }
        }

        /// Clear all groups. O(k) where k is number of keys.
        pub fn clear(self: *Self) void {
            var iter = self.groups.iterator();
            while (iter.next()) |entry| {
                entry.value_ptr.deinit(self.allocator);
            }
            self.groups.clearRetainingCapacity();
        }

        /// Iterator over all keys that have values.
        pub fn keyIterator(self: *const Self) KeyIterator {
            return .{ .inner = self.groups.keyIterator() };
        }

        /// Iterator over all (key, values) pairs.
        pub fn iterator(self: *const Self) Iterator {
            return .{ .inner = self.groups.iterator() };
        }

        pub const KeyIterator = struct {
            inner: std.AutoHashMapUnmanaged(K, ValueList).KeyIterator,

            pub fn next(self: *KeyIterator) ?K {
                if (self.inner.next()) |key_ptr| {
                    return key_ptr.*;
                }
                return null;
            }
        };

        pub const Iterator = struct {
            inner: std.AutoHashMapUnmanaged(K, ValueList).Iterator,

            pub const Entry = struct {
                key: K,
                values: []const V,
            };

            pub fn next(self: *Iterator) ?Entry {
                if (self.inner.next()) |entry| {
                    return .{
                        .key = entry.key_ptr.*,
                        .values = entry.value_ptr.items,
                    };
                }
                return null;
            }
        };

        fn valuesEqual(a: V, b: V) bool {
            // For pointer types, compare addresses
            if (@typeInfo(V) == .pointer) {
                return a == b;
            }
            // For other types, use standard equality
            return std.meta.eql(a, b);
        }
    };
}

/// Convenience alias for TypeId-keyed groups.
pub fn TypeGroupedMap(comptime V: type) type {
    return GroupedMap(u16, V);
}

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;

test "GroupedMap basic operations" {
    var groups = GroupedMap(u16, u32).init(testing.allocator);
    defer groups.deinit();

    try groups.add(1, 100);
    try groups.add(1, 101);
    try groups.add(1, 102);
    try groups.add(2, 200);
    try groups.add(2, 201);

    try testing.expectEqual(@as(usize, 3), groups.countForKey(1));
    try testing.expectEqual(@as(usize, 2), groups.countForKey(2));
    try testing.expectEqual(@as(usize, 0), groups.countForKey(3));

    try testing.expectEqual(@as(usize, 5), groups.totalCount());
    try testing.expectEqual(@as(usize, 2), groups.keyCount());
}

test "GroupedMap getForKey" {
    var groups = GroupedMap(u16, u32).init(testing.allocator);
    defer groups.deinit();

    try groups.add(1, 10);
    try groups.add(1, 20);
    try groups.add(1, 30);

    const values = groups.getForKey(1);
    try testing.expectEqual(@as(usize, 3), values.len);
    try testing.expectEqual(@as(u32, 10), values[0]);
    try testing.expectEqual(@as(u32, 20), values[1]);
    try testing.expectEqual(@as(u32, 30), values[2]);

    // Non-existent key returns empty slice
    const empty = groups.getForKey(999);
    try testing.expectEqual(@as(usize, 0), empty.len);
}

test "GroupedMap remove" {
    var groups = GroupedMap(u16, u32).init(testing.allocator);
    defer groups.deinit();

    try groups.add(1, 100);
    try groups.add(1, 200);
    try groups.add(1, 300);

    try testing.expect(groups.remove(1, 200));
    try testing.expectEqual(@as(usize, 2), groups.countForKey(1));

    try testing.expect(!groups.remove(1, 200)); // Already removed
    try testing.expect(!groups.remove(1, 999)); // Never existed

    // Remove last item should remove the key
    try testing.expect(groups.remove(1, 100));
    try testing.expect(groups.remove(1, 300));
    try testing.expect(!groups.hasKey(1));
}

test "GroupedMap removeKey" {
    var groups = GroupedMap(u16, u32).init(testing.allocator);
    defer groups.deinit();

    try groups.add(1, 100);
    try groups.add(1, 200);
    try groups.add(2, 300);

    groups.removeKey(1);

    try testing.expect(!groups.hasKey(1));
    try testing.expect(groups.hasKey(2));
    try testing.expectEqual(@as(usize, 1), groups.keyCount());
}

test "GroupedMap hasKey" {
    var groups = GroupedMap(u16, u32).init(testing.allocator);
    defer groups.deinit();

    try testing.expect(!groups.hasKey(1));

    try groups.add(1, 100);
    try testing.expect(groups.hasKey(1));
    try testing.expect(!groups.hasKey(2));
}

test "GroupedMap clear" {
    var groups = GroupedMap(u16, u32).init(testing.allocator);
    defer groups.deinit();

    try groups.add(1, 100);
    try groups.add(2, 200);
    try groups.add(3, 300);

    groups.clear();

    try testing.expectEqual(@as(usize, 0), groups.keyCount());
    try testing.expectEqual(@as(usize, 0), groups.totalCount());
}

test "GroupedMap with pointers" {
    const Item = struct { value: u32 };
    var groups = GroupedMap(u16, *Item).init(testing.allocator);
    defer groups.deinit();

    var item1 = Item{ .value = 1 };
    var item2 = Item{ .value = 2 };
    var item3 = Item{ .value = 3 };

    try groups.add(1, &item1);
    try groups.add(1, &item2);
    try groups.add(2, &item3);

    const type1_items = groups.getForKey(1);
    try testing.expectEqual(@as(usize, 2), type1_items.len);
    try testing.expectEqual(@as(u32, 1), type1_items[0].value);
    try testing.expectEqual(@as(u32, 2), type1_items[1].value);

    // Remove by pointer identity
    try testing.expect(groups.remove(1, &item1));
    try testing.expectEqual(@as(usize, 1), groups.countForKey(1));
}

test "GroupedMap iterator" {
    var groups = GroupedMap(u16, u32).init(testing.allocator);
    defer groups.deinit();

    try groups.add(1, 100);
    try groups.add(2, 200);

    var found_keys: u32 = 0;
    var iter = groups.iterator();
    while (iter.next()) |entry| {
        found_keys += 1;
        if (entry.key == 1) {
            try testing.expectEqual(@as(usize, 1), entry.values.len);
            try testing.expectEqual(@as(u32, 100), entry.values[0]);
        } else if (entry.key == 2) {
            try testing.expectEqual(@as(usize, 1), entry.values.len);
            try testing.expectEqual(@as(u32, 200), entry.values[0]);
        }
    }
    try testing.expectEqual(@as(u32, 2), found_keys);
}

test "TypeGroupedMap alias" {
    var groups = TypeGroupedMap(u32).init(testing.allocator);
    defer groups.deinit();

    try groups.add(1, 100); // TypeId 1
    try groups.add(2, 200); // TypeId 2

    try testing.expectEqual(@as(usize, 1), groups.countForKey(1));
    try testing.expectEqual(@as(usize, 1), groups.countForKey(2));
}
