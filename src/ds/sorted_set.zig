///! Generic sorted set with binary search operations.
///!
///! Maintains elements in sorted order for O(log n) lookups.
///! Insertion and removal are O(n) due to array shifting.
///!
///! Usage:
///! ```
///! var set = SortedSet(u64).init(allocator);
///! defer set.deinit();
///!
///! try set.add(5);
///! try set.add(3);
///! try set.add(7);
///! // Set now contains: [3, 5, 7]
///!
///! if (set.contains(5)) { ... }
///! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const Order = std.math.Order;

/// Generic sorted set using an array with binary search.
///
/// For types that don't have natural ordering, use SortedSetContext
/// with a custom comparison function.
pub fn SortedSet(comptime T: type) type {
    return SortedSetContext(T, void, defaultCompare(T));
}

/// Generic sorted set with custom comparison context.
pub fn SortedSetContext(
    comptime T: type,
    comptime Context: type,
    comptime compareFn: fn (Context, T, T) Order,
) type {
    return struct {
        items: std.ArrayListUnmanaged(T),
        allocator: Allocator,
        context: Context,

        const Self = @This();

        pub fn init(allocator: Allocator) Self {
            return initWithContext(allocator, undefined);
        }

        pub fn initWithContext(allocator: Allocator, context: Context) Self {
            return .{
                .items = .{},
                .allocator = allocator,
                .context = context,
            };
        }

        pub fn deinit(self: *Self) void {
            self.items.deinit(self.allocator);
        }

        /// Add an element, maintaining sorted order. O(log n) search + O(n) insert.
        /// No-op if element already exists (set semantics).
        pub fn add(self: *Self, item: T) !void {
            const idx = self.findInsertIndex(item);

            // Check if already present
            if (idx < self.items.items.len) {
                if (compareFn(self.context, self.items.items[idx], item) == .eq) {
                    return; // Already exists, idempotent
                }
            }

            try self.items.insert(self.allocator, idx, item);
        }

        /// Remove an element. O(log n) search + O(n) removal.
        /// Returns true if the element was present.
        pub fn remove(self: *Self, item: T) bool {
            const idx = self.findInsertIndex(item);

            if (idx < self.items.items.len) {
                if (compareFn(self.context, self.items.items[idx], item) == .eq) {
                    _ = self.items.orderedRemove(idx);
                    return true;
                }
            }
            return false;
        }

        /// Check if an element is present. O(log n).
        pub fn contains(self: *const Self, item: T) bool {
            const idx = self.findInsertIndex(item);
            if (idx < self.items.items.len) {
                return compareFn(self.context, self.items.items[idx], item) == .eq;
            }
            return false;
        }

        /// Get the number of elements.
        pub fn count(self: *const Self) usize {
            return self.items.items.len;
        }

        /// Check if empty.
        pub fn isEmpty(self: *const Self) bool {
            return self.items.items.len == 0;
        }

        /// Get the first element, or null if empty.
        pub fn first(self: *const Self) ?T {
            if (self.items.items.len > 0) {
                return self.items.items[0];
            }
            return null;
        }

        /// Get the last element, or null if empty.
        pub fn last(self: *const Self) ?T {
            if (self.items.items.len > 0) {
                return self.items.items[self.items.items.len - 1];
            }
            return null;
        }

        /// Get element at index.
        pub fn get(self: *const Self, index: usize) ?T {
            if (index < self.items.items.len) {
                return self.items.items[index];
            }
            return null;
        }

        /// Get all elements as a slice.
        pub fn slice(self: *const Self) []const T {
            return self.items.items;
        }

        /// Find index where item should be inserted to maintain order. O(log n).
        pub fn findInsertIndex(self: *const Self, item: T) usize {
            var left: usize = 0;
            var right: usize = self.items.items.len;

            while (left < right) {
                const mid = left + (right - left) / 2;
                const cmp = compareFn(self.context, self.items.items[mid], item);
                if (cmp == .lt) {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
            return left;
        }

        /// Find the index of an item. O(log n).
        /// Returns null if not found.
        pub fn indexOf(self: *const Self, item: T) ?usize {
            const idx = self.findInsertIndex(item);
            if (idx < self.items.items.len) {
                if (compareFn(self.context, self.items.items[idx], item) == .eq) {
                    return idx;
                }
            }
            return null;
        }

        /// Clone the set.
        pub fn clone(self: *const Self) !Self {
            var new = Self.initWithContext(self.allocator, self.context);
            try new.items.appendSlice(self.allocator, self.items.items);
            return new;
        }

        /// Clear all elements.
        pub fn clear(self: *Self) void {
            self.items.clearRetainingCapacity();
        }

        /// Range query: get all elements in [low, high]. O(log n) + O(k) where k is result count.
        pub fn range(self: *const Self, low: T, high: T) []const T {
            const start = self.findInsertIndex(low);
            var end = start;

            while (end < self.items.items.len) {
                if (compareFn(self.context, self.items.items[end], high) == .gt) {
                    break;
                }
                end += 1;
            }

            return self.items.items[start..end];
        }

        /// Iterator over elements.
        pub fn iterator(self: *const Self) Iterator {
            return .{ .items = self.items.items, .index = 0 };
        }

        pub const Iterator = struct {
            items: []const T,
            index: usize,

            pub fn next(self: *Iterator) ?T {
                if (self.index < self.items.len) {
                    const item = self.items[self.index];
                    self.index += 1;
                    return item;
                }
                return null;
            }
        };
    };
}

/// Default comparison function for types with natural ordering.
fn defaultCompare(comptime T: type) fn (void, T, T) Order {
    return struct {
        fn compare(_: void, a: T, b: T) Order {
            return std.math.order(a, b);
        }
    }.compare;
}

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;

test "SortedSet basic operations" {
    var set = SortedSet(u64).init(testing.allocator);
    defer set.deinit();

    try testing.expect(set.isEmpty());
    try testing.expectEqual(@as(usize, 0), set.count());

    try set.add(5);
    try set.add(3);
    try set.add(7);
    try set.add(1);
    try set.add(9);

    try testing.expectEqual(@as(usize, 5), set.count());
    try testing.expect(!set.isEmpty());

    // Verify sorted order
    const items = set.slice();
    try testing.expectEqual(@as(u64, 1), items[0]);
    try testing.expectEqual(@as(u64, 3), items[1]);
    try testing.expectEqual(@as(u64, 5), items[2]);
    try testing.expectEqual(@as(u64, 7), items[3]);
    try testing.expectEqual(@as(u64, 9), items[4]);
}

test "SortedSet contains" {
    var set = SortedSet(u64).init(testing.allocator);
    defer set.deinit();

    try set.add(10);
    try set.add(20);
    try set.add(30);

    try testing.expect(set.contains(10));
    try testing.expect(set.contains(20));
    try testing.expect(set.contains(30));
    try testing.expect(!set.contains(15));
    try testing.expect(!set.contains(0));
    try testing.expect(!set.contains(100));
}

test "SortedSet remove" {
    var set = SortedSet(u64).init(testing.allocator);
    defer set.deinit();

    try set.add(1);
    try set.add(2);
    try set.add(3);

    try testing.expect(set.remove(2));
    try testing.expectEqual(@as(usize, 2), set.count());
    try testing.expect(!set.contains(2));

    try testing.expect(!set.remove(2)); // Already removed
    try testing.expect(!set.remove(100)); // Never existed
}

test "SortedSet idempotent add" {
    var set = SortedSet(u64).init(testing.allocator);
    defer set.deinit();

    try set.add(5);
    try set.add(5);
    try set.add(5);

    try testing.expectEqual(@as(usize, 1), set.count());
}

test "SortedSet first and last" {
    var set = SortedSet(u64).init(testing.allocator);
    defer set.deinit();

    try testing.expectEqual(@as(?u64, null), set.first());
    try testing.expectEqual(@as(?u64, null), set.last());

    try set.add(50);
    try set.add(10);
    try set.add(90);

    try testing.expectEqual(@as(?u64, 10), set.first());
    try testing.expectEqual(@as(?u64, 90), set.last());
}

test "SortedSet indexOf" {
    var set = SortedSet(u64).init(testing.allocator);
    defer set.deinit();

    try set.add(100);
    try set.add(200);
    try set.add(300);

    try testing.expectEqual(@as(?usize, 0), set.indexOf(100));
    try testing.expectEqual(@as(?usize, 1), set.indexOf(200));
    try testing.expectEqual(@as(?usize, 2), set.indexOf(300));
    try testing.expectEqual(@as(?usize, null), set.indexOf(150));
}

test "SortedSet range query" {
    var set = SortedSet(u64).init(testing.allocator);
    defer set.deinit();

    try set.add(10);
    try set.add(20);
    try set.add(30);
    try set.add(40);
    try set.add(50);

    const r1 = set.range(20, 40);
    try testing.expectEqual(@as(usize, 3), r1.len);
    try testing.expectEqual(@as(u64, 20), r1[0]);
    try testing.expectEqual(@as(u64, 30), r1[1]);
    try testing.expectEqual(@as(u64, 40), r1[2]);

    const r2 = set.range(25, 35);
    try testing.expectEqual(@as(usize, 1), r2.len);
    try testing.expectEqual(@as(u64, 30), r2[0]);

    const r3 = set.range(100, 200);
    try testing.expectEqual(@as(usize, 0), r3.len);
}

test "SortedSet clone" {
    var set = SortedSet(u64).init(testing.allocator);
    defer set.deinit();

    try set.add(1);
    try set.add(2);
    try set.add(3);

    var cloned = try set.clone();
    defer cloned.deinit();

    try testing.expectEqual(@as(usize, 3), cloned.count());
    try testing.expect(cloned.contains(1));
    try testing.expect(cloned.contains(2));
    try testing.expect(cloned.contains(3));

    // Modify original, clone should be unaffected
    try set.add(4);
    try testing.expect(!cloned.contains(4));
}

test "SortedSet iterator" {
    var set = SortedSet(u64).init(testing.allocator);
    defer set.deinit();

    try set.add(30);
    try set.add(10);
    try set.add(20);

    var iter = set.iterator();
    try testing.expectEqual(@as(?u64, 10), iter.next());
    try testing.expectEqual(@as(?u64, 20), iter.next());
    try testing.expectEqual(@as(?u64, 30), iter.next());
    try testing.expectEqual(@as(?u64, null), iter.next());
}

test "SortedSet clear" {
    var set = SortedSet(u64).init(testing.allocator);
    defer set.deinit();

    try set.add(1);
    try set.add(2);
    try set.add(3);

    set.clear();

    try testing.expect(set.isEmpty());
    try testing.expectEqual(@as(usize, 0), set.count());
}

// Test with custom comparison
const ReverseU64Set = SortedSetContext(u64, void, struct {
    fn compare(_: void, a: u64, b: u64) Order {
        // Reverse order
        return std.math.order(b, a);
    }
}.compare);

test "SortedSetContext custom comparison" {
    var set = ReverseU64Set.init(testing.allocator);
    defer set.deinit();

    try set.add(1);
    try set.add(2);
    try set.add(3);

    // Should be in reverse order
    const items = set.slice();
    try testing.expectEqual(@as(u64, 3), items[0]);
    try testing.expectEqual(@as(u64, 2), items[1]);
    try testing.expectEqual(@as(u64, 1), items[2]);
}
