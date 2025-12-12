///! Generic intrusive doubly-linked list.
///!
///! An intrusive list where nodes embed their own prev/next pointers.
///! This enables O(1) insert/remove operations without separate node allocation.
///!
///! Usage:
///! ```
///! const MyNode = struct {
///!     data: u32,
///!     prev: ?*MyNode = null,
///!     next: ?*MyNode = null,
///! };
///!
///! var list = IntrusiveList(MyNode, "prev", "next").init();
///! list.pushBack(&node1);
///! list.pushBack(&node2);
///! ```

const std = @import("std");

/// Generic intrusive doubly-linked list.
///
/// NodeType must have fields named `prev_field` and `next_field` of type `?*NodeType`.
pub fn IntrusiveList(
    comptime NodeType: type,
    comptime prev_field: []const u8,
    comptime next_field: []const u8,
) type {
    return struct {
        head: ?*NodeType = null,
        tail: ?*NodeType = null,
        len: usize = 0,

        const Self = @This();

        pub fn init() Self {
            return .{};
        }

        /// Returns true if the list is empty.
        pub fn isEmpty(self: *const Self) bool {
            return self.head == null;
        }

        /// Returns the number of nodes in the list.
        pub fn length(self: *const Self) usize {
            return self.len;
        }

        /// Get the first node, or null if empty.
        pub fn first(self: *const Self) ?*NodeType {
            return self.head;
        }

        /// Get the last node, or null if empty.
        pub fn last(self: *const Self) ?*NodeType {
            return self.tail;
        }

        /// Get the next node after the given node.
        pub fn next(node: *NodeType) ?*NodeType {
            return @field(node, next_field);
        }

        /// Get the previous node before the given node.
        pub fn prev(node: *NodeType) ?*NodeType {
            return @field(node, prev_field);
        }

        /// Insert a node at the front of the list. O(1).
        pub fn pushFront(self: *Self, node: *NodeType) void {
            @field(node, prev_field) = null;
            @field(node, next_field) = self.head;

            if (self.head) |h| {
                @field(h, prev_field) = node;
            } else {
                self.tail = node;
            }

            self.head = node;
            self.len += 1;
        }

        /// Insert a node at the back of the list. O(1).
        pub fn pushBack(self: *Self, node: *NodeType) void {
            @field(node, prev_field) = self.tail;
            @field(node, next_field) = null;

            if (self.tail) |t| {
                @field(t, next_field) = node;
            } else {
                self.head = node;
            }

            self.tail = node;
            self.len += 1;
        }

        /// Insert a node after the given node. O(1).
        /// If `after` is null, inserts at the front.
        pub fn insertAfter(self: *Self, node: *NodeType, after: ?*NodeType) void {
            if (after) |a| {
                @field(node, prev_field) = a;
                @field(node, next_field) = @field(a, next_field);

                if (@field(a, next_field)) |n| {
                    @field(n, prev_field) = node;
                } else {
                    self.tail = node;
                }

                @field(a, next_field) = node;
                self.len += 1;
            } else {
                self.pushFront(node);
            }
        }

        /// Insert a node before the given node. O(1).
        /// If `before` is null, inserts at the back.
        pub fn insertBefore(self: *Self, node: *NodeType, before: ?*NodeType) void {
            if (before) |b| {
                @field(node, next_field) = b;
                @field(node, prev_field) = @field(b, prev_field);

                if (@field(b, prev_field)) |p| {
                    @field(p, next_field) = node;
                } else {
                    self.head = node;
                }

                @field(b, prev_field) = node;
                self.len += 1;
            } else {
                self.pushBack(node);
            }
        }

        /// Remove a node from the list. O(1).
        /// The node must be in this list.
        pub fn remove(self: *Self, node: *NodeType) void {
            if (@field(node, prev_field)) |p| {
                @field(p, next_field) = @field(node, next_field);
            } else {
                self.head = @field(node, next_field);
            }

            if (@field(node, next_field)) |n| {
                @field(n, prev_field) = @field(node, prev_field);
            } else {
                self.tail = @field(node, prev_field);
            }

            @field(node, prev_field) = null;
            @field(node, next_field) = null;
            self.len -= 1;
        }

        /// Remove and return the first node. O(1).
        pub fn popFront(self: *Self) ?*NodeType {
            const node = self.head orelse return null;
            self.remove(node);
            return node;
        }

        /// Remove and return the last node. O(1).
        pub fn popBack(self: *Self) ?*NodeType {
            const node = self.tail orelse return null;
            self.remove(node);
            return node;
        }

        /// Move a node to a new position after `after`. O(1).
        /// If `after` is null, moves to the front.
        pub fn moveAfter(self: *Self, node: *NodeType, after: ?*NodeType) void {
            if (@field(node, prev_field) == after) return; // Already in position
            if (after == node) return; // Can't insert after itself

            self.remove(node);
            self.len += 1; // Compensate for remove's decrement
            self.insertAfter(node, after);
            self.len -= 1; // Compensate for insertAfter's increment
        }

        /// Move a node to a new position before `before`. O(1).
        /// If `before` is null, moves to the back.
        pub fn moveBefore(self: *Self, node: *NodeType, before: ?*NodeType) void {
            if (@field(node, next_field) == before) return; // Already in position
            if (before == node) return; // Can't insert before itself

            self.remove(node);
            self.len += 1;
            self.insertBefore(node, before);
            self.len -= 1;
        }

        /// Move a contiguous range of nodes [range_first..range_last] to after `after`. O(1).
        /// If `after` is null, moves to the front.
        /// The range must be contiguous (range_first..range_last connected via next pointers).
        /// `range_len` is the number of nodes in the range (caller must provide).
        pub fn moveRangeAfter(
            self: *Self,
            range_first: *NodeType,
            range_last: *NodeType,
            range_len: usize,
            after: ?*NodeType,
        ) void {
            // Check if already in position
            if (@field(range_first, prev_field) == after) return;

            // Can't insert range after a node that's in the range
            if (after) |a| {
                var current: ?*NodeType = range_first;
                while (current) |c| {
                    if (c == a) return;
                    if (c == range_last) break;
                    current = @field(c, next_field);
                }
            }

            // Unlink the range
            self.unlinkRange(range_first, range_last, range_len);

            // Relink at new position
            self.linkRangeAfter(range_first, range_last, range_len, after);
        }

        /// Unlink a contiguous range [range_first..range_last] from the list. O(1).
        /// Does not free nodes - caller is responsible.
        pub fn unlinkRange(self: *Self, range_first: *NodeType, range_last: *NodeType, range_len: usize) void {
            // Update surrounding nodes
            if (@field(range_first, prev_field)) |p| {
                @field(p, next_field) = @field(range_last, next_field);
            } else {
                self.head = @field(range_last, next_field);
            }

            if (@field(range_last, next_field)) |n| {
                @field(n, prev_field) = @field(range_first, prev_field);
            } else {
                self.tail = @field(range_first, prev_field);
            }

            // Clear range endpoints (internal links remain intact)
            @field(range_first, prev_field) = null;
            @field(range_last, next_field) = null;

            self.len -= range_len;
        }

        /// Link a contiguous range [range_first..range_last] after `after`. O(1).
        /// If `after` is null, links at front.
        pub fn linkRangeAfter(
            self: *Self,
            range_first: *NodeType,
            range_last: *NodeType,
            range_len: usize,
            after: ?*NodeType,
        ) void {
            if (after) |a| {
                @field(range_first, prev_field) = a;
                @field(range_last, next_field) = @field(a, next_field);

                if (@field(a, next_field)) |n| {
                    @field(n, prev_field) = range_last;
                } else {
                    self.tail = range_last;
                }
                @field(a, next_field) = range_first;
            } else {
                // Insert at head
                @field(range_first, prev_field) = null;
                @field(range_last, next_field) = self.head;

                if (self.head) |h| {
                    @field(h, prev_field) = range_last;
                } else {
                    self.tail = range_last;
                }
                self.head = range_first;
            }

            self.len += range_len;
        }

        /// Check if a node is in any list (has prev or next set, or is head/tail of a 1-element list).
        pub fn isLinked(self: *const Self, node: *const NodeType) bool {
            return @field(node, prev_field) != null or
                @field(node, next_field) != null or
                self.head == node;
        }

        /// Forward iterator over the list.
        pub fn iterator(self: *const Self) Iterator {
            return .{ .current = self.head };
        }

        /// Reverse iterator over the list.
        pub fn reverseIterator(self: *const Self) ReverseIterator {
            return .{ .current = self.tail };
        }

        pub const Iterator = struct {
            current: ?*NodeType,

            pub fn next(self: *Iterator) ?*NodeType {
                const node = self.current orelse return null;
                self.current = @field(node, next_field);
                return node;
            }
        };

        pub const ReverseIterator = struct {
            current: ?*NodeType,

            pub fn next(self: *ReverseIterator) ?*NodeType {
                const node = self.current orelse return null;
                self.current = @field(node, prev_field);
                return node;
            }
        };
    };
}

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;

const TestNode = struct {
    value: u32,
    prev: ?*TestNode = null,
    next: ?*TestNode = null,
};

const TestList = IntrusiveList(TestNode, "prev", "next");

test "IntrusiveList empty list" {
    var list = TestList.init();

    try testing.expect(list.isEmpty());
    try testing.expectEqual(@as(usize, 0), list.length());
    try testing.expectEqual(@as(?*TestNode, null), list.first());
    try testing.expectEqual(@as(?*TestNode, null), list.last());
}

test "IntrusiveList pushBack" {
    var list = TestList.init();
    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };

    list.pushBack(&n1);
    try testing.expectEqual(@as(usize, 1), list.length());
    try testing.expectEqual(&n1, list.first().?);
    try testing.expectEqual(&n1, list.last().?);

    list.pushBack(&n2);
    list.pushBack(&n3);
    try testing.expectEqual(@as(usize, 3), list.length());
    try testing.expectEqual(&n1, list.first().?);
    try testing.expectEqual(&n3, list.last().?);

    // Verify links
    try testing.expectEqual(&n2, TestList.next(&n1).?);
    try testing.expectEqual(&n3, TestList.next(&n2).?);
    try testing.expectEqual(@as(?*TestNode, null), TestList.next(&n3));

    try testing.expectEqual(@as(?*TestNode, null), TestList.prev(&n1));
    try testing.expectEqual(&n1, TestList.prev(&n2).?);
    try testing.expectEqual(&n2, TestList.prev(&n3).?);
}

test "IntrusiveList pushFront" {
    var list = TestList.init();
    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };

    list.pushFront(&n1);
    list.pushFront(&n2);
    list.pushFront(&n3);

    // Order should be: n3 -> n2 -> n1
    try testing.expectEqual(&n3, list.first().?);
    try testing.expectEqual(&n1, list.last().?);
    try testing.expectEqual(&n2, TestList.next(&n3).?);
    try testing.expectEqual(&n1, TestList.next(&n2).?);
}

test "IntrusiveList remove" {
    var list = TestList.init();
    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };

    list.pushBack(&n1);
    list.pushBack(&n2);
    list.pushBack(&n3);

    // Remove middle
    list.remove(&n2);
    try testing.expectEqual(@as(usize, 2), list.length());
    try testing.expectEqual(&n3, TestList.next(&n1).?);
    try testing.expectEqual(&n1, TestList.prev(&n3).?);

    // Remove head
    list.remove(&n1);
    try testing.expectEqual(@as(usize, 1), list.length());
    try testing.expectEqual(&n3, list.first().?);
    try testing.expectEqual(&n3, list.last().?);

    // Remove last
    list.remove(&n3);
    try testing.expect(list.isEmpty());
}

test "IntrusiveList insertAfter" {
    var list = TestList.init();
    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };

    list.pushBack(&n1);
    list.pushBack(&n3);

    // Insert n2 after n1
    list.insertAfter(&n2, &n1);

    try testing.expectEqual(@as(usize, 3), list.length());
    try testing.expectEqual(&n2, TestList.next(&n1).?);
    try testing.expectEqual(&n3, TestList.next(&n2).?);
    try testing.expectEqual(&n1, TestList.prev(&n2).?);
    try testing.expectEqual(&n2, TestList.prev(&n3).?);
}

test "IntrusiveList insertAfter null inserts at front" {
    var list = TestList.init();
    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };

    list.pushBack(&n1);
    list.insertAfter(&n2, null);

    try testing.expectEqual(&n2, list.first().?);
    try testing.expectEqual(&n1, list.last().?);
}

test "IntrusiveList moveAfter" {
    var list = TestList.init();
    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };

    list.pushBack(&n1);
    list.pushBack(&n2);
    list.pushBack(&n3);

    // Move n1 after n2: n2 -> n1 -> n3
    list.moveAfter(&n1, &n2);

    try testing.expectEqual(@as(usize, 3), list.length());
    try testing.expectEqual(&n2, list.first().?);
    try testing.expectEqual(&n1, TestList.next(&n2).?);
    try testing.expectEqual(&n3, TestList.next(&n1).?);
    try testing.expectEqual(&n3, list.last().?);
}

test "IntrusiveList iterator" {
    var list = TestList.init();
    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };

    list.pushBack(&n1);
    list.pushBack(&n2);
    list.pushBack(&n3);

    var iter = list.iterator();
    try testing.expectEqual(@as(u32, 1), iter.next().?.value);
    try testing.expectEqual(@as(u32, 2), iter.next().?.value);
    try testing.expectEqual(@as(u32, 3), iter.next().?.value);
    try testing.expectEqual(@as(?*TestNode, null), iter.next());
}

test "IntrusiveList reverseIterator" {
    var list = TestList.init();
    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };

    list.pushBack(&n1);
    list.pushBack(&n2);
    list.pushBack(&n3);

    var iter = list.reverseIterator();
    try testing.expectEqual(@as(u32, 3), iter.next().?.value);
    try testing.expectEqual(@as(u32, 2), iter.next().?.value);
    try testing.expectEqual(@as(u32, 1), iter.next().?.value);
    try testing.expectEqual(@as(?*TestNode, null), iter.next());
}

test "IntrusiveList popFront and popBack" {
    var list = TestList.init();
    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };

    list.pushBack(&n1);
    list.pushBack(&n2);
    list.pushBack(&n3);

    try testing.expectEqual(@as(u32, 1), list.popFront().?.value);
    try testing.expectEqual(@as(u32, 3), list.popBack().?.value);
    try testing.expectEqual(@as(usize, 1), list.length());
    try testing.expectEqual(@as(u32, 2), list.first().?.value);
}

test "IntrusiveList moveRangeAfter forward" {
    var list = TestList.init();
    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };
    var n4 = TestNode{ .value = 4 };
    var n5 = TestNode{ .value = 5 };

    // Initial: 1 -> 2 -> 3 -> 4 -> 5
    list.pushBack(&n1);
    list.pushBack(&n2);
    list.pushBack(&n3);
    list.pushBack(&n4);
    list.pushBack(&n5);

    // Move range [1,2,3] after 4: 4 -> 1 -> 2 -> 3 -> 5
    list.moveRangeAfter(&n1, &n3, 3, &n4);

    try testing.expectEqual(@as(usize, 5), list.length());
    try testing.expectEqual(&n4, list.first().?);
    try testing.expectEqual(&n1, TestList.next(&n4).?);
    try testing.expectEqual(&n2, TestList.next(&n1).?);
    try testing.expectEqual(&n3, TestList.next(&n2).?);
    try testing.expectEqual(&n5, TestList.next(&n3).?);
    try testing.expectEqual(&n5, list.last().?);
}

test "IntrusiveList moveRangeAfter backward" {
    var list = TestList.init();
    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };
    var n4 = TestNode{ .value = 4 };
    var n5 = TestNode{ .value = 5 };

    // Initial: 1 -> 2 -> 3 -> 4 -> 5
    list.pushBack(&n1);
    list.pushBack(&n2);
    list.pushBack(&n3);
    list.pushBack(&n4);
    list.pushBack(&n5);

    // Move range [4,5] after 1: 1 -> 4 -> 5 -> 2 -> 3
    list.moveRangeAfter(&n4, &n5, 2, &n1);

    try testing.expectEqual(@as(usize, 5), list.length());
    try testing.expectEqual(&n1, list.first().?);
    try testing.expectEqual(&n4, TestList.next(&n1).?);
    try testing.expectEqual(&n5, TestList.next(&n4).?);
    try testing.expectEqual(&n2, TestList.next(&n5).?);
    try testing.expectEqual(&n3, TestList.next(&n2).?);
    try testing.expectEqual(&n3, list.last().?);
}

test "IntrusiveList moveRangeAfter to front" {
    var list = TestList.init();
    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };
    var n4 = TestNode{ .value = 4 };

    // Initial: 1 -> 2 -> 3 -> 4
    list.pushBack(&n1);
    list.pushBack(&n2);
    list.pushBack(&n3);
    list.pushBack(&n4);

    // Move range [3,4] to front: 3 -> 4 -> 1 -> 2
    list.moveRangeAfter(&n3, &n4, 2, null);

    try testing.expectEqual(@as(usize, 4), list.length());
    try testing.expectEqual(&n3, list.first().?);
    try testing.expectEqual(&n4, TestList.next(&n3).?);
    try testing.expectEqual(&n1, TestList.next(&n4).?);
    try testing.expectEqual(&n2, TestList.next(&n1).?);
    try testing.expectEqual(&n2, list.last().?);
}

test "IntrusiveList unlinkRange and linkRangeAfter" {
    var list = TestList.init();
    var n1 = TestNode{ .value = 1 };
    var n2 = TestNode{ .value = 2 };
    var n3 = TestNode{ .value = 3 };
    var n4 = TestNode{ .value = 4 };

    list.pushBack(&n1);
    list.pushBack(&n2);
    list.pushBack(&n3);
    list.pushBack(&n4);

    // Unlink [2,3]
    list.unlinkRange(&n2, &n3, 2);

    try testing.expectEqual(@as(usize, 2), list.length());
    try testing.expectEqual(&n1, list.first().?);
    try testing.expectEqual(&n4, TestList.next(&n1).?);
    try testing.expectEqual(&n4, list.last().?);

    // Internal links in range should be preserved
    try testing.expectEqual(&n3, TestList.next(&n2).?);

    // Relink at end
    list.linkRangeAfter(&n2, &n3, 2, &n4);

    try testing.expectEqual(@as(usize, 4), list.length());
    try testing.expectEqual(&n2, TestList.next(&n4).?);
    try testing.expectEqual(&n3, TestList.next(&n2).?);
    try testing.expectEqual(&n3, list.last().?);
}
