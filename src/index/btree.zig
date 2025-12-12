///! Generic B+ tree implementation for index storage.
///!
///! A B+ tree optimized for range scans with all values stored in leaf nodes.
///! Internal nodes only contain keys for routing. Leaf nodes are linked for
///! efficient range iteration.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Order = std.math.Order;

/// Generic B+ tree with configurable key and value types.
/// K must implement `fn order(K, K) Order` for comparison.
pub fn BPlusTree(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();

        /// B+ tree branching factor. Each node holds up to ORDER-1 keys.
        pub const ORDER: usize = 32;
        const MIN_KEYS: usize = ORDER / 2;

        root: ?*Node = null,
        allocator: Allocator,
        count: usize = 0,

        /// Internal or leaf node
        pub const Node = struct {
            keys: KeyArray = .{ .buffer = undefined, .len = 0 },
            is_leaf: bool,
            parent: ?*Node = null,
            /// Total number of entries in the subtree rooted at this node.
            /// For leaf nodes: equals keys.len
            /// For internal nodes: sum of all children's subtree_count
            subtree_count: u64 = 0,

            /// Union of internal node children or leaf node values
            data: union {
                /// For internal nodes: child pointers
                children: ChildArray,
                /// For leaf nodes: values and sibling links
                leaf: struct {
                    values: ValueArray = .{ .buffer = undefined, .len = 0 },
                    next: ?*Node = null,
                    prev: ?*Node = null,
                },
            },

            fn initLeaf() Node {
                return .{
                    .is_leaf = true,
                    .data = .{ .leaf = .{} },
                };
            }

            fn initInternal() Node {
                return .{
                    .is_leaf = false,
                    .data = .{ .children = .{ .buffer = undefined, .len = 0 } },
                };
            }
        };

        // Custom bounded arrays since std.BoundedArray was removed
        const KeyArray = struct {
            buffer: [ORDER - 1]K,
            len: usize,

            fn slice(self: *const KeyArray) []const K {
                return self.buffer[0..self.len];
            }

            fn constSlice(self: *const KeyArray) []const K {
                return self.buffer[0..self.len];
            }

            fn appendAssumeCapacity(self: *KeyArray, item: K) void {
                self.buffer[self.len] = item;
                self.len += 1;
            }

            fn insert(self: *KeyArray, index: usize, item: K) !void {
                if (self.len >= ORDER - 1) return error.NoSpace;
                // Shift elements right
                var i = self.len;
                while (i > index) : (i -= 1) {
                    self.buffer[i] = self.buffer[i - 1];
                }
                self.buffer[index] = item;
                self.len += 1;
            }

            fn orderedRemove(self: *KeyArray, index: usize) K {
                const item = self.buffer[index];
                var i = index;
                while (i < self.len - 1) : (i += 1) {
                    self.buffer[i] = self.buffer[i + 1];
                }
                self.len -= 1;
                return item;
            }
        };

        const ValueArray = struct {
            buffer: [ORDER - 1]V,
            len: usize,

            fn appendAssumeCapacity(self: *ValueArray, item: V) void {
                self.buffer[self.len] = item;
                self.len += 1;
            }

            fn insert(self: *ValueArray, index: usize, item: V) !void {
                if (self.len >= ORDER - 1) return error.NoSpace;
                var i = self.len;
                while (i > index) : (i -= 1) {
                    self.buffer[i] = self.buffer[i - 1];
                }
                self.buffer[index] = item;
                self.len += 1;
            }

            fn orderedRemove(self: *ValueArray, index: usize) V {
                const item = self.buffer[index];
                var i = index;
                while (i < self.len - 1) : (i += 1) {
                    self.buffer[i] = self.buffer[i + 1];
                }
                self.len -= 1;
                return item;
            }
        };

        const ChildArray = struct {
            buffer: [ORDER]*Node,
            len: usize,

            fn slice(self: *const ChildArray) []*Node {
                return @constCast(self.buffer[0..self.len]);
            }

            fn appendAssumeCapacity(self: *ChildArray, item: *Node) void {
                self.buffer[self.len] = item;
                self.len += 1;
            }

            fn insert(self: *ChildArray, index: usize, item: *Node) !void {
                if (self.len >= ORDER) return error.NoSpace;
                var i = self.len;
                while (i > index) : (i -= 1) {
                    self.buffer[i] = self.buffer[i - 1];
                }
                self.buffer[index] = item;
                self.len += 1;
            }
        };

        pub fn init(allocator: Allocator) Self {
            return .{
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.root) |root| {
                self.freeNode(root);
            }
            self.root = null;
            self.count = 0;
        }

        fn freeNode(self: *Self, node: *Node) void {
            if (!node.is_leaf) {
                for (node.data.children.slice()) |child| {
                    self.freeNode(child);
                }
            }
            self.allocator.destroy(node);
        }

        /// Insert a key-value pair. If key exists, updates the value.
        pub fn insert(self: *Self, key: K, value: V) !void {
            if (self.root == null) {
                // Create first leaf node
                const leaf = try self.allocator.create(Node);
                leaf.* = Node.initLeaf();
                leaf.keys.appendAssumeCapacity(key);
                leaf.data.leaf.values.appendAssumeCapacity(value);
                leaf.subtree_count = 1;
                self.root = leaf;
                self.count = 1;
                return;
            }

            // Find the leaf node where key belongs
            const leaf = self.findLeaf(key);

            // Find position in leaf
            const pos = self.findKeyPos(leaf, key);

            // Check if key already exists
            if (pos < leaf.keys.len and keyOrder(leaf.keys.buffer[pos], key) == .eq) {
                // Update existing value
                leaf.data.leaf.values.buffer[pos] = value;
                return;
            }

            // Insert new key-value
            if (leaf.keys.len < ORDER - 1) {
                // Room in current leaf
                self.insertInLeaf(leaf, pos, key, value);
                self.count += 1;
            } else {
                // Need to split
                try self.insertAndSplitLeaf(leaf, pos, key, value);
                self.count += 1;
            }
        }

        /// Get value for a key, or null if not found.
        pub fn get(self: *const Self, key: K) ?V {
            if (self.root == null) return null;

            const leaf = self.findLeafConst(key);
            const pos = self.findKeyPosConst(leaf, key);

            if (pos < leaf.keys.len and keyOrder(leaf.keys.buffer[pos], key) == .eq) {
                return leaf.data.leaf.values.buffer[pos];
            }
            return null;
        }

        /// Remove a key. Returns the removed value, or null if not found.
        pub fn remove(self: *Self, key: K) ?V {
            if (self.root == null) return null;

            const leaf = self.findLeaf(key);
            const pos = self.findKeyPos(leaf, key);

            if (pos >= leaf.keys.len or keyOrder(leaf.keys.buffer[pos], key) != .eq) {
                return null;
            }

            const value = leaf.data.leaf.values.buffer[pos];

            // Remove from leaf
            _ = leaf.keys.orderedRemove(pos);
            _ = leaf.data.leaf.values.orderedRemove(pos);
            self.count -= 1;

            // Update subtree counts: decrement leaf and propagate up
            leaf.subtree_count -= 1;
            var ancestor = leaf.parent;
            while (ancestor) |node| {
                node.subtree_count -= 1;
                ancestor = node.parent;
            }

            // Handle underflow (simplified - just check if empty root)
            if (leaf.keys.len == 0 and leaf.parent == null) {
                self.allocator.destroy(leaf);
                self.root = null;
            }
            // Note: Full underflow handling with rebalancing would go here
            // For now we accept some degradation after deletes

            return value;
        }

        /// Range scan returning iterator for keys in [low, high)
        pub fn range(self: *const Self, low: K, high: K) Iterator {
            return .{
                .tree = self,
                .current_node = if (self.root != null) self.findLeafConst(low) else null,
                .current_pos = if (self.root != null) self.findKeyPosConst(self.findLeafConst(low), low) else 0,
                .high = high,
            };
        }

        /// Scan all keys with given prefix (for prefix-based lookups)
        pub fn prefixScan(self: *const Self, prefix: K) Iterator {
            // For prefix scans, we scan from prefix to the next possible key
            // This relies on key ordering where prefix < prefix + any suffix
            return .{
                .tree = self,
                .current_node = if (self.root != null) self.findLeafConst(prefix) else null,
                .current_pos = if (self.root != null) self.findKeyPosConst(self.findLeafConst(prefix), prefix) else 0,
                .high = null, // Will use prefix comparison
                .prefix = prefix,
            };
        }

        /// Scan all entries from the beginning
        pub fn scan(self: *const Self) Iterator {
            var first_leaf: ?*const Node = null;
            if (self.root) |root| {
                var node: *const Node = root;
                while (!node.is_leaf) {
                    node = node.data.children.buffer[0];
                }
                first_leaf = node;
            }
            return .{
                .tree = self,
                .current_node = first_leaf,
                .current_pos = 0,
                .high = null,
            };
        }

        pub const Iterator = struct {
            tree: *const Self,
            current_node: ?*const Node,
            current_pos: usize,
            high: ?K = null,
            prefix: ?K = null,

            pub fn next(self: *Iterator) ?V {
                while (self.current_node) |node| {
                    if (self.current_pos >= node.keys.len) {
                        // Move to next leaf
                        self.current_node = node.data.leaf.next;
                        self.current_pos = 0;
                        continue;
                    }

                    const key = node.keys.buffer[self.current_pos];

                    // Check high bound
                    if (self.high) |high| {
                        if (keyOrder(key, high) != .lt) {
                            self.current_node = null;
                            return null;
                        }
                    }

                    // Check prefix (if doing prefix scan)
                    if (self.prefix) |prefix| {
                        if (!keyHasPrefix(key, prefix)) {
                            self.current_node = null;
                            return null;
                        }
                    }

                    const value = node.data.leaf.values.buffer[self.current_pos];
                    self.current_pos += 1;
                    return value;
                }
                return null;
            }

            /// Skip n entries
            pub fn skip(self: *Iterator, n: u32) void {
                var remaining = n;
                while (remaining > 0 and self.current_node != null) {
                    _ = self.next();
                    remaining -= 1;
                }
            }

            /// Count remaining entries (consumes iterator)
            pub fn countRemaining(self: *Iterator) u32 {
                var cnt: u32 = 0;
                while (self.next() != null) {
                    cnt += 1;
                }
                return cnt;
            }

            /// O(log n) skip to absolute position using subtree counts.
            /// Position 0 is the first element in the tree.
            /// After calling, the iterator is positioned to return element at `target` on next().
            pub fn skipToPosition(self: *Iterator, target: u64) void {
                if (self.tree.root == null) {
                    self.current_node = null;
                    return;
                }

                var remaining = target;
                var current: ?*const Node = self.tree.root;

                while (current) |node| {
                    if (node.is_leaf) {
                        // At leaf: set position within leaf
                        if (remaining < node.keys.len) {
                            self.current_node = node;
                            self.current_pos = @intCast(remaining);
                            return;
                        }
                        // Target beyond this leaf - move to next
                        remaining -= node.keys.len;
                        current = node.data.leaf.next;
                    } else {
                        // At internal node: find child containing position
                        var offset: u64 = 0;
                        var found = false;
                        for (node.data.children.slice()) |child| {
                            if (offset + child.subtree_count > remaining) {
                                // Target is in this subtree
                                remaining -= offset;
                                current = child;
                                found = true;
                                break;
                            }
                            offset += child.subtree_count;
                        }
                        if (!found) {
                            // Position beyond tree - iterator exhausted
                            self.current_node = null;
                            return;
                        }
                    }
                }

                self.current_node = null;
            }

            /// O(1) total count from tree root.
            pub fn totalCount(self: *const Iterator) u64 {
                if (self.tree.root) |root| {
                    return root.subtree_count;
                }
                return 0;
            }
        };

        // ====================================================================
        // Internal helpers
        // ====================================================================

        fn findLeaf(self: *Self, key: K) *Node {
            var node = self.root.?;
            while (!node.is_leaf) {
                const pos = findChildIndex(node.keys.slice(), key);
                node = node.data.children.buffer[pos];
            }
            return node;
        }

        fn findLeafConst(self: *const Self, key: K) *const Node {
            var node: *const Node = self.root.?;
            while (!node.is_leaf) {
                const pos = findChildIndex(node.keys.constSlice(), key);
                node = node.data.children.buffer[pos];
            }
            return node;
        }

        fn findKeyPos(self: *Self, node: *Node, key: K) usize {
            _ = self;
            return findKeyPosInner(node.keys.slice(), key);
        }

        fn findKeyPosConst(self: *const Self, node: *const Node, key: K) usize {
            _ = self;
            return findKeyPosInner(node.keys.constSlice(), key);
        }

        fn findKeyPosInner(keys: []const K, key: K) usize {
            // For leaf nodes: find first index where keys[i] >= key
            // This is used for insertion/lookup position
            var left: usize = 0;
            var right: usize = keys.len;
            while (left < right) {
                const mid = left + (right - left) / 2;
                if (keyOrder(keys[mid], key) == .lt) {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
            return left;
        }

        fn findChildIndex(keys: []const K, key: K) usize {
            // For internal nodes: find which child to descend into
            // Returns index i such that child[i] contains keys in range [keys[i-1], keys[i])
            // For key K, we want the first i where keys[i] > K
            var left: usize = 0;
            var right: usize = keys.len;
            while (left < right) {
                const mid = left + (right - left) / 2;
                if (keyOrder(keys[mid], key) != .gt) {
                    // keys[mid] <= key, search right half
                    left = mid + 1;
                } else {
                    // keys[mid] > key, search left half
                    right = mid;
                }
            }
            return left;
        }

        fn insertInLeaf(self: *Self, leaf: *Node, pos: usize, key: K, value: V) void {
            _ = self;
            leaf.keys.insert(pos, key) catch unreachable;
            leaf.data.leaf.values.insert(pos, value) catch unreachable;

            // Update subtree counts: increment leaf and propagate up
            leaf.subtree_count += 1;
            var ancestor = leaf.parent;
            while (ancestor) |node| {
                node.subtree_count += 1;
                ancestor = node.parent;
            }
        }

        fn insertAndSplitLeaf(self: *Self, leaf: *Node, pos: usize, key: K, value: V) Allocator.Error!void {
            // Create new leaf
            const new_leaf = try self.allocator.create(Node);
            new_leaf.* = Node.initLeaf();

            // Temporary arrays for all keys/values including new one
            var all_keys: [ORDER]K = undefined;
            var all_values: [ORDER]V = undefined;

            // Copy existing + new into temp arrays
            var i: usize = 0;
            var j: usize = 0;
            while (i < ORDER) : (i += 1) {
                if (i == pos) {
                    all_keys[i] = key;
                    all_values[i] = value;
                } else {
                    all_keys[i] = leaf.keys.buffer[j];
                    all_values[i] = leaf.data.leaf.values.buffer[j];
                    j += 1;
                }
            }

            // Split: left gets first half, right gets second half
            const split = ORDER / 2;

            // Reset left leaf
            leaf.keys.len = 0;
            leaf.data.leaf.values.len = 0;
            for (0..split) |idx| {
                leaf.keys.appendAssumeCapacity(all_keys[idx]);
                leaf.data.leaf.values.appendAssumeCapacity(all_values[idx]);
            }

            // Fill new leaf
            for (split..ORDER) |idx| {
                new_leaf.keys.appendAssumeCapacity(all_keys[idx]);
                new_leaf.data.leaf.values.appendAssumeCapacity(all_values[idx]);
            }

            // Set subtree counts for both halves
            // Before split: leaf had ORDER-1 entries (subtree_count = ORDER-1)
            // After split: leaf has 'split' entries, new_leaf has 'ORDER-split' entries
            // Total after = ORDER = ORDER-1 + 1 (the new entry)
            leaf.subtree_count = split;
            new_leaf.subtree_count = ORDER - split;

            // Propagate +1 to all ancestors (we added one new entry)
            var ancestor = leaf.parent;
            while (ancestor) |node| {
                node.subtree_count += 1;
                ancestor = node.parent;
            }

            // Update sibling pointers
            new_leaf.data.leaf.next = leaf.data.leaf.next;
            new_leaf.data.leaf.prev = leaf;
            if (leaf.data.leaf.next) |next| {
                next.data.leaf.prev = new_leaf;
            }
            leaf.data.leaf.next = new_leaf;

            // Propagate split key up
            const split_key = new_leaf.keys.buffer[0];
            try self.insertInParent(leaf, split_key, new_leaf);
        }

        fn insertInParent(self: *Self, left: *Node, key: K, right: *Node) Allocator.Error!void {
            if (left.parent == null) {
                // Create new root
                const new_root = try self.allocator.create(Node);
                new_root.* = Node.initInternal();
                new_root.keys.appendAssumeCapacity(key);
                new_root.data.children.appendAssumeCapacity(left);
                new_root.data.children.appendAssumeCapacity(right);
                new_root.subtree_count = left.subtree_count + right.subtree_count;
                left.parent = new_root;
                right.parent = new_root;
                self.root = new_root;
                return;
            }

            const parent = left.parent.?;
            const pos = self.findChildPos(parent, left);

            if (parent.keys.len < ORDER - 1) {
                // Room in parent
                parent.keys.insert(pos, key) catch unreachable;
                parent.data.children.insert(pos + 1, right) catch unreachable;
                right.parent = parent;
                // Note: subtree_count propagation is NOT done here.
                // - For leaf splits: counts are set in insertAndSplitLeaf, and +1 propagated there
                // - For internal splits: splitInternalNode recalculates from children
            } else {
                // Need to split internal node
                try self.splitInternalNode(parent, pos, key, right);
            }
        }

        fn findChildPos(self: *Self, parent: *Node, child: *Node) usize {
            _ = self;
            for (parent.data.children.slice(), 0..) |c, i| {
                if (c == child) return i;
            }
            unreachable;
        }

        fn splitInternalNode(self: *Self, node: *Node, pos: usize, key: K, right_child: *Node) Allocator.Error!void {
            // Create new internal node
            const new_node = try self.allocator.create(Node);
            new_node.* = Node.initInternal();

            // Temporary arrays
            var all_keys: [ORDER]K = undefined;
            var all_children: [ORDER + 1]*Node = undefined;

            // Copy keys with insertion
            var ki: usize = 0;
            var kj: usize = 0;
            while (ki < ORDER) : (ki += 1) {
                if (ki == pos) {
                    all_keys[ki] = key;
                } else {
                    all_keys[ki] = node.keys.buffer[kj];
                    kj += 1;
                }
            }

            // Copy children with insertion
            var ci: usize = 0;
            var cj: usize = 0;
            while (ci < ORDER + 1) : (ci += 1) {
                if (ci == pos + 1) {
                    all_children[ci] = right_child;
                } else {
                    all_children[ci] = node.data.children.buffer[cj];
                    cj += 1;
                }
            }

            const split = ORDER / 2;
            const push_up_key = all_keys[split];

            // Reset left node
            node.keys.len = 0;
            node.data.children.len = 0;
            for (0..split) |idx| {
                node.keys.appendAssumeCapacity(all_keys[idx]);
            }
            for (0..split + 1) |idx| {
                node.data.children.appendAssumeCapacity(all_children[idx]);
            }

            // Fill new node
            for (split + 1..ORDER) |idx| {
                new_node.keys.appendAssumeCapacity(all_keys[idx]);
            }
            for (split + 1..ORDER + 1) |idx| {
                const child = all_children[idx];
                new_node.data.children.appendAssumeCapacity(child);
                child.parent = new_node;
            }

            // Update parent pointers for left node children
            for (node.data.children.slice()) |child| {
                child.parent = node;
            }

            // Recalculate subtree counts from children
            var left_count: u64 = 0;
            for (node.data.children.slice()) |child| {
                left_count += child.subtree_count;
            }
            node.subtree_count = left_count;

            var right_count: u64 = 0;
            for (new_node.data.children.slice()) |child| {
                right_count += child.subtree_count;
            }
            new_node.subtree_count = right_count;

            // Propagate up
            try self.insertInParent(node, push_up_key, new_node);
        }
    };
}

/// Compare two keys using their order method
fn keyOrder(a: anytype, b: @TypeOf(a)) Order {
    return a.order(b);
}

/// Check if key starts with prefix (for prefix scans)
fn keyHasPrefix(key: anytype, prefix: @TypeOf(key)) bool {
    // Default: just check if key >= prefix
    // Specific implementations can override for true prefix matching
    return keyOrder(key, prefix) != .lt;
}

// ============================================================================
// Unit Tests
// ============================================================================

const TestKey = struct {
    value: i64,

    pub fn order(self: TestKey, other: TestKey) Order {
        return std.math.order(self.value, other.value);
    }
};

test "BPlusTree basic insert and get" {
    var tree = BPlusTree(TestKey, u64).init(std.testing.allocator);
    defer tree.deinit();

    try tree.insert(.{ .value = 5 }, 50);
    try tree.insert(.{ .value = 3 }, 30);
    try tree.insert(.{ .value = 7 }, 70);
    try tree.insert(.{ .value = 1 }, 10);

    try std.testing.expectEqual(@as(?u64, 50), tree.get(.{ .value = 5 }));
    try std.testing.expectEqual(@as(?u64, 30), tree.get(.{ .value = 3 }));
    try std.testing.expectEqual(@as(?u64, 70), tree.get(.{ .value = 7 }));
    try std.testing.expectEqual(@as(?u64, 10), tree.get(.{ .value = 1 }));
    try std.testing.expectEqual(@as(?u64, null), tree.get(.{ .value = 99 }));
}

test "BPlusTree update existing key" {
    var tree = BPlusTree(TestKey, u64).init(std.testing.allocator);
    defer tree.deinit();

    try tree.insert(.{ .value = 5 }, 50);
    try std.testing.expectEqual(@as(?u64, 50), tree.get(.{ .value = 5 }));

    try tree.insert(.{ .value = 5 }, 500);
    try std.testing.expectEqual(@as(?u64, 500), tree.get(.{ .value = 5 }));
    try std.testing.expectEqual(@as(usize, 1), tree.count);
}

test "BPlusTree remove" {
    var tree = BPlusTree(TestKey, u64).init(std.testing.allocator);
    defer tree.deinit();

    try tree.insert(.{ .value = 1 }, 10);
    try tree.insert(.{ .value = 2 }, 20);
    try tree.insert(.{ .value = 3 }, 30);

    try std.testing.expectEqual(@as(?u64, 20), tree.remove(.{ .value = 2 }));
    try std.testing.expectEqual(@as(?u64, null), tree.get(.{ .value = 2 }));
    try std.testing.expectEqual(@as(usize, 2), tree.count);

    try std.testing.expectEqual(@as(?u64, null), tree.remove(.{ .value = 99 }));
}

test "BPlusTree range scan" {
    var tree = BPlusTree(TestKey, u64).init(std.testing.allocator);
    defer tree.deinit();

    for (0..10) |i| {
        try tree.insert(.{ .value = @intCast(i) }, i * 10);
    }

    // Range [3, 7)
    var iter = tree.range(.{ .value = 3 }, .{ .value = 7 });
    try std.testing.expectEqual(@as(?u64, 30), iter.next());
    try std.testing.expectEqual(@as(?u64, 40), iter.next());
    try std.testing.expectEqual(@as(?u64, 50), iter.next());
    try std.testing.expectEqual(@as(?u64, 60), iter.next());
    try std.testing.expectEqual(@as(?u64, null), iter.next());
}

test "BPlusTree scan all" {
    var tree = BPlusTree(TestKey, u64).init(std.testing.allocator);
    defer tree.deinit();

    try tree.insert(.{ .value = 3 }, 30);
    try tree.insert(.{ .value = 1 }, 10);
    try tree.insert(.{ .value = 2 }, 20);

    var iter = tree.scan();
    try std.testing.expectEqual(@as(?u64, 10), iter.next());
    try std.testing.expectEqual(@as(?u64, 20), iter.next());
    try std.testing.expectEqual(@as(?u64, 30), iter.next());
    try std.testing.expectEqual(@as(?u64, null), iter.next());
}

test "BPlusTree many inserts trigger splits" {
    var tree = BPlusTree(TestKey, u64).init(std.testing.allocator);
    defer tree.deinit();

    // Insert enough to cause multiple splits
    for (0..100) |i| {
        try tree.insert(.{ .value = @intCast(i) }, i);
    }

    try std.testing.expectEqual(@as(usize, 100), tree.count);

    // Verify all values retrievable
    for (0..100) |i| {
        const val = tree.get(.{ .value = @intCast(i) });
        try std.testing.expectEqual(@as(?u64, i), val);
    }

    // Verify ordered scan
    var iter = tree.scan();
    var expected: u64 = 0;
    while (iter.next()) |val| {
        try std.testing.expectEqual(expected, val);
        expected += 1;
    }
    try std.testing.expectEqual(@as(u64, 100), expected);
}

test "BPlusTree iterator skip and count" {
    var tree = BPlusTree(TestKey, u64).init(std.testing.allocator);
    defer tree.deinit();

    for (0..20) |i| {
        try tree.insert(.{ .value = @intCast(i) }, i);
    }

    var iter = tree.scan();
    iter.skip(5);
    try std.testing.expectEqual(@as(?u64, 5), iter.next());

    var iter2 = tree.scan();
    iter2.skip(10);
    const remaining = iter2.countRemaining();
    try std.testing.expectEqual(@as(u32, 10), remaining);
}

test "BPlusTree subtree counts correct after inserts" {
    var tree = BPlusTree(TestKey, u64).init(std.testing.allocator);
    defer tree.deinit();

    // Insert elements and verify root subtree_count matches tree.count
    for (0..100) |i| {
        try tree.insert(.{ .value = @intCast(i) }, i);
        try std.testing.expectEqual(@as(u64, i + 1), tree.root.?.subtree_count);
        try std.testing.expectEqual(i + 1, tree.count);
    }
}

test "BPlusTree subtree counts correct after removes" {
    var tree = BPlusTree(TestKey, u64).init(std.testing.allocator);
    defer tree.deinit();

    // Insert 50 elements
    for (0..50) |i| {
        try tree.insert(.{ .value = @intCast(i) }, i);
    }
    try std.testing.expectEqual(@as(u64, 50), tree.root.?.subtree_count);

    // Remove elements and verify counts decrease
    for (0..25) |i| {
        _ = tree.remove(.{ .value = @intCast(i) });
        try std.testing.expectEqual(@as(u64, 50 - i - 1), tree.root.?.subtree_count);
    }
}

test "BPlusTree totalCount returns correct count" {
    var tree = BPlusTree(TestKey, u64).init(std.testing.allocator);
    defer tree.deinit();

    // Empty tree
    var iter = tree.scan();
    try std.testing.expectEqual(@as(u64, 0), iter.totalCount());

    // Add elements
    for (0..75) |i| {
        try tree.insert(.{ .value = @intCast(i) }, i);
    }

    var iter2 = tree.scan();
    try std.testing.expectEqual(@as(u64, 75), iter2.totalCount());
}

test "BPlusTree skipToPosition finds correct element" {
    var tree = BPlusTree(TestKey, u64).init(std.testing.allocator);
    defer tree.deinit();

    // Insert 100 elements
    for (0..100) |i| {
        try tree.insert(.{ .value = @intCast(i) }, i);
    }

    // Test skipToPosition at various positions
    {
        var iter = tree.scan();
        iter.skipToPosition(0);
        try std.testing.expectEqual(@as(?u64, 0), iter.next());
    }
    {
        var iter = tree.scan();
        iter.skipToPosition(50);
        try std.testing.expectEqual(@as(?u64, 50), iter.next());
    }
    {
        var iter = tree.scan();
        iter.skipToPosition(99);
        try std.testing.expectEqual(@as(?u64, 99), iter.next());
        try std.testing.expectEqual(@as(?u64, null), iter.next());
    }
    {
        // Position beyond end
        var iter = tree.scan();
        iter.skipToPosition(100);
        try std.testing.expectEqual(@as(?u64, null), iter.next());
    }
}

test "BPlusTree skipToPosition works with splits" {
    var tree = BPlusTree(TestKey, u64).init(std.testing.allocator);
    defer tree.deinit();

    // Insert enough to cause multiple splits (ORDER = 32)
    const count: usize = 200;
    for (0..count) |i| {
        try tree.insert(.{ .value = @intCast(i) }, i);
    }

    // Verify skipToPosition works at every position
    for (0..count) |pos| {
        var iter = tree.scan();
        iter.skipToPosition(@intCast(pos));
        const val = iter.next();
        try std.testing.expectEqual(@as(?u64, pos), val);
    }
}

test "BPlusTree skipToPosition matches skip behavior" {
    var tree = BPlusTree(TestKey, u64).init(std.testing.allocator);
    defer tree.deinit();

    for (0..100) |i| {
        try tree.insert(.{ .value = @intCast(i) }, i);
    }

    // Compare skipToPosition with skip for various offsets
    const test_offsets = [_]u32{ 0, 1, 15, 16, 31, 32, 50, 63, 64, 99 };
    for (test_offsets) |offset| {
        var iter1 = tree.scan();
        var iter2 = tree.scan();

        iter1.skip(offset);
        iter2.skipToPosition(@intCast(offset));

        const val1 = iter1.next();
        const val2 = iter2.next();
        try std.testing.expectEqual(val1, val2);
    }
}
