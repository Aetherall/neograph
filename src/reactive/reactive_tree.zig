///! Unified Reactive Tree for virtualized hierarchical data.
///!
///! This module provides a single source of truth for reactive tree state,
///! combining hierarchical structure, sort order, visibility tracking,
///! expansion state, and flattened traversal into one coherent structure.
///!
///! Key guarantees:
///! - O(1) scroll up/down operations
///! - O(1) index lookup with lazy caching
///! - Atomic visibility chain updates (no desync possible)
///! - Expansion state tied to node lifecycle
///! - Visibility counts propagated up tree automatically

const std = @import("std");
const Allocator = std.mem.Allocator;
const Order = std.math.Order;

const NodeId = @import("../node.zig").NodeId;
const CompoundKey = @import("../index/key.zig").CompoundKey;
const profiling = @import("../profiling.zig");

// ============================================================================
// TreeNode
// ============================================================================

/// A node in the reactive tree.
///
/// Each node participates in three linked structures:
/// 1. Hierarchy: parent → edges → children
/// 2. Sibling order: prev_sibling ↔ next_sibling (sorted within parent)
/// 3. Visible chain: prev_visible ↔ next_visible (DFS order of visible nodes)
pub const TreeNode = struct {
    // === Identity ===
    id: NodeId,
    sort_key: CompoundKey,

    // === Hierarchy ===
    parent: ?*TreeNode = null,
    edges: std.StringHashMapUnmanaged(EdgeChildren) = .{},

    // === Sibling order (within same edge of parent) ===
    edge_name: ?[]const u8 = null, // Which edge of parent this node belongs to
    prev_sibling: ?*TreeNode = null,
    next_sibling: ?*TreeNode = null,

    // === Flattened visible chain (DFS traversal order) ===
    prev_visible: ?*TreeNode = null,
    next_visible: ?*TreeNode = null,

    // === Visibility ===
    /// Number of visible nodes in this subtree (self + visible descendants).
    /// Updated automatically when expansion changes.
    visible_count: u32 = 1,

    /// Set of expanded edge names. Owned by node, deleted with node.
    expanded_edges: std.StringHashMapUnmanaged(void) = .{},

    // === Cached flat index ===
    flat_index: u32 = 0,
    index_valid: bool = false,

    // === Depth (for lazy loading where parent may not be in tree) ===
    depth: u32 = 0,

    const Self = @This();

    /// Initialize a new tree node.
    pub fn init(id: NodeId, sort_key: CompoundKey) Self {
        return .{
            .id = id,
            .sort_key = sort_key,
        };
    }

    /// Free all resources owned by this node.
    /// Does NOT free child nodes - tree is responsible for that.
    pub fn deinit(self: *Self, allocator: Allocator) void {
        // Free expanded_edges keys that were duplicated (not shared with edges)
        var exp_iter = self.expanded_edges.iterator();
        while (exp_iter.next()) |entry| {
            // Check by POINTER if the key is shared with edges HashMap
            // If pointers differ (or edges doesn't have this key), we own it
            const edges_key = self.edges.getKey(entry.key_ptr.*);
            if (edges_key == null or edges_key.?.ptr != entry.key_ptr.*.ptr) {
                allocator.free(entry.key_ptr.*);
            }
        }
        self.expanded_edges.deinit(allocator);

        // Free edge children structures (but not the child nodes themselves)
        var edge_iter = self.edges.iterator();
        while (edge_iter.next()) |entry| {
            allocator.free(entry.key_ptr.*);
        }
        self.edges.deinit(allocator);
    }

    /// Check if an edge is expanded.
    pub fn isExpanded(self: *const Self, edge_name: []const u8) bool {
        return self.expanded_edges.contains(edge_name);
    }

    /// Mark an edge as expanded, owning the key.
    /// Uses the owned key from edges HashMap if available, otherwise duplicates.
    pub fn markExpanded(self: *Self, allocator: Allocator, edge_name: []const u8) !void {
        if (self.expanded_edges.contains(edge_name)) return;

        // Try to use owned key from edges HashMap first
        const key = self.getOwnedEdgeName(edge_name) orelse
            // Edge doesn't exist yet (lazy loading), duplicate the key
            try allocator.dupe(u8, edge_name);

        try self.expanded_edges.put(allocator, key, {});
    }

    /// Mark an edge as collapsed, freeing the key if we own it.
    pub fn markCollapsed(self: *Self, allocator: Allocator, edge_name: []const u8) void {
        // Get the stored key before removing
        const stored_key = self.expanded_edges.getKey(edge_name);
        _ = self.expanded_edges.remove(edge_name);

        // Free the key if it's not shared with edges HashMap (compare by pointer)
        if (stored_key) |key| {
            const edges_key = self.edges.getKey(edge_name);
            if (edges_key == null or edges_key.?.ptr != key.ptr) {
                // Key was duplicated for expanded_edges, free it
                allocator.free(key);
            }
        }
    }

    /// Clear all expanded edges.
    pub fn clearExpanded(self: *Self, allocator: Allocator) void {
        var iter = self.expanded_edges.iterator();
        while (iter.next()) |entry| {
            // Free the key if it's not shared with edges HashMap
            const edges_key = self.edges.getKey(entry.key_ptr.*);
            if (edges_key == null or edges_key.?.ptr != entry.key_ptr.*.ptr) {
                allocator.free(@constCast(entry.key_ptr.*));
            }
        }
        self.expanded_edges.clearRetainingCapacity();
    }

    /// Get children for an edge.
    pub fn getEdge(self: *const Self, edge_name: []const u8) ?*const EdgeChildren {
        return self.edges.getPtr(edge_name);
    }

    /// Get mutable children for an edge.
    pub fn getEdgeMut(self: *Self, edge_name: []const u8) ?*EdgeChildren {
        return self.edges.getPtr(edge_name);
    }

    /// Get the owned edge name key from the edges HashMap.
    /// Returns the internal owned slice that lives as long as the edge exists.
    /// This is used to avoid storing dangling pointers in child nodes.
    pub fn getOwnedEdgeName(self: *const Self, edge_name: []const u8) ?[]const u8 {
        return self.edges.getKey(edge_name);
    }

    /// Get or create edge children structure.
    pub fn getOrCreateEdge(self: *Self, allocator: Allocator, edge_name: []const u8) !*EdgeChildren {
        const result = try self.edges.getOrPut(allocator, edge_name);
        if (!result.found_existing) {
            // Duplicate the key so we own it
            const owned_key = try allocator.dupe(u8, edge_name);
            result.key_ptr.* = owned_key;
            result.value_ptr.* = .{};
        }
        return result.value_ptr;
    }

    /// Count total children across all edges.
    pub fn totalChildCount(self: *const Self) u32 {
        var count: u32 = 0;
        var iter = self.edges.iterator();
        while (iter.next()) |entry| {
            count += entry.value_ptr.count;
        }
        return count;
    }

    /// Find last visible descendant based on expanded_edges structure.
    /// Returns self if no expanded edges with children.
    ///
    /// WARNING: This function iterates edges using HashMap which has undefined
    /// iteration order. For nodes with multiple expanded edges, the result may
    /// not match the actual visible chain order. Use ReactiveTree.findLastVisible()
    /// instead when you need the actual last node in the visible chain.
    ///
    /// This function is kept for backwards compatibility but should be avoided.
    pub fn lastVisibleDescendant(self: *Self) *TreeNode {
        var current: *TreeNode = self;

        // Keep going down while we have expanded edges with children
        while (true) {
            var last_tail: ?*TreeNode = null;

            // Find the LAST expanded edge's tail (iterate all, keep last found)
            var edge_iter = current.edges.iterator();
            while (edge_iter.next()) |entry| {
                if (current.expanded_edges.contains(entry.key_ptr.*)) {
                    if (entry.value_ptr.tail) |tail| {
                        last_tail = tail;
                    }
                }
            }

            if (last_tail) |tail| {
                current = tail;
            } else {
                break;
            }
        }

        return current;
    }
};

/// Children of a specific edge.
pub const EdgeChildren = struct {
    head: ?*TreeNode = null,
    tail: ?*TreeNode = null,
    count: u32 = 0,
    total: u32 = 0, // Total count including unloaded (for pagination)
    loaded: bool = false,
    loading: bool = false,

    /// Check if more children need to be loaded.
    pub fn needsMore(self: *const EdgeChildren) bool {
        return self.count < self.total;
    }
};

// ============================================================================
// VisibleChainObserver
// ============================================================================

/// Observer for visible chain mutations.
///
/// The ReactiveTree calls these callbacks when the visible chain changes,
/// allowing external code (like Viewport) to react without the tree knowing
/// about viewport concepts.
///
/// Timing guarantees:
/// - on_will_remove: Called BEFORE nodes are unlinked (nodes still traversable)
/// - on_did_remove: Called AFTER nodes are unlinked (chain updated)
/// - on_did_insert: Called AFTER nodes are linked (nodes traversable)
/// - on_did_move: Called AFTER node moved to new position
pub const VisibleChainObserver = struct {
    /// Called BEFORE nodes are unlinked from visible chain.
    /// Use: emit on_leave for nodes in viewport range.
    /// Parameters:
    ///   - first: First node being removed (still in chain)
    ///   - start_index: Index of first node
    ///   - count: Number of nodes being removed
    on_will_remove: ?*const fn (
        ctx: ?*anyopaque,
        first: *TreeNode,
        start_index: u32,
        count: u32,
    ) void = null,

    /// Called AFTER nodes are unlinked from visible chain.
    /// Use: detect scroll-in, emit on_enter for nodes now in viewport.
    /// Parameters:
    ///   - removed_at_index: Index where removal occurred
    ///   - count: Number of nodes removed
    ///   - new_total: New total visible count
    on_did_remove: ?*const fn (
        ctx: ?*anyopaque,
        removed_at_index: u32,
        count: u32,
        new_total: u32,
    ) void = null,

    /// Called AFTER nodes are linked into visible chain.
    /// Use: emit on_enter for inserted nodes in viewport,
    ///      emit on_leave for nodes pushed out.
    /// Parameters:
    ///   - first: First inserted node
    ///   - start_index: Index of first inserted node
    ///   - count: Number of nodes inserted
    ///   - new_total: New total visible count
    on_did_insert: ?*const fn (
        ctx: ?*anyopaque,
        first: *TreeNode,
        start_index: u32,
        count: u32,
        new_total: u32,
    ) void = null,

    /// Called AFTER a node moved to a new position (sort key change).
    /// Use: emit on_move if both indices in viewport,
    ///      or on_enter/on_leave if crossing boundary.
    /// Parameters:
    ///   - node: The node that moved
    ///   - old_index: Previous index
    ///   - new_index: New index
    on_did_move: ?*const fn (
        ctx: ?*anyopaque,
        node: *TreeNode,
        old_index: u32,
        new_index: u32,
    ) void = null,

    context: ?*anyopaque = null,
};

// ============================================================================
// ReactiveTree
// ============================================================================

/// A reactive tree with integrated virtualization support.
///
/// The tree maintains three invariants:
/// 1. visible_count of each node = 1 + sum of visible descendants
/// 2. prev_visible/next_visible form a valid DFS traversal of visible nodes
/// 3. flat_index is valid (or index_valid is false)
pub const ReactiveTree = struct {
    allocator: Allocator,

    // === Root nodes (sorted) ===
    roots_head: ?*TreeNode = null,
    roots_tail: ?*TreeNode = null,
    roots_count: u32 = 0,

    // === Flattened visible chain ===
    visible_head: ?*TreeNode = null,
    visible_tail: ?*TreeNode = null,
    total_visible: u32 = 0,

    // === Index cache state ===
    indices_dirty: bool = true,
    dirty_from_index: u32 = 0,

    // === Node storage (for memory management) ===
    all_nodes: std.AutoHashMapUnmanaged(NodeId, *TreeNode) = .{},

    // === Observer for visible chain changes ===
    observer: VisibleChainObserver = .{},

    const Self = @This();

    /// Child entry for setChildren - exported for external use
    pub const ChildEntry = struct { id: NodeId, sort_key: CompoundKey };

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        // Free all nodes
        var iter = self.all_nodes.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.*.deinit(self.allocator);
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.all_nodes.deinit(self.allocator);
    }

    /// Clear all nodes without deallocating the tree structure.
    /// Used for viewport-based reloading.
    pub fn clear(self: *Self) void {
        // Free all nodes
        var iter = self.all_nodes.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.*.deinit(self.allocator);
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.all_nodes.clearRetainingCapacity();

        // Reset all state
        self.roots_head = null;
        self.roots_tail = null;
        self.roots_count = 0;
        self.visible_head = null;
        self.visible_tail = null;
        self.total_visible = 0;
    }

    // ========================================================================
    // Node Lookup
    // ========================================================================

    /// Get a node by ID. O(1).
    pub fn get(self: *const Self, id: NodeId) ?*TreeNode {
        return self.all_nodes.get(id);
    }

    /// Check if a node exists. O(1).
    pub fn contains(self: *const Self, id: NodeId) bool {
        return self.all_nodes.contains(id);
    }

    // ========================================================================
    // Root Operations
    // ========================================================================

    /// Insert a new root node in sorted position.
    /// Returns the created node.
    pub fn insertRoot(self: *Self, id: NodeId, sort_key: CompoundKey) !*TreeNode {
        // Create node
        const node = try self.allocator.create(TreeNode);
        errdefer self.allocator.destroy(node);
        node.* = TreeNode.init(id, sort_key);
        profiling.global.countNodeCreated();

        // Register in all_nodes
        try self.all_nodes.put(self.allocator, id, node);
        errdefer _ = self.all_nodes.remove(id);

        // Find sorted position among roots
        const after = self.findRootInsertPosition(sort_key);

        // Link into root sibling chain
        self.linkRootAfter(node, after);

        // Link into visible chain
        self.linkVisibleAfter(node, self.findPrevVisibleForRoot(after));

        // Update counts
        self.total_visible += 1;
        self.roots_count += 1;

        // Mark indices dirty
        self.markIndicesDirtyFrom(node.flat_index);

        return node;
    }

    /// Insert a root node at a specific index.
    /// Use this when you know the exact position (e.g., from ResultSet).
    pub fn insertRootAt(self: *Self, id: NodeId, sort_key: CompoundKey, index: u32) !*TreeNode {
        const node = try self.allocator.create(TreeNode);
        errdefer self.allocator.destroy(node);
        node.* = TreeNode.init(id, sort_key);
        profiling.global.countNodeCreated();

        try self.all_nodes.put(self.allocator, id, node);
        errdefer _ = self.all_nodes.remove(id);

        // Find node at index - 1 to insert after
        const after = if (index == 0) null else self.rootAtIndex(index - 1);

        // Link into root sibling chain
        self.linkRootAfter(node, after);

        // Link into visible chain
        const prev_visible = if (after) |a| findLastVisible(a) else null;
        self.linkVisibleAfter(node, prev_visible);

        self.total_visible += 1;
        self.roots_count += 1;
        self.markIndicesDirtyFrom(index);

        // Notify observer AFTER insertion
        if (self.observer.on_did_insert) |cb| {
            cb(self.observer.context, node, index, 1, self.total_visible);
        }

        return node;
    }

    /// Remove a root node and all its descendants.
    pub fn removeRoot(self: *Self, id: NodeId) void {
        const node = self.all_nodes.get(id) orelse return;
        if (node.parent != null) return; // Not a root
        profiling.global.countNodeVisited();

        const removed_visible = node.visible_count;
        const index = self.indexOf(node);

        // Notify observer BEFORE unlinking (nodes still traversable)
        if (self.observer.on_will_remove) |cb| {
            cb(self.observer.context, node, index, removed_visible);
        }

        // Unlink from visible chain (node and all descendants)
        self.unlinkVisibleSubtree(node);

        // Unlink from root chain
        self.unlinkRoot(node);

        // Recursively free node and descendants
        self.freeNodeRecursive(node);

        self.total_visible -= removed_visible;
        self.roots_count -= 1;
        self.markIndicesDirtyFrom(index);

        // Notify observer AFTER unlinking
        if (self.observer.on_did_remove) |cb| {
            cb(self.observer.context, index, removed_visible, self.total_visible);
        }
    }

    /// Move a root node to a new position.
    pub fn moveRoot(self: *Self, id: NodeId, new_index: u32) void {
        const node = self.all_nodes.get(id) orelse return;
        if (node.parent != null) return; // Not a root

        const old_index = self.indexOf(node);
        if (old_index == new_index) return;

        // Unlink from current position
        self.unlinkVisibleSubtree(node);
        self.unlinkRoot(node);

        // Find insert position in the shrunken list.
        // To end up at final index N, we insert after the node at shrunken index N-1.
        // For forward moves: shrunken index N-1 = rootAtIndex(N-1) since one element was removed before N
        // For backward moves: same logic applies
        const after = if (new_index == 0) null else self.rootAtIndex(new_index - 1);

        // Relink at new position
        self.linkRootAfter(node, after);
        const prev_visible = if (after) |a| findLastVisible(a) else null;
        self.relinkVisibleSubtree(node, prev_visible);

        // Mark indices dirty
        self.markIndicesDirtyFrom(@min(old_index, new_index));

        // Notify observer of move
        if (self.observer.on_did_move) |cb| {
            cb(self.observer.context, node, old_index, new_index);
        }
    }

    /// Update a root node's sort key and reposition if needed.
    pub fn updateRootKey(self: *Self, id: NodeId, new_key: CompoundKey) ?struct { old_index: u32, new_index: u32 } {
        const node = self.all_nodes.get(id) orelse return null;
        if (node.parent != null) return null;

        const old_key = node.sort_key;
        if (old_key.order(new_key) == .eq) return null;

        node.sort_key = new_key;

        // Find new position
        const old_index = self.indexOf(node);

        // Temporarily unlink to find correct position
        self.unlinkRoot(node);
        const after = self.findRootInsertPosition(new_key);
        self.linkRootAfter(node, after);

        const new_index = self.computeRootIndex(node);

        if (old_index != new_index) {
            // Need to update visible chain
            self.unlinkVisibleSubtree(node);
            const prev_visible = if (after) |a| findLastVisible(a) else null;
            self.relinkVisibleSubtree(node, prev_visible);
            self.markIndicesDirtyFrom(@min(old_index, new_index));

            // Notify observer of move
            if (self.observer.on_did_move) |cb| {
                cb(self.observer.context, node, old_index, new_index);
            }
        }

        return .{ .old_index = old_index, .new_index = new_index };
    }

    // ========================================================================
    // Child Operations
    // ========================================================================

    /// Set children for an edge. Replaces any existing children.
    pub fn setChildren(
        self: *Self,
        parent_id: NodeId,
        edge_name: []const u8,
        children: []const ChildEntry,
    ) !void {
        const parent = self.all_nodes.get(parent_id) orelse return;
        const edge = try parent.getOrCreateEdge(self.allocator, edge_name);
        profiling.global.countNodeVisited();

        // Get the owned edge name from the parent's edges HashMap
        // IMPORTANT: We must use the owned key, not the passed-in slice which may be freed
        const owned_edge_name = parent.getOwnedEdgeName(edge_name) orelse return;

        // Remove existing children if any
        if (edge.head != null) {
            self.removeEdgeChildren(parent, owned_edge_name);
        }

        // Create and link new children
        var prev_sibling: ?*TreeNode = null;
        for (children) |child_data| {
            const child = try self.allocator.create(TreeNode);
            child.* = TreeNode.init(child_data.id, child_data.sort_key);
            child.parent = parent;
            child.edge_name = owned_edge_name;
            child.depth = parent.depth + 1;
            profiling.global.countNodeCreated();

            try self.all_nodes.put(self.allocator, child_data.id, child);

            // Link as sibling
            child.prev_sibling = prev_sibling;
            if (prev_sibling) |ps| {
                ps.next_sibling = child;
            } else {
                edge.head = child;
            }
            prev_sibling = child;
            edge.count += 1;
        }
        edge.tail = prev_sibling;
        edge.loaded = true;
        profiling.global.countChildrenLinked(children.len);

        // If children would be visible, link them into visible chain and update counts
        if (self.areChildrenVisible(parent, edge_name)) {
            // Get parent index before linking
            const parent_idx = self.indexOf(parent);

            self.linkEdgeChildrenVisible(parent, edge);

            // Update visibility counts - propagateVisibilityDelta updates both
            // ancestor visible_counts and total_visible
            const added_visible: u32 = @intCast(children.len);
            self.propagateVisibilityDelta(parent, @intCast(added_visible));
            self.markIndicesDirtyFrom(parent_idx + 1);

            // Notify observer AFTER insertion
            if (self.observer.on_did_insert) |cb| {
                if (edge.head) |first_child| {
                    cb(self.observer.context, first_child, parent_idx + 1, added_visible, self.total_visible);
                }
            }
        }
    }

    /// Insert a child node into an edge in sorted position.
    pub fn insertChild(
        self: *Self,
        parent_id: NodeId,
        edge_name: []const u8,
        child_id: NodeId,
        sort_key: CompoundKey,
    ) !*TreeNode {
        const parent = self.all_nodes.get(parent_id) orelse return error.ParentNotFound;
        const edge = try parent.getOrCreateEdge(self.allocator, edge_name);
        profiling.global.countNodeVisited();

        // Get the owned edge name from the parent's edges HashMap
        // IMPORTANT: We must use the owned key, not the passed-in slice which may be freed
        const owned_edge_name = parent.getOwnedEdgeName(edge_name) orelse return error.EdgeNotFound;

        // Create child node
        const child = try self.allocator.create(TreeNode);
        errdefer self.allocator.destroy(child);
        child.* = TreeNode.init(child_id, sort_key);
        child.parent = parent;
        child.edge_name = owned_edge_name;
        child.depth = parent.depth + 1;
        profiling.global.countNodeCreated();

        try self.all_nodes.put(self.allocator, child_id, child);
        errdefer _ = self.all_nodes.remove(child_id);

        // Find sorted position among siblings
        const after = self.findChildInsertPosition(edge, sort_key);

        // Link into sibling chain
        child.prev_sibling = after;
        if (after) |a| {
            child.next_sibling = a.next_sibling;
            if (a.next_sibling) |ns| ns.prev_sibling = child;
            a.next_sibling = child;
            if (edge.tail == a) edge.tail = child;
        } else {
            child.next_sibling = edge.head;
            if (edge.head) |h| h.prev_sibling = child;
            edge.head = child;
            if (edge.tail == null) edge.tail = child;
        }
        edge.count += 1;

        // If child would be visible, link into visible chain
        if (self.areChildrenVisible(parent, edge_name)) {
            const prev_visible = self.findPrevVisibleForChild(parent, edge_name, after);
            self.linkVisibleAfter(child, prev_visible);
            self.propagateVisibilityDelta(parent, 1);

            // Get child's index for observer (indices not dirty yet)
            const child_index = self.indexOf(child);
            self.markIndicesDirtyFrom(child_index);

            // Notify observer AFTER insertion
            if (self.observer.on_did_insert) |cb| {
                cb(self.observer.context, child, child_index, 1, self.total_visible);
            }
        }

        return child;
    }

    /// Remove a child node.
    pub fn removeChild(self: *Self, child_id: NodeId) void {
        const child = self.all_nodes.get(child_id) orelse return;
        const parent = child.parent orelse return; // Can't remove root this way
        const edge_name = child.edge_name orelse return;
        const edge = parent.getEdgeMut(edge_name) orelse return;

        // Check if child is actually in the visible chain
        const is_visible = self.areChildrenVisible(parent, edge_name);
        const removed_visible = child.visible_count;
        const index = if (is_visible) self.indexOf(child) else 0;

        // Notify observer BEFORE unlinking (nodes still traversable)
        if (is_visible) {
            if (self.observer.on_will_remove) |cb| {
                cb(self.observer.context, child, index, removed_visible);
            }
        }

        // Unlink from visible chain if visible
        if (is_visible) {
            self.unlinkVisibleSubtree(child);
            self.propagateVisibilityDelta(parent, -@as(i32, @intCast(removed_visible)));
        }

        // Unlink from sibling chain
        if (child.prev_sibling) |ps| ps.next_sibling = child.next_sibling;
        if (child.next_sibling) |ns| ns.prev_sibling = child.prev_sibling;
        if (edge.head == child) edge.head = child.next_sibling;
        if (edge.tail == child) edge.tail = child.prev_sibling;
        edge.count -= 1;

        // Free node and descendants
        self.freeNodeRecursive(child);

        if (is_visible) {
            self.markIndicesDirtyFrom(index);

            // Notify observer AFTER unlinking
            if (self.observer.on_did_remove) |cb| {
                cb(self.observer.context, index, removed_visible, self.total_visible);
            }
        }
    }

    // ========================================================================
    // Expansion Operations
    // ========================================================================

    /// Expand an edge, making its children visible.
    /// Can be called before children are loaded (for lazy loading pattern).
    /// If the node itself is not visible (ancestor collapsed), only marks the edge
    /// as expanded and updates local visible_count - children will be linked when
    /// an ancestor is expanded (via linkEdgeChildrenVisible recursion).
    pub fn expand(self: *Self, node_id: NodeId, edge_name: []const u8) void {
        const node = self.all_nodes.get(node_id) orelse return;
        if (node.expanded_edges.contains(edge_name)) return; // Already expanded
        profiling.global.countNodeVisited();
        profiling.global.countEdgeExpanded();

        // Mark as expanded (even if no children yet - supports lazy loading)
        // Uses owned key from edges HashMap if available, otherwise duplicates
        node.markExpanded(self.allocator, edge_name) catch return;

        // If edge has children, compute visibility delta
        const edge = node.getEdge(edge_name) orelse return;
        if (edge.count == 0) return; // No children to show yet

        const added_visible = self.computeEdgeVisibleCount(edge);

        // Always update this node's visible_count to track expanded children.
        // This is needed so that when an ancestor is expanded, computeEdgeVisibleCount
        // returns the correct value including pre-expanded grandchildren.
        node.visible_count += @intCast(added_visible);

        // If node is not visible, don't link children or propagate to ancestors/total.
        // Children will be linked when an ancestor is expanded.
        if (!self.isVisible(node)) return;

        // Get node index BEFORE linking (for observer)
        const node_index = self.indexOf(node);

        // Link children into visible chain
        self.linkEdgeChildrenVisible(node, @constCast(edge));

        // Propagate to ancestors (node already updated above)
        var current = node.parent;
        while (current) |p| {
            p.visible_count += @intCast(added_visible);
            current = p.parent;
        }
        self.total_visible += added_visible;

        // Mark indices dirty
        self.markIndicesDirtyFrom(node_index + 1);

        // Notify observer AFTER insertion is complete
        if (self.observer.on_did_insert) |cb| {
            if (edge.head) |first_child| {
                cb(self.observer.context, first_child, node_index + 1, added_visible, self.total_visible);
            }
        }

        // Debug validation
        if (std.debug.runtime_safety) {
            self.validate() catch |err| {
                std.debug.print("EXPAND validation failed: {}\n", .{err});
                @panic("expand corrupted tree state");
            };
        }
    }

    /// Collapse an edge, hiding its children.
    /// If the node itself is not visible (ancestor collapsed), only marks the edge
    /// as collapsed and updates local visible_count.
    pub fn collapse(self: *Self, node_id: NodeId, edge_name: []const u8) void {
        const node = self.all_nodes.get(node_id) orelse return;
        if (!node.expanded_edges.contains(edge_name)) return; // Already collapsed
        profiling.global.countNodeVisited();
        profiling.global.countEdgeCollapsed();

        const edge = node.getEdge(edge_name) orelse return;

        // Compute how many visible nodes we're removing
        const removed_visible = self.computeEdgeVisibleCount(edge);

        // Mark as collapsed (frees key if we duplicated it)
        node.markCollapsed(self.allocator, edge_name);

        // Always update this node's visible_count
        node.visible_count -= @intCast(removed_visible);

        // If node is not visible, children aren't in visible chain - nothing more to do
        if (!self.isVisible(node)) return;

        const node_index = self.indexOf(node);

        // Notify observer BEFORE unlinking (nodes still traversable)
        if (self.observer.on_will_remove) |cb| {
            if (edge.head) |first_child| {
                cb(self.observer.context, first_child, node_index + 1, removed_visible);
            }
        }

        // Unlink children from visible chain
        self.unlinkEdgeChildrenVisible(node, edge);

        // Propagate to ancestors (node already updated above)
        var current = node.parent;
        while (current) |p| {
            p.visible_count -= @intCast(removed_visible);
            current = p.parent;
        }
        self.total_visible -= removed_visible;

        // Mark indices dirty
        self.markIndicesDirtyFrom(node_index + 1);

        // Notify observer AFTER unlinking is complete
        if (self.observer.on_did_remove) |cb| {
            cb(self.observer.context, node_index + 1, removed_visible, self.total_visible);
        }

        // Debug validation
        if (std.debug.runtime_safety) {
            self.validate() catch |err| {
                std.debug.print("COLLAPSE validation failed: {}\n", .{err});
                @panic("collapse corrupted tree state");
            };
        }
    }

    /// Toggle expansion state.
    pub fn toggleExpand(self: *Self, node_id: NodeId, edge_name: []const u8) bool {
        const node = self.all_nodes.get(node_id) orelse return false;
        if (node.expanded_edges.contains(edge_name)) {
            self.collapse(node_id, edge_name);
            return false;
        } else {
            self.expand(node_id, edge_name);
            return true;
        }
    }

    // ========================================================================
    // Index Operations
    // ========================================================================

    /// Get the flat index of a node. O(1) if indices valid, O(n) to recompute.
    pub fn indexOf(self: *Self, node: *TreeNode) u32 {
        self.ensureIndicesValid();
        return node.flat_index;
    }

    /// Get the flat index of a node by ID.
    pub fn indexOfId(self: *Self, id: NodeId) ?u32 {
        const node = self.all_nodes.get(id) orelse return null;
        // Check if node is actually visible
        if (!self.isVisible(node)) return null;
        return self.indexOf(node);
    }

    /// Get node at flat index. O(index) walk from head.
    pub fn nodeAtIndex(self: *Self, index: u32) ?*TreeNode {
        if (index >= self.total_visible) return null;

        // Walk from whichever end is closer
        if (index <= self.total_visible / 2) {
            var current = self.visible_head;
            var i: u32 = 0;
            while (i < index and current != null) : (i += 1) {
                current = current.?.next_visible;
                profiling.global.countNodeVisited();
            }
            return current;
        } else {
            var current = self.visible_tail;
            var i: u32 = self.total_visible - 1;
            while (i > index and current != null) : (i -= 1) {
                current = current.?.prev_visible;
                profiling.global.countNodeVisited();
            }
            return current;
        }
    }

    /// Ensure all flat indices are valid.
    pub fn ensureIndicesValid(self: *Self) void {
        if (!self.indices_dirty) {
            profiling.global.countCacheHit();
            return;
        }
        profiling.global.countCacheMiss();

        var idx: u32 = 0;
        var current = self.visible_head;
        while (current) |node| : (idx += 1) {
            node.flat_index = idx;
            node.index_valid = true;
            current = node.next_visible;
            profiling.global.countIndexComputation();
        }

        self.indices_dirty = false;
    }

    /// Mark indices as needing recomputation.
    fn markIndicesDirtyFrom(self: *Self, from_index: u32) void {
        if (!self.indices_dirty) {
            self.dirty_from_index = from_index;
            self.indices_dirty = true;
            profiling.global.countCacheInvalidation();
        } else {
            self.dirty_from_index = @min(self.dirty_from_index, from_index);
        }
    }

    // ========================================================================
    // Visibility Helpers
    // ========================================================================

    /// Check if a node is currently visible in the flattened tree.
    /// A node is visible if all ancestors have the path to it expanded.
    fn isVisible(_: *const Self, node: *const TreeNode) bool {
        var current: ?*const TreeNode = node;
        while (current) |n| {
            if (n.parent) |parent| {
                const edge_name = n.edge_name orelse return false;
                if (!parent.expanded_edges.contains(edge_name)) return false;
            }
            current = n.parent;
        }
        return true;
    }

    /// Check if an edge's children are currently in the visible chain.
    /// This is the ONLY correct way to check before linking/unlinking children.
    /// Children are visible iff: edge is expanded AND parent is visible.
    fn areChildrenVisible(self: *const Self, parent: *const TreeNode, edge_name: []const u8) bool {
        return parent.expanded_edges.contains(edge_name) and self.isVisible(parent);
    }

    /// Find the last visible node in a subtree by walking the visible chain.
    /// This is more reliable than lastVisibleDescendant() which uses expanded_edges
    /// and can fail with multiple expanded edges (HashMap iteration order issues).
    fn findLastVisible(node: *TreeNode) *TreeNode {
        var last = node;
        var remaining: u32 = node.visible_count - 1;
        while (remaining > 0) : (remaining -= 1) {
            last = last.next_visible orelse break;
        }
        return last;
    }

    /// Propagate visibility delta up the tree.
    fn propagateVisibilityDelta(self: *Self, start: ?*TreeNode, delta: i32) void {
        var current = start;
        var propagation_depth: u64 = 0;
        while (current) |node| {
            const new_count = @as(i64, node.visible_count) + delta;
            node.visible_count = @intCast(@max(0, new_count));
            current = node.parent;
            propagation_depth += 1;
        }
        const new_total = @as(i64, self.total_visible) + delta;
        self.total_visible = @intCast(@max(0, new_total));
        profiling.global.countVisibilityPropagation(delta);
        profiling.global.countNodesVisited(propagation_depth);
    }

    /// Compute visible count for an edge's children.
    fn computeEdgeVisibleCount(self: *const Self, edge: *const EdgeChildren) u32 {
        var count: u32 = 0;
        var child = edge.head;
        while (child) |c| {
            count += c.visible_count;
            child = c.next_sibling;
        }
        _ = self;
        return count;
    }

    // ========================================================================
    // Visible Chain Helpers
    // ========================================================================

    /// Link a node into the visible chain after the given node.
    /// Precondition: node must not already be in the visible chain (prev/next must be null).
    fn linkVisibleAfter(self: *Self, node: *TreeNode, after: ?*TreeNode) void {
        // Check precondition with diagnostic info
        if (node.prev_visible != null or node.next_visible != null) {
            // Write to file for debugging
            if (std.fs.cwd().createFile("/tmp/neograph_crash.log", .{})) |file| {
                defer file.close();
                var buf: [4096]u8 = undefined;
                const msg = std.fmt.bufPrint(&buf, "BUG: linkVisibleAfter node={d} prev={?d} next={?d} after={?d} isVisible={} parent={?d} edge={?s}\n", .{
                    node.id,
                    if (node.prev_visible) |p| p.id else null,
                    if (node.next_visible) |n| n.id else null,
                    if (after) |a| a.id else null,
                    self.isVisible(node),
                    if (node.parent) |p| p.id else null,
                    node.edge_name,
                }) catch "format error";
                _ = file.writeAll(msg) catch {};
                // Walk up tree to find where visibility breaks
                var current: ?*const TreeNode = node;
                while (current) |n| {
                    if (n.parent) |parent| {
                        const en = n.edge_name orelse "null";
                        const expanded = if (n.edge_name) |e| parent.expanded_edges.contains(e) else false;
                        const msg2 = std.fmt.bufPrint(&buf, "  ancestor: {d} -> {d} via '{s}' expanded={}\n", .{
                            parent.id, n.id, en, expanded,
                        }) catch "format error";
                        _ = file.writeAll(msg2) catch {};
                    }
                    current = n.parent;
                }
            } else |_| {}
            @panic("linkVisibleAfter: node already has visible pointers");
        }

        node.prev_visible = after;
        if (after) |a| {
            node.next_visible = a.next_visible;
            if (a.next_visible) |nv| nv.prev_visible = node;
            a.next_visible = node;
            if (self.visible_tail == a) self.visible_tail = node;
        } else {
            node.next_visible = self.visible_head;
            if (self.visible_head) |vh| vh.prev_visible = node;
            self.visible_head = node;
            if (self.visible_tail == null) self.visible_tail = node;
        }
    }

    /// Unlink a node from the visible chain.
    fn unlinkVisible(self: *Self, node: *TreeNode) void {
        if (node.prev_visible) |pv| pv.next_visible = node.next_visible;
        if (node.next_visible) |nv| nv.prev_visible = node.prev_visible;
        if (self.visible_head == node) self.visible_head = node.next_visible;
        if (self.visible_tail == node) self.visible_tail = node.prev_visible;
        node.prev_visible = null;
        node.next_visible = null;
    }

    /// Unlink a node and all its visible descendants from the visible chain.
    /// Clears ALL prev_visible/next_visible pointers in the subtree to maintain
    /// the invariant that unlinked nodes have null pointers.
    ///
    /// Uses visible_count to know exactly how many nodes to unlink, avoiding
    /// any issues with lastVisibleDescendant and multiple expanded edges.
    fn unlinkVisibleSubtree(self: *Self, root: *TreeNode) void {
        const count = root.visible_count;
        const last = findLastVisible(root);
        const after_last = last.next_visible;

        // Splice out the entire range [root, last]
        if (root.prev_visible) |pv| pv.next_visible = after_last;
        if (after_last) |nv| nv.prev_visible = root.prev_visible;
        if (self.visible_head == root) self.visible_head = after_last;
        if (self.visible_tail == last) self.visible_tail = root.prev_visible;

        // Clear ALL pointers by walking the chain
        var current: ?*TreeNode = root;
        var cleared: u32 = 0;
        while (current) |node| : (cleared += 1) {
            const next = node.next_visible;
            node.prev_visible = null;
            node.next_visible = null;
            if (cleared >= count - 1) break;
            current = next;
        }
    }

    /// Relink a subtree into the visible chain after the given node.
    /// NOTE: This assumes the subtree nodes already have their internal
    /// prev_visible/next_visible pointers set correctly. It just splices
    /// the whole subtree into the main chain.
    fn relinkVisibleSubtree(self: *Self, root: *TreeNode, after: ?*TreeNode) void {
        const last = findLastVisible(root);

        root.prev_visible = after;
        if (after) |a| {
            last.next_visible = a.next_visible;
            if (a.next_visible) |nv| nv.prev_visible = last;
            a.next_visible = root;
            if (self.visible_tail == a) self.visible_tail = last;
        } else {
            last.next_visible = self.visible_head;
            if (self.visible_head) |vh| vh.prev_visible = last;
            self.visible_head = root;
            if (self.visible_tail == null) self.visible_tail = last;
        }
    }

    /// Link all children of an edge into the visible chain.
    ///
    /// This is recursive: if a child has expanded sub-edges, those grandchildren
    /// are also linked. This preserves expansion state across collapse/expand cycles.
    ///
    /// The recursion is necessary because:
    /// 1. Children remember their expanded_edges even when parent is collapsed
    /// 2. When parent re-expands, we want to restore the full visible subtree
    /// 3. After linking a child and its expanded sub-edges, we use findLastVisible()
    ///    to find the insertion point for the next sibling
    fn linkEdgeChildrenVisible(self: *Self, parent: *TreeNode, edge: *EdgeChildren) void {
        var insert_after: *TreeNode = parent;
        var child = edge.head;
        while (child) |c| {
            self.linkVisibleAfter(c, insert_after);

            // Recursively link any expanded sub-edges
            var sub_edge_iter = c.edges.iterator();
            while (sub_edge_iter.next()) |sub_entry| {
                if (c.expanded_edges.contains(sub_entry.key_ptr.*)) {
                    self.linkEdgeChildrenVisible(c, sub_entry.value_ptr);
                }
            }

            // Find last visible descendant by walking the chain
            // After linking above, c.visible_count is correct and the chain is linked
            insert_after = findLastVisible(c);

            child = c.next_sibling;
        }
    }

    /// Unlink all children of an edge from the visible chain.
    fn unlinkEdgeChildrenVisible(self: *Self, parent: *TreeNode, edge: *const EdgeChildren) void {
        _ = parent;
        var child = edge.head;
        while (child) |c| {
            self.unlinkVisibleSubtree(c);
            child = c.next_sibling;
        }
    }

    // ========================================================================
    // Root Chain Helpers
    // ========================================================================

    /// Link a root node after the given root.
    fn linkRootAfter(self: *Self, node: *TreeNode, after: ?*TreeNode) void {
        node.prev_sibling = after;
        if (after) |a| {
            node.next_sibling = a.next_sibling;
            if (a.next_sibling) |ns| ns.prev_sibling = node;
            a.next_sibling = node;
            if (self.roots_tail == a) self.roots_tail = node;
        } else {
            node.next_sibling = self.roots_head;
            if (self.roots_head) |rh| rh.prev_sibling = node;
            self.roots_head = node;
            if (self.roots_tail == null) self.roots_tail = node;
        }
    }

    /// Unlink a root node from the root chain.
    fn unlinkRoot(self: *Self, node: *TreeNode) void {
        if (node.prev_sibling) |ps| ps.next_sibling = node.next_sibling;
        if (node.next_sibling) |ns| ns.prev_sibling = node.prev_sibling;
        if (self.roots_head == node) self.roots_head = node.next_sibling;
        if (self.roots_tail == node) self.roots_tail = node.prev_sibling;
        node.prev_sibling = null;
        node.next_sibling = null;
    }

    /// Find position to insert a root by sort key.
    fn findRootInsertPosition(self: *const Self, key: CompoundKey) ?*TreeNode {
        var current = self.roots_tail;
        while (current) |node| {
            if (node.sort_key.order(key) != .gt) return node;
            current = node.prev_sibling;
        }
        return null;
    }

    /// Find previous visible node for a new root.
    fn findPrevVisibleForRoot(_: *const Self, after_sibling: ?*TreeNode) ?*TreeNode {
        if (after_sibling) |after| {
            return findLastVisible(after);
        }
        return null;
    }

    /// Get root at index. O(index).
    fn rootAtIndex(self: *const Self, index: u32) ?*TreeNode {
        var current = self.roots_head;
        var i: u32 = 0;
        while (i < index and current != null) : (i += 1) {
            current = current.?.next_sibling;
        }
        return current;
    }

    /// Compute root index by counting predecessors.
    fn computeRootIndex(self: *const Self, root: *const TreeNode) u32 {
        var idx: u32 = 0;
        var current = self.roots_head;
        while (current) |node| {
            if (node == root) return idx;
            idx += 1;
            current = node.next_sibling;
        }
        return idx;
    }

    // ========================================================================
    // Child Chain Helpers
    // ========================================================================

    /// Find position to insert a child by sort key.
    fn findChildInsertPosition(self: *const Self, edge: *const EdgeChildren, key: CompoundKey) ?*TreeNode {
        var current = edge.tail;
        while (current) |node| {
            const cmp = node.sort_key.order(key);
            if (cmp != .gt) return node;
            current = node.prev_sibling;
        }
        _ = self;
        return null;
    }

    /// Find previous visible node for inserting a child.
    fn findPrevVisibleForChild(
        self: *const Self,
        parent: *TreeNode,
        edge_name: []const u8,
        after_sibling: ?*TreeNode,
    ) ?*TreeNode {
        if (after_sibling) |after| {
            return findLastVisible(after);
        }

        // Insert as first child - go after parent but before any other expanded edges
        // For simplicity, insert right after parent
        _ = self;
        _ = edge_name;
        return parent;
    }

    /// Remove all children of an edge.
    fn removeEdgeChildren(self: *Self, parent: *TreeNode, edge_name: []const u8) void {
        const edge = parent.getEdgeMut(edge_name) orelse return;
        // Check if children are currently in the visible chain
        const children_visible = self.areChildrenVisible(parent, edge_name);

        // Compute visible count BEFORE freeing children
        const removed_visible = if (children_visible) self.computeEdgeVisibleCount(edge) else 0;

        // Get parent index for observer (if visible)
        const parent_idx = if (children_visible) self.indexOf(parent) else 0;

        // Notify observer BEFORE unlinking (nodes still traversable)
        if (children_visible and removed_visible > 0) {
            if (self.observer.on_will_remove) |cb| {
                if (edge.head) |first_child| {
                    cb(self.observer.context, first_child, parent_idx + 1, removed_visible);
                }
            }
        }

        var child = edge.head;
        while (child) |c| {
            const next = c.next_sibling;
            if (children_visible) {
                self.unlinkVisibleSubtree(c);
            }
            self.freeNodeRecursive(c);
            child = next;
        }

        if (children_visible) {
            self.propagateVisibilityDelta(parent, -@as(i32, @intCast(removed_visible)));

            // Notify observer AFTER unlinking
            if (self.observer.on_did_remove) |cb| {
                cb(self.observer.context, parent_idx + 1, removed_visible, self.total_visible);
            }
        }

        edge.head = null;
        edge.tail = null;
        edge.count = 0;
    }

    // ========================================================================
    // Memory Management
    // ========================================================================

    /// Free a node and all its descendants.
    fn freeNodeRecursive(self: *Self, node: *TreeNode) void {
        // First, recursively free all children
        var edge_iter = node.edges.iterator();
        while (edge_iter.next()) |entry| {
            var child = entry.value_ptr.head;
            while (child) |c| {
                const next = c.next_sibling;
                self.freeNodeRecursive(c);
                child = next;
            }
        }

        // Remove from all_nodes
        _ = self.all_nodes.remove(node.id);
        profiling.global.countNodeRemoved();

        // Free node
        node.deinit(self.allocator);
        self.allocator.destroy(node);
    }

    // ========================================================================
    // Debug/Validation
    // ========================================================================

    /// Validate tree invariants. For testing.
    pub fn validate(self: *Self) !void {
        // Check visible chain length matches total_visible
        var visible_count: u32 = 0;
        var current = self.visible_head;
        while (current) |node| {
            visible_count += 1;
            current = node.next_visible;
        }
        if (visible_count != self.total_visible) {
            return error.VisibleCountMismatch;
        }

        // Check reverse chain
        var reverse_count: u32 = 0;
        current = self.visible_tail;
        while (current) |node| {
            reverse_count += 1;
            current = node.prev_visible;
        }
        if (reverse_count != self.total_visible) {
            return error.ReverseChainMismatch;
        }

        // Check root count
        var root_count: u32 = 0;
        var root = self.roots_head;
        while (root) |r| {
            root_count += 1;
            root = r.next_sibling;
        }
        if (root_count != self.roots_count) {
            return error.RootCountMismatch;
        }

        // Check that non-visible nodes have null visible pointers
        var node_iter = self.all_nodes.iterator();
        while (node_iter.next()) |entry| {
            const node = entry.value_ptr.*;
            const visible = self.isVisible(node);
            if (!visible) {
                if (node.prev_visible != null or node.next_visible != null) {
                    std.debug.print("VALIDATE FAIL: node {d} not visible but has pointers prev={?d} next={?d}\n", .{
                        node.id,
                        if (node.prev_visible) |p| p.id else null,
                        if (node.next_visible) |n| n.id else null,
                    });
                    return error.StalePointers;
                }
            }
        }
    }
};

// ============================================================================
// Viewport
// ============================================================================

/// A sliding window viewport over the reactive tree.
///
/// The viewport provides efficient scrolling without rebuilding.
/// It maintains a pointer into the visible chain and can scroll
/// up/down in O(1) or to arbitrary positions in O(position).
pub const Viewport = struct {
    tree: *ReactiveTree,
    first: ?*TreeNode = null,
    offset: u32 = 0,
    height: u32,

    const Self = @This();

    pub fn init(tree: *ReactiveTree, height: u32) Self {
        var self = Self{
            .tree = tree,
            .height = height,
        };
        self.first = tree.visible_head;
        return self;
    }

    /// Scroll down by one item. O(1).
    pub fn scrollDown(self: *Self) void {
        if (self.first == null) return;
        const max_offset = self.tree.total_visible -| self.height;
        if (self.offset >= max_offset) return;

        self.first = self.first.?.next_visible;
        self.offset += 1;
        profiling.global.countScrollStep();
    }

    /// Scroll up by one item. O(1).
    pub fn scrollUp(self: *Self) void {
        if (self.first == null or self.offset == 0) return;

        self.first = self.first.?.prev_visible;
        self.offset -= 1;
        profiling.global.countScrollStep();
    }

    /// Scroll by delta items. O(|delta|).
    pub fn scrollBy(self: *Self, delta: i32) void {
        if (delta == 0) return;

        if (delta > 0) {
            const max_offset = self.tree.total_visible -| self.height;
            var remaining: u32 = @intCast(@min(delta, @as(i32, @intCast(max_offset -| self.offset))));
            while (remaining > 0 and self.first != null) : (remaining -= 1) {
                self.first = self.first.?.next_visible;
                self.offset += 1;
                profiling.global.countScrollStep();
            }
        } else {
            var remaining: u32 = @intCast(@min(-delta, @as(i32, @intCast(self.offset))));
            while (remaining > 0 and self.first != null) : (remaining -= 1) {
                self.first = self.first.?.prev_visible;
                self.offset -= 1;
                profiling.global.countScrollStep();
            }
        }
    }

    /// Scroll to absolute offset. O(offset).
    pub fn scrollTo(self: *Self, new_offset: u32) void {
        const max_offset = self.tree.total_visible -| self.height;
        const target = @min(new_offset, max_offset);

        // Always walk from head for consistency
        self.first = self.tree.visible_head;
        self.offset = 0;

        var remaining = target;
        while (remaining > 0 and self.first != null) : (remaining -= 1) {
            self.first = self.first.?.next_visible;
            self.offset += 1;
            profiling.global.countScrollStep();
        }
    }

    /// Scroll to show a specific node.
    pub fn scrollToNode(self: *Self, node_id: NodeId) bool {
        const node = self.tree.get(node_id) orelse return false;
        if (!self.tree.isVisible(node)) return false;

        const index = self.tree.indexOf(node);
        self.scrollTo(index);
        return true;
    }

    /// Check if an index is within the current viewport.
    pub fn containsIndex(self: *const Self, index: u32) bool {
        return index >= self.offset and index < self.offset + self.height;
    }

    /// Get the number of items currently visible in viewport.
    pub fn visibleCount(self: *const Self) u32 {
        return @min(self.height, self.tree.total_visible -| self.offset);
    }

    /// Iterate over items in the viewport.
    pub fn items(self: *const Self) Iterator {
        return .{
            .current = self.first,
            .remaining = self.visibleCount(),
        };
    }

    pub const Iterator = struct {
        current: ?*TreeNode,
        remaining: u32,

        pub fn next(self: *Iterator) ?*TreeNode {
            if (self.remaining == 0) return null;
            const node = self.current orelse return null;
            self.current = node.next_visible;
            self.remaining -= 1;
            profiling.global.countViewportIteration();
            return node;
        }
    };

    /// Adjust viewport after tree modifications.
    /// Call this after inserts/removes to keep viewport valid.
    pub fn adjustAfterInsert(self: *Self, at_index: u32, count: u32) void {
        if (at_index < self.offset) {
            // Insert before viewport - adjust offset
            self.offset += count;
        }
        // Insert at or after viewport start doesn't need offset adjustment
    }

    pub fn adjustAfterRemove(self: *Self, at_index: u32, count: u32) void {
        if (at_index < self.offset) {
            // Remove before viewport - adjust offset
            self.offset -|= count;
        }

        // Ensure offset is still valid
        const max_offset = self.tree.total_visible -| self.height;
        if (self.offset > max_offset) {
            self.scrollTo(max_offset);
        }

        // Ensure first pointer is still valid
        if (self.first != null and !self.tree.contains(self.first.?.id)) {
            self.scrollTo(self.offset);
        }
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;
const Value = @import("../value.zig").Value;
const SortDir = @import("../schema.zig").SortDir;

fn testKey(val: i64) CompoundKey {
    return CompoundKey.encodePartial(&.{Value{ .int = val }}, &.{.asc});
}

test "ReactiveTree: insert and remove roots" {
    var tree = ReactiveTree.init(testing.allocator);
    defer tree.deinit();

    // Insert roots
    _ = try tree.insertRoot(1, testKey(10));
    _ = try tree.insertRoot(2, testKey(30));
    _ = try tree.insertRoot(3, testKey(20));

    try testing.expectEqual(@as(u32, 3), tree.roots_count);
    try testing.expectEqual(@as(u32, 3), tree.total_visible);

    // Check sorted order: 1(10), 3(20), 2(30)
    try testing.expectEqual(@as(NodeId, 1), tree.roots_head.?.id);
    try testing.expectEqual(@as(NodeId, 3), tree.roots_head.?.next_sibling.?.id);
    try testing.expectEqual(@as(NodeId, 2), tree.roots_tail.?.id);

    // Check visible chain matches
    try testing.expectEqual(@as(NodeId, 1), tree.visible_head.?.id);
    try testing.expectEqual(@as(NodeId, 3), tree.visible_head.?.next_visible.?.id);
    try testing.expectEqual(@as(NodeId, 2), tree.visible_tail.?.id);

    // Remove middle
    tree.removeRoot(3);
    try testing.expectEqual(@as(u32, 2), tree.roots_count);
    try testing.expectEqual(@as(u32, 2), tree.total_visible);
    try testing.expectEqual(@as(NodeId, 1), tree.roots_head.?.id);
    try testing.expectEqual(@as(NodeId, 2), tree.roots_head.?.next_sibling.?.id);

    try tree.validate();
}

test "ReactiveTree: indexOf with caching" {
    var tree = ReactiveTree.init(testing.allocator);
    defer tree.deinit();

    const n1 = try tree.insertRoot(1, testKey(10));
    const n2 = try tree.insertRoot(2, testKey(20));
    const n3 = try tree.insertRoot(3, testKey(30));

    // First call computes indices
    try testing.expectEqual(@as(u32, 0), tree.indexOf(n1));
    try testing.expectEqual(@as(u32, 1), tree.indexOf(n2));
    try testing.expectEqual(@as(u32, 2), tree.indexOf(n3));

    // Indices should be cached now
    try testing.expect(!tree.indices_dirty);
    try testing.expect(n1.index_valid);
    try testing.expect(n2.index_valid);
    try testing.expect(n3.index_valid);

    // Insert invalidates cache
    _ = try tree.insertRoot(4, testKey(15));
    try testing.expect(tree.indices_dirty);

    // Recompute
    try testing.expectEqual(@as(u32, 0), tree.indexOf(n1));
    try testing.expectEqual(@as(u32, 1), tree.indexOfId(4).?);
    try testing.expectEqual(@as(u32, 2), tree.indexOf(n2));
    try testing.expectEqual(@as(u32, 3), tree.indexOf(n3));

    try tree.validate();
}

test "ReactiveTree: expand and collapse" {
    var tree = ReactiveTree.init(testing.allocator);
    defer tree.deinit();

    // Create parent with children
    const parent = try tree.insertRoot(1, testKey(10));
    try tree.setChildren(1, "items", &.{
        .{ .id = 10, .sort_key = testKey(1) },
        .{ .id = 11, .sort_key = testKey(2) },
        .{ .id = 12, .sort_key = testKey(3) },
    });

    // Initially collapsed - only parent visible
    try testing.expectEqual(@as(u32, 1), tree.total_visible);
    try testing.expectEqual(@as(u32, 1), parent.visible_count);

    // Expand
    tree.expand(1, "items");

    // Now parent + 3 children visible
    try testing.expectEqual(@as(u32, 4), tree.total_visible);
    try testing.expectEqual(@as(u32, 4), parent.visible_count);

    // Check visible chain: parent -> child1 -> child2 -> child3
    try testing.expectEqual(@as(NodeId, 1), tree.visible_head.?.id);
    try testing.expectEqual(@as(NodeId, 10), tree.visible_head.?.next_visible.?.id);
    try testing.expectEqual(@as(NodeId, 11), tree.visible_head.?.next_visible.?.next_visible.?.id);
    try testing.expectEqual(@as(NodeId, 12), tree.visible_tail.?.id);

    // Collapse
    tree.collapse(1, "items");

    // Back to just parent
    try testing.expectEqual(@as(u32, 1), tree.total_visible);
    try testing.expectEqual(@as(u32, 1), parent.visible_count);
    try testing.expectEqual(@as(NodeId, 1), tree.visible_head.?.id);
    try testing.expectEqual(@as(NodeId, 1), tree.visible_tail.?.id);

    try tree.validate();
}

test "ReactiveTree: nested expansion visibility propagation" {
    var tree = ReactiveTree.init(testing.allocator);
    defer tree.deinit();

    // Create: Root -> Sessions -> Threads
    const root = try tree.insertRoot(1, testKey(1));
    try tree.setChildren(1, "sessions", &.{
        .{ .id = 10, .sort_key = testKey(1) },
        .{ .id = 11, .sort_key = testKey(2) },
    });

    // Add threads to session 10
    try tree.setChildren(10, "threads", &.{
        .{ .id = 100, .sort_key = testKey(1) },
        .{ .id = 101, .sort_key = testKey(2) },
        .{ .id = 102, .sort_key = testKey(3) },
    });

    // Initially: just root visible
    try testing.expectEqual(@as(u32, 1), tree.total_visible);

    // Expand sessions
    tree.expand(1, "sessions");
    try testing.expectEqual(@as(u32, 3), tree.total_visible); // root + 2 sessions
    try testing.expectEqual(@as(u32, 3), root.visible_count);

    // Expand threads on session 10
    const session10 = tree.get(10).?;
    tree.expand(10, "threads");

    // Now: root + session10 + 3 threads + session11 = 6 visible
    try testing.expectEqual(@as(u32, 6), tree.total_visible);

    // KEY TEST: Session 10's visible_count should include threads
    try testing.expectEqual(@as(u32, 4), session10.visible_count); // self + 3 threads

    // KEY TEST: Root's visible_count should include everything
    try testing.expectEqual(@as(u32, 6), root.visible_count); // self + session10(4) + session11(1)

    // Verify visible chain order
    var idx: u32 = 0;
    var current = tree.visible_head;
    const expected_order = [_]NodeId{ 1, 10, 100, 101, 102, 11 };
    while (current) |node| {
        try testing.expectEqual(expected_order[idx], node.id);
        idx += 1;
        current = node.next_visible;
    }
    try testing.expectEqual(@as(u32, 6), idx);

    try tree.validate();
}

test "ReactiveTree: collapse and re-expand preserves nested expansion" {
    var tree = ReactiveTree.init(testing.allocator);
    defer tree.deinit();

    // Create: Root -> Children -> Grandchildren
    _ = try tree.insertRoot(1, testKey(1));
    try tree.setChildren(1, "children", &.{
        .{ .id = 10, .sort_key = testKey(1) },
        .{ .id = 11, .sort_key = testKey(2) },
        .{ .id = 12, .sort_key = testKey(3) },
    });
    try tree.setChildren(10, "grandchildren", &.{
        .{ .id = 100, .sort_key = testKey(1) },
        .{ .id = 101, .sort_key = testKey(2) },
    });

    // Expand children
    tree.expand(1, "children");
    try testing.expectEqual(@as(u32, 4), tree.total_visible); // root + 3 children

    // Expand grandchildren of child 10
    tree.expand(10, "grandchildren");
    try testing.expectEqual(@as(u32, 6), tree.total_visible); // + 2 grandchildren

    // Verify order: 1, 10, 100, 101, 11, 12
    {
        const expected = [_]NodeId{ 1, 10, 100, 101, 11, 12 };
        var i: usize = 0;
        var node = tree.visible_head;
        while (node) |n| : (i += 1) {
            try testing.expectEqual(expected[i], n.id);
            node = n.next_visible;
        }
        try testing.expectEqual(@as(usize, 6), i);
    }

    // Collapse root's children
    tree.collapse(1, "children");
    try testing.expectEqual(@as(u32, 1), tree.total_visible); // just root

    // Verify all nodes have clean pointers (critical invariant!)
    const child10 = tree.get(10).?;
    const child11 = tree.get(11).?;
    const gc100 = tree.get(100).?;
    try testing.expect(child10.prev_visible == null);
    try testing.expect(child10.next_visible == null);
    try testing.expect(child11.prev_visible == null);
    try testing.expect(gc100.prev_visible == null);

    // Re-expand - should restore full subtree including grandchildren
    tree.expand(1, "children");
    try testing.expectEqual(@as(u32, 6), tree.total_visible);

    // Verify same order as before: 1, 10, 100, 101, 11, 12
    {
        const expected = [_]NodeId{ 1, 10, 100, 101, 11, 12 };
        var i: usize = 0;
        var node = tree.visible_head;
        while (node) |n| : (i += 1) {
            try testing.expectEqual(expected[i], n.id);
            node = n.next_visible;
        }
        try testing.expectEqual(@as(usize, 6), i);
    }

    try tree.validate();
}

test "ReactiveTree: stress test expand/collapse combinations" {
    var tree = ReactiveTree.init(testing.allocator);
    defer tree.deinit();

    // Create: Thread -> Frames -> Scopes (like DAP structure)
    _ = try tree.insertRoot(1, testKey(1));
    try tree.setChildren(1, "frames", &.{
        .{ .id = 10, .sort_key = testKey(1) },
        .{ .id = 11, .sort_key = testKey(2) },
        .{ .id = 12, .sort_key = testKey(3) },
    });
    try tree.setChildren(10, "scopes", &.{
        .{ .id = 100, .sort_key = testKey(1) },
        .{ .id = 101, .sort_key = testKey(2) },
    });
    try tree.setChildren(11, "scopes", &.{
        .{ .id = 110, .sort_key = testKey(1) },
    });

    // Helper to verify all visible nodes have null pointers for non-visible nodes
    const verifyPointers = struct {
        fn check(t: *ReactiveTree) !void {
            var iter = t.all_nodes.iterator();
            while (iter.next()) |entry| {
                const n = entry.value_ptr.*;
                const visible = t.isVisible(n);
                if (!visible) {
                    // Non-visible nodes must have null pointers
                    if (n.prev_visible != null or n.next_visible != null) {
                        std.debug.print("BUG: node {d} not visible but has pointers prev={?d} next={?d}\n", .{
                            n.id,
                            if (n.prev_visible) |p| p.id else null,
                            if (n.next_visible) |nx| nx.id else null,
                        });
                        return error.StalePointers;
                    }
                }
            }
        }
    }.check;

    // Test sequence that mimics user interaction
    try verifyPointers(&tree);

    // 1. Expand frames
    tree.expand(1, "frames");
    try tree.validate();
    try verifyPointers(&tree);
    try testing.expectEqual(@as(u32, 4), tree.total_visible);

    // 2. Expand first frame's scopes
    tree.expand(10, "scopes");
    try tree.validate();
    try verifyPointers(&tree);
    try testing.expectEqual(@as(u32, 6), tree.total_visible);

    // 3. Collapse frames (should unlink frames AND their expanded scopes)
    tree.collapse(1, "frames");
    try tree.validate();
    try verifyPointers(&tree);
    try testing.expectEqual(@as(u32, 1), tree.total_visible);

    // 4. Re-expand frames (scopes should reappear because still marked expanded)
    tree.expand(1, "frames");
    try tree.validate();
    try verifyPointers(&tree);
    try testing.expectEqual(@as(u32, 6), tree.total_visible);

    // 5. Expand second frame's scopes while visible
    tree.expand(11, "scopes");
    try tree.validate();
    try verifyPointers(&tree);
    try testing.expectEqual(@as(u32, 7), tree.total_visible);

    // 6. Collapse frames again
    tree.collapse(1, "frames");
    try tree.validate();
    try verifyPointers(&tree);
    try testing.expectEqual(@as(u32, 1), tree.total_visible);

    // 7. Expand scopes on frame 12 while frames collapsed (should not corrupt)
    tree.expand(12, "scopes"); // Frame 12 has no scopes, but edge should be marked expanded
    try tree.validate();
    try verifyPointers(&tree);
    try testing.expectEqual(@as(u32, 1), tree.total_visible);

    // 8. Collapse scopes on frame 10 while frames collapsed
    tree.collapse(10, "scopes");
    try tree.validate();
    try verifyPointers(&tree);

    // 9. Re-expand frames - frame 10's scopes should NOT appear (we collapsed them)
    tree.expand(1, "frames");
    try tree.validate();
    try verifyPointers(&tree);
    // Now: Thread + 3 frames + frame 11's 1 scope = 5
    try testing.expectEqual(@as(u32, 5), tree.total_visible);

    // 10. Toggle frames multiple times
    _ = tree.toggleExpand(1, "frames"); // collapse
    try tree.validate();
    try verifyPointers(&tree);
    _ = tree.toggleExpand(1, "frames"); // expand
    try tree.validate();
    try verifyPointers(&tree);
    _ = tree.toggleExpand(1, "frames"); // collapse
    try tree.validate();
    try verifyPointers(&tree);
    _ = tree.toggleExpand(1, "frames"); // expand
    try tree.validate();
    try verifyPointers(&tree);

    try testing.expectEqual(@as(u32, 5), tree.total_visible);
}

test "ReactiveTree: expand while not visible does not corrupt pointers" {
    var tree = ReactiveTree.init(testing.allocator);
    defer tree.deinit();

    // Create: Root -> Child -> Grandchild
    _ = try tree.insertRoot(1, testKey(1));
    try tree.setChildren(1, "children", &.{
        .{ .id = 10, .sort_key = testKey(1) },
    });
    try tree.setChildren(10, "grandchildren", &.{
        .{ .id = 100, .sort_key = testKey(1) },
    });

    // Root's children are collapsed, so child 10 is not visible
    try testing.expectEqual(@as(u32, 1), tree.total_visible);

    // Expand grandchildren while parent (10) is not visible
    // This should NOT corrupt child 10's pointers
    tree.expand(10, "grandchildren");

    // Child 10 should still have null pointers
    const child10 = tree.get(10).?;
    try testing.expect(child10.prev_visible == null);
    try testing.expect(child10.next_visible == null);

    // Grandchild should also have null pointers
    const gc100 = tree.get(100).?;
    try testing.expect(gc100.prev_visible == null);
    try testing.expect(gc100.next_visible == null);

    // Now expand root's children - should show both child and grandchild
    tree.expand(1, "children");
    try testing.expectEqual(@as(u32, 3), tree.total_visible); // root + child + grandchild

    // Verify order: 1, 10, 100
    const expected = [_]NodeId{ 1, 10, 100 };
    var i: usize = 0;
    var node = tree.visible_head;
    while (node) |n| : (i += 1) {
        try testing.expectEqual(expected[i], n.id);
        node = n.next_visible;
    }
    try testing.expectEqual(@as(usize, 3), i);

    try tree.validate();
}

test "ReactiveTree: move root" {
    var tree = ReactiveTree.init(testing.allocator);
    defer tree.deinit();

    _ = try tree.insertRoot(1, testKey(10));
    _ = try tree.insertRoot(2, testKey(20));
    _ = try tree.insertRoot(3, testKey(30));
    _ = try tree.insertRoot(4, testKey(40));

    // Move node 1 (index 0) to end (index 3)
    tree.moveRoot(1, 3);

    // Check new order: 2, 3, 4, 1
    try testing.expectEqual(@as(NodeId, 2), tree.roots_head.?.id);
    try testing.expectEqual(@as(NodeId, 1), tree.roots_tail.?.id);

    // Visible chain should match
    try testing.expectEqual(@as(NodeId, 2), tree.visible_head.?.id);
    try testing.expectEqual(@as(NodeId, 1), tree.visible_tail.?.id);

    try tree.validate();
}

test "ReactiveTree: remove root clears expansion state" {
    var tree = ReactiveTree.init(testing.allocator);
    defer tree.deinit();

    // Create parent with children
    _ = try tree.insertRoot(1, testKey(10));
    try tree.setChildren(1, "items", &.{
        .{ .id = 10, .sort_key = testKey(1) },
        .{ .id = 11, .sort_key = testKey(2) },
    });

    // Expand
    tree.expand(1, "items");
    try testing.expectEqual(@as(u32, 3), tree.total_visible);

    // Remove root
    tree.removeRoot(1);
    try testing.expectEqual(@as(u32, 0), tree.total_visible);
    try testing.expectEqual(@as(u32, 0), tree.roots_count);

    // All nodes should be gone
    try testing.expect(tree.get(1) == null);
    try testing.expect(tree.get(10) == null);
    try testing.expect(tree.get(11) == null);

    // Re-insert same ID
    const new_root = try tree.insertRoot(1, testKey(10));
    try tree.setChildren(1, "other", &.{
        .{ .id = 20, .sort_key = testKey(1) },
    });

    // Should start collapsed (no stale expansion state)
    try testing.expectEqual(@as(u32, 1), tree.total_visible);
    try testing.expect(!new_root.isExpanded("other"));
    try testing.expect(!new_root.isExpanded("items")); // Old edge name shouldn't exist

    try tree.validate();
}

test "Viewport: scroll operations" {
    var tree = ReactiveTree.init(testing.allocator);
    defer tree.deinit();

    // Create 20 roots
    for (0..20) |i| {
        _ = try tree.insertRoot(@intCast(i + 1), testKey(@intCast(i)));
    }

    var viewport = Viewport.init(&tree, 5);

    // Initial state
    try testing.expectEqual(@as(u32, 0), viewport.offset);
    try testing.expectEqual(@as(NodeId, 1), viewport.first.?.id);

    // Scroll down
    viewport.scrollDown();
    try testing.expectEqual(@as(u32, 1), viewport.offset);
    try testing.expectEqual(@as(NodeId, 2), viewport.first.?.id);

    // Scroll up
    viewport.scrollUp();
    try testing.expectEqual(@as(u32, 0), viewport.offset);
    try testing.expectEqual(@as(NodeId, 1), viewport.first.?.id);

    // Scroll to position
    viewport.scrollTo(10);
    try testing.expectEqual(@as(u32, 10), viewport.offset);
    try testing.expectEqual(@as(NodeId, 11), viewport.first.?.id);

    // Scroll by
    viewport.scrollBy(-5);
    try testing.expectEqual(@as(u32, 5), viewport.offset);
    try testing.expectEqual(@as(NodeId, 6), viewport.first.?.id);

    // Cannot scroll past end
    viewport.scrollTo(100);
    try testing.expectEqual(@as(u32, 15), viewport.offset); // 20 - 5 = 15 max

    // Cannot scroll before start
    viewport.scrollBy(-100);
    try testing.expectEqual(@as(u32, 0), viewport.offset);
}

test "Viewport: iterator" {
    var tree = ReactiveTree.init(testing.allocator);
    defer tree.deinit();

    for (0..10) |i| {
        _ = try tree.insertRoot(@intCast(i + 1), testKey(@intCast(i)));
    }

    var viewport = Viewport.init(&tree, 3);
    viewport.scrollTo(2);

    // Should iterate over items at indices 2, 3, 4
    var iter = viewport.items();
    var count: u32 = 0;
    const expected = [_]NodeId{ 3, 4, 5 };
    while (iter.next()) |node| {
        try testing.expectEqual(expected[count], node.id);
        count += 1;
    }
    try testing.expectEqual(@as(u32, 3), count);
}

test "Viewport: adjust after modifications" {
    var tree = ReactiveTree.init(testing.allocator);
    defer tree.deinit();

    for (0..10) |i| {
        _ = try tree.insertRoot(@intCast(i + 1), testKey(@intCast(i * 10)));
    }

    var viewport = Viewport.init(&tree, 3);
    viewport.scrollTo(5);
    try testing.expectEqual(@as(u32, 5), viewport.offset);
    try testing.expectEqual(@as(NodeId, 6), viewport.first.?.id);

    // Insert before viewport
    _ = try tree.insertRootAt(100, testKey(5), 0);
    viewport.adjustAfterInsert(0, 1);
    try testing.expectEqual(@as(u32, 6), viewport.offset);

    // First should still point to same logical item
    // (though we need to refresh it after tree modification)
    viewport.scrollTo(viewport.offset);
    try testing.expectEqual(@as(NodeId, 6), viewport.first.?.id);

    // Remove before viewport
    tree.removeRoot(100);
    viewport.adjustAfterRemove(0, 1);
    try testing.expectEqual(@as(u32, 5), viewport.offset);
}

test "ReactiveTree: deep nesting doesn't truncate" {
    var tree = ReactiveTree.init(testing.allocator);
    defer tree.deinit();

    // Build a 50-level deep tree
    const DEPTH = 50;
    _ = try tree.insertRoot(1, testKey(1));

    var parent_id: NodeId = 1;
    for (1..DEPTH) |i| {
        const child_id: NodeId = @intCast(i + 1);
        try tree.setChildren(parent_id, "child", &.{
            .{ .id = child_id, .sort_key = testKey(@intCast(i)) },
        });
        tree.expand(parent_id, "child");
        parent_id = child_id;
    }

    // All nodes should be visible
    try testing.expectEqual(@as(u32, DEPTH), tree.total_visible);

    // Should be able to get index of deepest node
    const deepest = tree.get(DEPTH).?;
    const idx = tree.indexOf(deepest);
    try testing.expectEqual(@as(u32, DEPTH - 1), idx);

    // nodeAtIndex should find it
    const found = tree.nodeAtIndex(DEPTH - 1);
    try testing.expect(found != null);
    try testing.expectEqual(@as(NodeId, DEPTH), found.?.id);

    try tree.validate();
}

test "ReactiveTree: insertChild in sorted position" {
    var tree = ReactiveTree.init(testing.allocator);
    defer tree.deinit();

    _ = try tree.insertRoot(1, testKey(1));
    tree.expand(1, "items");

    // Insert children out of order
    _ = try tree.insertChild(1, "items", 30, testKey(30));
    _ = try tree.insertChild(1, "items", 10, testKey(10));
    _ = try tree.insertChild(1, "items", 20, testKey(20));

    // Should be in sorted order
    const parent = tree.get(1).?;
    const edge = parent.getEdge("items").?;
    try testing.expectEqual(@as(NodeId, 10), edge.head.?.id);
    try testing.expectEqual(@as(NodeId, 20), edge.head.?.next_sibling.?.id);
    try testing.expectEqual(@as(NodeId, 30), edge.tail.?.id);

    // Visible chain should match
    try testing.expectEqual(@as(NodeId, 1), tree.visible_head.?.id);
    try testing.expectEqual(@as(NodeId, 10), tree.visible_head.?.next_visible.?.id);
    try testing.expectEqual(@as(NodeId, 20), tree.visible_head.?.next_visible.?.next_visible.?.id);
    try testing.expectEqual(@as(NodeId, 30), tree.visible_tail.?.id);

    try tree.validate();
}
