///! View API for reactive query results with optional virtualization.
///!
///! Provides a unified interface for both flat lists and hierarchical trees.
///! Supports expand/collapse and windowed viewing when limit > 0.
///! Uses ReactiveTree internally for correct visibility propagation,
///! expansion state lifecycle, and efficient O(1) index caching.

const std = @import("std");
const Allocator = std.mem.Allocator;

const Query = @import("../query/builder.zig").Query;
const Item = @import("../query/executor.zig").Item;
const PathSegment = @import("../query/executor.zig").PathSegment;
const Path = @import("../query/executor.zig").Path;
const Subscription = @import("subscription.zig").Subscription;
const Callbacks = @import("subscription.zig").Callbacks;
const tracker_mod = @import("tracker.zig");
const ChangeTracker = tracker_mod.ChangeTracker;
const appendSortKey = tracker_mod.appendSortKey;
const IndexCoverage = @import("../index/index.zig").IndexCoverage;
const NodeId = @import("../node.zig").NodeId;
const NodeStore = @import("../node_store.zig").NodeStore;
const Schema = @import("../schema.zig").Schema;
const Value = @import("../value.zig").Value;
const CompoundKey = @import("../index/key.zig").CompoundKey;

// Use the new unified ReactiveTree
const reactive_tree = @import("reactive_tree.zig");
const ReactiveTree = reactive_tree.ReactiveTree;
const TreeNode = reactive_tree.TreeNode;
const VisibleChainObserver = reactive_tree.VisibleChainObserver;
const Viewport = reactive_tree.Viewport;

/// View options.
pub const ViewOpts = struct {
    /// Maximum number of visible items (0 = no limit).
    limit: u32 = 0,
    default_expanded: bool = false,
};

/// Tracks expanded edges for a single node (can have multiple edges expanded)
pub const ExpandedNodeEdges = struct {
    /// Map of edge_name -> children_count for each expanded edge
    edges: std.StringHashMapUnmanaged(u32) = .{},

    pub fn deinit(self: *ExpandedNodeEdges, allocator: Allocator) void {
        self.edges.deinit(allocator);
    }

    pub fn totalChildren(self: *const ExpandedNodeEdges) u32 {
        var total: u32 = 0;
        var iter = self.edges.valueIterator();
        while (iter.next()) |count| {
            total += count.*;
        }
        return total;
    }
};

/// View API for reactive query results.
///
/// Lazy loading architecture:
/// - View.init() is O(1) - no data loaded
/// - First access loads O(limit) items (or all if limit=0)
/// - Scroll reloads viewport O(offset + limit) iterations, O(limit) loads
/// - Expansion state persists in `expanded_nodes` even when node leaves viewport
///
/// A flat list is just a view with no expansions and limit=0.
pub const View = struct {
    // Core reactive tree (holds currently loaded nodes)
    reactive_tree: ReactiveTree,
    viewport: Viewport,

    // Configuration
    default_expanded: bool,

    // References
    subscription: *Subscription,
    tracker: *ChangeTracker,
    store: *const NodeStore,
    schema: *const Schema,
    allocator: Allocator,

    // === Lazy loading state ===

    // Persistent expansion state (survives viewport changes)
    // Maps node_id -> expanded edges (a node can have multiple edges expanded)
    expanded_nodes: std.AutoHashMap(NodeId, ExpandedNodeEdges),

    // Cached root count from index (computed once)
    cached_root_count: ?u32 = null,

    // Track what's currently loaded
    loaded_viewport_offset: u32 = 0,
    viewport_dirty: bool = true,

    // External callbacks for reactive updates
    external_callbacks: Callbacks = .{},

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        tracker: *ChangeTracker,
        query: *const Query,
        coverage: IndexCoverage,
        opts: ViewOpts,
    ) !Self {
        const sub = try tracker.subscribe(query, coverage);

        // Convert limit=0 to maxInt (no limit)
        const effective_limit = if (opts.limit == 0) std.math.maxInt(u32) else opts.limit;

        var self = Self{
            .reactive_tree = ReactiveTree.init(allocator),
            .viewport = undefined,
            .default_expanded = opts.default_expanded,
            .subscription = sub,
            .tracker = tracker,
            .store = tracker.store,
            .schema = tracker.schema,
            .allocator = allocator,
            .expanded_nodes = std.AutoHashMap(NodeId, ExpandedNodeEdges).init(allocator),
            .cached_root_count = null,
            .loaded_viewport_offset = 0,
            .viewport_dirty = true,
        };

        // Initialize viewport (tree is empty, will load on first access)
        self.viewport = Viewport.init(&self.reactive_tree, effective_limit);

        // NOTE: Callbacks are NOT set up here because `self` is a local variable
        // that gets copied on return. Caller must call activate() after init.
        return self;
    }

    /// Reload viewport with items at current offset.
    /// Uses position computation to handle expansions correctly.
    /// Handles recursive expansions (children that are also expanded).
    fn reloadViewport(self: *Self) !void {
        // Untrack old nodes from this subscription
        self.untrackLoadedNodes();

        // Clear current state
        self.reactive_tree.clear();
        self.subscription.result_set.clear();

        const offset = self.viewport.offset;
        const height = self.viewport.height;
        // Use saturating add to avoid overflow when height is maxInt
        const viewport_end = offset +| height;

        var position: u32 = 0;

        // Direct node ID lookup - bypass index scan entirely
        if (self.subscription.query.root_id) |root_id| {
            const node = self.store.get(root_id) orelse {
                // Node doesn't exist - empty viewport
                self.viewport.first = self.reactive_tree.visible_head;
                self.loaded_viewport_offset = offset;
                self.viewport_dirty = false;
                self.subscription.initialized = true;
                return;
            };

            // Verify type matches
            if (node.type_id == self.subscription.query.root_type_id) {
                position = try self.loadSubtreeInViewport(root_id, null, null, position, offset, viewport_end, 0);
            }
        } else {
            // Scan index and compute positions accounting for expansions
            var iter = self.tracker.indexes.scan(self.subscription.coverage, self.subscription.query.filters);

            while (iter.next()) |root_id| {
                const node = self.store.get(root_id) orelse continue;

                // Apply filters
                if (!self.tracker.executor.matchesFilters(node, self.subscription.coverage.post_filters)) continue;
                if (!self.tracker.executor.matchesFilters(node, self.subscription.query.filters)) continue;

                // Process this root and its expanded subtree, updating position
                position = try self.loadSubtreeInViewport(root_id, null, null, position, offset, viewport_end, 0);

                // Early exit if we've passed the viewport
                if (position >= viewport_end) break;
            }
        }

        // Update viewport pointer
        self.viewport.first = self.reactive_tree.visible_head;
        self.loaded_viewport_offset = offset;
        self.viewport_dirty = false;

        // Mark subscription as initialized so reactive callbacks work
        self.subscription.initialized = true;
    }

    /// Recursively load a node and its expanded children into the viewport.
    /// Returns the position after this subtree (for next sibling).
    /// @param parent_edge_name: The edge name used by the parent to link to this node (null for roots)
    fn loadSubtreeInViewport(
        self: *Self,
        node_id: NodeId,
        parent_id: ?NodeId,
        parent_edge_name: ?[]const u8,
        start_position: u32,
        viewport_start: u32,
        viewport_end: u32,
        depth: u32,
    ) !u32 {
        var position = start_position;

        // Get expanded edges for this node (may have multiple edges expanded)
        const node_edges = self.expanded_nodes.get(node_id);

        // Load this node if in viewport
        if (position >= viewport_start and position < viewport_end) {
            const node = self.store.get(node_id) orelse return position + 1;

            // Use key from result_set if already there (has full composite key),
            // otherwise build a short key for roots
            const key = if (self.subscription.result_set.getNode(node_id)) |result_node|
                result_node.key
            else blk: {
                var k = CompoundKey{};
                appendSortKey(&k, node, self.subscription.query.sorts);
                break :blk k;
            };

            if (parent_id == null) {
                // Root node
                if (self.subscription.query.virtual) {
                    // Virtual root - don't add to reactive_tree, result_set, or node_to_subs
                    // The tracker already added this to virtual_to_subs via ensureInitialized
                    // Just ensure we process expanded children below (position not incremented for virtual)
                } else {
                    // Non-virtual root - add to reactive_tree and tracking
                    const tree_node = try self.reactive_tree.insertRootAt(node_id, key, self.reactive_tree.roots_count);
                    tree_node.depth = depth;

                    _ = try self.subscription.result_set.insertSorted(node_id, key, null);
                    try self.tracker.node_to_subs.add(node_id, self.subscription);

                    // Mark all expanded edges in tree_node
                    if (node_edges) |edges| {
                        var edge_iter = edges.edges.keyIterator();
                        while (edge_iter.next()) |edge_name| {
                            try tree_node.markExpanded(self.allocator, edge_name.*);
                        }
                    }
                }
            } else {
                // Child node - try to insert as child of parent
                const edge_for_parent = parent_edge_name orelse "children";
                const parent_in_tree = self.reactive_tree.get(parent_id.?) != null;
                if (parent_in_tree) {
                    const tree_node = try self.reactive_tree.insertChild(parent_id.?, edge_for_parent, node_id, key);
                    // Mark all expanded edges in tree_node
                    if (node_edges) |edges| {
                        var edge_iter = edges.edges.keyIterator();
                        while (edge_iter.next()) |edge_name| {
                            try tree_node.markExpanded(self.allocator, edge_name.*);
                        }
                    }
                } else {
                    // Parent not in viewport - insert as root with correct depth
                    const tree_node = try self.reactive_tree.insertRootAt(node_id, key, self.reactive_tree.roots_count);
                    tree_node.depth = depth;
                    // Mark all expanded edges in tree_node
                    if (node_edges) |edges| {
                        var edge_iter = edges.edges.keyIterator();
                        while (edge_iter.next()) |edge_name| {
                            try tree_node.markExpanded(self.allocator, edge_name.*);
                        }
                    }
                }

                // Ensure children are in result_set for reactivity
                const loaded_children = try self.tracker.loadChildrenLazy(self.subscription, parent_id.?, edge_for_parent);
                defer self.allocator.free(loaded_children);
            }
        }

        // Only increment position for visible nodes (not virtual roots)
        const is_virtual_root = parent_id == null and self.subscription.query.virtual;
        if (!is_virtual_root) {
            position += 1;
        }

        // Process children for ALL expanded edges of this node
        if (node_edges) |edges| {
            // Collect and sort edge names for deterministic iteration order
            var edge_names = std.ArrayListUnmanaged([]const u8){};
            defer edge_names.deinit(self.allocator);

            var key_iter = edges.edges.keyIterator();
            while (key_iter.next()) |key| {
                try edge_names.append(self.allocator, key.*);
            }

            // Sort edge names alphabetically for consistent ordering
            std.mem.sort([]const u8, edge_names.items, {}, struct {
                fn lessThan(_: void, a: []const u8, b: []const u8) bool {
                    return std.mem.order(u8, a, b) == .lt;
                }
            }.lessThan);

            for (edge_names.items) |edge_name| {
                const children_count = edges.edges.get(edge_name) orelse 0;

                // Load children IDs from result_set (they were added by loadChildrenLazy or are already there)
                const child_ids = try self.getChildIdsFromResultSetForEdge(node_id, edge_name);
                defer self.allocator.free(child_ids);

                // If we don't have children in result_set yet, load them
                if (child_ids.len == 0 and children_count > 0) {
                    const loaded_children = try self.tracker.loadChildrenLazy(self.subscription, node_id, edge_name);
                    defer self.allocator.free(loaded_children);

                    // Get the newly loaded children
                    const new_child_ids = try self.getChildIdsFromResultSetForEdge(node_id, edge_name);
                    defer self.allocator.free(new_child_ids);

                    for (new_child_ids) |child_id| {
                        // Recursively process child and its subtree
                        position = try self.loadSubtreeInViewport(child_id, node_id, edge_name, position, viewport_start, viewport_end, depth + 1);
                        if (position >= viewport_end) break;
                    }
                } else {
                    for (child_ids) |child_id| {
                        // Recursively process child and its subtree
                        position = try self.loadSubtreeInViewport(child_id, node_id, edge_name, position, viewport_start, viewport_end, depth + 1);
                        if (position >= viewport_end) break;
                    }
                }

                if (position >= viewport_end) break;
            }
        }

        return position;
    }

    /// Get child IDs from result_set for a given parent.
    fn getChildIdsFromResultSet(self: *Self, parent_id: NodeId) ![]NodeId {
        var children = std.ArrayListUnmanaged(NodeId){};
        errdefer children.deinit(self.allocator);

        var iter = self.subscription.result_set.iterator();
        while (iter.next()) |result_node| {
            if (result_node.ancestry.len > 0 and result_node.ancestry[result_node.ancestry.len - 1] == parent_id) {
                try children.append(self.allocator, result_node.id);
            }
        }

        return try children.toOwnedSlice(self.allocator);
    }

    /// Get child IDs for a specific edge directly from result_set (sorted order).
    /// Returns children that are in result_set (already loaded) for the given parent and edge.
    fn getChildIdsFromResultSetForEdge(self: *Self, parent_id: NodeId, edge_name: []const u8) ![]NodeId {
        var children = std.ArrayListUnmanaged(NodeId){};
        errdefer children.deinit(self.allocator);

        // Iterate through result_set in sorted order and filter by parent and edge
        var iter = self.subscription.result_set.iterator();
        while (iter.next()) |result_node| {
            // Check if this node is a child of parent_id
            if (result_node.ancestry.len > 0 and
                result_node.ancestry[result_node.ancestry.len - 1] == parent_id)
            {
                // Check if the edge name matches
                if (result_node.edge_name) |e_name| {
                    if (std.mem.eql(u8, e_name, edge_name)) {
                        try children.append(self.allocator, result_node.id);
                    }
                }
            }
        }

        return try children.toOwnedSlice(self.allocator);
    }

    /// Untrack all currently loaded nodes from node_to_subs.
    fn untrackLoadedNodes(self: *Self) void {
        var iter = self.subscription.result_set.iterator();
        while (iter.next()) |result_node| {
            _ = self.tracker.node_to_subs.remove(result_node.id, self.subscription);
        }
    }


    /// Ensure viewport is loaded. Called lazily on first access.
    fn ensureViewportLoaded(self: *Self) !void {
        if (!self.viewport_dirty) return;
        try self.reloadViewport();
    }

    /// Activate the view to receive reactive updates.
    /// Must be called after init() once the View is at its final memory location.
    ///
    /// @param immediate: If true, initializes subscription immediately so reactive
    ///                   callbacks work before first items() call. Use for flat lists.
    ///                   If false, uses lazy initialization (better for trees with expansions).
    pub fn activate(self: *Self, immediate: bool) void {
        // Fix viewport's tree pointer (it was set to point to local var in init)
        self.viewport.tree = &self.reactive_tree;

        // Now set up callbacks with the correct context pointer
        self.subscription.callbacks = .{
            .on_enter = handleEnter,
            .on_leave = handleLeave,
            .on_change = handleChange,
            .on_move = handleMove,
            .context = self,
        };

        // Register as visible chain observer
        self.reactive_tree.observer = .{
            .on_will_remove = observerWillRemove,
            .on_did_remove = observerDidRemove,
            .on_did_insert = observerDidInsert,
            .on_did_move = observerDidMove,
            .context = self,
        };

        // Initialize subscription immediately if requested
        if (immediate) {
            self.tracker.ensureInitialized(self.subscription) catch {};
        }
    }

    /// Set callbacks for reactive updates.
    /// These are called in addition to the internal tree update handlers.
    pub fn setCallbacks(self: *Self, callbacks: Callbacks) void {
        self.external_callbacks = callbacks;
    }

    /// Convenience method to set on_enter callback.
    /// Pass null to clear the callback (context is only updated if non-null).
    pub fn onEnter(self: *Self, callback: ?@import("subscription.zig").OnEnterFn, ctx: ?*anyopaque) void {
        self.external_callbacks.on_enter = callback;
        if (ctx != null) self.external_callbacks.context = ctx;
    }

    /// Convenience method to set on_leave callback.
    /// Pass null to clear the callback (context is only updated if non-null).
    pub fn onLeave(self: *Self, callback: ?@import("subscription.zig").OnLeaveFn, ctx: ?*anyopaque) void {
        self.external_callbacks.on_leave = callback;
        if (ctx != null) self.external_callbacks.context = ctx;
    }

    /// Convenience method to set on_change callback.
    /// Pass null to clear the callback (context is only updated if non-null).
    pub fn onChange(self: *Self, callback: ?@import("subscription.zig").OnChangeFn, ctx: ?*anyopaque) void {
        self.external_callbacks.on_change = callback;
        if (ctx != null) self.external_callbacks.context = ctx;
    }

    /// Convenience method to set on_move callback.
    /// Pass null to clear the callback (context is only updated if non-null).
    pub fn onMove(self: *Self, callback: ?@import("subscription.zig").OnMoveFn, ctx: ?*anyopaque) void {
        self.external_callbacks.on_move = callback;
        if (ctx != null) self.external_callbacks.context = ctx;
    }

    pub fn deinit(self: *Self) void {
        self.untrackLoadedNodes();
        self.reactive_tree.deinit();
        // Clean up nested edge maps
        var expanded_iter = self.expanded_nodes.valueIterator();
        while (expanded_iter.next()) |node_edges| {
            node_edges.deinit(self.allocator);
        }
        self.expanded_nodes.deinit();
        self.tracker.unsubscribe(self.subscription.id);
    }

    // ========================================================================
    // Public API
    // ========================================================================

    /// Get current window items.
    pub fn items(self: *Self) ItemIterator {
        // Ensure viewport is loaded before accessing
        self.ensureViewportLoaded() catch {};

        // Use reactive_tree's total_visible as the count of loaded items
        // (not viewport.visibleCount() which uses offset)
        const loaded_count = @min(self.viewport.height, self.reactive_tree.total_visible);

        return ItemIterator{
            .tree_iter = .{
                .current = self.viewport.first,
                .remaining = loaded_count,
            },
            .store = self.store,
            .schema = self.schema,
        };
    }

    /// Get total visible item count.
    /// Computed as: root_count + sum(children_count for all expanded edges of all nodes)
    pub fn total(self: *Self) u32 {
        // Get root count (computed once, cached)
        const root_count = self.getRootCount();

        // Add expanded children counts (each node can have multiple edges expanded)
        var expanded_total: u32 = 0;
        var expanded_iter = self.expanded_nodes.valueIterator();
        while (expanded_iter.next()) |node_edges| {
            expanded_total += node_edges.totalChildren();
        }

        return root_count + expanded_total;
    }

    /// Get root count from index (cached).
    fn getRootCount(self: *Self) u32 {
        if (self.cached_root_count) |count| {
            return count;
        }

        // Scan and count matching roots
        var iter = self.tracker.indexes.scan(self.subscription.coverage, self.subscription.query.filters);
        var count: u32 = 0;
        while (iter.next()) |root_id| {
            const node = self.store.get(root_id) orelse continue;
            if (!self.tracker.executor.matchesFilters(node, self.subscription.coverage.post_filters)) continue;
            if (!self.tracker.executor.matchesFilters(node, self.subscription.query.filters)) continue;
            count += 1;
        }

        self.cached_root_count = count;
        return count;
    }

    /// Scroll by delta (positive = down, negative = up).
    pub fn move(self: *Self, delta: i32) void {
        // Calculate new offset with bounds checking
        const total_count = self.total();
        const max_offset = total_count -| self.viewport.height;

        const old_offset = self.viewport.offset;
        var new_offset: u32 = undefined;

        if (delta > 0) {
            new_offset = @min(old_offset +| @as(u32, @intCast(delta)), max_offset);
        } else {
            const abs_delta: u32 = @intCast(-delta);
            new_offset = old_offset -| abs_delta;
        }

        if (new_offset != old_offset) {
            self.viewport.offset = new_offset;
            self.viewport_dirty = true;
        }
    }

    /// Scroll to absolute position.
    pub fn scrollTo(self: *Self, position: u32) void {
        const total_count = self.total();
        const max_offset = total_count -| self.viewport.height;
        const new_offset = @min(position, max_offset);

        if (new_offset != self.viewport.offset) {
            self.viewport.offset = new_offset;
            self.viewport_dirty = true;
        }
    }

    /// Get current scroll offset.
    pub fn getOffset(self: *const Self) u32 {
        return self.viewport.offset;
    }

    /// Get current viewport height.
    pub fn getHeight(self: *const Self) u32 {
        return self.viewport.height;
    }

    /// Set viewport height.
    pub fn setHeight(self: *Self, height: u32) void {
        if (height != self.viewport.height) {
            self.viewport.height = height;
            self.viewport_dirty = true;
        }
    }

    /// Expand an edge at path.
    /// Supports lazy loading: can be called before children are loaded.
    pub fn expand(self: *Self, path: Path, edge_name: []const u8) !void {
        const node_id = self.nodeIdFromPath(path) orelse return;

        // Use expandById to handle both lazy state (expanded_nodes) and reactive_tree
        try self.expandById(node_id, edge_name);

        // Also mark in reactive_tree if node is currently loaded
        if (self.reactive_tree.get(node_id)) |tree_node| {
            try tree_node.markExpanded(self.allocator, edge_name);

            // Load children if needed
            const edge = tree_node.getEdge(edge_name);
            if (edge == null or !edge.?.loaded) {
                try self.loadChildren(node_id, edge_name);
            }
        }
    }

    /// Collapse an edge at path.
    pub fn collapse(self: *Self, path: Path, edge_name: []const u8) void {
        const node_id = self.nodeIdFromPath(path) orelse return;
        // Use collapseById to handle both lazy state (expanded_nodes) and reactive_tree
        self.collapseById(node_id, edge_name);
    }

    /// Toggle expansion of an edge at path.
    pub fn toggle(self: *Self, path: Path, edge_name: []const u8) !void {
        const node_id = self.nodeIdFromPath(path) orelse return;
        const tree_node = self.reactive_tree.get(node_id) orelse return;

        if (tree_node.isExpanded(edge_name)) {
            self.collapse(path, edge_name);
        } else {
            try self.expand(path, edge_name);
        }
    }

    /// Check if an edge is expanded.
    pub fn isExpanded(self: *const Self, path: Path, edge_name: []const u8) bool {
        const node_id = self.nodeIdFromPath(path) orelse return false;
        const tree_node = self.reactive_tree.get(node_id) orelse return false;
        return tree_node.isExpanded(edge_name);
    }

    // ========================================================================
    // NodeId-based operations (for Lua bindings that parse paths externally)
    // ========================================================================

    /// Expand an edge by node ID directly.
    /// Does incremental updates when parent is in reactive_tree, emitting enter events
    /// for children within viewport bounds.
    pub fn expandById(self: *Self, node_id: NodeId, edge_name: []const u8) !void {
        // Ensure viewport is loaded before expand so that:
        // 1. Parent node is in node_to_subs for future link events to trigger enter callbacks
        // 2. Parent node is in reactive_tree so enter events can be emitted for children
        try self.ensureViewportLoaded();

        // Get or create entry for this node
        const gop = try self.expanded_nodes.getOrPut(node_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{};
        }

        // Already expanded this edge?
        if (gop.value_ptr.edges.contains(edge_name)) return;

        // Count children for this node using the specific edge
        const children_count = try self.countChildrenForEdge(node_id, edge_name);

        // Add this edge to the expanded state
        try gop.value_ptr.edges.put(self.allocator, edge_name, children_count);

        // Check if parent is in reactive_tree
        const parent_tree_node = self.reactive_tree.get(node_id);

        // Load children into result_set for reactive tracking
        // Note: children may already be in result_set from reactive link events
        const loaded_ids = try self.tracker.loadChildrenLazy(self.subscription, node_id, edge_name);
        defer if (loaded_ids.len > 0) self.allocator.free(loaded_ids);

        // Incremental update: add children to reactive_tree
        // Observer callbacks handle enter/leave events automatically
        if (parent_tree_node) |parent| {
            // Check if children are already in reactive_tree (from previous expand/collapse cycle)
            const existing_edge = parent.getEdge(edge_name);
            if (existing_edge != null and existing_edge.?.head != null) {
                // Children already in tree, just unlinked - call expand() to re-link them
                // Note: expand() also marks the edge as expanded
                // Observer will emit enter/leave events
                self.reactive_tree.expand(node_id, edge_name);
            } else {
                // Children not in tree - mark expanded first (needed for areChildrenVisible in setChildren)
                try parent.markExpanded(self.allocator, edge_name);

                // Get children from result_set
                const child_ids = try self.getChildIdsFromResultSetForEdge(node_id, edge_name);
                defer self.allocator.free(child_ids);

                if (child_ids.len == 0) return; // No children to add

                var children_entries = std.ArrayListUnmanaged(ReactiveTree.ChildEntry){};
                defer children_entries.deinit(self.allocator);

                for (child_ids) |child_id| {
                    const result_node = self.subscription.result_set.getNode(child_id) orelse continue;
                    try children_entries.append(self.allocator, .{
                        .id = child_id,
                        .sort_key = result_node.key,
                    });
                }

                // Add children to reactive_tree (setChildren links into visible chain if edge expanded)
                // Observer will emit enter/leave events
                try self.reactive_tree.setChildren(node_id, edge_name, children_entries.items);
            }
        } else {
            // Parent not in tree - mark viewport dirty for full reload
            self.viewport_dirty = true;
        }
    }

    /// Emit enter event for a child node by ID and index.
    fn emitEnterForChild(self: *Self, child_id: NodeId, index: u32) void {
        if (self.external_callbacks.on_enter == null) return;

        const node = self.store.get(child_id) orelse return;

        // Create Item for the callback
        const path = self.allocator.alloc(PathSegment, 1) catch return;
        path[0] = .{ .root = child_id };

        var visited = std.AutoHashMapUnmanaged(NodeId, void){};
        defer visited.deinit(self.allocator);

        var item = self.tracker.executor.materialize(node, &.{}, path, &visited) catch {
            self.allocator.free(path);
            return;
        };
        defer item.deinit();

        self.external_callbacks.on_enter.?(self.external_callbacks.context, item, index);
    }

    /// Collapse an edge by node ID directly.
    /// This also recursively clears expansion state of all descendants.
    /// Observer callbacks handle enter/leave events automatically.
    pub fn collapseById(self: *Self, node_id: NodeId, edge_name: []const u8) void {
        // First, recursively clear expansion state of children under this edge
        self.clearDescendantExpansions(node_id, edge_name);

        // Remove this specific edge from expanded state
        if (self.expanded_nodes.getPtr(node_id)) |node_edges| {
            _ = node_edges.edges.remove(edge_name);
            if (node_edges.edges.count() == 0) {
                node_edges.deinit(self.allocator);
                _ = self.expanded_nodes.remove(node_id);
            }
        }

        self.viewport_dirty = true;
    }

    /// Emit leave event for a child node by ID and index.
    fn emitLeaveForChild(self: *Self, child_id: NodeId, index: u32) void {
        if (self.external_callbacks.on_leave == null) return;

        const node = self.store.get(child_id) orelse return;

        // Create Item for the callback
        const path = self.allocator.alloc(PathSegment, 1) catch return;
        path[0] = .{ .root = child_id };

        var visited = std.AutoHashMapUnmanaged(NodeId, void){};
        defer visited.deinit(self.allocator);

        var item = self.tracker.executor.materialize(node, &.{}, path, &visited) catch {
            self.allocator.free(path);
            return;
        };
        defer item.deinit();

        self.external_callbacks.on_leave.?(self.external_callbacks.context, item, index);
    }

    /// Recursively clear expansion state of all descendants under a given edge.
    /// Uses the reactive_tree to find actual visible children (handles virtual edges correctly).
    fn clearDescendantExpansions(self: *Self, parent_id: NodeId, edge_name: []const u8) void {
        // Get the tree node to find actual visible children
        const tree_node = self.reactive_tree.get(parent_id) orelse return;
        const edge = tree_node.getEdge(edge_name) orelse return;

        // Iterate through children for this edge (using sibling chain)
        var child = edge.head;
        while (child) |c| {
            const child_id = c.id;
            child = c.next_sibling;

            // Recursively clear this child's expansions
            if (self.expanded_nodes.getPtr(child_id)) |child_edges| {
                // Collect edge names to iterate (can't modify while iterating)
                var edges_to_clear: [16][]const u8 = undefined;
                var edge_count: usize = 0;
                var edge_iter = child_edges.edges.keyIterator();
                while (edge_iter.next()) |key| {
                    if (edge_count < 16) {
                        edges_to_clear[edge_count] = key.*;
                        edge_count += 1;
                    }
                }

                // Recursively clear each expanded edge
                for (edges_to_clear[0..edge_count]) |child_edge| {
                    self.clearDescendantExpansions(child_id, child_edge);
                }

                // Clear this child's expansion state
                child_edges.deinit(self.allocator);
                _ = self.expanded_nodes.remove(child_id);
            }
        }
    }

    /// Toggle expansion of an edge by node ID directly.
    pub fn toggleById(self: *Self, node_id: NodeId, edge_name: []const u8) !bool {
        // Check if this specific edge is expanded
        if (self.expanded_nodes.get(node_id)) |node_edges| {
            if (node_edges.edges.contains(edge_name)) {
                self.collapseById(node_id, edge_name);
                return false;
            }
        }
        try self.expandById(node_id, edge_name);
        return true;
    }

    /// Count children of a node for a specific edge.
    fn countChildrenForEdge(self: *Self, node_id: NodeId, edge_name: []const u8) !u32 {
        const node = self.store.get(node_id) orelse return 0;

        // Get edge definition from schema
        const edge_def = self.schema.getEdgeDef(node.type_id, edge_name) orelse return 0;
        // Get edge targets
        const targets = node.getEdgeTargets(edge_def.id);
        return @intCast(targets.len);
    }

    /// Count children of a node (finds first matching edge from query).
    fn countChildren(self: *Self, node_id: NodeId) !u32 {
        const node = self.store.get(node_id) orelse return 0;

        // Look for an edge selection that matches this node
        const query = self.subscription.query;
        for (query.selections) |sel| {
            // Get edge definition from schema
            const edge_def = self.schema.getEdgeDef(node.type_id, sel.name) orelse continue;
            // Get edge targets
            const targets = node.getEdgeTargets(edge_def.id);
            return @intCast(targets.len);
        }

        return 0;
    }

    /// Check if an edge is expanded by node ID.
    pub fn isExpandedById(self: *const Self, node_id: NodeId, edge_name: []const u8) bool {
        // Check if this specific edge is expanded for this node
        if (self.expanded_nodes.get(node_id)) |node_edges| {
            return node_edges.edges.contains(edge_name);
        }
        return false;
    }

    /// Collapse all expanded nodes.
    pub fn collapseAll(self: *Self) void {
        // Clear all expansion state
        var iter = self.expanded_nodes.valueIterator();
        while (iter.next()) |node_edges| {
            node_edges.deinit(self.allocator);
        }
        self.expanded_nodes.clearRetainingCapacity();

        // Mark all TreeNodes as collapsed
        var tree_iter = self.reactive_tree.all_nodes.valueIterator();
        while (tree_iter.next()) |tree_node| {
            tree_node.*.clearExpanded(self.allocator);
        }

        // Mark viewport dirty
        self.viewport_dirty = true;
    }

    /// Expand all nodes up to a maximum depth.
    /// If max_depth is null, expands everything (be careful with large graphs).
    pub fn expandAll(self: *Self, max_depth: ?u32) !void {
        // Get items currently in viewport to start expansion from
        var items_iter = self.items();
        var items_to_expand = std.ArrayListUnmanaged(struct { id: NodeId, depth: u32 }){};
        defer items_to_expand.deinit(self.allocator);

        while (items_iter.next()) |item| {
            // Check if this item can be expanded and is within depth limit
            if (max_depth) |limit| {
                if (item.depth >= limit) continue;
            }

            // Get expandable edges for this item
            const node = self.store.get(item.id) orelse continue;
            const query = self.subscription.query;

            // Find edge selections that apply to this node type
            for (query.selections) |sel| {
                // Check if node type matches
                const edge_def = self.schema.getEdgeDef(node.type_id, sel.name) orelse continue;
                const targets = node.getEdgeTargets(edge_def.id);

                if (targets.len > 0 and !self.isExpandedById(item.id, sel.name)) {
                    try items_to_expand.append(self.allocator, .{ .id = item.id, .depth = item.depth });
                    break;
                }
            }
        }

        // Expand all collected items
        for (items_to_expand.items) |item| {
            const node = self.store.get(item.id) orelse continue;
            const query = self.subscription.query;

            for (query.selections) |sel| {
                const edge_def = self.schema.getEdgeDef(node.type_id, sel.name) orelse continue;
                const targets = node.getEdgeTargets(edge_def.id);

                if (targets.len > 0 and !self.isExpandedById(item.id, sel.name)) {
                    try self.expandById(item.id, sel.name);
                }
            }
        }

        // If we expanded anything and there's more depth to go, recurse
        if (items_to_expand.items.len > 0) {
            const new_max = if (max_depth) |d| d else null;
            // Recursively expand children (the newly expanded items will be in viewport after reload)
            if (new_max == null or new_max.? > 1) {
                try self.expandAll(if (new_max) |d| d - 1 else null);
            }
        }
    }

    /// Get node by ID.
    pub fn get(self: *const Self, node_id: NodeId) ?*TreeNode {
        return self.reactive_tree.get(node_id);
    }

    /// Get index of a node by ID.
    /// Returns the index within the currently loaded viewport, or null if not loaded.
    pub fn indexOfId(self: *Self, node_id: NodeId) ?u32 {
        // Ensure viewport is loaded before looking up
        self.ensureViewportLoaded() catch {};
        return self.reactive_tree.indexOfId(node_id);
    }

    /// Get node at path.
    pub fn nodeAt(self: *Self, path: Path) ?*TreeNode {
        const node_id = self.nodeIdFromPath(path) orelse return null;
        return self.reactive_tree.get(node_id);
    }

    /// Get index for a path.
    pub fn pathToIndex(self: *Self, path: Path) ?u32 {
        const node_id = self.nodeIdFromPath(path) orelse return null;
        return self.reactive_tree.indexOfId(node_id);
    }

    // ========================================================================
    // Child Loading
    // ========================================================================

    /// Lazy load children from store via tracker (query-aware, respects filters).
    /// Children are added to result_set and node_to_subs for reactive updates.
    fn loadChildren(self: *Self, parent_id: NodeId, edge_name: []const u8) !void {
        // Ask tracker to lazy-load children (adds to result_set and node_to_subs)
        const loaded_ids = try self.tracker.loadChildrenLazy(
            self.subscription,
            parent_id,
            edge_name,
        );
        defer if (loaded_ids.len > 0) self.allocator.free(loaded_ids);

        // Also check for children already in result_set (from onLink during this session)
        const parent_node = self.store.get(parent_id) orelse return;
        const edge_def = self.schema.getEdgeDef(parent_node.type_id, edge_name) orelse return;

        // Build children array from result_set
        var children = std.ArrayListUnmanaged(ReactiveTree.ChildEntry){};
        defer children.deinit(self.allocator);

        var iter = self.subscription.result_set.iterator();
        while (iter.next()) |result_node| {
            if (result_node.ancestry.len == 0) continue;
            if (result_node.ancestry[result_node.ancestry.len - 1] != parent_id) continue;

            const child_store_node = self.store.get(result_node.id) orelse continue;
            if (child_store_node.type_id != edge_def.target_type_id) continue;

            try children.append(self.allocator, .{
                .id = result_node.id,
                .sort_key = result_node.key,
            });
        }

        if (children.items.len == 0) return;

        // Set children in reactive tree
        try self.reactive_tree.setChildren(parent_id, edge_name, children.items);
    }

    // ========================================================================
    // Path Conversion
    // ========================================================================

    /// Convert a Path to a NodeId.
    /// Path structure: [{root: NodeId}, {edge: {name, index}}, ...]
    fn nodeIdFromPath(self: *const Self, path: Path) ?NodeId {
        if (path.len == 0) return null;

        // First segment must be a root
        const root_id = switch (path[0]) {
            .root => |id| id,
            .edge => return null,
        };

        if (path.len == 1) return root_id;

        // Walk down the path
        var current_id = root_id;
        for (path[1..]) |segment| {
            switch (segment) {
                .root => return null, // Invalid: root in middle of path
                .edge => |e| {
                    const tree_node = self.reactive_tree.get(current_id) orelse return null;
                    const edge = tree_node.getEdge(e.name) orelse return null;

                    // Find child at index
                    var child = edge.head;
                    var idx: u32 = 0;
                    while (child) |c| {
                        if (idx == e.index) {
                            current_id = c.id;
                            break;
                        }
                        idx += 1;
                        child = c.next_sibling;
                    } else {
                        return null; // Index out of bounds
                    }
                },
            }
        }

        return current_id;
    }

    // ========================================================================
    // Internal callback handlers (called by subscription)
    // ========================================================================

    fn handleEnter(ctx: ?*anyopaque, item: Item, index: u32) void {
        const self: *Self = @ptrCast(@alignCast(ctx));

        // Invalidate cached root count
        self.cached_root_count = null;

        // Get result node from subscription's result set
        const result_node = self.subscription.result_set.getNode(item.id) orelse return;
        const sort_key = result_node.key;

        // Note: external on_enter callbacks are handled by the observer when the tree changes

        if (result_node.ancestry.len == 0) {
            // Root node - insert as root
            // Observer will emit on_enter callback
            _ = self.reactive_tree.insertRootAt(item.id, sort_key, index) catch return;
            // Only adjust viewport if it's been loaded (not dirty)
            // If dirty, reloadViewport will resync from scratch
            if (!self.viewport_dirty) {
                self.viewport.adjustAfterInsert(index, 1);
                // Sync viewport.first with reactive_tree.visible_head in case
                // the new node was inserted at the beginning
                self.viewport.first = self.reactive_tree.visible_head;
            }
        } else {
            // Child node - find parent and check if edge is expanded
            const parent_id = result_node.ancestry[result_node.ancestry.len - 1];

            // Find which edge from parent leads to this child's type
            const child_store_node = self.store.get(item.id) orelse return;
            const parent_store_node = self.store.get(parent_id) orelse return;

            // First, find edge name using result_node.edge_name (set by loadChildrenLazy)
            // or by looking up in schema
            var edge_name: ?[]const u8 = result_node.edge_name;

            // If not set, try to find from schema by looking at parent's expanded edges
            if (edge_name == null) {
                if (self.expanded_nodes.get(parent_id)) |parent_expanded| {
                    var edge_iter = parent_expanded.edges.keyIterator();
                    while (edge_iter.next()) |e_name_ptr| {
                        const e_name = e_name_ptr.*;
                        const edge_def = self.schema.getEdgeDef(parent_store_node.type_id, e_name) orelse continue;
                        if (edge_def.target_type_id == child_store_node.type_id) {
                            edge_name = e_name;
                            break;
                        }
                    }
                }
            }

            // Increment expanded_nodes count for parent's edge
            // This must happen whether or not parent is in reactive_tree
            if (edge_name) |e_name| {
                const is_edge_expanded = if (self.expanded_nodes.getPtr(parent_id)) |parent_edges|
                    parent_edges.edges.contains(e_name)
                else
                    false;

                if (is_edge_expanded) {
                    if (self.expanded_nodes.getPtr(parent_id)) |parent_edges| {
                        if (parent_edges.edges.getPtr(e_name)) |count| {
                            count.* += 1;
                        }
                    }
                }

                // Add to reactive_tree
                // Observer will emit on_enter callback if node becomes visible
                if (self.reactive_tree.get(parent_id)) |parent_tree_node| {
                    // Parent is in tree - add as child if expanded
                    // Use is_edge_expanded (from expanded_nodes) NOT tree_node.isExpanded()
                    // because expandById doesn't update TreeNode.expanded_edges
                    if (is_edge_expanded) {
                        // Ensure parent's TreeNode.expanded_edges is in sync with expanded_nodes
                        // This is needed because insertChild checks areChildrenVisible internally
                        if (!parent_tree_node.isExpanded(e_name)) {
                            parent_tree_node.markExpanded(self.allocator, e_name) catch return;
                        }
                        _ = self.reactive_tree.insertChild(parent_id, e_name, item.id, sort_key) catch return;
                        // Viewport adjustment handled by insertChild's visibility propagation
                    }
                } else if (is_edge_expanded) {
                    // Parent is NOT in tree (e.g., virtual root), but edge is expanded
                    // Insert child as a root-level item
                    _ = self.reactive_tree.insertRootAt(item.id, sort_key, index) catch return;
                    if (!self.viewport_dirty) {
                        self.viewport.adjustAfterInsert(index, 1);
                        self.viewport.first = self.reactive_tree.visible_head;
                    }
                }
            }
            // If parent's edge isn't expanded, child stays in result_set for later
        }
    }

    fn handleLeave(ctx: ?*anyopaque, item: Item, index: u32) void {
        // Safety check: verify context pointer is valid
        if (ctx == null) return;
        const self: *Self = @ptrCast(@alignCast(ctx));

        // Invalidate cached root count
        self.cached_root_count = null;

        // Remove from expanded_nodes if present (clean up expansion state)
        if (self.expanded_nodes.getPtr(item.id)) |node_edges| {
            node_edges.deinit(self.allocator);
            _ = self.expanded_nodes.remove(item.id);
        }

        // Try to decrement parent's expanded children count
        // First check if node is in reactive_tree (fast path)
        if (self.reactive_tree.get(item.id)) |node| {
            if (node.parent) |parent| {
                if (node.edge_name) |edge_name| {
                    if (self.expanded_nodes.getPtr(parent.id)) |parent_edges| {
                        if (parent_edges.edges.getPtr(edge_name)) |count| {
                            if (count.* > 0) count.* -= 1;
                        }
                    }
                }
            }

            // IMPORTANT: Update viewport.first BEFORE removing from tree
            // because removeRoot/removeChild frees the node memory.
            // If viewport.first points to the removed node, it becomes dangling.
            if (self.viewport.first == node) {
                self.viewport.first = node.next_visible;
            }

            // Remove from reactive_tree (this frees the node)
            const removed_count = node.visible_count;
            if (node.parent == null) {
                self.reactive_tree.removeRoot(item.id);
            } else {
                self.reactive_tree.removeChild(item.id);
            }

            // Adjust viewport offset if needed
            if (!self.viewport_dirty) {
                self.viewport.adjustAfterRemove(index, removed_count);
            }
        } else {
            // Node not in reactive_tree - check result_set for parent/edge info
            if (self.subscription.result_set.getNode(item.id)) |result_node| {
                if (result_node.ancestry.len > 0) {
                    const parent_id = result_node.ancestry[result_node.ancestry.len - 1];
                    // Use stored edge_name to decrement parent's children count
                    if (result_node.edge_name) |edge_name| {
                        if (self.expanded_nodes.getPtr(parent_id)) |parent_edges| {
                            if (parent_edges.edges.getPtr(edge_name)) |count| {
                                if (count.* > 0) count.* -= 1;
                            }
                        }
                    }
                }
            }
            // Node not in tree, so observer won't fire - emit directly
            if (self.external_callbacks.on_leave) |cb| {
                cb(self.external_callbacks.context, item, index);
            }
        }
        // When node IS in tree, observer handles on_leave - see observerWillRemove
    }

    fn handleChange(ctx: ?*anyopaque, item: Item, index: u32, old: Item) void {
        const self: *Self = @ptrCast(@alignCast(ctx));

        // Update reactive_tree if node is loaded (optional - tree may not be loaded yet)
        if (self.subscription.result_set.getNode(item.id)) |result_node| {
            const new_sort_key = result_node.key;
            if (self.reactive_tree.get(item.id)) |node| {
                if (node.sort_key.order(new_sort_key) != .eq) {
                    _ = self.reactive_tree.updateRootKey(item.id, new_sort_key);
                }
            }
        }

        // Call external callback if set (always, regardless of tree load state)
        if (self.external_callbacks.on_change) |cb| {
            cb(self.external_callbacks.context, item, index, old);
        }
    }

    fn handleMove(ctx: ?*anyopaque, item: Item, _: u32, to: u32) void {
        const self: *Self = @ptrCast(@alignCast(ctx));

        // Update reactive_tree if node is loaded (optional - tree may not be loaded yet)
        // The observer will handle emitting on_move callback
        if (self.reactive_tree.get(item.id) != null) {
            self.reactive_tree.moveRoot(item.id, to);
            // Sync viewport.first with reactive_tree.visible_head after move
            self.viewport.first = self.reactive_tree.visible_head;
        }
        // Observer handles on_move callback - see observerDidMove
    }

    // ========================================================================
    // Visible chain observer callbacks
    // ========================================================================

    /// Called BEFORE nodes are removed from visible chain.
    /// Emits on_leave for nodes that were in the viewport.
    fn observerWillRemove(ctx: ?*anyopaque, first: *TreeNode, start_index: u32, count: u32) void {
        const self: *Self = @ptrCast(@alignCast(ctx));
        if (self.external_callbacks.on_leave == null) return;

        const vp_start = self.viewport.offset;
        const vp_end = self.viewport.offset +| self.viewport.height;

        // Walk the chain and emit leave for nodes in viewport
        var node: ?*TreeNode = first;
        var idx = start_index;
        var remaining = count;

        while (node) |n| {
            if (remaining == 0) break;
            if (idx >= vp_end) break; // Past viewport, no more to emit

            if (idx >= vp_start) {
                self.emitLeaveForChild(n.id, idx);
            }

            idx += 1;
            remaining -= 1;
            node = n.next_visible;
        }
    }

    /// Called AFTER nodes are removed from visible chain.
    /// Emits on_enter for nodes that scrolled into the viewport.
    fn observerDidRemove(ctx: ?*anyopaque, _: u32, count: u32, new_total: u32) void {
        const self: *Self = @ptrCast(@alignCast(ctx));
        if (self.external_callbacks.on_enter == null) return;

        const vp_end = self.viewport.offset +| self.viewport.height;

        // After removal, items that were at [vp_end, vp_end + count) are now at [vp_end - count, vp_end)
        // These scrolled into the viewport and need enter events
        if (new_total <= self.viewport.offset) return; // Nothing in viewport

        // Calculate where scroll-in items are now
        const scroll_in_start = if (vp_end > count) vp_end - count else 0;
        const scroll_in_start_capped = @max(scroll_in_start, self.viewport.offset);
        const scroll_in_end = @min(vp_end, new_total);

        if (scroll_in_start_capped >= scroll_in_end) return;

        // Walk to the scroll-in start position
        var node = self.reactive_tree.visible_head;
        var idx: u32 = 0;

        while (node) |n| {
            if (idx >= scroll_in_start_capped) break;
            idx += 1;
            node = n.next_visible;
        }

        // Emit enter for scroll-in items
        while (node) |n| {
            if (idx >= scroll_in_end) break;
            self.emitEnterForChild(n.id, idx);
            idx += 1;
            node = n.next_visible;
        }
    }

    /// Called AFTER nodes are inserted into visible chain.
    /// Emits on_enter for inserted nodes in viewport, on_leave for pushed-out nodes.
    fn observerDidInsert(ctx: ?*anyopaque, first: *TreeNode, start_index: u32, count: u32, new_total: u32) void {
        const self: *Self = @ptrCast(@alignCast(ctx));

        const vp_start = self.viewport.offset;
        const vp_end = self.viewport.offset +| self.viewport.height;

        // Emit on_enter for inserted nodes that are in viewport
        if (self.external_callbacks.on_enter != null) {
            var node: ?*TreeNode = first;
            var idx = start_index;
            var remaining = count;

            while (node) |n| {
                if (remaining == 0) break;
                if (idx >= vp_end) break; // Past viewport

                if (idx >= vp_start) {
                    self.emitEnterForChild(n.id, idx);
                }

                idx += 1;
                remaining -= 1;
                node = n.next_visible;
            }
        }

        // Emit on_leave for nodes pushed out of viewport
        // Nodes that were at [vp_end - count, vp_end) are now at [vp_end, vp_end + count)
        if (self.external_callbacks.on_leave != null and start_index < vp_end) {
            const old_vp_end = vp_end -| count;
            const pushed_start = @max(old_vp_end, vp_start);

            if (pushed_start < vp_end and new_total > vp_end) {
                // Walk to find the pushed-out nodes (now at vp_end onwards)
                var node = self.reactive_tree.visible_head;
                var idx: u32 = 0;

                // Skip to vp_end
                while (node) |n| {
                    if (idx >= vp_end) break;
                    idx += 1;
                    node = n.next_visible;
                }

                // Emit leave for pushed-out nodes
                var emitted: u32 = 0;
                const to_emit = @min(count, new_total - vp_end);
                while (node) |n| {
                    if (emitted >= to_emit) break;
                    // Use original index (before insertion) for the callback
                    self.emitLeaveForChild(n.id, pushed_start + emitted);
                    emitted += 1;
                    node = n.next_visible;
                }
            }
        }
    }

    /// Called AFTER a node moved to a new position.
    /// Emits on_move if both indices in viewport, or on_enter/on_leave if crossing boundary.
    fn observerDidMove(ctx: ?*anyopaque, node: *TreeNode, old_index: u32, new_index: u32) void {
        const self: *Self = @ptrCast(@alignCast(ctx));

        const vp_start = self.viewport.offset;
        const vp_end = self.viewport.offset +| self.viewport.height;

        const was_in_vp = old_index >= vp_start and old_index < vp_end;
        const is_in_vp = new_index >= vp_start and new_index < vp_end;

        if (was_in_vp and is_in_vp) {
            // Both in viewport - emit move
            if (self.external_callbacks.on_move) |cb| {
                const store_node = self.store.get(node.id) orelse return;
                const path = self.allocator.alloc(PathSegment, 1) catch return;
                path[0] = .{ .root = node.id };

                var visited = std.AutoHashMapUnmanaged(NodeId, void){};
                defer visited.deinit(self.allocator);

                var item = self.tracker.executor.materialize(store_node, &.{}, path, &visited) catch {
                    self.allocator.free(path);
                    return;
                };
                defer item.deinit();

                cb(self.external_callbacks.context, item, old_index, new_index);
            }
        } else if (was_in_vp and !is_in_vp) {
            // Left viewport
            self.emitLeaveForChild(node.id, old_index);
        } else if (!was_in_vp and is_in_vp) {
            // Entered viewport
            self.emitEnterForChild(node.id, new_index);
        }
        // If both outside viewport, nothing to emit
    }
};

// ============================================================================
// Item Iterator
// ============================================================================

/// Iterator adapter that wraps TreeNode iteration with store lookups.
pub const ItemIterator = struct {
    /// Max number of expanded edges to track per node (most nodes have few edges)
    const MAX_EXPANDED_EDGES = 8;

    tree_iter: Viewport.Iterator,
    store: *const NodeStore,
    schema: *const Schema,
    /// Temporary storage for expanded edge names (reused per next() call)
    expanded_buffer: [MAX_EXPANDED_EDGES][]const u8 = undefined,

    pub const ItemView = struct {
        id: NodeId,
        node: *TreeNode,
        depth: u32,
        has_children: bool,
        expanded_edges: []const []const u8,
    };

    pub fn next(self: *ItemIterator) ?ItemView {
        const tree_node = self.tree_iter.next() orelse return null;

        // Use stored depth (works even when parent is not in tree)
        const depth = tree_node.depth;

        // Check if node has any children (from any edge)
        const has_children = tree_node.totalChildCount() > 0 or
            self.nodeHasEdgesInSchema(tree_node.id);

        // Collect expanded edge names into buffer
        var exp_count: usize = 0;
        var exp_iter = tree_node.expanded_edges.iterator();
        while (exp_iter.next()) |entry| {
            if (exp_count >= MAX_EXPANDED_EDGES) break;
            self.expanded_buffer[exp_count] = entry.key_ptr.*;
            exp_count += 1;
        }

        return .{
            .id = tree_node.id,
            .node = tree_node,
            .depth = depth,
            .has_children = has_children,
            .expanded_edges = self.expanded_buffer[0..exp_count],
        };
    }

    fn nodeHasEdgesInSchema(self: *const ItemIterator, node_id: NodeId) bool {
        const node = self.store.get(node_id) orelse return false;
        const type_def = self.schema.getTypeById(node.type_id) orelse return false;
        return type_def.edges.len > 0;
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;

fn testKey(val: i64) CompoundKey {
    return CompoundKey.encodePartial(&.{Value{ .int = val }}, &.{.asc});
}

test "Tree basic creation" {
    // This test verifies Tree can be created and basic operations work
    // Full integration test requires complete setup with schema/store/tracker
}
