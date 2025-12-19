///! Change tracker for reactive updates.
///!
///! Tracks all subscriptions and dispatches events when data changes.

const std = @import("std");
const Allocator = std.mem.Allocator;

const Node = @import("../node.zig").Node;
const NodeId = @import("../node.zig").NodeId;
const TypeId = @import("../node.zig").TypeId;
const EdgeId = @import("../node.zig").EdgeId;
const NodeStore = @import("../node_store.zig").NodeStore;
const Schema = @import("../schema.zig").Schema;
const IndexManager = @import("../index/index.zig").IndexManager;
const IndexCoverage = @import("../index/index.zig").IndexCoverage;
const RollupCache = @import("../rollup/cache.zig").RollupCache;
const query_builder = @import("../query/builder.zig");
const Query = query_builder.Query;
const EdgeSelection = query_builder.EdgeSelection;
const Sort = query_builder.Sort;
const Filter = query_builder.Filter;
const executor_mod = @import("../query/executor.zig");
const Executor = executor_mod.Executor;
const Item = executor_mod.Item;
const PathSegment = executor_mod.PathSegment;
const key_mod = @import("../index/key.zig");
const CompoundKey = key_mod.CompoundKey;
const MAX_KEY_SIZE = key_mod.MAX_KEY_SIZE;
const GroupedMap = @import("../ds.zig").GroupedMap;

const result_set_mod = @import("result_set.zig");
const Subscription = @import("subscription.zig").Subscription;
const SubscriptionId = @import("subscription.zig").SubscriptionId;
const Callbacks = @import("subscription.zig").Callbacks;

// ============================================================================
// Node Callbacks - Per-node change notifications for SDK entity mapper
// ============================================================================

/// Callback function types for individual node events.
pub const NodeChangeFn = *const fn (ctx: ?*anyopaque, id: NodeId, node: *const Node, old_node: *const Node) void;
pub const NodeDeleteFn = *const fn (ctx: ?*anyopaque, id: NodeId) void;
pub const NodeLinkFn = *const fn (ctx: ?*anyopaque, id: NodeId, edge_name: []const u8, target: NodeId) void;

/// Callbacks for a watched node.
pub const NodeCallbacks = struct {
    on_change: ?NodeChangeFn = null,
    on_delete: ?NodeDeleteFn = null,
    on_link: ?NodeLinkFn = null,
    on_unlink: ?NodeLinkFn = null,
    context: ?*anyopaque = null,
};

/// Manages all subscriptions and dispatches change events.
pub const ChangeTracker = struct {
    subscriptions: std.AutoHashMapUnmanaged(SubscriptionId, *Subscription),
    by_type: GroupedMap(TypeId, *Subscription),
    next_id: SubscriptionId,
    store: *const NodeStore,
    schema: *const Schema,
    indexes: *const IndexManager,
    rollups: *RollupCache,
    executor: Executor,
    allocator: Allocator,

    /// Reverse index: visible NodeId → subscriptions containing that node.
    /// Used for O(1) lookup when a visible node changes.
    node_to_subs: GroupedMap(NodeId, *Subscription),

    /// Reverse index: virtual NodeId → subscriptions traversing that node.
    /// Used for O(1) lookup when a virtual ancestor changes.
    virtual_to_subs: GroupedMap(NodeId, *Subscription),

    /// Per-node callbacks for SDK entity mapper.
    /// Maps NodeId → callbacks for that specific node.
    node_callbacks: std.AutoHashMapUnmanaged(NodeId, NodeCallbacks),

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        store: *const NodeStore,
        schema: *const Schema,
        indexes: *const IndexManager,
        rollups: *RollupCache,
    ) Self {
        return .{
            .subscriptions = .{},
            .by_type = GroupedMap(TypeId, *Subscription).init(allocator),
            .next_id = 1,
            .store = store,
            .schema = schema,
            .indexes = indexes,
            .rollups = rollups,
            .executor = Executor.init(allocator, store, schema, indexes, rollups),
            .allocator = allocator,
            .node_to_subs = GroupedMap(NodeId, *Subscription).init(allocator),
            .virtual_to_subs = GroupedMap(NodeId, *Subscription).init(allocator),
            .node_callbacks = .{},
        };
    }

    pub fn deinit(self: *Self) void {
        // Free subscriptions
        var sub_iter = self.subscriptions.iterator();
        while (sub_iter.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.subscriptions.deinit(self.allocator);

        // Free type groups
        self.by_type.deinit();

        // Free reverse indexes
        self.node_to_subs.deinit();
        self.virtual_to_subs.deinit();

        // Free node callbacks
        self.node_callbacks.deinit(self.allocator);
    }

    /// Create a new subscription.
    pub fn subscribe(
        self: *Self,
        query: *const Query,
        coverage: IndexCoverage,
    ) !*Subscription {
        const id = self.next_id;
        self.next_id += 1;

        const sub = try self.allocator.create(Subscription);
        sub.* = Subscription.init(self.allocator, id, query, coverage);

        try self.subscriptions.put(self.allocator, id, sub);

        // Add to type index
        try self.by_type.add(query.root_type_id, sub);

        // Initialize result set with current matching nodes
        try self.initializeResultSet(sub);

        return sub;
    }

    /// Remove a subscription.
    pub fn unsubscribe(self: *Self, id: SubscriptionId) void {
        if (self.subscriptions.fetchRemove(id)) |entry| {
            const sub = entry.value;

            // Remove from type index
            _ = self.by_type.remove(sub.query.root_type_id, sub);

            // Remove all visible nodes from reverse index
            var iter = sub.result_set.iterator();
            while (iter.next()) |result_node| {
                _ = self.node_to_subs.remove(result_node.id, sub);
            }

            // Remove all virtual nodes from reverse index
            var virt_iter = sub.virtual_descendants.keyIterator();
            while (virt_iter.next()) |virtual_id| {
                _ = self.virtual_to_subs.remove(virtual_id, sub);
            }

            sub.deinit();
            self.allocator.destroy(sub);
        }
    }

    // ========================================================================
    // Node Callbacks - Per-node watching for SDK entity mapper
    // ========================================================================

    /// Register callbacks for a specific node.
    /// Callbacks fire when the node is updated, deleted, or edges are linked/unlinked.
    pub fn watchNode(self: *Self, id: NodeId, callbacks: NodeCallbacks) !void {
        try self.node_callbacks.put(self.allocator, id, callbacks);
    }

    /// Unregister callbacks for a specific node.
    pub fn unwatchNode(self: *Self, id: NodeId) void {
        _ = self.node_callbacks.remove(id);
    }

    /// Fire on_change callback for a watched node.
    fn fireNodeChange(self: *Self, id: NodeId, node: *const Node, old_node: *const Node) void {
        if (self.node_callbacks.get(id)) |cbs| {
            if (cbs.on_change) |cb| {
                cb(cbs.context, id, node, old_node);
            }
        }
    }

    /// Fire on_delete callback for a watched node.
    fn fireNodeDelete(self: *Self, id: NodeId) void {
        if (self.node_callbacks.get(id)) |cbs| {
            if (cbs.on_delete) |cb| {
                cb(cbs.context, id);
            }
        }
        // Clean up the watch after delete
        _ = self.node_callbacks.remove(id);
    }

    /// Fire on_link callback for a watched node.
    fn fireNodeLink(self: *Self, id: NodeId, edge_name: []const u8, target: NodeId) void {
        if (self.node_callbacks.get(id)) |cbs| {
            if (cbs.on_link) |cb| {
                cb(cbs.context, id, edge_name, target);
            }
        }
    }

    /// Fire on_unlink callback for a watched node.
    fn fireNodeUnlink(self: *Self, id: NodeId, edge_name: []const u8, target: NodeId) void {
        if (self.node_callbacks.get(id)) |cbs| {
            if (cbs.on_unlink) |cb| {
                cb(cbs.context, id, edge_name, target);
            }
        }
    }

    /// Called when a node is inserted.
    /// This handles root-level nodes that directly match a subscription's root type.
    pub fn onInsert(self: *Self, node: *const Node) void {
        const subs = self.by_type.getForKey(node.type_id);
        if (subs.len == 0) return;

        for (subs) |sub| {
            // Only handle root-level inserts here (non-virtual roots)
            // Virtual roots and nested nodes are handled via onLink
            if (sub.query.virtual) continue;

            // Check if new node matches query filters
            if (!self.executor.matchesFilters(node, sub.query.filters)) continue;

            // Skip if query specifies a root_id that doesn't match this node
            if (sub.query.root_id) |root_id| {
                if (root_id != node.id) continue;
            }

            // Invalidate cached total (a matching node was added)
            sub.cached_total = null;

            // If subscription is initialized, add to result_set and track
            if (sub.initialized) {
                // Skip if somehow already in result set
                if (sub.result_set.contains(node.id)) continue;

                // Use query sorts for key to match recomputeKey and loadResultSet
                var key = CompoundKey{};
                appendSortKey(&key, node, sub.query.sorts);
                _ = sub.result_set.insertSorted(node.id, key, null) catch continue;
                self.node_to_subs.add(node.id, sub) catch continue;

                const idx = sub.result_set.indexOf(node.id) orelse continue;

                var item = self.materializeItem(node, sub.query.selections) catch continue;
                defer item.deinit();

                sub.emitEnter(item, idx);
            } else {
                // Not initialized - emit callback with index 0 (position unknown until loaded)
                // This allows reactive callbacks to work before first render
                var item = self.materializeItem(node, sub.query.selections) catch continue;
                defer item.deinit();

                sub.emitEnter(item, 0);
            }
        }
    }

    /// Called when a node is updated.
    /// Uses hybrid reactivity: fast path for loaded nodes, slow path for unloaded.
    pub fn onUpdate(self: *Self, node: *const Node, old_node: *const Node) void {
        // Fast path: Handle updates to loaded/visible nodes via reverse index
        const visible_subs = self.node_to_subs.getForKey(node.id);
        for (visible_subs) |sub| {
            self.handleVisibleNodeUpdate(sub, node, old_node);
        }

        // Handle updates to virtual nodes - affects their visible descendants
        const virtual_subs = self.virtual_to_subs.getForKey(node.id);
        for (virtual_subs) |sub| {
            self.handleVirtualNodeUpdate(sub, node, old_node);
        }

        // Slow path: Check all subscriptions by type for enter/leave transitions
        // This works even for unloaded subscriptions by comparing old vs new filter match
        const type_subs = self.by_type.getForKey(node.type_id);
        for (type_subs) |sub| {
            // Only handle root-level for now
            if (sub.query.virtual) continue;

            // Skip if already handled by fast path (node is loaded/tracked)
            const tracked_subs = self.node_to_subs.getForKey(node.id);
            var already_tracked = false;
            for (tracked_subs) |tracked_sub| {
                if (tracked_sub == sub) {
                    already_tracked = true;
                    break;
                }
            }
            if (already_tracked) continue;

            // Determine if filter matching changed
            const matched_before = self.executor.matchesFilters(old_node, sub.query.filters);
            const matches_now = self.executor.matchesFilters(node, sub.query.filters);

            if (matched_before and matches_now) {
                // Node matched before and still matches - check for on_change
                // This handles unloaded nodes that had selected fields change
                if (self.selectedFieldsChanged(sub.query.selections, node, old_node)) {
                    sub.cached_total = null; // Invalidate cached total just in case

                    var item = self.materializeItem(node, sub.query.selections) catch continue;
                    defer item.deinit();

                    var old_item = self.materializeItem(old_node, sub.query.selections) catch continue;
                    defer old_item.deinit();

                    sub.emitChange(item, 0, old_item);
                }
                continue;
            }

            if (!matched_before and !matches_now) {
                // Node didn't match before and still doesn't - nothing to do
                continue;
            }

            // Matching status changed - invalidate cached total
            sub.cached_total = null;

            if (!matched_before and matches_now) {
                // Node now matches (on_enter)
                if (sub.initialized) {
                    // Use query sorts for key to match recomputeKey and loadResultSet
                    var key = CompoundKey{};
                    appendSortKey(&key, node, sub.query.sorts);
                    _ = sub.result_set.insertSorted(node.id, key, null) catch continue;
                    self.node_to_subs.add(node.id, sub) catch continue;

                    const idx = sub.result_set.indexOf(node.id) orelse continue;

                    var item = self.materializeItem(node, sub.query.selections) catch continue;
                    defer item.deinit();

                    sub.emitEnter(item, idx);
                } else {
                    // Not initialized - emit callback with index 0
                    var item = self.materializeItem(node, sub.query.selections) catch continue;
                    defer item.deinit();

                    sub.emitEnter(item, 0);
                }
            } else {
                // Node no longer matches (on_leave)
                if (sub.initialized) {
                    // Only emit if node is actually in result_set (not already handled by fast path)
                    const idx = sub.result_set.indexOf(node.id) orelse continue;

                    var item = self.materializeItem(old_node, sub.query.selections) catch continue;
                    defer item.deinit();

                    // Emit callback BEFORE removing from result_set
                    sub.emitLeave(item, idx);

                    // NOW remove from indexes
                    _ = self.node_to_subs.remove(node.id, sub);
                    sub.result_set.removeAndFree(node.id);
                } else {
                    // Not initialized - emit callback with index 0
                    var item = self.materializeItem(old_node, sub.query.selections) catch continue;
                    defer item.deinit();

                    sub.emitLeave(item, 0);
                }
            }
        }

        // Fire per-node callback for SDK entity mapper
        self.fireNodeChange(node.id, node, old_node);
    }

    fn handleVisibleNodeUpdate(self: *Self, sub: *Subscription, node: *const Node, old_node: *const Node) void {
        const result_node = sub.result_set.getNode(node.id) orelse return;

        // Get the filter for this node's level
        const filters = self.getFiltersForNode(sub, result_node);

        const still_matches = self.executor.matchesFilters(node, filters);

        if (!still_matches) {
            // Node no longer matches filter - emit on_leave and remove
            const idx = sub.result_set.indexOf(node.id) orelse return;

            var item = self.materializeItem(old_node, sub.query.selections) catch return;
            defer item.deinit();

            // Emit callback BEFORE removing from result_set
            sub.emitLeave(item, idx);

            // NOW remove from indexes
            _ = self.node_to_subs.remove(node.id, sub);
            sub.result_set.removeAndFree(node.id);
            return;
        }

        // Check if sort key changed (needs move)
        const old_key = result_node.key;
        const new_key = self.recomputeKey(sub, node, result_node.ancestry);
        const old_idx = sub.result_set.indexOf(node.id) orelse return;

        if (!old_key.eql(new_key)) {
            // Sort key changed - need to reposition
            // Find new position BEFORE updating key, so current node doesn't match
            const new_pos = sub.result_set.findInsertPos(new_key);
            result_node.key = new_key;
            sub.result_set.move(result_node, new_pos);

            const new_idx = sub.result_set.indexOf(node.id) orelse return;

            if (old_idx != new_idx) {
                var item = self.materializeItem(node, sub.query.selections) catch return;
                defer item.deinit();
                sub.emitMove(item, old_idx, new_idx);
            }
        }

        // Emit on_change if selected fields changed
        if (self.selectedFieldsChanged(sub.query.selections, node, old_node)) {
            var item = self.materializeItem(node, sub.query.selections) catch return;
            defer item.deinit();

            var old_item = self.materializeItem(old_node, sub.query.selections) catch return;
            defer old_item.deinit();

            const idx = sub.result_set.indexOf(node.id) orelse return;
            sub.emitChange(item, idx, old_item);
        }
    }

    fn handleVirtualNodeUpdate(self: *Self, sub: *Subscription, node: *const Node, old_node: *const Node) void {
        // Virtual ancestor changed - need to recompute keys for all visible descendants
        const affected_visible = sub.virtual_descendants.getForKey(node.id);
        if (affected_visible.len == 0) return;

        // Check if sort key component from this virtual node changed
        const sorts = self.getSortsForNode(sub, node);
        var old_key_part = CompoundKey{};
        var new_key_part = CompoundKey{};
        appendSortKey(&old_key_part, old_node, sorts);
        appendSortKey(&new_key_part, node, sorts);

        if (old_key_part.eql(new_key_part)) {
            // Sort key didn't change - no need to reposition descendants
            return;
        }

        // Recompute keys and reposition all affected visible nodes
        for (affected_visible) |visible_id| {
            const result_node = sub.result_set.getNode(visible_id) orelse continue;
            const visible_node = self.store.get(visible_id) orelse continue;

            const old_idx = sub.result_set.indexOf(visible_id) orelse continue;
            const new_key = self.recomputeKey(sub, visible_node, result_node.ancestry);

            if (!result_node.key.eql(new_key)) {
                result_node.key = new_key;
                const new_pos = sub.result_set.findInsertPos(new_key);
                sub.result_set.move(result_node, new_pos);

                const new_idx = sub.result_set.indexOf(visible_id) orelse continue;

                if (old_idx != new_idx) {
                    var item = self.materializeItem(visible_node, sub.query.selections) catch continue;
                    defer item.deinit();
                    sub.emitMove(item, old_idx, new_idx);
                }
            }
        }
    }

    /// Recompute composite key for a node given its ancestry.
    fn recomputeKey(self: *Self, sub: *const Subscription, node: *const Node, ancestry: []const NodeId) CompoundKey {
        var key = CompoundKey{};

        // Add sort values from each ancestor
        var query_level: QueryLevel = .{ .sorts = sub.query.sorts, .selections = sub.query.selections, .is_virtual = sub.query.virtual };
        for (ancestry) |ancestor_id| {
            const ancestor = self.store.get(ancestor_id) orelse continue;
            appendSortKey(&key, ancestor, query_level.sorts);

            // Move to next level in query
            query_level = self.nextQueryLevel(query_level, ancestor.type_id) orelse break;
        }

        // Add this node's sort values
        appendSortKey(&key, node, query_level.sorts);

        return key;
    }

    const QueryLevel = struct {
        sorts: []const Sort,
        selections: []const EdgeSelection,
        is_virtual: bool,
    };

    fn nextQueryLevel(self: *Self, current: QueryLevel, node_type_id: TypeId) ?QueryLevel {
        // Find the edge selection whose TARGET matches this node's type.
        // This is used when building keys for ancestors - we want to find which
        // edge selection the node was REACHED BY, not which edge it CAN TRAVERSE.
        for (current.selections) |sel| {
            // Get edge definition to find target type
            // We need to check ALL types that might have this edge
            const target_type_id = self.schema.getEdgeTargetType(sel.name);
            if (target_type_id == null) continue;

            // Check if this edge's target type matches the node we're processing
            if (target_type_id.? != node_type_id) continue;

            // For recursive edges with no nested selections, loop back to the same selection
            const next_selections = if (sel.recursive and sel.selections.len == 0)
                current.selections
            else
                sel.selections;

            return .{
                .sorts = sel.sorts,
                .selections = next_selections,
                .is_virtual = sel.virtual,
            };
        }
        return null;
    }

    /// Get filters for a node based on its position in the query.
    fn getFiltersForNode(self: *Self, sub: *const Subscription, result_node: *const result_set_mod.ResultNode) []const Filter {
        if (result_node.ancestry.len == 0) {
            // Root level node
            return sub.query.filters;
        }

        // Walk through ancestry to find the filter at this level
        var current_selections = sub.query.selections;
        for (result_node.ancestry[1..]) |ancestor_id| {
            const ancestor = self.store.get(ancestor_id) orelse return &.{};
            for (current_selections) |sel| {
                const edge_def = self.schema.getEdgeDef(ancestor.type_id, sel.name) orelse continue;
                _ = edge_def;
                current_selections = sel.selections;
                break;
            }
        }

        // Get filter from last selection level
        if (result_node.ancestry.len > 0) {
            const last_ancestor_id = result_node.ancestry[result_node.ancestry.len - 1];
            const last_ancestor = self.store.get(last_ancestor_id) orelse return &.{};
            for (current_selections) |sel| {
                const edge_def = self.schema.getEdgeDef(last_ancestor.type_id, sel.name) orelse continue;
                _ = edge_def;
                return sel.filters;
            }
        }

        return &.{};
    }

    /// Get sorts for a virtual node based on its position in the query.
    fn getSortsForNode(self: *Self, sub: *const Subscription, node: *const Node) []const Sort {
        // If this is a root virtual node
        if (sub.query.virtual and sub.query.root_type_id == node.type_id) {
            return sub.query.sorts;
        }

        // Search through edge selections for this node's type
        return self.findSortsInSelections(sub.query.selections, node.type_id);
    }

    fn findSortsInSelections(self: *Self, selections: []const EdgeSelection, type_id: TypeId) []const Sort {
        for (selections) |sel| {
            // Check if this selection targets the type we're looking for
            // Note: We need to check edge target type, which requires looking up the edge def
            // For now, recursively search and return sorts if we find virtual at this level
            if (sel.virtual) {
                // This is a virtual level - could be what we're looking for
                // TODO: Properly check target type
                return sel.sorts;
            }
            // Recurse into nested selections
            const nested = self.findSortsInSelections(sel.selections, type_id);
            if (nested.len > 0) return nested;
        }
        return &.{};
    }

    /// Called when a node is deleted.
    /// Uses hybrid reactivity: fast path for loaded nodes, slow path for unloaded.
    pub fn onDelete(self: *Self, node: *const Node) void {
        // Fire per-node callback BEFORE processing (node still exists)
        self.fireNodeDelete(node.id);

        // Fast path: Handle deletion of loaded/visible nodes via reverse index
        const visible_subs = self.node_to_subs.getForKey(node.id);
        for (visible_subs) |sub| {
            const idx = sub.result_set.indexOf(node.id) orelse continue;

            var item = self.materializeItem(node, sub.query.selections) catch continue;
            defer item.deinit();

            // Emit callback BEFORE removing from result_set
            sub.emitLeave(item, idx);

            // NOW remove from result set (frees ancestry)
            sub.result_set.removeAndFree(node.id);
        }

        // Clean up reverse index for this node
        self.node_to_subs.removeKey(node.id);

        // Handle deletion of virtual nodes - affects their visible descendants
        const virtual_subs = self.virtual_to_subs.getForKey(node.id);
        for (virtual_subs) |sub| {
            self.handleUnlinkVirtualTarget(sub, node.id);
        }

        // Clean up virtual index for this node
        self.virtual_to_subs.removeKey(node.id);

        // Slow path: Check all subscriptions by type for unloaded matching nodes
        const type_subs = self.by_type.getForKey(node.type_id);
        for (type_subs) |sub| {
            // Skip virtual queries
            if (sub.query.virtual) continue;

            // For initialized subscriptions, only process if node is still in result_set
            // (if not in result_set, either fast path handled it or node wasn't a member)
            if (sub.initialized) {
                const idx = sub.result_set.indexOf(node.id) orelse continue;

                var item = self.materializeItem(node, sub.query.selections) catch continue;
                defer item.deinit();

                sub.cached_total = null;
                sub.emitLeave(item, idx);
                sub.result_set.removeAndFree(node.id);
                continue;
            }

            // For uninitialized subscriptions, check if node matched filters
            if (!self.executor.matchesFilters(node, sub.query.filters)) continue;

            // Node was a member - invalidate cached total and emit on_leave
            sub.cached_total = null;

            var item = self.materializeItem(node, sub.query.selections) catch continue;
            defer item.deinit();

            sub.emitLeave(item, 0);
        }
    }

    /// Called when an edge is linked.
    /// This is where nested nodes become reachable and should emit on_enter.
    pub fn onLink(self: *Self, source: NodeId, edge_id: EdgeId, target: NodeId) void {
        const source_node = self.store.get(source) orelse return;
        const edge_name = self.schema.getEdgeNameById(source_node.type_id, edge_id) orelse return;
        const target_node = self.store.get(target) orelse return;

        // Find subscriptions where source is visible (non-virtual)
        const visible_subs = self.node_to_subs.getForKey(source);
        for (visible_subs) |sub| {
            self.handleLinkForSubscription(sub, source_node, edge_name, target_node);
        }

        // Find subscriptions where source is virtual
        const virtual_subs = self.virtual_to_subs.getForKey(source);
        for (virtual_subs) |sub| {
            self.handleLinkForSubscription(sub, source_node, edge_name, target_node);
        }

        // Also emit on_change for parent if it's in the result set (legacy behavior)
        self.handleEdgeChange(source_node, edge_name);

        // Fire per-node callbacks for SDK entity mapper (bidirectional)
        // 1. Fire for source node
        self.fireNodeLink(source, edge_name, target);
        // 2. Fire for target node with reverse edge name
        if (self.schema.getEdgeDef(source_node.type_id, edge_name)) |edge_def| {
            if (edge_def.reverse_name.len > 0) {
                self.fireNodeLink(target, edge_def.reverse_name, source);
            }
        }
    }

    /// Called when an edge is unlinked.
    /// Target node (and descendants) become unreachable and should emit on_leave.
    pub fn onUnlink(self: *Self, source: NodeId, edge_id: EdgeId, target: NodeId) void {
        // Check if target is visible in any subscription
        const visible_subs = self.node_to_subs.getForKey(target);
        for (visible_subs) |sub| {
            self.handleUnlinkTarget(sub, target);
        }

        // Check if target is virtual in any subscription (affects its descendants)
        const virtual_subs = self.virtual_to_subs.getForKey(target);
        for (virtual_subs) |sub| {
            self.handleUnlinkVirtualTarget(sub, target);
        }

        // Also emit on_change for parent if it's in the result set (legacy behavior)
        if (self.store.get(source)) |src_node| {
            if (self.schema.getEdgeNameById(src_node.type_id, edge_id)) |e_name| {
                self.handleEdgeChange(src_node, e_name);

                // Fire per-node callbacks for SDK entity mapper (bidirectional)
                // 1. Fire for source node
                self.fireNodeUnlink(source, e_name, target);
                // 2. Fire for target node with reverse edge name
                if (self.schema.getEdgeDef(src_node.type_id, e_name)) |edge_def| {
                    if (edge_def.reverse_name.len > 0) {
                        self.fireNodeUnlink(target, edge_def.reverse_name, source);
                    }
                }
            }
        }
    }

    fn handleLinkForSubscription(
        self: *Self,
        sub: *Subscription,
        source_node: *const Node,
        edge_name: []const u8,
        target_node: *const Node,
    ) void {
        // Find the edge selection that matches this edge
        const edge_sel = self.findEdgeSelection(sub, source_node, edge_name) orelse return;

        // Check if target matches filters for this level
        if (!self.executor.matchesFilters(target_node, edge_sel.filters)) return;

        // Build two ancestry lists:
        // - visible_ancestry: only visible (non-virtual) ancestors, used for result_set storage
        // - virtual_ancestry: all ancestors including virtual, used for virtual_descendants tracking
        var visible_ancestry = std.ArrayListUnmanaged(NodeId){};
        defer visible_ancestry.deinit(self.allocator);
        var virtual_ancestry = std.ArrayListUnmanaged(NodeId){};
        defer virtual_ancestry.deinit(self.allocator);

        // Get source's ancestry (NOT including source yet)
        const source_result_opt = sub.result_set.getNode(source_node.id);
        if (source_result_opt) |source_result| {
            visible_ancestry.appendSlice(self.allocator, source_result.ancestry) catch return;
            virtual_ancestry.appendSlice(self.allocator, source_result.ancestry) catch return;
        }

        // Build composite key for target - match loadChildrenLazy structure:
        // 1. Loop over ancestors (excluding source)
        // 2. Add source separately after loop
        // 3. Add target
        var key = CompoundKey{};
        var query_level: QueryLevel = .{
            .sorts = sub.query.sorts,
            .selections = sub.query.selections,
            .is_virtual = sub.query.virtual,
        };

        // Add ancestor sort values (excluding source)
        for (visible_ancestry.items) |ancestor_id| {
            const ancestor = self.store.get(ancestor_id) orelse continue;
            appendSortKey(&key, ancestor, query_level.sorts);
            const next = self.nextQueryLevel(query_level, ancestor.type_id);
            if (next) |n| {
                query_level = n;
            } else {
                break;
            }
        }
        // Add source's sort values (using current query_level after ancestors)
        appendSortKey(&key, source_node, query_level.sorts);
        // Add target's sort values
        appendSortKey(&key, target_node, edge_sel.sorts);

        // Now add source to ancestry for the target's ancestry tracking
        visible_ancestry.append(self.allocator, source_node.id) catch return;
        virtual_ancestry.append(self.allocator, source_node.id) catch return;

        if (edge_sel.virtual) {
            // Target is virtual - add to virtual index and traverse its edges
            self.virtual_to_subs.add(target_node.id, sub) catch return;
            // Only add to virtual_ancestry (visible_ancestry unchanged for virtual nodes)
            virtual_ancestry.append(self.allocator, target_node.id) catch return;
            self.addReachableDescendants(sub, target_node, edge_sel.selections, key, &visible_ancestry, &virtual_ancestry, edge_name) catch return;
        } else {
            // Target is visible - add to result set (with edge_name for expand tracking)
            const owned_ancestry = self.allocator.dupe(NodeId, visible_ancestry.items) catch return;
            _ = sub.result_set.insertSortedWithEdge(target_node.id, key, owned_ancestry, edge_name) catch {
                self.allocator.free(owned_ancestry);
                return;
            };

            // Add to reverse index
            self.node_to_subs.add(target_node.id, sub) catch return;

            // Track virtual ancestors → this visible descendant
            for (virtual_ancestry.items) |ancestor_id| {
                if (self.virtual_to_subs.getForKey(ancestor_id).len > 0) {
                    sub.virtual_descendants.add(ancestor_id, target_node.id) catch {};
                }
            }

            // Emit on_enter
            const idx = sub.result_set.indexOf(target_node.id) orelse return;
            var item = self.materializeItem(target_node, sub.query.selections) catch return;
            defer item.deinit();
            sub.emitEnter(item, idx);

            // Continue traversing for target's edges (both ancestry lists get the visible node)
            visible_ancestry.append(self.allocator, target_node.id) catch return;
            virtual_ancestry.append(self.allocator, target_node.id) catch return;
            self.addReachableDescendants(sub, target_node, edge_sel.selections, key, &visible_ancestry, &virtual_ancestry, edge_name) catch return;
        }
    }

    /// Add reachable descendants through edge selections.
    /// Uses two ancestry lists:
    /// - visible_ancestry: only visible ancestors (for result_set storage)
    /// - virtual_ancestry: all ancestors including virtual (for virtual_descendants tracking)
    /// - original_edge_name: the edge name being expanded (for tree view parent-child relationship)
    fn addReachableDescendants(
        self: *Self,
        sub: *Subscription,
        node: *const Node,
        selections: []const EdgeSelection,
        parent_key: CompoundKey,
        visible_ancestry: *std.ArrayListUnmanaged(NodeId),
        virtual_ancestry: *std.ArrayListUnmanaged(NodeId),
        original_edge_name: []const u8,
    ) !void {
        for (selections) |edge_sel| {
            const edge_def = self.schema.getEdgeDef(node.type_id, edge_sel.name) orelse continue;
            const targets = node.getEdgeTargets(edge_def.id);

            for (targets) |target_id| {
                // Skip if already in ancestry (cycle detection)
                var in_ancestry = false;
                for (virtual_ancestry.items) |ancestor_id| {
                    if (ancestor_id == target_id) {
                        in_ancestry = true;
                        break;
                    }
                }
                if (in_ancestry) continue;

                // Skip if already in result set
                if (sub.result_set.contains(target_id)) continue;

                const target = self.store.get(target_id) orelse continue;
                if (!self.executor.matchesFilters(target, edge_sel.filters)) continue;

                var key = parent_key;
                appendSortKey(&key, target, edge_sel.sorts);

                if (edge_sel.virtual) {
                    // Virtual node - add to virtual index, only update virtual_ancestry
                    try self.virtual_to_subs.add(target_id, sub);
                    try virtual_ancestry.append(self.allocator, target_id);
                    try self.addReachableDescendants(sub, target, edge_sel.selections, key, visible_ancestry, virtual_ancestry, original_edge_name);
                    _ = virtual_ancestry.pop();
                } else {
                    // Visible node - use visible_ancestry for result_set, virtual_ancestry for tracking
                    const owned_ancestry = try self.allocator.dupe(NodeId, visible_ancestry.items);
                    _ = try sub.result_set.insertSortedWithEdge(target_id, key, owned_ancestry, original_edge_name);
                    try self.node_to_subs.add(target_id, sub);

                    // Track virtual ancestors using virtual_ancestry
                    for (virtual_ancestry.items) |ancestor_id| {
                        if (self.virtual_to_subs.getForKey(ancestor_id).len > 0) {
                            try sub.virtual_descendants.add(ancestor_id, target_id);
                        }
                    }

                    const idx = sub.result_set.indexOf(target_id) orelse continue;
                    var item = self.materializeItem(target, sub.query.selections) catch continue;
                    defer item.deinit();
                    sub.emitEnter(item, idx);

                    // Visible node added to both ancestry lists
                    try visible_ancestry.append(self.allocator, target_id);
                    try virtual_ancestry.append(self.allocator, target_id);
                    try self.addReachableDescendants(sub, target, edge_sel.selections, key, visible_ancestry, virtual_ancestry, original_edge_name);
                    _ = visible_ancestry.pop();
                    _ = virtual_ancestry.pop();
                }
            }
        }
    }

    fn handleUnlinkTarget(self: *Self, sub: *Subscription, target: NodeId) void {
        // Check if target is still reachable via another path (multi-parent)
        // For now, assume single-parent - just remove it
        // TODO: Implement proper reachability check for multi-parent

        const idx = sub.result_set.indexOf(target) orelse return;

        if (self.store.get(target)) |target_node| {
            var item = self.materializeItem(target_node, sub.query.selections) catch return;
            defer item.deinit();

            // Emit callback BEFORE removing from result_set
            // This allows callbacks to look up parent/edge info from result_set
            sub.emitLeave(item, idx);

            // NOW remove from indexes (after callback has accessed result_set)
            _ = self.node_to_subs.remove(target, sub);
            sub.result_set.removeAndFree(target);
        }
    }

    fn handleUnlinkVirtualTarget(self: *Self, sub: *Subscription, target: NodeId) void {
        // Virtual node was unlinked - remove all its visible descendants
        const affected_visible = sub.virtual_descendants.getForKey(target);

        // Copy the list since we'll be modifying it
        var to_remove = std.ArrayListUnmanaged(NodeId){};
        defer to_remove.deinit(self.allocator);
        to_remove.appendSlice(self.allocator, affected_visible) catch return;

        for (to_remove.items) |visible_id| {
            self.handleUnlinkTarget(sub, visible_id);
        }

        // Remove virtual node from indexes
        _ = self.virtual_to_subs.remove(target, sub);
        sub.virtual_descendants.removeKey(target);
    }

    fn findEdgeSelection(self: *Self, sub: *const Subscription, source_node: *const Node, edge_name: []const u8) ?*const EdgeSelection {
        // First check if source is the root type
        if (sub.query.root_type_id == source_node.type_id) {
            for (sub.query.selections) |*sel| {
                if (std.mem.eql(u8, sel.name, edge_name)) return sel;
            }
        }

        // Check for recursive edges: if an edge selection targets source_type and is recursive,
        // and the source has an edge with the same name, use that selection
        for (sub.query.selections) |*sel| {
            if (sel.recursive) {
                // Get the edge definition from the root type
                const edge_def = self.schema.getEdgeDef(sub.query.root_type_id, sel.name) orelse continue;
                // Check if this selection's target type matches the source type
                if (edge_def.target_type_id == source_node.type_id) {
                    // Check if source has an edge with the same name
                    if (self.schema.getEdgeDef(source_node.type_id, edge_name) != null) {
                        if (std.mem.eql(u8, sel.name, edge_name)) return sel;
                    }
                }
            }
        }

        // Search through nested selections (for multi-level hierarchies)
        return self.findEdgeSelectionInSelections(sub.query.selections, source_node.type_id, edge_name);
    }

    fn findEdgeSelectionInSelections(self: *Self, selections: []const EdgeSelection, source_type_id: TypeId, edge_name: []const u8) ?*const EdgeSelection {
        for (selections) |*sel| {
            // Check if this selection's edge targets the source type
            // Then check if its nested selections have the edge we're looking for
            for (sel.selections) |*nested| {
                if (std.mem.eql(u8, nested.name, edge_name)) {
                    // Verify this edge belongs to the source type
                    if (self.schema.getEdgeDef(source_type_id, edge_name) != null) {
                        return nested;
                    }
                }
            }
            // Recurse
            if (self.findEdgeSelectionInSelections(sel.selections, source_type_id, edge_name)) |found| {
                return found;
            }
        }
        return null;
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    fn initializeResultSet(self: *Self, sub: *Subscription) !void {
        // For non-virtual queries, defer loading entirely until first use.
        // This makes subscribe() O(1) instead of O(n).
        // Virtual queries still need eager loading for ancestor tracking.
        // Hybrid reactivity (Phase 4) ensures callbacks work even without loading.
        if (!sub.query.virtual) {
            sub.initialized = false;
            sub.cached_total = null;
            return;
        }

        // Virtual queries: load eagerly (for ancestor tracking)
        try self.loadResultSet(sub);
    }

    /// Actually load the result set. Called by ensureInitialized() or for virtual queries.
    fn loadResultSet(self: *Self, sub: *Subscription) !void {
        // Direct node ID lookup - bypass index scan entirely
        if (sub.query.root_id) |root_id| {
            try self.loadDirectNode(sub, root_id);
            return;
        }

        // Direct index scan - no Item materialization overhead.
        // This is O(n) in matching nodes but avoids:
        // - Item struct allocation per node
        // - Path array allocation per node
        // - VisitedSet allocation per node
        // - Field copying into Item.fields HashMap
        var iter = self.indexes.scan(sub.coverage, sub.query.filters);

        while (iter.next()) |node_id| {
            const node = self.store.get(node_id) orelse continue;

            // Apply filters: post-filters (not covered by index) + query filters
            // Both are needed because index may only partially cover filters
            if (!self.executor.matchesFilters(node, sub.coverage.post_filters)) continue;
            if (!self.executor.matchesFilters(node, sub.query.filters)) continue;

            var key = CompoundKey{};
            appendSortKey(&key, node, sub.query.sorts);

            if (sub.query.virtual) {
                // Root is virtual - add to virtual index (children loaded on expand)
                try self.virtual_to_subs.add(node_id, sub);
            } else {
                // Root is visible - add to result set and node index
                _ = try sub.result_set.insertSorted(node_id, key, null);
                try self.node_to_subs.add(node_id, sub);
            }
        }

        sub.initialized = true;
        sub.cached_total = sub.result_set.count();
    }

    /// Load a single node directly by ID (for root_id queries).
    fn loadDirectNode(self: *Self, sub: *Subscription, node_id: NodeId) !void {
        const node = self.store.get(node_id) orelse {
            // Node doesn't exist - empty result set
            sub.initialized = true;
            sub.cached_total = 0;
            return;
        };

        // Verify type matches (root_id could be for wrong type)
        if (node.type_id != sub.query.root_type_id) {
            sub.initialized = true;
            sub.cached_total = 0;
            return;
        }

        var key = CompoundKey{};
        appendSortKey(&key, node, sub.query.sorts);

        if (sub.query.virtual) {
            // Root is virtual - add to virtual index (children loaded on expand)
            try self.virtual_to_subs.add(node_id, sub);
        } else {
            // Root is visible - add to result set and node index
            _ = try sub.result_set.insertSorted(node_id, key, null);
            try self.node_to_subs.add(node_id, sub);
        }

        sub.initialized = true;
        sub.cached_total = sub.result_set.count();
    }

    /// Ensure the subscription is initialized. Call this before accessing result_set.
    /// This is a no-op if already initialized.
    pub fn ensureInitialized(self: *Self, sub: *Subscription) !void {
        if (sub.initialized) return;
        try self.loadResultSet(sub);
    }

    /// Lazy load children for a parent node via an edge.
    /// Called by Tree when user expands an edge.
    /// Returns slice of child IDs that were loaded (caller must free).
    pub fn loadChildrenLazy(
        self: *Self,
        sub: *Subscription,
        parent_id: NodeId,
        edge_name: []const u8,
    ) ![]const NodeId {
        const parent_node = self.store.get(parent_id) orelse return &.{};

        // Find edge selection in query (optional - use defaults if not found)
        const edge_sel = self.findEdgeSelection(sub, parent_node, edge_name);

        // Get edge definition
        const edge_def = self.schema.getEdgeDef(parent_node.type_id, edge_name) orelse return &.{};
        const targets = parent_node.getEdgeTargets(edge_def.id);

        if (targets.len == 0) return &.{};

        // Build ancestry for children
        var ancestry = std.ArrayListUnmanaged(NodeId){};
        defer ancestry.deinit(self.allocator);

        // Get parent's ancestry from result_set
        if (sub.result_set.getNode(parent_id)) |parent_result| {
            try ancestry.appendSlice(self.allocator, parent_result.ancestry);
        }
        try ancestry.append(self.allocator, parent_id);

        // Build parent key for composite child keys
        var parent_key = CompoundKey{};
        var query_level: QueryLevel = .{
            .sorts = sub.query.sorts,
            .selections = sub.query.selections,
            .is_virtual = sub.query.virtual,
        };

        for (ancestry.items[0 .. ancestry.items.len - 1]) |ancestor_id| {
            const ancestor = self.store.get(ancestor_id) orelse continue;
            appendSortKey(&parent_key, ancestor, query_level.sorts);
            const next = self.nextQueryLevel(query_level, ancestor.type_id);
            if (next) |n| {
                query_level = n;
            } else {
                break;
            }
        }
        appendSortKey(&parent_key, parent_node, query_level.sorts);

        // Get filters and sorts from edge selection, or use defaults
        const filters = if (edge_sel) |sel| sel.filters else &.{};
        const sorts = if (edge_sel) |sel| sel.sorts else sub.query.sorts;
        const is_virtual = if (edge_sel) |sel| sel.virtual else false;

        // Collect matching children
        var loaded_children = std.ArrayListUnmanaged(NodeId){};
        errdefer loaded_children.deinit(self.allocator);

        for (targets) |target_id| {
            const target = self.store.get(target_id) orelse continue;

            // Apply edge selection filters (empty filters = no filtering)
            if (!self.executor.matchesFilters(target, filters)) continue;

            // Build child's sort key
            var key = parent_key;
            appendSortKey(&key, target, sorts);

            if (is_virtual) {
                // Virtual child - add to virtual index (even if already visible elsewhere)
                // Don't skip based on result_set - we need to load nested children through virtual hops
                // even when the target is already visible via another path
                try self.virtual_to_subs.add(target_id, sub);

                // Also load nested children through the virtual node
                // This allows items under virtual edges to be visible as children of the parent
                if (edge_sel) |sel| {
                    // Track virtual_ancestry separately for virtual_descendants tracking
                    var virtual_ancestry = std.ArrayListUnmanaged(NodeId){};
                    defer virtual_ancestry.deinit(self.allocator);
                    try virtual_ancestry.appendSlice(self.allocator, ancestry.items);
                    try virtual_ancestry.append(self.allocator, target_id);

                    // visible_ancestry = ancestry (doesn't include virtual target_id)
                    // Pass the original edge_name so children appear as direct children of parent
                    try self.loadNestedVisibleChildren(sub, target, sel.selections, key, &ancestry, &virtual_ancestry, edge_name, &loaded_children);
                }
            } else {
                // Skip visible children that are already in result set
                if (sub.result_set.contains(target_id)) continue;

                // Visible child - add to result set with edge_name for tracking
                const owned_ancestry = try self.allocator.dupe(NodeId, ancestry.items);
                _ = try sub.result_set.insertSortedWithEdge(target_id, key, owned_ancestry, edge_name);
                try self.node_to_subs.add(target_id, sub);
                try loaded_children.append(self.allocator, target_id);
            }
        }

        return try loaded_children.toOwnedSlice(self.allocator);
    }

    /// Load nested visible children through a virtual node.
    /// Used when expanding a virtual edge - the visible descendants become children of the parent.
    /// Parameters:
    /// - visible_ancestry: Ancestry containing only visible (non-virtual) ancestors
    /// - virtual_ancestry: Full ancestry including virtual nodes (for virtual_descendants tracking)
    /// - original_edge_name: The edge name being expanded (the virtual edge)
    fn loadNestedVisibleChildren(
        self: *Self,
        sub: *Subscription,
        node: *const Node,
        selections: []const EdgeSelection,
        parent_key: CompoundKey,
        visible_ancestry: *std.ArrayListUnmanaged(NodeId),
        virtual_ancestry: *std.ArrayListUnmanaged(NodeId),
        original_edge_name: []const u8,
        loaded_children: *std.ArrayListUnmanaged(NodeId),
    ) !void {
        for (selections) |edge_sel| {
            const edge_def = self.schema.getEdgeDef(node.type_id, edge_sel.name) orelse continue;
            const targets = node.getEdgeTargets(edge_def.id);

            for (targets) |target_id| {
                // Skip if already in result set
                if (sub.result_set.contains(target_id)) continue;

                const target = self.store.get(target_id) orelse continue;
                if (!self.executor.matchesFilters(target, edge_sel.filters)) continue;

                // Build composite key: parent key + this node's sort values
                var key = parent_key;
                appendSortKey(&key, target, edge_sel.sorts);

                if (edge_sel.virtual) {
                    // Virtual node - add to virtual index and continue traversing
                    try self.virtual_to_subs.add(target_id, sub);
                    // Only add to virtual_ancestry (visible_ancestry unchanged for virtual nodes)
                    try virtual_ancestry.append(self.allocator, target_id);
                    try self.loadNestedVisibleChildren(sub, target, edge_sel.selections, key, visible_ancestry, virtual_ancestry, original_edge_name, loaded_children);
                    _ = virtual_ancestry.pop();
                } else {
                    // Visible node - add to result set with visible ancestry and original edge name
                    // This makes the node appear as a direct child of the visible parent
                    const owned_ancestry = try self.allocator.dupe(NodeId, visible_ancestry.items);
                    _ = try sub.result_set.insertSortedWithEdge(target_id, key, owned_ancestry, original_edge_name);

                    // Add to reverse index
                    try self.node_to_subs.add(target_id, sub);

                    // Track virtual ancestors → this visible descendant
                    for (virtual_ancestry.items) |ancestor_id| {
                        if (self.virtual_to_subs.getForKey(ancestor_id).len > 0) {
                            try sub.virtual_descendants.add(ancestor_id, target_id);
                        }
                    }

                    // Track as loaded child (returned to caller)
                    try loaded_children.append(self.allocator, target_id);
                }
            }
        }
    }


    fn materializeItem(self: *Self, node: *const Node, selections: []const EdgeSelection) !Item {
        _ = selections; // Reactive views load edges lazily via expandById/loadChildrenLazy
        var visited = std.AutoHashMapUnmanaged(NodeId, void){};
        defer visited.deinit(self.allocator);

        const path = try self.allocator.alloc(PathSegment, 1);
        path[0] = .{ .root = node.id };

        // Pass empty selections - reactive system handles edge loading separately
        return self.executor.materialize(node, &.{}, path, &visited);
    }

    fn selectedFieldsChanged(_: *Self, _: []const EdgeSelection, node: *const Node, old_node: *const Node) bool {
        // Check if any property changed between old and new node
        // All properties are returned, so any property change is relevant
        if (node.properties.count() != old_node.properties.count()) return true;

        var iter = node.properties.iterator();
        while (iter.next()) |entry| {
            const old_value = old_node.properties.get(entry.key_ptr.*) orelse return true;
            if (!entry.value_ptr.*.eql(old_value)) return true;
        }
        return false;
    }

    fn handleEdgeChange(self: *Self, source_node: *const Node, edge_name: []const u8) void {
        const subs = self.by_type.getForKey(source_node.type_id);
        if (subs.len == 0) return;

        for (subs) |sub| {
            if (!sub.result_set.contains(source_node.id)) continue;

            // Check if query selects this edge
            if (!self.querySelectsEdge(sub.query, edge_name)) continue;

            const idx = sub.result_set.indexOf(source_node.id) orelse continue;

            var item = self.materializeItem(source_node, sub.query.selections) catch continue;
            defer item.deinit();

            // For edge changes, we emit on_change with same item as old
            // (the edge content is what changed)
            var old_item = self.materializeItem(source_node, sub.query.selections) catch continue;
            defer old_item.deinit();

            sub.emitChange(item, idx, old_item);
        }
    }

    fn querySelectsEdge(_: *Self, query: *const Query, edge_name: []const u8) bool {
        for (query.selections) |sel| {
            if (std.mem.eql(u8, sel.name, edge_name)) return true;
        }
        return false;
    }
};

// ============================================================================
// Key Encoding Helpers
// ============================================================================

const Value = @import("../value.zig").Value;
const SortDir = @import("../schema.zig").SortDir;

/// Append sort field values from a node to a compound key.
pub fn appendSortKey(key: *CompoundKey, node: *const Node, sorts: []const Sort) void {
    for (sorts) |sort| {
        const value = node.getProperty(sort.field) orelse Value{ .null = {} };
        appendValue(key, value, sort.direction);
    }
    // Append node ID for uniqueness
    appendU64(key, node.id, .asc);
}

fn appendValue(key: *CompoundKey, value: Value, direction: SortDir) void {
    appendByte(key, value.tagByte(), direction);
    switch (value) {
        .null => {},
        .bool => |b| appendByte(key, if (b) 1 else 0, direction),
        .int => |i| {
            const unsigned: u64 = @bitCast(i);
            appendU64(key, unsigned ^ (1 << 63), direction);
        },
        .number => |n| {
            const bits: u64 = @bitCast(n);
            const encoded = if (bits & (1 << 63) != 0) ~bits else bits ^ (1 << 63);
            appendU64(key, encoded, direction);
        },
        .string => |s| {
            for (s) |c| {
                if (key.len >= MAX_KEY_SIZE - 1) break;
                if (c == 0) {
                    appendByte(key, 0, direction);
                    appendByte(key, 1, direction);
                } else {
                    appendByte(key, c, direction);
                }
            }
            appendByte(key, 0, direction);
            appendByte(key, 0, direction);
        },
    }
}

fn appendByte(key: *CompoundKey, byte: u8, direction: SortDir) void {
    if (key.len >= MAX_KEY_SIZE) return;
    key.buffer[key.len] = if (direction == .desc) ~byte else byte;
    key.len += 1;
}

fn appendU64(key: *CompoundKey, value: u64, direction: SortDir) void {
    // Cast to u16 to avoid overflow when key.len is near MAX_KEY_SIZE
    if (@as(u16, key.len) + 8 > MAX_KEY_SIZE) return;
    var be: [8]u8 = undefined;
    std.mem.writeInt(u64, &be, value, .big);
    for (be) |b| appendByte(key, b, direction);
}

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;
const parseSchema = @import("../json.zig").parseSchema;

fn createTestSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "User",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "active", "type": "bool" }
        \\      ],
        \\      "edges": [{ "name": "posts", "target": "Post", "reverse": "author" }],
        \\      "indexes": [{ "fields": [{ "field": "active", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Post",
        \\      "properties": [{ "name": "title", "type": "string" }],
        \\      "edges": [{ "name": "author", "target": "User", "reverse": "posts" }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

test "ChangeTracker subscribe and unsubscribe" {
    var schema = try createTestSchema(testing.allocator);
    defer schema.deinit();

    var store = NodeStore.init(testing.allocator, &schema);
    defer store.deinit();

    var indexes = try IndexManager.init(testing.allocator, &schema);
    defer indexes.deinit();

    var rollups = RollupCache.init(testing.allocator, &schema, &store, &indexes);
    defer rollups.deinit();

    var tracker = ChangeTracker.init(testing.allocator, &store, &schema, &indexes, &rollups);
    defer tracker.deinit();

    const query = Query{
        .root_type = "User",
        .root_type_id = 0,
        .filters = &.{},
        .sorts = &.{},
        .selections = &.{},
    };

    const coverage = indexes.selectIndex(0, &.{}, &.{}) orelse {
        return; // No index, skip test
    };

    const sub = try tracker.subscribe(&query, coverage);
    try testing.expectEqual(@as(SubscriptionId, 1), sub.id);

    tracker.unsubscribe(sub.id);
    try testing.expect(tracker.subscriptions.get(1) == null);
}
