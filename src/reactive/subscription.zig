///! Subscription management for reactive queries.

const std = @import("std");
const Allocator = std.mem.Allocator;

const Query = @import("../query/builder.zig").Query;
const Item = @import("../query/executor.zig").Item;
const IndexCoverage = @import("../index/index.zig").IndexCoverage;
const ResultSet = @import("result_set.zig").ResultSet;
const NodeId = @import("../node.zig").NodeId;
const GroupedMap = @import("../ds.zig").GroupedMap;

/// Subscription ID type.
pub const SubscriptionId = u64;

/// Callback function types.
pub const OnEnterFn = *const fn (ctx: ?*anyopaque, item: Item, index: u32) void;
pub const OnLeaveFn = *const fn (ctx: ?*anyopaque, item: Item, index: u32) void;
pub const OnChangeFn = *const fn (ctx: ?*anyopaque, item: Item, index: u32, old: Item) void;
pub const OnMoveFn = *const fn (ctx: ?*anyopaque, item: Item, from: u32, to: u32) void;

/// Callback configuration.
pub const Callbacks = struct {
    on_enter: ?OnEnterFn = null,
    on_leave: ?OnLeaveFn = null,
    on_change: ?OnChangeFn = null,
    on_move: ?OnMoveFn = null,
    context: ?*anyopaque = null,
};

/// A subscription to a query result set.
pub const Subscription = struct {
    id: SubscriptionId,
    query: *const Query,
    coverage: IndexCoverage,
    result_set: ResultSet,
    callbacks: Callbacks,
    allocator: Allocator,

    /// Maps virtual ancestor NodeId → visible descendant NodeIds.
    /// Used to find affected visible nodes when a virtual ancestor changes.
    virtual_descendants: GroupedMap(NodeId, NodeId),

    // Lazy loading state
    /// Whether the result_set has been populated. False means loading is deferred.
    initialized: bool = false,
    /// Cached total count. Null means count needs to be computed.
    cached_total: ?u32 = null,

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        id: SubscriptionId,
        query: *const Query,
        coverage: IndexCoverage,
    ) Self {
        return .{
            .id = id,
            .query = query,
            .coverage = coverage,
            .result_set = ResultSet.init(allocator),
            .callbacks = .{},
            .allocator = allocator,
            .virtual_descendants = GroupedMap(NodeId, NodeId).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.result_set.deinit();
        self.virtual_descendants.deinit();
        @constCast(&self.coverage).deinit();
    }

    /// Set callbacks for this subscription.
    pub fn setCallbacks(self: *Self, callbacks: Callbacks) void {
        self.callbacks = callbacks;
    }

    /// Emit on_enter callback.
    pub fn emitEnter(self: *Self, item: Item, index: u32) void {
        if (self.callbacks.on_enter) |cb| {
            cb(self.callbacks.context, item, index);
        }
    }

    /// Emit on_leave callback.
    pub fn emitLeave(self: *Self, item: Item, index: u32) void {
        if (self.callbacks.on_leave) |cb| {
            cb(self.callbacks.context, item, index);
        }
    }

    /// Emit on_change callback.
    pub fn emitChange(self: *Self, item: Item, index: u32, old: Item) void {
        if (self.callbacks.on_change) |cb| {
            cb(self.callbacks.context, item, index, old);
        }
    }

    /// Emit on_move callback.
    pub fn emitMove(self: *Self, item: Item, from: u32, to: u32) void {
        if (self.callbacks.on_move) |cb| {
            cb(self.callbacks.context, item, from, to);
        }
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "Subscription callbacks" {
    const testing = std.testing;

    const TestContext = struct {
        enter_count: u32 = 0,
        leave_count: u32 = 0,

        fn onEnter(ctx: ?*anyopaque, _: Item, _: u32) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.enter_count += 1;
        }

        fn onLeave(ctx: ?*anyopaque, _: Item, _: u32) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.leave_count += 1;
        }
    };

    var ctx = TestContext{};

    // Create a minimal query for testing
    const query = Query{
        .root_type = "Test",
        .root_type_id = 0,
        .filters = &.{},
        .sorts = &.{},
        .selections = &.{},
    };

    // Create minimal coverage
    const coverage = IndexCoverage{
        .index = undefined,
        .equality_prefix = 0,
        .range_field = null,
        .sort_suffix = 0,
        .score = 0,
        .post_filters = &.{},
    };

    var sub = Subscription.init(testing.allocator, 1, &query, coverage);
    defer sub.deinit();

    sub.setCallbacks(.{
        .on_enter = TestContext.onEnter,
        .on_leave = TestContext.onLeave,
        .context = &ctx,
    });

    var item = Item.init(testing.allocator, 1, 0);
    defer item.deinit();

    sub.emitEnter(item, 0);
    sub.emitEnter(item, 1);
    sub.emitLeave(item, 0);

    try testing.expectEqual(@as(u32, 2), ctx.enter_count);
    try testing.expectEqual(@as(u32, 1), ctx.leave_count);
}
