///! Profile test reproducing exact DAP demo scenario
///! Run with: zig build test-dap-profile
///!
///! This test uses the public Tree API (not ReactiveTree directly) to ensure
///! we're testing the same code path as Lua bindings.

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const watchdog = @import("test_watchdog.zig");

// Global watchdog - started by first test (30s for benchmarks)
var global_watchdog: ?watchdog.Watchdog = null;

fn ensureWatchdog() void {
    if (global_watchdog == null) {
        global_watchdog = watchdog.Watchdog.start(30); // 30s for benchmarks
    }
}

const profiling = @import("profiling.zig");
const Profiler = profiling.Profiler;

const ng = @import("neograph.zig");
const Schema = ng.Schema;
const NodeStore = ng.NodeStore;
const IndexManager = ng.IndexManager;
const RollupCache = ng.RollupCache;
const ChangeTracker = ng.reactive.ChangeTracker;
const Value = ng.Value;
const NodeId = ng.NodeId;
const Query = ng.Query;
const Sort = ng.Sort;
const parseSchema = ng.parseSchema;

// Use the public Tree API (not ReactiveTree)
const View = ng.reactive.View;
const ViewOpts = ng.reactive.ViewOpts;

fn printReport(profiler: *const Profiler) void {
    var buf: [16384]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);
    profiler.report(stream.writer()) catch {};
    std.debug.print("{s}", .{stream.getWritten()});
}

fn globalProfiler() *Profiler {
    return &profiling.global;
}

// ============================================================================
// Test Context - Full stack setup
// ============================================================================

const TestContext = struct {
    allocator: Allocator,
    interner: StringInterner,
    schema: Schema,
    store: NodeStore,
    indexes: IndexManager,
    rollups: RollupCache,
    tracker: ChangeTracker,

    const Self = @This();

    fn init(allocator: Allocator, schema: Schema) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        self.allocator = allocator;
        self.interner = StringInterner.init(allocator);
        self.schema = schema;

        self.store = NodeStore.init(allocator, &self.schema);
        errdefer self.store.deinit();

        self.indexes = try IndexManager.init(allocator, &self.schema);
        errdefer self.indexes.deinit();

        self.rollups = RollupCache.init(allocator, &self.schema, &self.store, &self.indexes);
        errdefer self.rollups.deinit();

        self.tracker = ChangeTracker.init(allocator, &self.store, &self.schema, &self.indexes, &self.rollups);

        return self;
    }

    fn deinit(self: *Self) void {
        self.tracker.deinit();
        self.rollups.deinit();
        self.indexes.deinit();
        self.store.deinit();
        self.schema.deinit();
        self.allocator.destroy(self);
    }

    fn insert(self: *Self, type_name: []const u8) !NodeId {
        const id = try self.store.insert(type_name);
        const node = self.store.get(id).?;
        try self.indexes.onInsert(node);
        self.tracker.onInsert(node);
        return id;
    }

    fn update(self: *Self, id: NodeId, props: anytype) !void {
        const node = self.store.get(id) orelse return error.NodeNotFound;
        var old_node = try node.clone();
        defer old_node.deinit();

        try self.store.update(id, props);

        const updated_node = self.store.get(id).?;
        try self.indexes.onUpdate(updated_node, &old_node);
        self.tracker.onUpdate(updated_node, &old_node);
    }

    fn link(self: *Self, source: NodeId, edge_name: []const u8, target: NodeId) !void {
        try self.store.link(source, edge_name, target);

        const source_node = self.store.get(source).?;
        const edge_def = self.schema.getEdgeDef(source_node.type_id, edge_name).?;
        self.tracker.onLink(source, edge_def.id, target);
    }
};

// ============================================================================
// Schema Creation
// ============================================================================

fn createDapSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "Thread",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "tid", "type": "int" },
        \\        { "name": "stopped", "type": "bool" }
        \\      ],
        \\      "edges": [{ "name": "frames", "target": "StackFrame", "reverse": "thread" }],
        \\      "indexes": [{ "fields": [{ "field": "tid", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "StackFrame",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "line", "type": "int" },
        \\        { "name": "file", "type": "string" }
        \\      ],
        \\      "edges": [
        \\        { "name": "thread", "target": "Thread", "reverse": "frames" },
        \\        { "name": "scopes", "target": "Scope", "reverse": "frame" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "line", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Scope",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "expensive", "type": "bool" }
        \\      ],
        \\      "edges": [
        \\        { "name": "frame", "target": "StackFrame", "reverse": "scopes" },
        \\        { "name": "variables", "target": "Variable", "reverse": "scope" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "name", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Variable",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "value", "type": "string" },
        \\        { "name": "vtype", "type": "string" }
        \\      ],
        \\      "edges": [
        \\        { "name": "scope", "target": "Scope", "reverse": "variables" },
        \\        { "name": "children", "target": "Variable", "reverse": "parent" },
        \\        { "name": "parent", "target": "Variable", "reverse": "children" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "name", "direction": "asc" }] }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

// ============================================================================
// Profile Test using Tree API
// ============================================================================

test "profile: DAP demo exact scenario - 72k nodes using Tree API" {
    ensureWatchdog();

    const profiler = globalProfiler();
    profiler.reset();
    profiler.startSession();

    const allocator = testing.allocator;

    // Build schema exactly like demo
    const schema = try createDapSchema(allocator);
    const ctx = try TestContext.init(allocator, schema);
    defer ctx.deinit();

    std.debug.print("\n=== Generating 72k nodes (like demo) ===\n", .{});

    const gen_timer = profiler.time(.insert_root);

    // Generate data exactly like demo
    const target_nodes: u32 = 72000;
    var node_count: u32 = 0;
    var var_idx: u32 = 0;

    const thread_count: u32 = 3;
    const frames_per_thread: u32 = 10;
    const scopes_per_frame: u32 = 3;
    const vars_per_scope: u32 = 200;
    const children_per_var: u32 = 8;
    const grandchildren_per_var: u32 = 5;

    // First create all threads before any children (so we have valid thread_ids)
    var thread_ids: [3]NodeId = undefined;
    var actual_thread_count: u32 = 0;
    for (0..thread_count) |t_usize| {
        const t: u32 = @intCast(t_usize);
        const thread_id = try ctx.insert("Thread");
        thread_ids[t] = thread_id;
        actual_thread_count += 1;
        try ctx.update(thread_id, .{ .tid = @as(i64, @intCast(t + 1)), .stopped = (t == 0) });
        node_count += 1;
    }

    // Now create children for each thread
    outer: for (0..thread_count) |t_usize| {
        const t: u32 = @intCast(t_usize);
        const thread_id = thread_ids[t];

        var f: u32 = 0;
        while (f < frames_per_thread) : (f += 1) {
            const frame_id = try ctx.insert("StackFrame");
            try ctx.update(frame_id, .{ .line = @as(i64, @intCast((t * f * 17) % 500 + 1)) });
            try ctx.link(thread_id, "frames", frame_id);
            node_count += 1;

            var s: u32 = 0;
            while (s < scopes_per_frame) : (s += 1) {
                const scope_id = try ctx.insert("Scope");
                try ctx.update(scope_id, .{ .expensive = (s == 2) });
                try ctx.link(frame_id, "scopes", scope_id);
                node_count += 1;

                var v: u32 = 0;
                while (v < vars_per_scope) : (v += 1) {
                    var_idx += 1;
                    const var_id = try ctx.insert("Variable");
                    try ctx.link(scope_id, "variables", var_id);
                    node_count += 1;

                    // Add children for container types (every 3rd var)
                    if (v % 3 == 0) {
                        var c: u32 = 0;
                        while (c < children_per_var) : (c += 1) {
                            var_idx += 1;
                            const child_id = try ctx.insert("Variable");
                            try ctx.link(var_id, "children", child_id);
                            node_count += 1;

                            // Grandchildren for every 2nd child
                            if (c % 2 == 0) {
                                var gc: u32 = 0;
                                while (gc < grandchildren_per_var) : (gc += 1) {
                                    var_idx += 1;
                                    const gc_id = try ctx.insert("Variable");
                                    try ctx.link(child_id, "children", gc_id);
                                    node_count += 1;
                                }
                            }

                            if (node_count >= target_nodes) break :outer;
                        }
                    }
                    if (node_count >= target_nodes) break :outer;
                }
                if (node_count >= target_nodes) break :outer;
            }
            if (node_count >= target_nodes) break :outer;
        }
    }

    gen_timer.stop();
    std.debug.print("Generated {} nodes\n", .{node_count});

    // Create Tree using public API (this is what Lua bindings should do)
    std.debug.print("\n=== Creating Tree using public API ===\n", .{});
    const tree_start = std.time.milliTimestamp();

    // Query for Thread nodes sorted by tid
    const query = Query{
        .root_type = "Thread",
        .root_type_id = 0, // Thread is type 0
        .filters = &.{},
        .sorts = &.{Sort{ .field = "tid", .direction = .asc }},
        .selections = &.{},
    };

    const coverage = ctx.indexes.selectIndex(0, query.filters, query.sorts).?;

    // Create tree from query - THIS is what Lua should be doing
    var tree = try View.init(allocator, &ctx.tracker, &query, coverage, .{ .limit = 30 });
    defer tree.deinit();
    tree.activate(false); // Fix viewport pointer and enable reactive updates

    const tree_time_ms = std.time.milliTimestamp() - tree_start;
    std.debug.print("Tree init took {}ms, {} roots visible\n", .{ tree_time_ms, tree.total() });

    // Test viewport operations
    std.debug.print("\n=== Testing viewport operations ===\n", .{});
    const vp_start = std.time.milliTimestamp();
    tree.scrollTo(0);
    var item_count: u32 = 0;
    var items_iter = tree.items();
    while (items_iter.next()) |_| {
        item_count += 1;
    }
    const vp_time = std.time.milliTimestamp() - vp_start;
    std.debug.print("Viewport ops took {}ms, got {} items\n", .{ vp_time, item_count });

    profiler.endSession();
    printReport(profiler);

    // Verify
    try testing.expectEqual(@as(u32, 3), tree.total()); // Only 3 threads visible initially
    try testing.expect(item_count <= 3); // Should have at most 3 items
}
