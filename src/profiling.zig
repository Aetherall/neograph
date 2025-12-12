///! Compile-time optional profiling instrumentation.
///!
///! Enable with: `zig build -Dprofile=true`
///!
///! When disabled, all profiling calls compile to no-ops with zero overhead.

const std = @import("std");
const builtin = @import("builtin");

/// Build-time flag to enable profiling. Set via `-Dprofile=true`.
pub const enabled = @import("profile_options").enabled;

/// Operation types that can be profiled.
pub const Op = enum {
    // ReactiveTree operations
    insert_root,
    insert_child,
    remove_root,
    remove_node,
    set_children,

    // Expansion operations
    expand,
    collapse,

    // Viewport operations
    scroll_to,
    scroll_by,
    viewport_items,

    // Index operations
    index_of,
    node_at_index,
    ensure_indices,

    // Visibility
    propagate_visibility,

    // Query execution
    query_execute,
    query_materialize,

    // Subscription/Tracker
    tracker_on_insert,
    tracker_on_update,
    tracker_on_remove,

    // ResultSet
    result_set_insert,
    result_set_remove,
    result_set_move,
};

/// Statistics for a single operation type.
pub const OpStats = struct {
    count: u64 = 0,
    total_ns: u64 = 0,
    min_ns: u64 = std.math.maxInt(u64),
    max_ns: u64 = 0,

    pub fn record(self: *OpStats, elapsed_ns: u64) void {
        self.count += 1;
        self.total_ns += elapsed_ns;
        self.min_ns = @min(self.min_ns, elapsed_ns);
        self.max_ns = @max(self.max_ns, elapsed_ns);
    }

    pub fn avg_ns(self: OpStats) u64 {
        if (self.count == 0) return 0;
        return self.total_ns / self.count;
    }

    pub fn reset(self: *OpStats) void {
        self.* = .{};
    }
};

/// Scoped timer that records elapsed time on scope exit.
pub const Timer = struct {
    profiler: *Profiler,
    op: Op,
    start: ?std.time.Instant,

    pub fn stop(self: Timer) void {
        if (self.start) |start| {
            if (std.time.Instant.now()) |end| {
                const elapsed = end.since(start);
                self.profiler.stats[@intFromEnum(self.op)].record(elapsed);
            } else |_| {}
        }
    }
};

/// Main profiler that collects timing statistics and work counters.
pub const Profiler = struct {
    stats: [std.meta.fields(Op).len]OpStats = [_]OpStats{.{}} ** std.meta.fields(Op).len,

    // === Work Counters ===
    // These track the actual work done, useful for verifying algorithmic complexity.

    // Tree traversal
    nodes_visited: u64 = 0,
    nodes_created: u64 = 0,
    nodes_removed: u64 = 0,

    // Expansion
    edges_expanded: u64 = 0,
    edges_collapsed: u64 = 0,
    children_linked: u64 = 0,
    children_unlinked: u64 = 0,

    // Visibility propagation
    visibility_propagations: u64 = 0,
    visibility_delta_total: i64 = 0,

    // Index operations
    index_computations: u64 = 0,
    index_cache_hits: u64 = 0,
    index_cache_misses: u64 = 0,
    cache_invalidations: u64 = 0,

    // Viewport
    viewport_iterations: u64 = 0,
    scroll_steps: u64 = 0,

    // Query execution
    query_nodes_scanned: u64 = 0,
    query_nodes_matched: u64 = 0,
    query_materializations: u64 = 0,

    // Tracker
    tracker_inserts: u64 = 0,
    tracker_updates: u64 = 0,
    tracker_removes: u64 = 0,

    // === Structural Metrics ===
    max_depth_seen: u32 = 0,
    max_visible_seen: u32 = 0,

    // Session timing
    session_start: ?std.time.Instant = null,
    session_end: ?std.time.Instant = null,

    /// Start a timed operation. Call `.stop()` when done (or use with `defer`).
    pub fn time(self: *Profiler, op: Op) Timer {
        return .{
            .profiler = self,
            .op = op,
            .start = std.time.Instant.now() catch null,
        };
    }

    /// Get stats for a specific operation.
    pub fn get(self: *const Profiler, op: Op) OpStats {
        return self.stats[@intFromEnum(op)];
    }

    // === Work Counter Methods ===

    pub fn countNodeVisited(self: *Profiler) void {
        self.nodes_visited += 1;
    }

    pub fn countNodesVisited(self: *Profiler, n: u64) void {
        self.nodes_visited += n;
    }

    pub fn countNodeCreated(self: *Profiler) void {
        self.nodes_created += 1;
    }

    pub fn countNodeRemoved(self: *Profiler) void {
        self.nodes_removed += 1;
    }

    pub fn countEdgeExpanded(self: *Profiler) void {
        self.edges_expanded += 1;
    }

    pub fn countEdgeCollapsed(self: *Profiler) void {
        self.edges_collapsed += 1;
    }

    pub fn countChildrenLinked(self: *Profiler, n: u64) void {
        self.children_linked += n;
    }

    pub fn countChildrenUnlinked(self: *Profiler, n: u64) void {
        self.children_unlinked += n;
    }

    pub fn countVisibilityPropagation(self: *Profiler, delta: i64) void {
        self.visibility_propagations += 1;
        self.visibility_delta_total += delta;
    }

    pub fn countIndexComputation(self: *Profiler) void {
        self.index_computations += 1;
    }

    pub fn countCacheHit(self: *Profiler) void {
        self.index_cache_hits += 1;
    }

    pub fn countCacheMiss(self: *Profiler) void {
        self.index_cache_misses += 1;
    }

    pub fn countCacheInvalidation(self: *Profiler) void {
        self.cache_invalidations += 1;
    }

    pub fn countViewportIteration(self: *Profiler) void {
        self.viewport_iterations += 1;
    }

    pub fn countScrollStep(self: *Profiler) void {
        self.scroll_steps += 1;
    }

    pub fn countScrollSteps(self: *Profiler, n: u64) void {
        self.scroll_steps += n;
    }

    pub fn countQueryNodeScanned(self: *Profiler) void {
        self.query_nodes_scanned += 1;
    }

    pub fn countQueryNodeMatched(self: *Profiler) void {
        self.query_nodes_matched += 1;
    }

    pub fn countQueryMaterialization(self: *Profiler) void {
        self.query_materializations += 1;
    }

    pub fn countTrackerInsert(self: *Profiler) void {
        self.tracker_inserts += 1;
    }

    pub fn countTrackerUpdate(self: *Profiler) void {
        self.tracker_updates += 1;
    }

    pub fn countTrackerRemove(self: *Profiler) void {
        self.tracker_removes += 1;
    }

    // === Structural Metrics ===

    pub fn recordDepth(self: *Profiler, depth: u32) void {
        self.max_depth_seen = @max(self.max_depth_seen, depth);
    }

    pub fn recordVisible(self: *Profiler, visible: u32) void {
        self.max_visible_seen = @max(self.max_visible_seen, visible);
    }

    /// Start a profiling session.
    pub fn startSession(self: *Profiler) void {
        self.session_start = std.time.Instant.now() catch null;
    }

    /// End a profiling session.
    pub fn endSession(self: *Profiler) void {
        self.session_end = std.time.Instant.now() catch null;
    }

    /// Get total session duration in nanoseconds.
    pub fn sessionDuration(self: *const Profiler) u64 {
        const start = self.session_start orelse return 0;
        const end = self.session_end orelse return 0;
        return end.since(start);
    }

    /// Reset all statistics.
    pub fn reset(self: *Profiler) void {
        for (&self.stats) |*s| s.reset();

        // Work counters
        self.nodes_visited = 0;
        self.nodes_created = 0;
        self.nodes_removed = 0;
        self.edges_expanded = 0;
        self.edges_collapsed = 0;
        self.children_linked = 0;
        self.children_unlinked = 0;
        self.visibility_propagations = 0;
        self.visibility_delta_total = 0;
        self.index_computations = 0;
        self.index_cache_hits = 0;
        self.index_cache_misses = 0;
        self.cache_invalidations = 0;
        self.viewport_iterations = 0;
        self.scroll_steps = 0;
        self.query_nodes_scanned = 0;
        self.query_nodes_matched = 0;
        self.query_materializations = 0;
        self.tracker_inserts = 0;
        self.tracker_updates = 0;
        self.tracker_removes = 0;

        // Structural metrics
        self.max_depth_seen = 0;
        self.max_visible_seen = 0;

        // Session timing
        self.session_start = null;
        self.session_end = null;
    }

    /// Get total work done (useful for complexity verification).
    pub fn totalWork(self: *const Profiler) u64 {
        return self.nodes_visited +
            self.nodes_created +
            self.nodes_removed +
            self.children_linked +
            self.children_unlinked +
            self.visibility_propagations +
            self.index_computations +
            self.viewport_iterations +
            self.scroll_steps;
    }

    /// Write a human-readable report to the given writer.
    pub fn report(self: *const Profiler, writer: anytype) !void {
        try writer.writeAll("\n");
        try writer.writeAll("╔══════════════════════════════════════════════════════════════════════════════╗\n");
        try writer.writeAll("║                            PROFILING REPORT                                  ║\n");
        try writer.writeAll("╠══════════════════════════════════════════════════════════════════════════════╣\n");

        // Session duration
        const duration_ns = self.sessionDuration();
        if (duration_ns > 0) {
            const dur_str = formatDuration(duration_ns);
            try writer.print("║ Session Duration: {s}                                             ║\n", .{
                dur_str[0..10],
            });
            try writer.writeAll("╠══════════════════════════════════════════════════════════════════════════════╣\n");
        }

        // Operations table header
        try writer.writeAll("║ OPERATIONS                                                                   ║\n");
        try writer.writeAll("╠──────────────────────┬─────────┬──────────┬──────────┬──────────┬────────────╣\n");
        try writer.writeAll("║ Operation            │  Count  │   Avg    │   Min    │   Max    │   Total    ║\n");
        try writer.writeAll("╠──────────────────────┼─────────┼──────────┼──────────┼──────────┼────────────╣\n");

        // Group operations by category
        try self.reportCategory(writer, "Tree Mutations", &.{ .insert_root, .insert_child, .remove_root, .remove_node, .set_children });
        try self.reportCategory(writer, "Expand/Collapse", &.{ .expand, .collapse });
        try self.reportCategory(writer, "Viewport", &.{ .scroll_to, .scroll_by, .viewport_items });
        try self.reportCategory(writer, "Index Operations", &.{ .index_of, .node_at_index, .ensure_indices });
        try self.reportCategory(writer, "Visibility", &.{ .propagate_visibility });
        try self.reportCategory(writer, "Query Execution", &.{ .query_execute, .query_materialize });
        try self.reportCategory(writer, "Tracker", &.{ .tracker_on_insert, .tracker_on_update, .tracker_on_remove });
        try self.reportCategory(writer, "ResultSet", &.{ .result_set_insert, .result_set_remove, .result_set_move });

        try writer.writeAll("╠══════════════════════════════════════════════════════════════════════════════╣\n");

        // Work counters (the core of complexity verification)
        try writer.writeAll("║ WORK COUNTERS                                                                ║\n");
        try writer.writeAll("╠──────────────────────────────────────────────────────────────────────────────╣\n");

        // Tree traversal
        if (self.nodes_visited > 0 or self.nodes_created > 0 or self.nodes_removed > 0) {
            try writer.writeAll("║   Tree Traversal:                                                            ║\n");
            try writer.print("║     Nodes Visited:       {d:>10}                                          ║\n", .{self.nodes_visited});
            try writer.print("║     Nodes Created:       {d:>10}                                          ║\n", .{self.nodes_created});
            try writer.print("║     Nodes Removed:       {d:>10}                                          ║\n", .{self.nodes_removed});
        }

        // Expansion
        if (self.edges_expanded > 0 or self.edges_collapsed > 0 or self.children_linked > 0) {
            try writer.writeAll("║   Expansion:                                                                 ║\n");
            try writer.print("║     Edges Expanded:      {d:>10}                                          ║\n", .{self.edges_expanded});
            try writer.print("║     Edges Collapsed:     {d:>10}                                          ║\n", .{self.edges_collapsed});
            try writer.print("║     Children Linked:     {d:>10}                                          ║\n", .{self.children_linked});
            try writer.print("║     Children Unlinked:   {d:>10}                                          ║\n", .{self.children_unlinked});
        }

        // Visibility
        if (self.visibility_propagations > 0) {
            try writer.writeAll("║   Visibility:                                                                ║\n");
            try writer.print("║     Propagations:        {d:>10}                                          ║\n", .{self.visibility_propagations});
            try writer.print("║     Total Delta:         {d:>10}                                          ║\n", .{self.visibility_delta_total});
        }

        // Index operations
        if (self.index_computations > 0 or self.index_cache_hits > 0 or self.index_cache_misses > 0) {
            try writer.writeAll("║   Index:                                                                     ║\n");
            try writer.print("║     Computations:        {d:>10}                                          ║\n", .{self.index_computations});
            try writer.print("║     Cache Hits:          {d:>10}                                          ║\n", .{self.index_cache_hits});
            try writer.print("║     Cache Misses:        {d:>10}                                          ║\n", .{self.index_cache_misses});
            try writer.print("║     Invalidations:       {d:>10}                                          ║\n", .{self.cache_invalidations});
        }

        // Viewport
        if (self.viewport_iterations > 0 or self.scroll_steps > 0) {
            try writer.writeAll("║   Viewport:                                                                  ║\n");
            try writer.print("║     Iterations:          {d:>10}                                          ║\n", .{self.viewport_iterations});
            try writer.print("║     Scroll Steps:        {d:>10}                                          ║\n", .{self.scroll_steps});
        }

        // Query execution
        if (self.query_nodes_scanned > 0 or self.query_nodes_matched > 0) {
            try writer.writeAll("║   Query Execution:                                                           ║\n");
            try writer.print("║     Nodes Scanned:       {d:>10}                                          ║\n", .{self.query_nodes_scanned});
            try writer.print("║     Nodes Matched:       {d:>10}                                          ║\n", .{self.query_nodes_matched});
            try writer.print("║     Materializations:    {d:>10}                                          ║\n", .{self.query_materializations});
        }

        // Tracker
        if (self.tracker_inserts > 0 or self.tracker_updates > 0 or self.tracker_removes > 0) {
            try writer.writeAll("║   Tracker:                                                                   ║\n");
            try writer.print("║     Inserts:             {d:>10}                                          ║\n", .{self.tracker_inserts});
            try writer.print("║     Updates:             {d:>10}                                          ║\n", .{self.tracker_updates});
            try writer.print("║     Removes:             {d:>10}                                          ║\n", .{self.tracker_removes});
        }

        // Total work
        try writer.writeAll("╠──────────────────────────────────────────────────────────────────────────────╣\n");
        try writer.print("║   TOTAL WORK:            {d:>10}                                          ║\n", .{self.totalWork()});

        try writer.writeAll("╠══════════════════════════════════════════════════════════════════════════════╣\n");

        // Structural metrics
        try writer.writeAll("║ STRUCTURAL METRICS                                                           ║\n");
        try writer.writeAll("╠──────────────────────────────────────────────────────────────────────────────╣\n");
        try writer.print("║   Max Depth Seen:        {d:>10}                                          ║\n", .{self.max_depth_seen});
        try writer.print("║   Max Visible Seen:      {d:>10}                                          ║\n", .{self.max_visible_seen});

        try writer.writeAll("╚══════════════════════════════════════════════════════════════════════════════╝\n");
        try writer.writeAll("\n");
    }

    fn reportCategory(self: *const Profiler, writer: anytype, category: []const u8, ops: []const Op) !void {
        var has_data = false;
        for (ops) |op| {
            if (self.get(op).count > 0) {
                has_data = true;
                break;
            }
        }
        if (!has_data) return;

        // Category header
        try writer.print("║ {s:<77}║\n", .{category});

        for (ops) |op| {
            const stats = self.get(op);
            if (stats.count == 0) continue;

            const name = @tagName(op);
            const avg_str = formatDuration(stats.avg_ns());
            const min_str = formatDuration(stats.min_ns);
            const max_str = formatDuration(stats.max_ns);
            const total_str = formatDuration(stats.total_ns);
            try writer.print("║   {s:<18}│{d:>8} │{s:>9} │{s:>9} │{s:>9} │{s:>11} ║\n", .{
                name,
                stats.count,
                avg_str[0..9],
                min_str[0..9],
                max_str[0..9],
                total_str[0..11],
            });
        }
    }
};

/// Format a duration in nanoseconds to a human-readable string.
fn formatDuration(ns: u64) [16]u8 {
    var buf: [16]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buf);

    if (ns == std.math.maxInt(u64)) {
        _ = writer.writeAll("       -") catch {};
    } else if (ns >= 1_000_000_000) {
        // Seconds
        const secs = @as(f64, @floatFromInt(ns)) / 1_000_000_000.0;
        writer.print("{d:>6.2}s", .{secs}) catch {};
    } else if (ns >= 1_000_000) {
        // Milliseconds
        const ms = @as(f64, @floatFromInt(ns)) / 1_000_000.0;
        writer.print("{d:>5.2}ms", .{ms}) catch {};
    } else if (ns >= 1_000) {
        // Microseconds
        const us = @as(f64, @floatFromInt(ns)) / 1_000.0;
        writer.print("{d:>5.2}μs", .{us}) catch {};
    } else {
        // Nanoseconds
        writer.print("{d:>5}ns", .{ns}) catch {};
    }

    // Pad remaining with spaces
    const written = writer.end;
    @memset(buf[written..], ' ');

    return buf;
}

// ============================================================================
// Compile-time conditional profiling
// ============================================================================

/// A no-op profiler used when profiling is disabled.
/// All methods compile to nothing.
pub const NullProfiler = struct {
    pub const NullTimer = struct {
        pub inline fn stop(_: NullTimer) void {}
    };

    pub inline fn time(_: *NullProfiler, _: Op) NullTimer {
        return .{};
    }

    pub inline fn get(_: *const NullProfiler, _: Op) OpStats {
        return .{};
    }

    // === Work Counter Methods (no-ops) ===
    pub inline fn countNodeVisited(_: *NullProfiler) void {}
    pub inline fn countNodesVisited(_: *NullProfiler, _: u64) void {}
    pub inline fn countNodeCreated(_: *NullProfiler) void {}
    pub inline fn countNodeRemoved(_: *NullProfiler) void {}
    pub inline fn countEdgeExpanded(_: *NullProfiler) void {}
    pub inline fn countEdgeCollapsed(_: *NullProfiler) void {}
    pub inline fn countChildrenLinked(_: *NullProfiler, _: u64) void {}
    pub inline fn countChildrenUnlinked(_: *NullProfiler, _: u64) void {}
    pub inline fn countVisibilityPropagation(_: *NullProfiler, _: i64) void {}
    pub inline fn countIndexComputation(_: *NullProfiler) void {}
    pub inline fn countCacheHit(_: *NullProfiler) void {}
    pub inline fn countCacheMiss(_: *NullProfiler) void {}
    pub inline fn countCacheInvalidation(_: *NullProfiler) void {}
    pub inline fn countViewportIteration(_: *NullProfiler) void {}
    pub inline fn countScrollStep(_: *NullProfiler) void {}
    pub inline fn countScrollSteps(_: *NullProfiler, _: u64) void {}
    pub inline fn countQueryNodeScanned(_: *NullProfiler) void {}
    pub inline fn countQueryNodeMatched(_: *NullProfiler) void {}
    pub inline fn countQueryMaterialization(_: *NullProfiler) void {}
    pub inline fn countTrackerInsert(_: *NullProfiler) void {}
    pub inline fn countTrackerUpdate(_: *NullProfiler) void {}
    pub inline fn countTrackerRemove(_: *NullProfiler) void {}

    // === Structural Metrics (no-ops) ===
    pub inline fn recordDepth(_: *NullProfiler, _: u32) void {}
    pub inline fn recordVisible(_: *NullProfiler, _: u32) void {}

    // === Session Methods (no-ops) ===
    pub inline fn startSession(_: *NullProfiler) void {}
    pub inline fn endSession(_: *NullProfiler) void {}
    pub inline fn sessionDuration(_: *const NullProfiler) u64 {
        return 0;
    }
    pub inline fn reset(_: *NullProfiler) void {}
    pub inline fn totalWork(_: *const NullProfiler) u64 {
        return 0;
    }
    pub inline fn report(_: *const NullProfiler, _: anytype) !void {}
};

/// The profiler type to use based on compile-time flag.
pub const ActiveProfiler = if (enabled) Profiler else NullProfiler;

/// Global profiler instance (when enabled).
pub var global: ActiveProfiler = .{};

/// Convenience function to time an operation using the global profiler.
pub inline fn time(op: Op) if (enabled) Timer else NullProfiler.NullTimer {
    return global.time(op);
}

// ============================================================================
// Tests
// ============================================================================

test "OpStats records timing correctly" {
    var stats = OpStats{};

    stats.record(100);
    stats.record(200);
    stats.record(50);

    try std.testing.expectEqual(@as(u64, 3), stats.count);
    try std.testing.expectEqual(@as(u64, 350), stats.total_ns);
    try std.testing.expectEqual(@as(u64, 50), stats.min_ns);
    try std.testing.expectEqual(@as(u64, 200), stats.max_ns);
    try std.testing.expectEqual(@as(u64, 116), stats.avg_ns()); // 350/3 = 116
}

test "Profiler times operations" {
    var profiler = Profiler{};

    // Simulate a timed operation
    var result: u64 = 0;
    {
        const timer = profiler.time(.expand);
        // Do some work (a simple loop to burn some time)
        var sum: u64 = 0;
        for (0..1000) |i| {
            sum +%= i;
        }
        result = sum;
        timer.stop();
    }

    // Use result to prevent optimizer from removing the loop
    try std.testing.expect(result > 0);

    const stats = profiler.get(.expand);
    try std.testing.expectEqual(@as(u64, 1), stats.count);
    try std.testing.expect(stats.total_ns > 0); // Some time should have elapsed
}

test "Profiler report format" {
    var profiler = Profiler{};

    // Add some sample data
    profiler.stats[@intFromEnum(Op.expand)].record(1_500);
    profiler.stats[@intFromEnum(Op.expand)].record(2_500);
    profiler.stats[@intFromEnum(Op.collapse)].record(800);
    profiler.stats[@intFromEnum(Op.scroll_to)].record(100);
    profiler.stats[@intFromEnum(Op.scroll_to)].record(150);
    profiler.stats[@intFromEnum(Op.scroll_to)].record(200);

    profiler.max_depth_seen = 12;
    profiler.max_visible_seen = 1500;
    profiler.cache_invalidations = 5;

    // Generate report to buffer
    var buf: [4096]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buf);

    try profiler.report(&writer);

    const output = writer.buffered();

    // Verify key sections exist
    try std.testing.expect(std.mem.indexOf(u8, output, "PROFILING REPORT") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "expand") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "collapse") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "scroll_to") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "Max Depth Seen") != null);
}

test "NullProfiler compiles to no-ops" {
    var profiler = NullProfiler{};

    // These should all be no-ops
    const timer = profiler.time(.expand);
    timer.stop();

    profiler.recordDepth(100);
    profiler.recordVisible(1000);

    // Stats should be empty
    const stats = profiler.get(.expand);
    try std.testing.expectEqual(@as(u64, 0), stats.count);
}
