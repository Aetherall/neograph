///! Visual tests for realistic graph structures using the public Graph API.
///!
///! Tests tree rendering and scrolling using a DAP (Debug Adapter Protocol)
///! inspired graph structure:
///!
///!   Debugger
///!   ├─ Session
///!   │  ├─ Thread
///!   │  │  └─ Stack
///!   │  │     └─ Frame
///!   │  │        └─ Scope
///!   │  │           └─ Variable
///!   │  │              └─ Variable* (recursive)
///!   │  └─ SourceBinding
///!   │     └─ Source
///!   ├─ Source
///!   │  └─ SourceBinding
///!   │     └─ Session
///!   └─ Breakpoint
///!      └─ BreakpointBinding
///!         └─ Source

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const watchdog = @import("test_watchdog.zig");

// Global watchdog - started by first test
var global_watchdog: ?watchdog.Watchdog = null;

fn ensureWatchdog() void {
    if (global_watchdog == null) {
        global_watchdog = watchdog.startDefault();
    }
}

// Public API imports
const ng = @import("neograph.zig");
const Graph = ng.Graph;
const Schema = ng.Schema;
const NodeId = ng.NodeId;
const View = ng.View;
const ViewOpts = ng.ViewOpts;
const parseSchema = ng.parseSchema;

// ============================================================================
// DAP Schema (JSON)
// ============================================================================

fn createDapSchema(allocator: Allocator) !Schema {
    return parseSchema(allocator,
        \\{
        \\  "types": [
        \\    {
        \\      "name": "Debugger",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [
        \\        { "name": "sessions", "target": "Session", "reverse": "_debugger" },
        \\        { "name": "sources", "target": "Source", "reverse": "_debugger" },
        \\        { "name": "breakpoints", "target": "Breakpoint", "reverse": "_debugger" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Session",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [
        \\        { "name": "threads", "target": "Thread", "reverse": "_session" },
        \\        { "name": "_debugger", "target": "Debugger", "reverse": "sessions" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Thread",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [
        \\        { "name": "stack", "target": "Stack", "reverse": "_thread" },
        \\        { "name": "_session", "target": "Session", "reverse": "threads" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Stack",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [
        \\        { "name": "frames", "target": "Frame", "reverse": "_stack" },
        \\        { "name": "_thread", "target": "Thread", "reverse": "stack" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Frame",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [
        \\        { "name": "scopes", "target": "Scope", "reverse": "_frame" },
        \\        { "name": "_stack", "target": "Stack", "reverse": "frames" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Scope",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [
        \\        { "name": "variables", "target": "Variable", "reverse": "_scope" },
        \\        { "name": "_frame", "target": "Frame", "reverse": "scopes" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Variable",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [
        \\        { "name": "children", "target": "Variable", "reverse": "_parent" },
        \\        { "name": "_parent", "target": "Variable", "reverse": "children" },
        \\        { "name": "_scope", "target": "Scope", "reverse": "variables" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Source",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [{ "name": "_debugger", "target": "Debugger", "reverse": "sources" }],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "Breakpoint",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [
        \\        { "name": "bindings", "target": "BreakpointBinding", "reverse": "_breakpoint" },
        \\        { "name": "_debugger", "target": "Debugger", "reverse": "breakpoints" }
        \\      ],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    },
        \\    {
        \\      "name": "BreakpointBinding",
        \\      "properties": [
        \\        { "name": "name", "type": "string" },
        \\        { "name": "order", "type": "int" }
        \\      ],
        \\      "edges": [{ "name": "_breakpoint", "target": "Breakpoint", "reverse": "bindings" }],
        \\      "indexes": [{ "fields": [{ "field": "order", "direction": "asc" }] }]
        \\    }
        \\  ]
        \\}
    ) catch return error.InvalidJson;
}

// ============================================================================
// DAP Node IDs Container
// ============================================================================

const DapIds = struct {
    debugger: NodeId,
    session_1: NodeId,
    thread_main: NodeId,
    thread_worker_1: NodeId,
    stack_main: NodeId,
    frame_main_0: NodeId,
    frame_main_1: NodeId,
    frame_main_2: NodeId,
    scope_local: NodeId,
    scope_args: NodeId,
    scope_global: NodeId,
    var_self: NodeId,
    var_count: NodeId,
    var_items: NodeId,
    var_item_0: NodeId,
    var_item_1: NodeId,
    var_item_2: NodeId,
    var_nested: NodeId,
    var_nested_child: NodeId,
    source_main: NodeId,
    source_utils: NodeId,
    source_config: NodeId,
    bp_1: NodeId,
    bp_2: NodeId,
    bp_3: NodeId,
    bp_binding_1: NodeId,
    bp_binding_2: NodeId,
};

// ============================================================================
// DAP Graph Builder
// ============================================================================

/// Build the full DAP debugger graph using Graph API
fn buildDapGraph(g: *Graph) !DapIds {
    var ids: DapIds = undefined;

    // Root: Debugger
    ids.debugger = try g.insert("Debugger");
    try g.update(ids.debugger, .{ .name = "Debugger", .order = @as(i64, 0) });

    // Sessions
    ids.session_1 = try g.insert("Session");
    try g.update(ids.session_1, .{ .name = "Session[main]", .order = @as(i64, 0) });
    try g.link(ids.debugger, "sessions", ids.session_1);

    // Threads
    ids.thread_main = try g.insert("Thread");
    try g.update(ids.thread_main, .{ .name = "Thread[main]", .order = @as(i64, 0) });
    try g.link(ids.session_1, "threads", ids.thread_main);

    ids.thread_worker_1 = try g.insert("Thread");
    try g.update(ids.thread_worker_1, .{ .name = "Thread[worker-1]", .order = @as(i64, 1) });
    try g.link(ids.session_1, "threads", ids.thread_worker_1);

    // Stack (main thread)
    ids.stack_main = try g.insert("Stack");
    try g.update(ids.stack_main, .{ .name = "Stack", .order = @as(i64, 0) });
    try g.link(ids.thread_main, "stack", ids.stack_main);

    // Frames
    ids.frame_main_0 = try g.insert("Frame");
    try g.update(ids.frame_main_0, .{ .name = "Frame[0] main()", .order = @as(i64, 0) });
    try g.link(ids.stack_main, "frames", ids.frame_main_0);

    ids.frame_main_1 = try g.insert("Frame");
    try g.update(ids.frame_main_1, .{ .name = "Frame[1] process()", .order = @as(i64, 1) });
    try g.link(ids.stack_main, "frames", ids.frame_main_1);

    ids.frame_main_2 = try g.insert("Frame");
    try g.update(ids.frame_main_2, .{ .name = "Frame[2] init()", .order = @as(i64, 2) });
    try g.link(ids.stack_main, "frames", ids.frame_main_2);

    // Scopes (frame 0)
    ids.scope_local = try g.insert("Scope");
    try g.update(ids.scope_local, .{ .name = "Scope[Local]", .order = @as(i64, 0) });
    try g.link(ids.frame_main_0, "scopes", ids.scope_local);

    ids.scope_args = try g.insert("Scope");
    try g.update(ids.scope_args, .{ .name = "Scope[Arguments]", .order = @as(i64, 1) });
    try g.link(ids.frame_main_0, "scopes", ids.scope_args);

    ids.scope_global = try g.insert("Scope");
    try g.update(ids.scope_global, .{ .name = "Scope[Global]", .order = @as(i64, 2) });
    try g.link(ids.frame_main_0, "scopes", ids.scope_global);

    // Variables (local scope)
    ids.var_self = try g.insert("Variable");
    try g.update(ids.var_self, .{ .name = "self: Object", .order = @as(i64, 0) });
    try g.link(ids.scope_local, "variables", ids.var_self);

    ids.var_count = try g.insert("Variable");
    try g.update(ids.var_count, .{ .name = "count: 42", .order = @as(i64, 1) });
    try g.link(ids.scope_local, "variables", ids.var_count);

    ids.var_items = try g.insert("Variable");
    try g.update(ids.var_items, .{ .name = "items: Array[3]", .order = @as(i64, 2) });
    try g.link(ids.scope_local, "variables", ids.var_items);

    ids.var_nested = try g.insert("Variable");
    try g.update(ids.var_nested, .{ .name = "nested: Object", .order = @as(i64, 3) });
    try g.link(ids.scope_local, "variables", ids.var_nested);

    // Array elements (children of items)
    ids.var_item_0 = try g.insert("Variable");
    try g.update(ids.var_item_0, .{ .name = "[0]: \"first\"", .order = @as(i64, 0) });
    try g.link(ids.var_items, "children", ids.var_item_0);

    ids.var_item_1 = try g.insert("Variable");
    try g.update(ids.var_item_1, .{ .name = "[1]: \"second\"", .order = @as(i64, 1) });
    try g.link(ids.var_items, "children", ids.var_item_1);

    ids.var_item_2 = try g.insert("Variable");
    try g.update(ids.var_item_2, .{ .name = "[2]: \"third\"", .order = @as(i64, 2) });
    try g.link(ids.var_items, "children", ids.var_item_2);

    // Nested child
    ids.var_nested_child = try g.insert("Variable");
    try g.update(ids.var_nested_child, .{ .name = "child: null", .order = @as(i64, 0) });
    try g.link(ids.var_nested, "children", ids.var_nested_child);

    // Sources
    ids.source_main = try g.insert("Source");
    try g.update(ids.source_main, .{ .name = "Source[main.zig]", .order = @as(i64, 0) });
    try g.link(ids.debugger, "sources", ids.source_main);

    ids.source_utils = try g.insert("Source");
    try g.update(ids.source_utils, .{ .name = "Source[utils.zig]", .order = @as(i64, 1) });
    try g.link(ids.debugger, "sources", ids.source_utils);

    ids.source_config = try g.insert("Source");
    try g.update(ids.source_config, .{ .name = "Source[config.zig]", .order = @as(i64, 2) });
    try g.link(ids.debugger, "sources", ids.source_config);

    // Breakpoints
    ids.bp_1 = try g.insert("Breakpoint");
    try g.update(ids.bp_1, .{ .name = "Breakpoint[main.zig:42]", .order = @as(i64, 0) });
    try g.link(ids.debugger, "breakpoints", ids.bp_1);

    ids.bp_2 = try g.insert("Breakpoint");
    try g.update(ids.bp_2, .{ .name = "Breakpoint[main.zig:100]", .order = @as(i64, 1) });
    try g.link(ids.debugger, "breakpoints", ids.bp_2);

    ids.bp_3 = try g.insert("Breakpoint");
    try g.update(ids.bp_3, .{ .name = "Breakpoint[utils.zig:10]", .order = @as(i64, 2) });
    try g.link(ids.debugger, "breakpoints", ids.bp_3);

    // Breakpoint bindings
    ids.bp_binding_1 = try g.insert("BreakpointBinding");
    try g.update(ids.bp_binding_1, .{ .name = "BreakpointBinding", .order = @as(i64, 0) });
    try g.link(ids.bp_1, "bindings", ids.bp_binding_1);

    ids.bp_binding_2 = try g.insert("BreakpointBinding");
    try g.update(ids.bp_binding_2, .{ .name = "BreakpointBinding", .order = @as(i64, 0) });
    try g.link(ids.bp_3, "bindings", ids.bp_binding_2);

    return ids;
}

// ============================================================================
// Visual Output Buffer
// ============================================================================

const RenderBuffer = struct {
    lines: std.ArrayListUnmanaged([]const u8),
    allocator: Allocator,

    fn init(allocator: Allocator) RenderBuffer {
        return .{ .lines = .{}, .allocator = allocator };
    }

    fn deinit(self: *RenderBuffer) void {
        for (self.lines.items) |line| self.allocator.free(line);
        self.lines.deinit(self.allocator);
    }

    fn clear(self: *RenderBuffer) void {
        for (self.lines.items) |line| self.allocator.free(line);
        self.lines.clearRetainingCapacity();
    }

    fn addFmt(self: *RenderBuffer, comptime fmt: []const u8, args: anytype) !void {
        const line = try std.fmt.allocPrint(self.allocator, fmt, args);
        try self.lines.append(self.allocator, line);
    }

    fn expectContainsLines(self: *const RenderBuffer, pattern: []const u8) !void {
        var pattern_iter = std.mem.splitScalar(u8, pattern, '\n');
        var output_idx: usize = 0;

        while (pattern_iter.next()) |pattern_line| {
            const trimmed = std.mem.trimRight(u8, pattern_line, " \t\r");
            if (trimmed.len == 0) continue;

            var found = false;
            while (output_idx < self.lines.items.len) : (output_idx += 1) {
                if (std.mem.indexOf(u8, self.lines.items[output_idx], trimmed) != null) {
                    found = true;
                    output_idx += 1;
                    break;
                }
            }

            if (!found) {
                std.debug.print("\nPattern not found: '{s}'\n", .{trimmed});
                std.debug.print("Full output:\n", .{});
                for (self.lines.items, 0..) |line, i| {
                    std.debug.print("  {d}: '{s}'\n", .{ i, line });
                }
                return error.PatternNotFound;
            }
        }
    }

    fn dump(self: *const RenderBuffer) void {
        std.debug.print("\n", .{});
        for (self.lines.items) |line| {
            std.debug.print("{s}\n", .{line});
        }
    }
};

// ============================================================================
// DAP Tree Renderer
// ============================================================================

const DapRenderer = struct {
    buffer: RenderBuffer,
    allocator: Allocator,
    graph: *Graph,

    fn init(allocator: Allocator, graph: *Graph) DapRenderer {
        return .{
            .buffer = RenderBuffer.init(allocator),
            .allocator = allocator,
            .graph = graph,
        };
    }

    fn deinit(self: *DapRenderer) void {
        self.buffer.deinit();
    }

    fn reset(self: *DapRenderer) void {
        self.buffer.clear();
    }

    fn renderTree(self: *DapRenderer, tree: *View, label: []const u8) !void {
        try self.buffer.addFmt("╔═══ {s} ═══╗", .{label});
        try self.buffer.addFmt("│ offset={d} height={d} total={d}", .{
            tree.getOffset(),
            tree.getHeight(),
            tree.total(),
        });
        try self.buffer.addFmt("├────────────────────────────────────────", .{});

        var iter = tree.items();
        var rendered: u32 = 0;

        while (iter.next()) |item| {
            try self.renderItem(item);
            rendered += 1;
        }

        try self.buffer.addFmt("├────────────────────────────────────────", .{});
        try self.buffer.addFmt("│ rendered: {d} items", .{rendered});
        try self.buffer.addFmt("╚════════════════════════════════════════", .{});
    }

    fn renderItem(self: *DapRenderer, item: anytype) !void {
        var indent_buf: [64]u8 = undefined;
        const indent_len = @min(item.depth * 2, 60);
        @memset(indent_buf[0..indent_len], ' ');

        // Get the node to check for edges and expansion state
        const store_node = self.graph.get(item.id);
        const name = if (store_node) |n| (if (n.getProperty("name")) |v| v.string else "Unknown") else "Unknown";

        // Determine icon based on whether the item has children and is expanded
        const is_expanded = item.node.expanded_edges.count() > 0;
        const icon = if (item.has_children) (if (is_expanded) "▼" else "▶") else "─";

        try self.buffer.addFmt("│ {s}{s} {s}", .{
            indent_buf[0..indent_len],
            icon,
            name,
        });
    }
};

// ============================================================================
// Visual Tests - DAP Graph Structure
// ============================================================================

test "DAP: collapsed debugger shows only root" {
    ensureWatchdog();

    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const ids = try buildDapGraph(g);

    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    var renderer = DapRenderer.init(testing.allocator, g);
    defer renderer.deinit();

    try renderer.renderTree(&tree, "Collapsed Debugger");

    try renderer.buffer.expectContainsLines(
        \\offset=0 height=20 total=1
        \\▶ Debugger
        \\rendered: 1 items
    );

    _ = ids;
}

test "DAP: expand sessions shows session list" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const ids = try buildDapGraph(g);

    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    // Expand Debugger -> sessions
    try tree.expandById(ids.debugger, "sessions");

    var renderer = DapRenderer.init(testing.allocator, g);
    defer renderer.deinit();

    try renderer.renderTree(&tree, "Sessions Expanded");

    try renderer.buffer.expectContainsLines(
        \\total=2
        \\▼ Debugger
        \\  ▶ Session[main]
        \\rendered: 2 items
    );
}

test "DAP: expand full thread hierarchy" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const ids = try buildDapGraph(g);

    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    // Expand path: Debugger -> sessions -> Session -> threads
    try tree.expandById(ids.debugger, "sessions");
    try tree.expandById(ids.session_1, "threads");

    var renderer = DapRenderer.init(testing.allocator, g);
    defer renderer.deinit();

    try renderer.renderTree(&tree, "Threads Expanded");

    try renderer.buffer.expectContainsLines(
        \\▼ Debugger
        \\  ▼ Session[main]
        \\    ▶ Thread[main]
        \\    ▶ Thread[worker-1]
    );
}

test "DAP: deep expansion to stack frames" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const ids = try buildDapGraph(g);

    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    // Expand: Debugger -> sessions -> Session -> threads -> Thread[main] -> stack -> Stack -> frames
    try tree.expandById(ids.debugger, "sessions");
    try tree.expandById(ids.session_1, "threads");
    try tree.expandById(ids.thread_main, "stack");
    try tree.expandById(ids.stack_main, "frames");

    var renderer = DapRenderer.init(testing.allocator, g);
    defer renderer.deinit();

    try renderer.renderTree(&tree, "Stack Frames");

    try renderer.buffer.expectContainsLines(
        \\▼ Debugger
        \\  ▼ Session[main]
        \\    ▼ Thread[main]
        \\      ▼ Stack
        \\        ▶ Frame[0] main()
        \\        ▶ Frame[1] process()
        \\        ▶ Frame[2] init()
        \\    ▶ Thread[worker-1]
    );
}

test "DAP: full variable expansion with nested objects" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const ids = try buildDapGraph(g);

    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 30 });
    defer tree.deinit();
    tree.activate(false);

    // Expand full path to variables
    try tree.expandById(ids.debugger, "sessions");
    try tree.expandById(ids.session_1, "threads");
    try tree.expandById(ids.thread_main, "stack");
    try tree.expandById(ids.stack_main, "frames");
    try tree.expandById(ids.frame_main_0, "scopes");
    try tree.expandById(ids.scope_local, "variables");

    var renderer = DapRenderer.init(testing.allocator, g);
    defer renderer.deinit();

    try renderer.renderTree(&tree, "Variables");

    try renderer.buffer.expectContainsLines(
        \\▼ Scope[Local]
        \\  ▶ self: Object
        \\  ▶ count: 42
        \\  ▶ items: Array[3]
        \\  ▶ nested: Object
        \\▶ Scope[Arguments]
        \\▶ Scope[Global]
    );
}

test "DAP: expand array variable children" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const ids = try buildDapGraph(g);

    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 30 });
    defer tree.deinit();
    tree.activate(false);

    // Expand to variables
    try tree.expandById(ids.debugger, "sessions");
    try tree.expandById(ids.session_1, "threads");
    try tree.expandById(ids.thread_main, "stack");
    try tree.expandById(ids.stack_main, "frames");
    try tree.expandById(ids.frame_main_0, "scopes");
    try tree.expandById(ids.scope_local, "variables");

    // Expand items array
    try tree.expandById(ids.var_items, "children");

    var renderer = DapRenderer.init(testing.allocator, g);
    defer renderer.deinit();

    try renderer.renderTree(&tree, "Array Expanded");

    try renderer.buffer.expectContainsLines(
        \\▶ self: Object
        \\▶ count: 42
        \\▼ items: Array[3]
        \\  ▶ [0]: "first"
        \\  ▶ [1]: "second"
        \\  ▶ [2]: "third"
        \\▶ nested: Object
    );
}

test "DAP: expand breakpoints with bindings" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const ids = try buildDapGraph(g);

    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    // Expand breakpoints
    try tree.expandById(ids.debugger, "breakpoints");
    try tree.expandById(ids.bp_1, "bindings");

    var renderer = DapRenderer.init(testing.allocator, g);
    defer renderer.deinit();

    try renderer.renderTree(&tree, "Breakpoints");

    try renderer.buffer.expectContainsLines(
        \\▼ Debugger
        \\  ▼ Breakpoint[main.zig:42]
        \\    ▶ BreakpointBinding
        \\  ▶ Breakpoint[main.zig:100]
        \\  ▶ Breakpoint[utils.zig:10]
    );
}

test "DAP: expand sources list" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const ids = try buildDapGraph(g);

    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 20 });
    defer tree.deinit();
    tree.activate(false);

    try tree.expandById(ids.debugger, "sources");

    var renderer = DapRenderer.init(testing.allocator, g);
    defer renderer.deinit();

    try renderer.renderTree(&tree, "Sources");

    try renderer.buffer.expectContainsLines(
        \\▼ Debugger
        \\  ▶ Source[main.zig]
        \\  ▶ Source[utils.zig]
        \\  ▶ Source[config.zig]
    );
}

// ============================================================================
// Visual Tests - Scrolling in DAP Graph
// ============================================================================

test "DAP: scroll through deeply nested structure" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const ids = try buildDapGraph(g);

    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 5 });
    defer tree.deinit();
    tree.activate(false);

    // Expand just sessions path for more predictable ordering
    try tree.expandById(ids.debugger, "sessions");
    try tree.expandById(ids.session_1, "threads");
    try tree.expandById(ids.thread_main, "stack");
    try tree.expandById(ids.stack_main, "frames");

    // Total visible: 8 items (debugger + session + 2 threads + stack + 3 frames)
    try testing.expectEqual(@as(u32, 8), tree.total());

    var renderer = DapRenderer.init(testing.allocator, g);
    defer renderer.deinit();

    // Top of tree
    try renderer.renderTree(&tree, "Top");
    try renderer.buffer.expectContainsLines(
        \\offset=0
        \\▼ Debugger
    );

    renderer.reset();

    // Scroll down by 2
    tree.move(2);
    try renderer.renderTree(&tree, "Middle");
    try renderer.buffer.expectContainsLines(
        \\offset=2
        \\Thread
    );

    renderer.reset();

    // Scroll to max (should clamp to 3 = 8-5)
    tree.scrollTo(100);
    try renderer.renderTree(&tree, "End");
    try renderer.buffer.expectContainsLines(
        \\offset=3
        \\Frame
    );
}

test "DAP: viewport spans multiple edge types" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const ids = try buildDapGraph(g);

    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 10 });
    defer tree.deinit();
    tree.activate(false);

    // Expand all top-level edges
    try tree.expandById(ids.debugger, "sessions");
    try tree.expandById(ids.debugger, "sources");
    try tree.expandById(ids.debugger, "breakpoints");

    var renderer = DapRenderer.init(testing.allocator, g);
    defer renderer.deinit();

    // View shows all edges in same viewport (only 8 items total)
    try renderer.renderTree(&tree, "Mixed Edges");

    // Should see both breakpoints and sources in same viewport
    // Order depends on sort keys - just verify both types appear
    try renderer.buffer.expectContainsLines(
        \\Source[
        \\Breakpoint[
    );
}

// ============================================================================
// Visual Tests - Profiling
// ============================================================================

test "DAP: visible count tracking through expansions" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const ids = try buildDapGraph(g);

    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 30 });
    defer tree.deinit();
    tree.activate(false);

    // Track visible count changes
    try testing.expectEqual(@as(u32, 1), tree.total());

    try tree.expandById(ids.debugger, "sessions");
    try testing.expectEqual(@as(u32, 2), tree.total());

    try tree.expandById(ids.debugger, "sources");
    try testing.expectEqual(@as(u32, 5), tree.total()); // +3 sources

    try tree.expandById(ids.debugger, "breakpoints");
    try testing.expectEqual(@as(u32, 8), tree.total()); // +3 breakpoints

    // Collapse sources
    tree.collapseById(ids.debugger, "sources");
    try testing.expectEqual(@as(u32, 5), tree.total()); // -3 sources
}

test "DAP: deep expansion visible count" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const ids = try buildDapGraph(g);

    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 30 });
    defer tree.deinit();
    tree.activate(false);

    var expected_visible: u32 = 1;
    try testing.expectEqual(expected_visible, tree.total());

    // Debugger -> sessions (+1 session)
    try tree.expandById(ids.debugger, "sessions");
    expected_visible += 1;
    try testing.expectEqual(expected_visible, tree.total());

    // Session -> threads (+2 threads)
    try tree.expandById(ids.session_1, "threads");
    expected_visible += 2;
    try testing.expectEqual(expected_visible, tree.total());

    // Thread -> stack (+1 stack)
    try tree.expandById(ids.thread_main, "stack");
    expected_visible += 1;
    try testing.expectEqual(expected_visible, tree.total());

    // Stack -> frames (+3 frames)
    try tree.expandById(ids.stack_main, "frames");
    expected_visible += 3;
    try testing.expectEqual(expected_visible, tree.total());

    // Frame -> scopes (+3 scopes)
    try tree.expandById(ids.frame_main_0, "scopes");
    expected_visible += 3;
    try testing.expectEqual(expected_visible, tree.total());

    // Scope -> variables (+4 variables)
    try tree.expandById(ids.scope_local, "variables");
    expected_visible += 4;
    try testing.expectEqual(expected_visible, tree.total());

    // Variable[items] -> children (+3 array elements)
    try tree.expandById(ids.var_items, "children");
    expected_visible += 3;
    try testing.expectEqual(expected_visible, tree.total());
}

test "DAP: viewport renders O(height) items" {
    const schema = try createDapSchema(testing.allocator);
    const g = try Graph.init(testing.allocator, schema);
    defer g.deinit();

    const ids = try buildDapGraph(g);

    var tree = try g.view(.{ .root = "Debugger", .sort = &.{"order"} }, .{ .limit = 30 });
    defer tree.deinit();
    tree.activate(false);

    // Fully expand to create many visible items
    try tree.expandById(ids.debugger, "sessions");
    try tree.expandById(ids.debugger, "sources");
    try tree.expandById(ids.debugger, "breakpoints");
    try tree.expandById(ids.session_1, "threads");
    try tree.expandById(ids.thread_main, "stack");
    try tree.expandById(ids.stack_main, "frames");
    try tree.expandById(ids.frame_main_0, "scopes");
    try tree.expandById(ids.scope_local, "variables");
    try tree.expandById(ids.bp_1, "bindings");

    const total_visible = tree.total();
    try testing.expect(total_visible > 15);

    // Test with different viewport heights
    const heights = [_]u32{ 3, 5, 10 };

    for (heights) |height| {
        tree.setHeight(height);

        // Count rendered items
        var count: u32 = 0;
        var iter = tree.items();
        while (iter.next()) |_| {
            count += 1;
        }

        // Should render exactly height items (or less if total < height)
        const expected = @min(height, total_visible);
        try testing.expectEqual(expected, count);
    }
}
