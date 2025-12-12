///! Reactive module for live query subscriptions.

pub const result_set = @import("reactive/result_set.zig");
pub const ResultSet = result_set.ResultSet;
pub const ResultNode = result_set.ResultNode;

pub const subscription = @import("reactive/subscription.zig");
pub const Subscription = subscription.Subscription;
pub const SubscriptionId = subscription.SubscriptionId;
pub const Callbacks = subscription.Callbacks;

pub const tracker = @import("reactive/tracker.zig");
pub const ChangeTracker = tracker.ChangeTracker;
pub const NodeCallbacks = tracker.NodeCallbacks;

pub const view = @import("reactive/tree.zig");
pub const View = view.View;
pub const ViewOpts = view.ViewOpts;

// Unified reactive tree (replaces count_tree + window + expansion_state)
pub const reactive_tree = @import("reactive/reactive_tree.zig");
pub const ReactiveTree = reactive_tree.ReactiveTree;
pub const TreeNode = reactive_tree.TreeNode;
pub const EdgeChildren = reactive_tree.EdgeChildren;
pub const Viewport = reactive_tree.Viewport;

// Tree path addressing
pub const tree_path = @import("reactive/tree_path.zig");
pub const TreePath = tree_path.TreePath;

test {
    @import("std").testing.refAllDecls(@This());
}
