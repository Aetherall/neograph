const std = @import("std");

/// Watchdog timer that terminates the process if tests hang.
/// Start this at the beginning of test files that might have infinite loops.
pub const Watchdog = struct {
    thread: ?std.Thread = null,

    /// Start the watchdog with a timeout in seconds.
    pub fn start(timeout_secs: u32) Watchdog {
        const thread = std.Thread.spawn(.{}, watchdogThread, .{timeout_secs}) catch |err| {
            std.debug.print("Warning: Failed to start watchdog: {}\n", .{err});
            return .{ .thread = null };
        };
        return .{ .thread = thread };
    }

    /// Cancel the watchdog (call this when tests complete successfully).
    pub fn cancel(self: *Watchdog) void {
        // We can't actually cancel the thread, but we can detach it
        // The process will exit normally before the watchdog triggers
        if (self.thread) |t| {
            t.detach();
            self.thread = null;
        }
    }

    fn watchdogThread(timeout_secs: u32) void {
        std.posix.nanosleep(timeout_secs, 0);
        std.debug.print("\n\n*** WATCHDOG TIMEOUT: Tests exceeded {}s limit ***\n\n", .{timeout_secs});
        std.posix.exit(124); // Same exit code as `timeout` command
    }
};

/// Default timeout for tests (5 seconds)
pub const DEFAULT_TIMEOUT_SECS: u32 = 5;

/// Start a watchdog with the default timeout.
pub fn startDefault() Watchdog {
    return Watchdog.start(DEFAULT_TIMEOUT_SECS);
}

test "watchdog does not trigger for fast tests" {
    var wd = Watchdog.start(1);
    defer wd.cancel();
    // This test completes immediately, watchdog won't trigger
}
