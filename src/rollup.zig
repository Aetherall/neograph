///! Rollup module for computed fields.

pub const cache = @import("rollup/cache.zig");
pub const RollupCache = cache.RollupCache;

pub const inverted_index = @import("rollup/inverted_index.zig");
pub const InvertedEdgeIndex = inverted_index.InvertedEdgeIndex;
pub const EdgeRef = inverted_index.EdgeRef;

test {
    @import("std").testing.refAllDecls(@This());
}
