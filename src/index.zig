///! Index module for query optimization.
///!
///! This module provides B+ tree indexes with compound key encoding
///! for efficient query execution.

pub const btree = @import("index/btree.zig");
pub const BPlusTree = btree.BPlusTree;

pub const key = @import("index/key.zig");
pub const CompoundKey = key.CompoundKey;

pub const index = @import("index/index.zig");
pub const Index = index.Index;
pub const IndexManager = index.IndexManager;
pub const IndexCoverage = index.IndexCoverage;
pub const Filter = index.Filter;
pub const FilterOp = index.FilterOp;
pub const Sort = index.Sort;
pub const ScanIterator = index.ScanIterator;

test {
    @import("std").testing.refAllDecls(@This());
}
