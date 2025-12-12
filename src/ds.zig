///! Generic data structures for neographzig.
///!
///! This module provides reusable, comptime-generic data structures
///! that form the foundation of the reactive graph database.
///!
///! Available structures:
///! - IntrusiveList: O(1) doubly-linked list operations with range support
///! - SortedSet: Sorted array with binary search
///! - GroupedMap: Values grouped by key (e.g., by TypeId)

pub const intrusive_list = @import("ds/intrusive_list.zig");
pub const IntrusiveList = intrusive_list.IntrusiveList;

pub const sorted_set = @import("ds/sorted_set.zig");
pub const SortedSet = sorted_set.SortedSet;
pub const SortedSetContext = sorted_set.SortedSetContext;

pub const type_grouped_map = @import("ds/type_grouped_map.zig");
pub const GroupedMap = type_grouped_map.GroupedMap;
pub const TypeGroupedMap = type_grouped_map.TypeGroupedMap;

test {
    // Run all tests in submodules
    @import("std").testing.refAllDecls(@This());
}
