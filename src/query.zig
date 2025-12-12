///! Query module for building and executing queries.

pub const builder = @import("query/builder.zig");
pub const Query = builder.Query;
pub const Filter = builder.Filter;
pub const FilterOp = builder.FilterOp;
pub const Sort = builder.Sort;
pub const EdgeSelection = builder.EdgeSelection;
pub const QueryBuilder = builder.QueryBuilder;
pub const QueryInput = builder.QueryInput;
pub const EdgeInput = builder.EdgeInput;
pub const FilterInput = builder.FilterInput;
pub const BuildError = builder.BuildError;

pub const executor = @import("query/executor.zig");
pub const Executor = executor.Executor;
pub const Item = executor.Item;
pub const EdgeResult = executor.EdgeResult;
pub const Path = executor.Path;
pub const PathSegment = executor.PathSegment;

pub const validator = @import("query/validator.zig");
pub const validate = validator.validate;
pub const ValidationError = validator.ValidationError;
pub const ValidationResult = validator.ValidationResult;

test {
    @import("std").testing.refAllDecls(@This());
}
