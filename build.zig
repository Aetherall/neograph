const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Profiling option: -Dprofile=true enables compile-time profiling instrumentation
    const profile_enabled = b.option(bool, "profile", "Enable profiling instrumentation") orelse false;

    // Create profile options module that profiling.zig imports
    const profile_options = b.addOptions();
    profile_options.addOption(bool, "enabled", profile_enabled);

    // Main library
    const lib = b.addLibrary(.{
        .name = "neograph",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/neograph.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    lib.root_module.addImport("profile_options", profile_options.createModule());
    b.installArtifact(lib);

    // Lua module for Neovim integration
    const zlua_dep = b.dependency("zlua", .{
        .target = target,
        .optimize = optimize,
        .lang = .luajit,
    });

    const lua_module = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "neograph_lua",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/lua/init.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zlua", .module = zlua_dep.module("zlua") },
                .{ .name = "profile_options", .module = profile_options.createModule() },
            },
        }),
    });

    // Add neograph as import to lua module
    lua_module.root_module.addImport("neograph", lib.root_module);

    // Install to lua/ directory for Neovim plugin compatibility
    // Plugin managers add lua/ to package.cpath automatically
    const install_lua = b.addInstallArtifact(lua_module, .{
        .dest_dir = .{ .override = .{ .custom = "../lua" } },
        .dest_sub_path = "neograph_lua.so",
    });
    b.getInstallStep().dependOn(&install_lua.step);

    const lua_step = b.step("lua", "Build Lua module for Neovim");
    lua_step.dependOn(&install_lua.step);

    // Helper to create test module with profiling support
    const createTestModule = struct {
        fn create(
            builder: *std.Build,
            source: std.Build.LazyPath,
            tgt: std.Build.ResolvedTarget,
            opt: std.builtin.OptimizeMode,
            prof_opts: *std.Build.Step.Options,
        ) *std.Build.Module {
            const module = builder.createModule(.{
                .root_source_file = source,
                .target = tgt,
                .optimize = opt,
            });
            module.addImport("profile_options", prof_opts.createModule());
            return module;
        }
    }.create;

    // Unit tests
    const lib_unit_tests = b.addTest(.{
        .root_module = createTestModule(b, b.path("src/neograph.zig"), target, optimize, profile_options),
    });

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    // Integration tests (black-box API tests)
    const integration_tests = b.addTest(.{
        .root_module = createTestModule(b, b.path("src/integration_test.zig"), target, optimize, profile_options),
    });

    const run_integration_tests = b.addRunArtifact(integration_tests);

    // Combined test step
    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_lib_unit_tests.step);
    test_step.dependOn(&run_integration_tests.step);

    // Separate step for just unit tests
    const unit_test_step = b.step("test-unit", "Run unit tests only");
    unit_test_step.dependOn(&run_lib_unit_tests.step);

    // Separate step for just integration tests
    const integration_test_step = b.step("test-integration", "Run integration tests only");
    integration_test_step.dependOn(&run_integration_tests.step);

    // Visual tests (tree rendering, scrolling, profiling)
    const visual_tests = b.addTest(.{
        .root_module = createTestModule(b, b.path("src/visual_test.zig"), target, optimize, profile_options),
    });

    const run_visual_tests = b.addRunArtifact(visual_tests);

    const visual_test_step = b.step("test-visual", "Run visual tests");
    visual_test_step.dependOn(&run_visual_tests.step);

    // DAP visual tests (realistic graph structures)
    const dap_visual_tests = b.addTest(.{
        .root_module = createTestModule(b, b.path("src/dap_visual_test.zig"), target, optimize, profile_options),
    });

    const run_dap_visual_tests = b.addRunArtifact(dap_visual_tests);

    const dap_visual_test_step = b.step("test-dap", "Run DAP graph visual tests");
    dap_visual_test_step.dependOn(&run_dap_visual_tests.step);

    // Add both visual test suites to the visual test step
    // visual_test_step.dependOn(&run_dap_visual_tests.step); // TEMP: disabled while converting

    // DAP end-to-end tests (full stack: schema, store, executor, tracker)
    const dap_e2e_tests = b.addTest(.{
        .root_module = createTestModule(b, b.path("src/dap_e2e_test.zig"), target, optimize, profile_options),
    });

    const run_dap_e2e_tests = b.addRunArtifact(dap_e2e_tests);

    const dap_e2e_test_step = b.step("test-e2e", "Run DAP end-to-end tests");
    dap_e2e_test_step.dependOn(&run_dap_e2e_tests.step);

    // Reactive bugs tests (documents known issues in reactive system)
    const reactive_bugs_tests = b.addTest(.{
        .root_module = createTestModule(b, b.path("src/reactive_bugs_test.zig"), target, optimize, profile_options),
    });

    const run_reactive_bugs_tests = b.addRunArtifact(reactive_bugs_tests);

    const reactive_bugs_test_step = b.step("test-bugs", "Run reactive bugs reproduction tests");
    reactive_bugs_test_step.dependOn(&run_reactive_bugs_tests.step);

    // Cross-entity index tests (acceptance tests for cross-entity index feature)
    const cross_entity_index_tests = b.addTest(.{
        .root_module = createTestModule(b, b.path("src/cross_entity_index_test.zig"), target, optimize, profile_options),
    });

    const run_cross_entity_index_tests = b.addRunArtifact(cross_entity_index_tests);

    const cross_entity_index_test_step = b.step("test-cross-entity", "Run cross-entity index acceptance tests");
    cross_entity_index_test_step.dependOn(&run_cross_entity_index_tests.step);

    // Profile benchmarks - always compiled with profiling enabled and ReleaseFast
    const profile_options_enabled = b.addOptions();
    profile_options_enabled.addOption(bool, "enabled", true);

    const profile_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/profile_test.zig"),
            .target = target,
            .optimize = .ReleaseFast, // Always optimize benchmarks
        }),
    });
    profile_tests.root_module.addImport("profile_options", profile_options_enabled.createModule());

    const run_profile_tests = b.addRunArtifact(profile_tests);

    const profile_test_step = b.step("test-profile", "Run profiling benchmarks");
    profile_test_step.dependOn(&run_profile_tests.step);

    // DAP profile test - reproduces exact demo scenario
    const dap_profile_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dap_profile_test.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    dap_profile_tests.root_module.addImport("profile_options", profile_options_enabled.createModule());

    const run_dap_profile_tests = b.addRunArtifact(dap_profile_tests);

    const dap_profile_test_step = b.step("test-dap-profile", "Run DAP demo profiling");
    dap_profile_test_step.dependOn(&run_dap_profile_tests.step);

    // Add visual tests to the main test step as well
    test_step.dependOn(&run_visual_tests.step);
    test_step.dependOn(&run_dap_visual_tests.step);
    test_step.dependOn(&run_dap_e2e_tests.step);
    test_step.dependOn(&run_reactive_bugs_tests.step);

    // Coverage tests - use LLVM backend for kcov compatibility
    // The self-hosted backend generates DWARF that kcov can't process correctly
    const coverage_option = b.option(bool, "coverage", "Build with LLVM backend for kcov coverage") orelse false;

    if (coverage_option) {
        // Force LLVM backend for all test artifacts when coverage is enabled
        lib_unit_tests.root_module.strip = false;
        lib_unit_tests.linkage = .static;
        integration_tests.root_module.strip = false;
        integration_tests.linkage = .static;
        visual_tests.root_module.strip = false;
        visual_tests.linkage = .static;
        dap_visual_tests.root_module.strip = false;
        dap_visual_tests.linkage = .static;
        dap_e2e_tests.root_module.strip = false;
        dap_e2e_tests.linkage = .static;
    }

    // Coverage step that builds all test binaries with LLVM backend
    const coverage_step = b.step("test-coverage", "Build tests with LLVM backend for kcov coverage");

    // Create coverage-specific test artifacts with LLVM backend forced
    const cov_unit_tests = b.addTest(.{
        .root_module = createTestModule(b, b.path("src/neograph.zig"), target, optimize, profile_options),
    });
    cov_unit_tests.root_module.strip = false;
    cov_unit_tests.use_llvm = true;
    cov_unit_tests.use_lld = true;

    const cov_integration_tests = b.addTest(.{
        .root_module = createTestModule(b, b.path("src/integration_test.zig"), target, optimize, profile_options),
    });
    cov_integration_tests.root_module.strip = false;
    cov_integration_tests.use_llvm = true;
    cov_integration_tests.use_lld = true;

    const cov_e2e_tests = b.addTest(.{
        .root_module = createTestModule(b, b.path("src/dap_e2e_test.zig"), target, optimize, profile_options),
    });
    cov_e2e_tests.root_module.strip = false;
    cov_e2e_tests.use_llvm = true;
    cov_e2e_tests.use_lld = true;

    const cov_visual_tests = b.addTest(.{
        .root_module = createTestModule(b, b.path("src/visual_test.zig"), target, optimize, profile_options),
    });
    cov_visual_tests.root_module.strip = false;
    cov_visual_tests.use_llvm = true;
    cov_visual_tests.use_lld = true;

    // Install coverage binaries for kcov to run
    const install_cov_unit = b.addInstallArtifact(cov_unit_tests, .{
        .dest_dir = .{ .override = .{ .custom = "coverage" } },
        .dest_sub_path = "unit_test",
    });
    const install_cov_integration = b.addInstallArtifact(cov_integration_tests, .{
        .dest_dir = .{ .override = .{ .custom = "coverage" } },
        .dest_sub_path = "integration_test",
    });
    const install_cov_e2e = b.addInstallArtifact(cov_e2e_tests, .{
        .dest_dir = .{ .override = .{ .custom = "coverage" } },
        .dest_sub_path = "e2e_test",
    });
    const install_cov_visual = b.addInstallArtifact(cov_visual_tests, .{
        .dest_dir = .{ .override = .{ .custom = "coverage" } },
        .dest_sub_path = "visual_test",
    });

    coverage_step.dependOn(&install_cov_unit.step);
    coverage_step.dependOn(&install_cov_integration.step);
    coverage_step.dependOn(&install_cov_e2e.step);
    coverage_step.dependOn(&install_cov_visual.step);
}
