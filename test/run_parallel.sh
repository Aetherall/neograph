#!/usr/bin/env bash
# Run the bug reproduction test N times in parallel

N=${1:-10}
TMPDIR=$(mktemp -d)

echo "Running test $N times in parallel..."

# Run tests in parallel and write results to temp files
for i in $(seq 1 $N); do
    (
        result=$(timeout 20 nvim --headless -u test/bug_link_expanded_edge.lua 2>&1)
        if echo "$result" | grep -q "=== PASS ==="; then
            echo "PASS" > "$TMPDIR/$i"
        elif echo "$result" | grep -q "=== BUG REPRODUCED ==="; then
            echo "FAIL" > "$TMPDIR/$i"
        else
            echo "ERROR" > "$TMPDIR/$i"
        fi
    ) &
done

wait

# Count results
PASS=$(grep -l "PASS" "$TMPDIR"/* 2>/dev/null | wc -l)
FAIL=$(grep -l "FAIL" "$TMPDIR"/* 2>/dev/null | wc -l)
ERROR=$(grep -l "ERROR" "$TMPDIR"/* 2>/dev/null | wc -l)

rm -rf "$TMPDIR"

echo "PASS: $PASS  FAIL: $FAIL  ERROR: $ERROR  ($(( FAIL * 100 / N ))% failure rate)"
