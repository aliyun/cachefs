import re
import sys
import os
from collections import defaultdict

coverage_file = "merged-coverage.out"
if len(sys.argv) > 1:
    coverage_file = sys.argv[1]

# Read module path from go.mod to strip it from coverage output paths
def read_module_prefix():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    go_mod = os.path.join(script_dir, "..", "..", "go.mod")
    try:
        with open(go_mod, "r") as f:
            for line in f:
                if line.startswith("module "):
                    return line.split()[1].strip() + "/"
    except FileNotFoundError:
        pass
    return ""

module_prefix = read_module_prefix()

pattern = re.compile(r"^(.*/)?([^/:]+\.go):\d+\.\d+,\d+\.\d+ (\d+) (\d+)")

global_coverage = defaultdict(int)

try:
    with open(coverage_file, "r") as f:
        for line in f:
            line = line.strip()
            if line.startswith("mode:"):
                continue
            match = pattern.match(line)
            if not match:
                continue
            _, filename, stmts_str, count_str = match.groups()
            if ".pb." in filename:
                continue
            stmts = int(stmts_str)
            count = int(count_str)
            # Re-extract full path from the line and strip module prefix
            full_match = re.match(r"^([^:]+\.go):", line)
            full_path = full_match.group(1) if full_match else filename
            if module_prefix and full_path.startswith(module_prefix):
                full_path = full_path[len(module_prefix):]
            global_coverage[full_path + "_covered"] += stmts if count > 0 else 0
            global_coverage[full_path + "_total"] += stmts
except FileNotFoundError:
    print(f"Error: File '{coverage_file}' not found.", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"Error reading file: {e}", file=sys.stderr)
    sys.exit(1)


def print_global_coverage(data):
    print("\nGroup: Global Coverage (All Included Files)")
    total_covered = 0
    total_stmts = 0
    file_coverage_list = []

    processed_files = set(k.rsplit("_", 1)[0] for k in data.keys())

    for fname in processed_files:
        covered = data.get(fname + "_covered", 0)
        total = data.get(fname + "_total", 0)
        coverage = (covered / total) if total else 0
        total_covered += covered
        total_stmts += total
        file_coverage_list.append((fname, coverage, covered, total))

    sorted_files = sorted(file_coverage_list, key=lambda x: x[1], reverse=True)

    max_len = max((len(f) for f, _, _, _ in sorted_files), default=0)

    for fname, coverage, covered, total in sorted_files:
        print(f"  {fname.ljust(max_len)}  {coverage:.2%} ({covered}/{total})")

    print("\n--- Summary ---")
    if total_stmts > 0:
        avg = total_covered / total_stmts
        print(f"  ➤ Weighted Avg Coverage: {avg:.2%}")
        print(f"  Total Covered Statements: {total_covered}")
        print(f"  Total Statements: {total_stmts}")
    else:
        print("  ⚠️  No coverage data found for the included files.")


print_global_coverage(global_coverage)
