import os
import re
import sys


def extract_execution_times(target_directory):
    data = {}
    time_pattern = re.compile(r"Avg execution time.*?: (\d+\.?\d*)s")

    # Walk through the directory tree
    for root, dirs, files in os.walk(target_directory):
        for file in files:
            if file.endswith(".log"):
                file_path = os.path.join(root, file)
                abs_path = os.path.abspath(file_path)
                parts = abs_path.split(os.sep)

                # Check path depth (logs/mode/sf/query.log)
                if len(parts) < 3:
                    continue

                try:
                    query_name = parts[-1].replace('.log', '')
                    scale_factor = parts[-2]
                    mode = parts[-3]

                    with open(file_path, 'r') as f:
                        content = f.read()
                        match = time_pattern.search(content)

                        if match:
                            time_val = float(match.group(1))

                            if mode not in data: data[mode] = {}
                            if scale_factor not in data[mode]: data[mode][scale_factor] = {}

                            data[mode][scale_factor][query_name] = time_val

                except Exception as e:
                    # Print errors to stderr so they don't mess up the markdown output
                    print(f"Error reading {file_path}: {e}", file=sys.stderr)

    return data


def natural_sort_key(item):
    """Sorts q2 before q10 properly"""
    query_name = item[0]
    numbers = re.findall(r'\d+', query_name)
    if numbers:
        return int(numbers[0])
    return 0


def print_markdown(data):
    if not data:
        print("**No data found.**")
        return

    # 1. Print Markdown Header
    print("| Mode | Scale Factor | Query | Time (s) |")
    print("| :--- | :--- | :--- | :--- |")

    # 2. Print Rows
    # Sort Modes (cpu, gpu)
    for mode in sorted(data.keys()):
        sfs = data[mode]
        # Sort Scale Factors (sf_1, sf_10)
        for sf in sorted(sfs.keys()):
            queries = sfs[sf]

            # Sort Queries Naturally (q2, q4... q9, q10)
            sorted_queries = sorted(queries.items(), key=natural_sort_key)

            for q, time in sorted_queries:
                # Print table row
                print(f"| {mode} | {sf} | {q} | {time} |")


# --- Main Execution ---
if __name__ == "__main__":
    # Default to "logs" if no argument provided
    input_path = sys.argv[1] if len(sys.argv) > 1 else "logs"

    if os.path.exists(input_path):
        results = extract_execution_times(input_path)
        print_markdown(results)
    else:
        print(f"Error: Path '{input_path}' not found.", file=sys.stderr)