#!/usr/bin/env bash

# Function to resolve the script path
get_script_dir() {
  local source="${BASH_SOURCE[0]}"
  while [ -h "$source" ]; do
    local dir
    dir=$(dirname "$source")
    source=$(readlink "$source")
    [[ $source != /* ]] && source="$dir/$source"
  done
  echo "$(cd -P "$(dirname "$source")" >/dev/null 2>&1 && pwd)"
}
script_dir=$(get_script_dir)

source "${script_dir}/common.sh"

log_dir="${script_dir}/logs"

function end2end() {
  gpu_partitions=(2 $(nproc) $(nproc) 2)
  queries=(q2 q4 q8 q10)
  for mode in gpu; do
    for scale in 1 10; do
       for i in "${!queries[@]}"; do
        query="${queries[i]}"
        log="${log_dir}/$mode/sf_${scale}/${query}.log"

        echo "${log}" | xargs dirname | xargs mkdir -p

        if [[ -f "$log" ]]; then
          echo "Skipping, $log exists"
        else
          if [[ $mode == "gpu" && $scale -eq 10 ]]; then
            partition="${gpu_partitions[i]}"
          else
            partition=$(nproc)
          fi
          echo python3 ${query}.py -d "$DATASET_ROOT/sf${scale}" -p $partition $mode
          python3 ${query}.py -d "$DATASET_ROOT/sf${scale}" -p $partition $mode -r 5 |& tee "${log}.tmp"
          if grep -q "execution time" "${log}.tmp"; then
            mv "${log}.tmp" "${log}"
          fi
        fi
      done
    done
  done
}

end2end
