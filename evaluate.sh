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
  for mode in cpu; do
    for scale in 1 10; do
      for query in q2 q4 q8 q9 q10 q11; do
        log="${log_dir}/$mode/sf_${scale}/${query}.log"
        echo "${log}" | xargs dirname | xargs mkdir -p
        if [[ -f "$log" ]]; then
          echo "Skipping, $log exists"
        else
          echo python3 ${query}.py -d $DATASET_ROOT/sf${scale} $mode
          python3 ${query}.py -d $DATASET_ROOT/sf${scale} $mode |& tee "${log}.tmp"
          if grep -q "execution time"; then
            mv "${log}.tmp" "${log}"
          fi
        fi
      done
    done
  done
}

end2end
