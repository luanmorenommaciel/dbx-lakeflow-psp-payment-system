#!/usr/bin/env bash

_workshop_java_candidate() {
  local candidate="${1:-}"
  [[ -x "$candidate/bin/java" ]] || return 1
  local raw_version
  local major_version
  raw_version="$("$candidate/bin/java" -version 2>&1 | awk -F'"' 'NR == 1 { print $2 }')"
  major_version="${raw_version%%.*}"
  if [[ "$major_version" == "17" || "$major_version" == "21" ]]; then
    printf '%s\n' "$candidate"
    return 0
  fi
  return 1
}

resolve_workshop_java_home() {
  local candidate
  if candidate="$(_workshop_java_candidate "${DBX_WORKSHOP_JAVA_HOME:-}")"; then
    printf '%s\n' "$candidate"
    return 0
  fi

  if command -v brew >/dev/null 2>&1; then
    for formula in openjdk@21 openjdk@17; do
      candidate="$(brew --prefix "$formula" 2>/dev/null || true)"
      candidate="$candidate/libexec/openjdk.jdk/Contents/Home"
      if candidate="$(_workshop_java_candidate "$candidate")"; then
        printf '%s\n' "$candidate"
        return 0
      fi
    done
  fi

  if [[ -x /usr/libexec/java_home ]]; then
    for java_version in 21 17; do
      candidate="$(/usr/libexec/java_home -v "$java_version" 2>/dev/null || true)"
      if candidate="$(_workshop_java_candidate "$candidate")"; then
        printf '%s\n' "$candidate"
        return 0
      fi
    done
  fi

  if [[ -n "${JAVA_HOME:-}" ]] && candidate="$(_workshop_java_candidate "$JAVA_HOME")"; then
    printf '%s\n' "$candidate"
    return 0
  fi
  return 1
}
