#!/usr/bin/env bash

check_version() {
  name="$1"
  cmd="$2"
  shift 2

  if command -v "$cmd" >/dev/null 2>&1; then
    path="$(command -v "$cmd")"
    version="$("$cmd" "$@" 2>&1 | head -n 1)"
    printf "%-10s %-35s %s\n" "$name" "$version" "$path"
  else
    printf "%-10s %-35s %s\n" "$name" "未安装或不在 PATH 中" ""
  fi
}

printf "%-10s %-35s %s\n" "Tool" "Version" "Path"
printf "%-10s %-35s %s\n" "----" "-------" "----"

check_version "Python" "python" "--version"
check_version "uv" "uv" "--version"
check_version "Node" "node" "--version"
check_version "npm" "npm" "--version"
check_version "pnpm" "pnpm" "--version"
check_version "nginx" "nginx" "-v"
