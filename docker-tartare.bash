#!/usr/bin/env bash
set -euo pipefail

if ((BASH_VERSINFO[0] < 3)); then
  echo "error: docker-tartare.bash requires Bash 3+" >&2
  exit 1
fi

ROOT_OPAQUE_MARKER="__docker_tartare_root_opaque__"

die() {
  echo "error: $*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage:
  docker-tartare.bash list <save_tar> [path] [--dirs]
  docker-tartare.bash extract <save_tar> <image_path> <output> [--dir]

Notes:
  extract prints the exported output tree to stderr.
EOF
}

require_deps() {
  local dep
  for dep in tar jq mktemp sort find; do
    command -v "$dep" >/dev/null 2>&1 || die "Missing required command: $dep"
  done
}

norm_image_path() {
  local p="$1"
  p="${p#/}"
  printf '%s' "$p"
}

opaque_whiteout_name() {
  local dirpath="$1"
  dirpath="${dirpath%/}"
  if [[ -n "$dirpath" ]]; then
    printf '%s/.wh..wh..opq' "$dirpath"
  else
    printf '.wh..wh..opq'
  fi
}

sanitize_member_name() {
  local raw="$1"
  local name="$raw"
  while [[ "$name" == ./* ]]; do
    name="${name#./}"
  done
  name="${name%/}"
  printf '%s' "$name"
}

read_layers() {
  local save_tar="$1"
  local line
  LAYERS=()
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    LAYERS[${#LAYERS[@]}]="$line"
  done < <(tar -xOf "$save_tar" manifest.json | jq -r '.[0].Layers[]')
  ((${#LAYERS[@]} > 0)) || die "No layers found in manifest.json"
}

set_add() {
  local set_file="$1"
  local key="$2"
  printf '%s\n' "$key" >>"$set_file"
}

set_has() {
  local set_file="$1"
  local key="$2"
  [[ -s "$set_file" ]] || return 1
  grep -Fxq -- "$key" "$set_file"
}

merge_set_file() {
  local dest_file="$1"
  local src_file="$2"
  [[ -s "$src_file" ]] || return 0
  cat "$src_file" >>"$dest_file"
}

deleted_path_matches() {
  local path="$1"
  local deleted_file="$2"
  local candidate="$path"

  while [[ -n "$candidate" ]]; do
    if set_has "$deleted_file" "$candidate"; then
      return 0
    fi
    [[ "$candidate" == */* ]] || break
    candidate="${candidate%/*}"
  done
  return 1
}

is_under_opaque() {
  local path="$1"
  local opaque_file="$2"
  local rest="$path"
  local part acc=""

  if set_has "$opaque_file" "$ROOT_OPAQUE_MARKER"; then
    return 0
  fi

  while [[ "$rest" == */* ]]; do
    part="${rest%%/*}"
    if [[ -z "$acc" ]]; then
      acc="$part"
    else
      acc="$acc/$part"
    fi
    if set_has "$opaque_file" "$acc"; then
      return 0
    fi
    rest="${rest#*/}"
  done
  return 1
}

tree_prefix() {
  local depth="$1"
  local prefix=""
  local i

  for ((i = 1; i < depth; i++)); do
    prefix="${prefix}|   "
  done

  printf '%s' "$prefix"
}

record_seen_entry() {
  local seen_all_file="$1"
  local seen_meta_file="$2"
  local name="$3"
  local kind="$4"

  [[ -n "$name" ]] || return 0
  if ! set_has "$seen_all_file" "$name"; then
    set_add "$seen_all_file" "$name"
    printf '%s\t%s\n' "$name" "$kind" >>"$seen_meta_file"
  fi
}

record_parent_dirs() {
  local seen_all_file="$1"
  local seen_meta_file="$2"
  local name="$3"
  local scope_root="${4:-}"
  local parent="$name"

  while [[ "$parent" == */* ]]; do
    parent="${parent%/*}"
    if [[ -n "$scope_root" && "$parent" != "$scope_root" && "$parent" != "$scope_root/"* ]]; then
      continue
    fi
    record_seen_entry "$seen_all_file" "$seen_meta_file" "$parent" "dir"
  done
}

mark_seen_path_and_parents() {
  local seen_file="$1"
  local name="$2"
  local scope_root="${3:-}"
  local parent="$name"

  while [[ -n "$parent" ]]; do
    if [[ -n "$scope_root" && "$parent" != "$scope_root" && "$parent" != "$scope_root/"* ]]; then
      break
    fi
    if ! set_has "$seen_file" "$parent"; then
      set_add "$seen_file" "$parent"
    fi
    [[ "$parent" == */* ]] || break
    parent="${parent%/*}"
  done
}

log_export_tree() {
  local target="$1"
  local entry rel slash_marks depth prefix found root_name

  if [[ ! -e "$target" && ! -L "$target" ]]; then
    printf 'exported tree: %s (missing after extraction)\n' "$target" >&2
    return
  fi

  root_name="$(basename "$target")"
  printf 'exported tree: %s\n' "$target" >&2

  if [[ -d "$target" && ! -L "$target" ]]; then
    printf '%s/\n' "$root_name" >&2
    found=0
    while IFS= read -r entry; do
      found=1
      rel="${entry#$target/}"
      slash_marks="${rel//[^\/]/}"
      depth=$(( ${#slash_marks} + 1 ))
      prefix="$(tree_prefix "$depth")"
      if [[ -d "$entry" && ! -L "$entry" ]]; then
        printf '%s|-- %s/\n' "$prefix" "$(basename "$entry")" >&2
      else
        printf '%s|-- %s\n' "$prefix" "$(basename "$entry")" >&2
      fi
    done < <(find "$target" -mindepth 1 -print | LC_ALL=C sort)

    if ((found == 0)); then
      printf '`-- [empty]\n' >&2
    fi
    return
  fi

  printf '`-- %s\n' "$root_name" >&2
}

extract_layer_to_dir() {
  local layer_tar="$1"
  local out_dir="$2"

  tar -xmf "$layer_tar" -C "$out_dir"
}

copy_extracted_path() {
  local extracted_root="$1"
  local member="$2"
  local out_path="$3"
  local src

  src="$extracted_root/${member#./}"
  src="${src%/}"

  if [[ ! -e "$src" && ! -L "$src" ]]; then
    return 1
  fi

  if [[ -d "$src" && ! -L "$src" ]]; then
    mkdir -p "$out_path"
    return 0
  fi

  mkdir -p "$(dirname "$out_path")"
  if [[ -e "$out_path" || -L "$out_path" ]]; then
    rm -rf "$out_path"
  fi
  cp -a "$src" "$out_path"
}

collect_layer_whiteouts() {
  local layer_list="$1"
  local prefix="${2:-}"
  local image_path="${3:-}"
  local deleted_file="$4"
  local opaque_file="$5"
  local raw name base real parent deleted_target key target_dir

  while IFS= read -r raw; do
    [[ -n "$raw" ]] || continue
    name="$(sanitize_member_name "$raw")"
    [[ -z "$name" || "$name" == "." ]] && continue

    if [[ -n "$prefix" && "$name" != "$image_path" && "$name" != "$prefix"* ]]; then
      continue
    fi

    base="${name##*/}"
    if [[ "$base" != .wh.* ]]; then
      continue
    fi

    real="${base#.wh.}"
    if [[ "$name" == */* ]]; then
      parent="${name%/*}"
    else
      parent=""
    fi

    if [[ "$real" == ".wh..opq" || "$base" == ".wh..wh..opq" ]]; then
      target_dir="$parent"
      key="$target_dir"
      if [[ -z "$key" ]]; then
        key="$ROOT_OPAQUE_MARKER"
      fi
      set_add "$opaque_file" "$key"
      continue
    fi

    if [[ -n "$parent" ]]; then
      deleted_target="$parent/$real"
    else
      deleted_target="$real"
    fi
    set_add "$deleted_file" "$deleted_target"
  done <"$layer_list"
}

cmd_list() {
  local save_tar="$1"
  local image_path="${2:-}"
  local dirs_only="${3:-0}"
  local prefix=""
  local layer_path raw name base kind
  local i is_dir
  local tmp_state deleted_file opaque_file seen_all_file seen_meta_file tab
  local layer_list layer_deleted_file layer_opaque_file

  if [[ -n "$image_path" ]]; then
    image_path="$(norm_image_path "$image_path")"
    image_path="${image_path%/}"
    if [[ -n "$image_path" ]]; then
      prefix="$image_path/"
    fi
  fi

  tmp_state="$(mktemp -d)"
  deleted_file="$tmp_state/deleted"
  opaque_file="$tmp_state/opaque"
  seen_all_file="$tmp_state/seen_all"
  seen_meta_file="$tmp_state/seen_meta"
  touch "$deleted_file" "$opaque_file" "$seen_all_file" "$seen_meta_file"

  read_layers "$save_tar"

  for ((i = ${#LAYERS[@]} - 1; i >= 0; i--)); do
    layer_path="${LAYERS[i]}"
    layer_list="$(mktemp)"
    layer_deleted_file="$tmp_state/layer_deleted_$i"
    layer_opaque_file="$tmp_state/layer_opaque_$i"
    touch "$layer_deleted_file" "$layer_opaque_file"

    tar -xOf "$save_tar" "$layer_path" | tar -tf - >"$layer_list"
    collect_layer_whiteouts "$layer_list" "$prefix" "$image_path" "$layer_deleted_file" "$layer_opaque_file"

    while IFS= read -r raw; do
      [[ -n "$raw" ]] || continue
      is_dir=0
      [[ "$raw" == */ ]] && is_dir=1

      name="$(sanitize_member_name "$raw")"
      [[ -z "$name" || "$name" == "." ]] && continue

      if [[ -n "$prefix" && "$name" != "$image_path" && "$name" != "$prefix"* ]]; then
        continue
      fi

      base="${name##*/}"
      if [[ "$base" == .wh.* ]]; then
        continue
      fi

      if deleted_path_matches "$name" "$deleted_file"; then
        continue
      fi

      if is_under_opaque "$name" "$opaque_file"; then
        continue
      fi

      if ((is_dir)); then
        kind="dir"
      else
        kind="file"
      fi
      record_seen_entry "$seen_all_file" "$seen_meta_file" "$name" "$kind"
      record_parent_dirs "$seen_all_file" "$seen_meta_file" "$name" "$image_path"
    done <"$layer_list"

    merge_set_file "$deleted_file" "$layer_deleted_file"
    merge_set_file "$opaque_file" "$layer_opaque_file"
    rm -f "$layer_list" "$layer_deleted_file" "$layer_opaque_file"
  done

  if [[ ! -s "$seen_meta_file" ]]; then
    rm -rf "$tmp_state"
    return 0
  fi

  tab="$(printf '\t')"
  LC_ALL=C sort -t "$tab" -k1,1 "$seen_meta_file" | while IFS=$'\t' read -r name kind; do
    if [[ "$dirs_only" == "1" && "$kind" != "dir" ]]; then
      continue
    fi
    if [[ "$kind" == "dir" ]]; then
      printf '/%s/\n' "$name"
    else
      printf '/%s\n' "$name"
    fi
  done

  rm -rf "$tmp_state"
}

cmd_extract_file() {
  local save_tar="$1"
  local image_path="$2"
  local out_path="$3"
  local layer_path raw name base
  local i found_raw tmp_layer tmp_root
  local tmp_state deleted_file opaque_file layer_list layer_deleted_file layer_opaque_file

  image_path="$(norm_image_path "$image_path")"

  tmp_state="$(mktemp -d)"
  deleted_file="$tmp_state/deleted"
  opaque_file="$tmp_state/opaque"
  touch "$deleted_file" "$opaque_file"

  read_layers "$save_tar"

  for ((i = ${#LAYERS[@]} - 1; i >= 0; i--)); do
    if deleted_path_matches "$image_path" "$deleted_file" || is_under_opaque "$image_path" "$opaque_file"; then
      rm -rf "$tmp_state"
      die "Deleted by whiteout in a higher layer"
    fi

    layer_path="${LAYERS[i]}"
    tmp_layer="$(mktemp)"
    layer_list="$(mktemp)"
    layer_deleted_file="$tmp_state/layer_deleted_$i"
    layer_opaque_file="$tmp_state/layer_opaque_$i"
    touch "$layer_deleted_file" "$layer_opaque_file"

    tar -xOf "$save_tar" "$layer_path" >"$tmp_layer"
    tar -tf "$tmp_layer" >"$layer_list"

    collect_layer_whiteouts "$layer_list" "" "" "$layer_deleted_file" "$layer_opaque_file"

    found_raw=""
    while IFS= read -r raw; do
      name="$(sanitize_member_name "$raw")"
      [[ -z "$name" || "$name" == "." ]] && continue
      base="${name##*/}"
      [[ "$base" == .wh.* ]] && continue
      if [[ "$name" == "$image_path" && -z "$found_raw" ]]; then
        found_raw="$raw"
      fi
    done <"$layer_list"

    if [[ -n "$found_raw" ]]; then
      tmp_root="$(mktemp -d)"
      extract_layer_to_dir "$tmp_layer" "$tmp_root" || {
        rm -f "$tmp_layer" "$layer_list" "$layer_deleted_file" "$layer_opaque_file"
        rm -rf "$tmp_root" "$tmp_state"
        die "Failed to unpack layer $layer_path"
      }
      copy_extracted_path "$tmp_root" "$found_raw" "$out_path" || {
        rm -f "$tmp_layer" "$layer_list" "$layer_deleted_file" "$layer_opaque_file"
        rm -rf "$tmp_root" "$tmp_state"
        die "Failed to extract $image_path from layer $layer_path"
      }
      rm -f "$tmp_layer" "$layer_list" "$layer_deleted_file" "$layer_opaque_file"
      rm -rf "$tmp_root" "$tmp_state"
      log_export_tree "$out_path"
      return 0
    fi

    merge_set_file "$deleted_file" "$layer_deleted_file"
    merge_set_file "$opaque_file" "$layer_opaque_file"
    if deleted_path_matches "$image_path" "$deleted_file" || is_under_opaque "$image_path" "$opaque_file"; then
      rm -f "$tmp_layer" "$layer_list" "$layer_deleted_file" "$layer_opaque_file"
      rm -rf "$tmp_state"
      die "Deleted by whiteout in layer $layer_path"
    fi

    rm -f "$tmp_layer" "$layer_list" "$layer_deleted_file" "$layer_opaque_file"
  done

  rm -rf "$tmp_state"
  die "Not found in any layer: $image_path"
}

cmd_extract_dir() {
  local save_tar="$1"
  local image_dir="$2"
  local out_dir="$3"
  local prefix opq layer_path tmp_layer raw name base
  local rel dest i
  local tmp_state deleted_file opaque_file seen_file
  local layer_list layer_deleted_file layer_opaque_file tmp_root

  image_dir="$(norm_image_path "$image_dir")"
  image_dir="${image_dir%/}"
  if [[ -n "$image_dir" ]]; then
    prefix="$image_dir/"
  else
    prefix=""
  fi
  opq="$(opaque_whiteout_name "$image_dir")"

  mkdir -p "$out_dir"

  tmp_state="$(mktemp -d)"
  deleted_file="$tmp_state/deleted"
  opaque_file="$tmp_state/opaque"
  seen_file="$tmp_state/seen"
  touch "$deleted_file" "$opaque_file" "$seen_file"

  read_layers "$save_tar"

  for ((i = ${#LAYERS[@]} - 1; i >= 0; i--)); do
    layer_path="${LAYERS[i]}"
    tmp_layer="$(mktemp)"
    layer_list="$(mktemp)"
    layer_deleted_file="$tmp_state/layer_deleted_$i"
    layer_opaque_file="$tmp_state/layer_opaque_$i"
    touch "$layer_deleted_file" "$layer_opaque_file"

    tar -xOf "$save_tar" "$layer_path" >"$tmp_layer"
    tar -tf "$tmp_layer" >"$layer_list"
    collect_layer_whiteouts "$layer_list" "$prefix" "$image_dir" "$layer_deleted_file" "$layer_opaque_file"

    tmp_root="$(mktemp -d)"
    extract_layer_to_dir "$tmp_layer" "$tmp_root" || {
      rm -f "$tmp_layer" "$layer_list" "$layer_deleted_file" "$layer_opaque_file"
      rm -rf "$tmp_root" "$tmp_state"
      die "Failed to unpack layer $layer_path"
    }

    while IFS= read -r raw; do
      name="$(sanitize_member_name "$raw")"
      [[ -z "$name" || "$name" == "." ]] && continue

      if [[ -n "$prefix" && "$name" != "$image_dir" && "$name" != "$prefix"* ]]; then
        continue
      fi

      if [[ "$name" == "$opq" ]]; then
        continue
      fi

      base="${name##*/}"
      if [[ "$base" == .wh.* ]]; then
        continue
      fi

      if deleted_path_matches "$name" "$deleted_file"; then
        continue
      fi

      if is_under_opaque "$name" "$opaque_file"; then
        continue
      fi

      if set_has "$seen_file" "$name"; then
        continue
      fi

      if [[ -n "$prefix" ]]; then
        if [[ "$name" == "$image_dir" ]]; then
          rel=""
        else
          rel="${name#"$prefix"}"
        fi
      else
        rel="$name"
      fi

      if [[ -n "$rel" ]]; then
        dest="$out_dir/$rel"
      else
        dest="$out_dir"
      fi

      copy_extracted_path "$tmp_root" "$raw" "$dest" || {
        rm -f "$tmp_layer" "$layer_list" "$layer_deleted_file" "$layer_opaque_file"
        rm -rf "$tmp_root" "$tmp_state"
        die "Failed to extract member $name from layer $layer_path"
      }
      mark_seen_path_and_parents "$seen_file" "$name" "$image_dir"
    done <"$layer_list"

    merge_set_file "$deleted_file" "$layer_deleted_file"
    merge_set_file "$opaque_file" "$layer_opaque_file"
    rm -f "$tmp_layer" "$layer_list" "$layer_deleted_file" "$layer_opaque_file"
    rm -rf "$tmp_root"
  done

  rm -rf "$tmp_state"
  log_export_tree "$out_dir"
}

main() {
  require_deps

  (($# >= 1)) || {
    usage
    exit 1
  }

  local cmd="$1"
  shift

  case "$cmd" in
  list)
    local dirs_only=0
    local -a pos=()
    local arg
    for arg in "$@"; do
      if [[ "$arg" == "--dirs" ]]; then
        dirs_only=1
      else
        pos+=("$arg")
      fi
    done
    ((${#pos[@]} >= 1 && ${#pos[@]} <= 2)) || die "list expects: <save_tar> [path] [--dirs]"
    cmd_list "${pos[0]}" "${pos[1]:-}" "$dirs_only"
    ;;
  extract)
    local dir_mode=0
    local -a pos=()
    local arg
    for arg in "$@"; do
      if [[ "$arg" == "--dir" ]]; then
        dir_mode=1
      else
        pos+=("$arg")
      fi
    done
    ((${#pos[@]} == 3)) || die "extract expects: <save_tar> <image_path> <output> [--dir]"
    if ((dir_mode)); then
      cmd_extract_dir "${pos[0]}" "${pos[1]}" "${pos[2]}"
    else
      cmd_extract_file "${pos[0]}" "${pos[1]}" "${pos[2]}"
    fi
    ;;
  *)
    usage
    exit 1
    ;;
  esac
}

main "$@"
