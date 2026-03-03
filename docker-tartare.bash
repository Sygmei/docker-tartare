#!/usr/bin/env bash
set -euo pipefail

if ((BASH_VERSINFO[0] < 3)); then
  echo "error: docker-tartare.bash requires Bash 3+" >&2
  exit 1
fi

die() {
  echo "error: $*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage:
  docker-tartare.bash list <save_tar> [path] [--dirs]
  docker-tartare.bash extract <save_tar> <image_path> <output> [--dir]
EOF
}

require_deps() {
  local dep
  for dep in tar jq mktemp sort; do
    command -v "$dep" >/dev/null 2>&1 || die "Missing required command: $dep"
  done
}

norm_image_path() {
  local p="$1"
  p="${p#/}"
  printf '%s' "$p"
}

whiteout_name() {
  local target="$1"
  local d b
  d="$(dirname "$target")"
  b="$(basename "$target")"
  if [[ "$d" == "." ]]; then
    printf '.wh.%s' "$b"
  else
    printf '%s/.wh.%s' "$d" "$b"
  fi
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

is_under_opaque() {
  local path="$1"
  local opaque_file="$2"
  local rest="$path"
  local part acc=""

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

extract_member_to_path() {
  local layer_tar="$1"
  local member="$2"
  local out_path="$3"
  local tmp_dir src

  tmp_dir="$(mktemp -d)"
  if ! tar -xmf "$layer_tar" -C "$tmp_dir" -- "$member" 2>/dev/null; then
    local alt="${member#./}"
    if ! tar -xmf "$layer_tar" -C "$tmp_dir" -- "$alt" 2>/dev/null; then
      rm -rf "$tmp_dir"
      return 1
    fi
    member="$alt"
  fi

  src="$tmp_dir/${member#./}"
  src="${src%/}"

  if [[ -d "$src" && ! -L "$src" ]]; then
    mkdir -p "$out_path"
    rm -rf "$tmp_dir"
    return 0
  fi

  mkdir -p "$(dirname "$out_path")"
  if [[ -e "$out_path" || -L "$out_path" ]]; then
    rm -rf "$out_path"
  fi
  cp -a "$src" "$out_path"
  rm -rf "$tmp_dir"
}

cmd_list() {
  local save_tar="$1"
  local image_path="${2:-}"
  local dirs_only="${3:-0}"
  local prefix=""
  local layer_path raw name base real parent deleted_target key
  local i is_dir kind
  local tmp_state deleted_file opaque_file seen_all_file seen_meta_file tab

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
    while IFS= read -r raw; do
      [[ -z "$raw" ]] && continue
      is_dir=0
      [[ "$raw" == */ ]] && is_dir=1

      name="$(sanitize_member_name "$raw")"
      [[ -z "$name" || "$name" == "." ]] && continue

      if [[ -n "$prefix" && "$name" != "$image_path" && "$name" != "$prefix"* ]]; then
        continue
      fi

      base="${name##*/}"
      if [[ "$base" == .wh.* ]]; then
        real="${base#.wh.}"
        if [[ "$name" == */* ]]; then
          parent="${name%/*}"
        else
          parent=""
        fi

        if [[ "$real" == ".wh..opq" || "$base" == ".wh..wh..opq" ]]; then
          key="$parent"
          if [[ -z "$key" ]]; then
            key="${name%/}"
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
        continue
      fi

      if set_has "$deleted_file" "$name"; then
        continue
      fi

      if is_under_opaque "$name" "$opaque_file"; then
        continue
      fi

      if ! set_has "$seen_all_file" "$name"; then
        set_add "$seen_all_file" "$name"
        if ((is_dir)); then
          kind="dir"
        else
          kind="file"
        fi
        printf '%s\t%s\n' "$name" "$kind" >>"$seen_meta_file"
      fi
    done < <(tar -xOf "$save_tar" "$layer_path" | tar -tf -)
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
  local wh layer_path raw name i found_wh found_raw
  local tmp_layer

  image_path="$(norm_image_path "$image_path")"
  wh="$(whiteout_name "$image_path")"

  read_layers "$save_tar"

  for ((i = ${#LAYERS[@]} - 1; i >= 0; i--)); do
    layer_path="${LAYERS[i]}"
    tmp_layer="$(mktemp)"
    tar -xOf "$save_tar" "$layer_path" >"$tmp_layer"

    found_wh=0
    found_raw=""
    while IFS= read -r raw; do
      name="$(sanitize_member_name "$raw")"
      [[ -z "$name" || "$name" == "." ]] && continue
      if [[ "$name" == "$wh" ]]; then
        found_wh=1
      fi
      if [[ "$name" == "$image_path" && -z "$found_raw" ]]; then
        found_raw="$raw"
      fi
    done < <(tar -tf "$tmp_layer")

    if ((found_wh)); then
      rm -f "$tmp_layer"
      die "Deleted by whiteout in layer $layer_path"
    fi

    if [[ -n "$found_raw" ]]; then
      extract_member_to_path "$tmp_layer" "$found_raw" "$out_path" || {
        rm -f "$tmp_layer"
        die "Failed to extract $image_path from layer $layer_path"
      }
      rm -f "$tmp_layer"
      return 0
    fi

    rm -f "$tmp_layer"
  done

  die "Not found in any layer: $image_path"
}

cmd_extract_dir() {
  local save_tar="$1"
  local image_dir="$2"
  local out_dir="$3"
  local prefix opq layer_path tmp_layer raw name base real parent deleted_target target_dir
  local rel dest i
  local tmp_state deleted_file opaque_file

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
  touch "$deleted_file" "$opaque_file"

  read_layers "$save_tar"

  for ((i = ${#LAYERS[@]} - 1; i >= 0; i--)); do
    layer_path="${LAYERS[i]}"
    tmp_layer="$(mktemp)"
    tar -xOf "$save_tar" "$layer_path" >"$tmp_layer"

    while IFS= read -r raw; do
      name="$(sanitize_member_name "$raw")"
      [[ -z "$name" || "$name" == "." ]] && continue

      if [[ -n "$prefix" && "$name" != "$image_dir" && "$name" != "$prefix"* ]]; then
        continue
      fi

      if [[ "$name" == "$opq" ]]; then
        set_add "$opaque_file" "$image_dir"
        continue
      fi

      base="${name##*/}"
      if [[ "$base" == .wh.* ]]; then
        real="${base#.wh.}"
        if [[ "$name" == */* ]]; then
          parent="${name%/*}"
        else
          parent=""
        fi

        if [[ "$real" == ".wh..opq" || "$base" == ".wh..wh..opq" ]]; then
          target_dir="$parent"
          if [[ "$target_dir" == "$image_dir" || "$target_dir" == "$prefix"* ]]; then
            set_add "$opaque_file" "$target_dir"
          fi
          continue
        fi

        if [[ -n "$parent" ]]; then
          deleted_target="$parent/$real"
        else
          deleted_target="$real"
        fi
        set_add "$deleted_file" "$deleted_target"
        continue
      fi

      if set_has "$deleted_file" "$name"; then
        continue
      fi

      if is_under_opaque "$name" "$opaque_file"; then
        continue
      fi

      if [[ -n "$prefix" ]]; then
        rel="${name#"$prefix"}"
      else
        rel="$name"
      fi

      if [[ -n "$rel" ]]; then
        dest="$out_dir/$rel"
      else
        dest="$out_dir"
      fi

      extract_member_to_path "$tmp_layer" "$raw" "$dest" || {
        rm -f "$tmp_layer"
        die "Failed to extract member $name from layer $layer_path"
      }
    done < <(tar -tf "$tmp_layer")

    rm -f "$tmp_layer"
  done

  rm -rf "$tmp_state"
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
