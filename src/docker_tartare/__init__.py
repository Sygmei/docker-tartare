import argparse
import io
import json
import os
import tarfile
from typing import Iterable, Optional, Set, Tuple

def norm_image_path(p: str) -> str:
    # docker tar members are stored without leading slash
    p = p.strip()
    if p.startswith("/"):
        p = p[1:]
    return p

def whiteout_name(target: str) -> str:
    d = os.path.dirname(target)
    b = os.path.basename(target)
    wh = f".wh.{b}"
    return wh if d == "" else f"{d}/{wh}"

def opaque_whiteout_name(dirpath: str) -> str:
    # marks directory as opaque: lower layers under this dir are ignored
    dirpath = dirpath.rstrip("/")
    return f"{dirpath}/.wh..wh..opq" if dirpath else ".wh..wh..opq"

def safe_mkdirs(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_member(tf: tarfile.TarFile, member: tarfile.TarInfo, out_path: str) -> None:
    if member.isdir():
        safe_mkdirs(out_path)
        return
    if member.issym() or member.islnk():
        # Recreate symlinks/hardlinks when possible
        safe_mkdirs(os.path.dirname(out_path) or ".")
        try:
            if os.path.lexists(out_path):
                os.remove(out_path)
        except IsADirectoryError:
            pass
        if member.issym():
            os.symlink(member.linkname, out_path)
        else:
            # hardlink: best-effort (link target may not exist yet)
            try:
                os.link(member.linkname, out_path)
            except OSError:
                # fallback: ignore hardlink if it can't be created
                pass
        return

    safe_mkdirs(os.path.dirname(out_path) or ".")
    f = tf.extractfile(member)
    if f is None:
        return
    with f, open(out_path, "wb") as out:
        out.write(f.read())
    # best-effort mode
    try:
        os.chmod(out_path, member.mode)
    except OSError:
        pass

def iter_layer_members(layer_tf: tarfile.TarFile) -> Iterable[tarfile.TarInfo]:
    for m in layer_tf.getmembers():
        # skip pax global headers etc.
        if m.name in ("", "./", "."):
            continue
        yield m

def load_layers_from_docker_save(save_tar_path: str, image_index: int = 0) -> Tuple[tarfile.TarFile, list[str]]:
    outer = tarfile.open(save_tar_path, "r:*")
    manifest_member = outer.getmember("manifest.json")
    manifest_bytes = outer.extractfile(manifest_member).read()
    manifest = json.loads(manifest_bytes.decode("utf-8"))
    layers = manifest[image_index]["Layers"]
    return outer, layers

def open_layer_tar(outer: tarfile.TarFile, layer_path: str) -> tarfile.TarFile:
    # layer_path like "<layerid>/layer.tar"
    layer_member = outer.getmember(layer_path)
    layer_fileobj = outer.extractfile(layer_member)
    # tarfile needs a seekable fileobj; buffer in memory for this layer header/index
    # (still far cheaper than unpacking rootfs)
    data = layer_fileobj.read()
    return tarfile.open(fileobj=io.BytesIO(data), mode="r:*")

def list_contents(save_tar: str, image_path: Optional[str] = None, dirs_only: bool = False) -> list[str]:
    """List files (and/or directories) visible in the final merged image."""
    prefix = ""
    if image_path:
        image_path = norm_image_path(image_path).rstrip("/")
        prefix = image_path + "/" if image_path else ""

    outer, layers = load_layers_from_docker_save(save_tar)
    try:
        deleted: Set[str] = set()
        opaque_dirs: Set[str] = set()
        seen: dict[str, str] = {}  # name -> "file" | "dir"

        for layer_path in reversed(layers):
            with open_layer_tar(outer, layer_path) as layer_tf:
                for m in iter_layer_members(layer_tf):
                    name = m.name.lstrip("./")

                    # scope filter
                    if prefix and not (name == image_path or name.startswith(prefix)):
                        continue

                    # whiteout handling
                    base = os.path.basename(name)
                    if base.startswith(".wh."):
                        real = base[len(".wh."):]
                        parent = os.path.dirname(name)
                        if real == ".wh..opq" or base == ".wh..wh..opq":
                            opaque_dirs.add(parent if parent else name.rstrip("/"))
                            continue
                        deleted_target = f"{parent}/{real}" if parent else real
                        deleted.add(deleted_target)
                        continue

                    if name in deleted:
                        continue

                    # skip entries under opaque dirs from higher layers
                    parts = name.split("/")
                    acc = []
                    skip = False
                    for p in parts[:-1]:
                        acc.append(p)
                        if "/".join(acc) in opaque_dirs:
                            skip = True
                            break
                    if skip:
                        continue

                    if name not in seen:
                        kind = "dir" if m.isdir() else "file"
                        seen[name] = kind

        results: list[str] = []
        for name, kind in sorted(seen.items()):
            if dirs_only and kind != "dir":
                continue
            display = f"/{name}/" if kind == "dir" else f"/{name}"
            results.append(display)
        return results
    finally:
        outer.close()


def extract_file(save_tar: str, image_path: str, out_path: str) -> int:
    image_path = norm_image_path(image_path)
    wh = whiteout_name(image_path)

    outer, layers = load_layers_from_docker_save(save_tar)
    try:
        for layer_path in reversed(layers):
            with open_layer_tar(outer, layer_path) as layer_tf:
                names = set(m.name.lstrip("./") for m in iter_layer_members(layer_tf))
                if wh in names:
                    raise FileNotFoundError(f"Deleted by whiteout in layer {layer_path}")
                if image_path in names:
                    member = layer_tf.getmember(image_path)
                    write_member(layer_tf, member, out_path)
                    return 0
        raise FileNotFoundError("Not found in any layer")
    finally:
        outer.close()

def extract_dir(save_tar: str, image_dir: str, out_dir: str) -> int:
    image_dir = norm_image_path(image_dir).rstrip("/")
    prefix = image_dir + "/" if image_dir else ""
    opq = opaque_whiteout_name(image_dir)

    outer, layers = load_layers_from_docker_save(save_tar)
    try:
        # Track deletions (whiteouts) and opaque subdirs we've hit while going top-down
        deleted: Set[str] = set()
        opaque_dirs: Set[str] = set()

        def is_under_opaque(path: str) -> bool:
            # If any parent dir is marked opaque, lower layers under it should be ignored
            parts = path.split("/")
            acc = []
            for p in parts[:-1]:
                acc.append(p)
                d = "/".join(acc)
                if d in opaque_dirs:
                    return True
            return False

        # Walk from top (latest) to bottom (oldest), extracting as we find entries.
        for layer_path in reversed(layers):
            with open_layer_tar(outer, layer_path) as layer_tf:
                for m in iter_layer_members(layer_tf):
                    name = m.name.lstrip("./")

                    # Only consider entries inside the requested directory
                    if prefix and not (name == image_dir or name.startswith(prefix)):
                        continue

                    # Handle opaque marker for the directory itself
                    if name == opq:
                        opaque_dirs.add(image_dir)
                        continue

                    # Handle whiteouts for children
                    base = os.path.basename(name)
                    if base.startswith(".wh."):
                        real = base[len(".wh."):]
                        parent = os.path.dirname(name)
                        # opaque marker inside subdirs
                        if real == ".wh..opq" or base == ".wh..wh..opq":
                            # ".wh..wh..opq" is the canonical opaque file name; treat it as such.
                            target_dir = parent
                            if target_dir.startswith(prefix) or target_dir == image_dir:
                                opaque_dirs.add(target_dir)
                            continue
                        # deletion of a specific child entry
                        deleted_target = f"{parent}/{real}" if parent else real
                        deleted.add(deleted_target)
                        continue

                    # Skip if deleted by a higher layer whiteout
                    if name in deleted:
                        continue
                    # Skip if a parent directory is opaque (from higher layer)
                    if is_under_opaque(name):
                        continue

                    # Map to output path
                    rel = name[len(prefix):] if prefix else name
                    dest = os.path.join(out_dir, rel) if rel else out_dir
                    write_member(layer_tf, m, dest)

        return 0
    finally:
        outer.close()

def cmd_list(args: argparse.Namespace) -> None:
    entries = list_contents(
        args.save_tar,
        image_path=args.path,
        dirs_only=args.dirs,
    )
    for e in entries:
        print(e)


def cmd_extract(args: argparse.Namespace) -> None:
    if args.dir:
        safe_mkdirs(args.output)
        extract_dir(args.save_tar, args.image_path, args.output)
    else:
        extract_file(args.save_tar, args.image_path, args.output)


def main():
    ap = argparse.ArgumentParser(
        description="Inspect or extract files from a docker save tar (docker-archive) without running Docker."
    )
    sub = ap.add_subparsers(dest="command", required=True)

    # ── list ──────────────────────────────────────────────
    lp = sub.add_parser("list", help="List files or directories inside the image")
    lp.add_argument("save_tar", help="Path to docker save tar (docker-archive)")
    lp.add_argument("path", nargs="?", default=None,
                    help="Directory inside image to list (default: root)")
    lp.add_argument("--dirs", action="store_true",
                    help="Show directories only")
    lp.set_defaults(func=cmd_list)

    # ── extract ───────────────────────────────────────────
    ep = sub.add_parser("extract", help="Extract a file or directory from the image")
    ep.add_argument("save_tar", help="Path to docker save tar (docker-archive)")
    ep.add_argument("image_path", help="Path inside image (e.g. /usr/local/bin/app or /etc)")
    ep.add_argument("output", help="Output file (for file) or output directory (for dir)")
    ep.add_argument("--dir", action="store_true",
                    help="Treat image_path as a directory")
    ep.set_defaults(func=cmd_extract)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()