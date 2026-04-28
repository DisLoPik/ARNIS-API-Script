import subprocess
import shutil
import math
import time
import csv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ================= SETTINGS =================

ARNIS_EXE = r"C:\Path\To\arnis.exe"

WEST = -86.0
EAST = -80.0
SOUTH = 34.5
NORTH = 37.0

STEP_LON = 0.02
STEP_LAT = 0.02

SCALE = 0.2

PARALLEL_WORKERS = 2  # use 1 or 2 first. Higher may cause rate limits.

AVG_TILE_TIME_SEC = 120

WORK_FOLDER = Path(r"C:\ArnisTiles\working")
DONE_FOLDER = Path(r"C:\ArnisTiles\done")
MASTER_WORLD = Path(r"C:\ArnisTiles\MasterEarth")

AUTO_MERGE_MCA = True
DELETE_TEMP_AFTER_MERGE = True

LOG_FILE = Path(r"C:\ArnisTiles\progress.csv")

# ================= LOCKS =================

print_lock = Lock()
merge_lock = Lock()
log_lock = Lock()

# ================= HELPERS =================

def meters_per_degree_lon(lat):
    return 111320 * math.cos(math.radians(lat))

def meters_per_degree_lat():
    return 111320

def format_time(seconds):
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    return f"{h}h {m}m"

def format_size(gb):
    return f"{gb / 1024:.2f} TB" if gb >= 1024 else f"{gb:.2f} GB"

def build_command(output, west, south, east, north):
    return [
        ARNIS_EXE,
        "--bbox", f"{south},{west},{north},{east}",
        "--output-dir", str(output),
        "--scale", str(SCALE),
        "--terrain",
        "--interior", "false",
        "--roof", "true",
        "--timeout", "600",
    ]

def get_tiles():
    tiles = []
    x = 0
    lon = WEST

    while lon < EAST:
        y = 1
        lat = SOUTH

        while lat < NORTH:
            tile_west = lon
            tile_east = min(lon + STEP_LON, EAST)
            tile_south = lat
            tile_north = min(lat + STEP_LAT, NORTH)

            tile_name = f"{x}_{y}"

            center_lat = (tile_south + tile_north) / 2

            offset_x_blocks = round(
                ((tile_west - WEST) * meters_per_degree_lon(center_lat)) * SCALE
            )

            offset_z_blocks = round(
                ((tile_south - SOUTH) * meters_per_degree_lat()) * SCALE
            )

            offset_region_x = round(offset_x_blocks / 512)
            offset_region_z = round(offset_z_blocks / 512)

            tiles.append({
                "name": tile_name,
                "x": x,
                "y": y,
                "west": tile_west,
                "east": tile_east,
                "south": tile_south,
                "north": tile_north,
                "offset_region_x": offset_region_x,
                "offset_region_z": offset_region_z,
            })

            y += 1
            lat += STEP_LAT

        x += 1
        lon += STEP_LON

    return tiles

def estimate(tiles):
    total_tiles = len(tiles)
    total_time = (total_tiles * AVG_TILE_TIME_SEC) / max(PARALLEL_WORKERS, 1)

    area_deg = (EAST - WEST) * (NORTH - SOUTH)
    estimated_gb = area_deg * 20 * (SCALE / 0.2) ** 2

    return total_tiles, total_time, estimated_gb

def log_status(tile_name, status, message=""):
    with log_lock:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        new_file = not LOG_FILE.exists()

        with LOG_FILE.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            if new_file:
                writer.writerow(["tile", "status", "message", "time"])

            writer.writerow([tile_name, status, message, time.strftime("%Y-%m-%d %H:%M:%S")])

def is_done(tile_name):
    return (DONE_FOLDER / tile_name).exists()

# ================= MCA MERGE =================

def copy_level_files_once(tile_world):
    MASTER_WORLD.mkdir(parents=True, exist_ok=True)

    for filename in ["level.dat", "session.lock"]:
        src = tile_world / filename
        dst = MASTER_WORLD / filename

        if src.exists() and not dst.exists():
            shutil.copy2(src, dst)

def merge_mca_regions(tile_world, offset_rx, offset_rz):
    src_region = tile_world / "region"
    dst_region = MASTER_WORLD / "region"

    if not src_region.exists():
        raise RuntimeError("No region folder found in generated tile.")

    dst_region.mkdir(parents=True, exist_ok=True)

    with merge_lock:
        copy_level_files_once(tile_world)

        for file in src_region.glob("r.*.*.mca"):
            parts = file.stem.split(".")
            old_rx = int(parts[1])
            old_rz = int(parts[2])

            new_rx = old_rx + offset_rx
            new_rz = old_rz + offset_rz

            dst = dst_region / f"r.{new_rx}.{new_rz}.mca"

            if dst.exists():
                with print_lock:
                    print(f"WARNING: Overwriting existing region {dst.name}")

            shutil.copy2(file, dst)

# ================= TILE WORK =================

def process_tile(tile, index, total):
    tile_name = tile["name"]

    if is_done(tile_name):
        return tile_name, "skipped"

    output_folder = WORK_FOLDER / tile_name

    if output_folder.exists():
        shutil.rmtree(output_folder)

    cmd = build_command(
        output_folder,
        tile["west"],
        tile["south"],
        tile["east"],
        tile["north"],
    )

    with print_lock:
        print(f"[{index}/{total}] Generating {tile_name}")

    result = subprocess.run(cmd)

    if result.returncode != 0:
        log_status(tile_name, "failed", f"Arnis error code {result.returncode}")
        return tile_name, "failed"

    if AUTO_MERGE_MCA:
        try:
            merge_mca_regions(
                output_folder,
                tile["offset_region_x"],
                tile["offset_region_z"],
            )
        except Exception as e:
            log_status(tile_name, "merge_failed", str(e))
            return tile_name, "merge_failed"

    done_folder = DONE_FOLDER / tile_name

    if done_folder.exists():
        shutil.rmtree(done_folder)

    if DELETE_TEMP_AFTER_MERGE:
        done_folder.mkdir(parents=True, exist_ok=True)
        marker = done_folder / "done.txt"
        marker.write_text("Generated and merged.\n", encoding="utf-8")
        shutil.rmtree(output_folder, ignore_errors=True)
    else:
        shutil.move(str(output_folder), str(done_folder))

    log_status(tile_name, "done")
    return tile_name, "done"

# ================= MAIN =================

def main():
    WORK_FOLDER.mkdir(parents=True, exist_ok=True)
    DONE_FOLDER.mkdir(parents=True, exist_ok=True)
    MASTER_WORLD.mkdir(parents=True, exist_ok=True)

    tiles = get_tiles()
    total_tiles, total_time, estimated_gb = estimate(tiles)

    print("\n========== ESTIMATE ==========")
    print(f"Tiles: {total_tiles}")
    print(f"Parallel workers: {PARALLEL_WORKERS}")
    print(f"Estimated time: {format_time(total_time)}")
    print(f"Estimated space: {format_size(estimated_gb)}")
    print(f"Scale: {SCALE}")
    print(f"Auto MCA merge: {AUTO_MERGE_MCA}")
    print("==============================\n")

    confirm = input("Start? Type Y to continue: ").strip().lower()

    if confirm != "y":
        print("Cancelled.")
        return

    completed = 0
    failed = 0
    skipped = 0

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = {}

        for i, tile in enumerate(tiles, start=1):
            future = executor.submit(process_tile, tile, i, total_tiles)
            futures[future] = tile

        for future in as_completed(futures):
            tile_name, status = future.result()

            if status == "done":
                completed += 1
            elif status == "skipped":
                skipped += 1
            else:
                failed += 1

            finished = completed + failed + skipped
            percent = (finished / total_tiles) * 100

            with print_lock:
                print(
                    f"Progress: {finished}/{total_tiles} "
                    f"({percent:.1f}%) | Done: {completed} | Skipped: {skipped} | Failed: {failed}"
                )

    print("\n========== FINISHED ==========")
    print(f"Done: {completed}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")
    print(f"Master world: {MASTER_WORLD}")
    print("==============================")

if __name__ == "__main__":
    main()