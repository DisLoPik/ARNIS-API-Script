# ALL CODE IN THIS FILE IS ORIGINAL AND CREATED BY DISLOPIK. ANY SIMILARITY TO OTHER CODE IS PURELY COINCIDENTAL.
# GNU AGPLv3
# ________  ___  ________  ___       ________  ________  ___  ___  __       
#|\   ___ \|\  \|\   ____\|\  \     |\   __  \|\   __  \|\  \|\  \|\  \     
#\ \  \_|\ \ \  \ \  \___|\ \  \    \ \  \|\  \ \  \|\  \ \  \ \  \/  /|_   
# \ \  \ \\ \ \  \ \_____  \ \  \    \ \  \\\  \ \   ____\ \  \ \   ___  \  
#  \ \  \_\\ \ \  \|____|\  \ \  \____\ \  \\\  \ \  \___|\ \  \ \  \\ \  \ 
#   \ \_______\ \__\____\_\  \ \_______\ \_______\ \__\    \ \__\ \__\\ \__\
#    \|_______|\|__|\_________\|_______|\|_______|\|__|     \|__|\|__| \|__|
#                  |_________|
#
# WARNING: I do NOT own any Arnis code, this is simply to automate big selections.

import subprocess
import shutil
import math
import time
import csv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock



try:
    from global_land_mask import globe # WARNING: IF THIS LINE SHOWS AN ERROR, THE SCRIPT WILL STILL WORK. VSCODE IS READING WRONG PYTHON ENV
    LAND_MASK_AVAILABLE = True
except ImportError:
    LAND_MASK_AVAILABLE = False

# ================= SETTINGS =================

ARNIS_EXE = r"PATH/TO/ARNIS/EXE"
# ================= CORDS =================
WEST = -170
EAST = -50
SOUTH = 10
NORTH = 75

STEP_LON = 1.0 # HOW MUCH TO MOVE AFTER CREATING A TILE (LOWER VALUE TAKES MORE SPACE)
STEP_LAT = 1.0

SCALE = 0.015 # GOING HIGHER CAN TAKE UP A LOT OF SPACE. (10TB - 50TB)

PARALLEL_WORKERS = 2 # HOW MANY TILES ARE MADE AT ONCE. (ONLY INCREASE IF YOU HAVE A STRONG CPU)
AVG_TILE_TIME_SEC = 120 # THIS CAN CHANGE DEPENDING ON CPU AND PC SPEED

WORK_FOLDER = Path(r"PATH/TO/WORK/FOLDER")
DONE_FOLDER = Path(r"PATH/TO/DONE/FOLDER")
MASTER_WORLD = Path(r"PATH/TO/FINAL/FOLDER")

# ================= OCEAN =================
SKIP_OCEAN_TILES = True # SKIPPING OCEAN TILES IS HIGHLY RECOMENED. WHY MAKE OCEAN TILES IF YOU DONT NEED TO?
OCEAN_THRESHOLD = 0.60
OCEAN_SAMPLE_GRID = 7 

LOG_FILE = Path(r"PATH/TO/progress.csv") # MAKE SURE TO CREATE THIS!!
OCEAN_CACHE_FILE = Path(r"PATH/TO/ocean_cache.csv") # MAKE SURE TO CREATE THIS!!

# ================= EXPERIMENTAL =================
AUTO_MERGE_MCA = False # MERGE ALL CHUNKS (CAN CAUSE CRASHES, ERRORS, OR BREAKS!!)
DELETE_TEMP_AFTER_MERGE = False # AUTO DELETE CACHE FILES FROM %APPDATA% (CAN CAUSE CRASHES, ERRORS, OR BREAKS!!)

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
        # ARNIS API COMMANDS
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

            offset_x_blocks = round(((tile_west - WEST) * meters_per_degree_lon(center_lat)) * SCALE)
            offset_z_blocks = round(((tile_south - SOUTH) * meters_per_degree_lat()) * SCALE)

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

# ================= OCEAN CHECK =================

def load_ocean_cache():
    cache = {}

    if not OCEAN_CACHE_FILE.exists():
        return cache

    with OCEAN_CACHE_FILE.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cache[row["tile"]] = {
                "ocean_percent": float(row["ocean_percent"]),
                "skip": row["skip"].lower() == "true",
            }

    return cache

def save_ocean_cache(tile_name, ocean_percent, should_skip):
    new_file = not OCEAN_CACHE_FILE.exists()

    with OCEAN_CACHE_FILE.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if new_file:
            writer.writerow(["tile", "ocean_percent", "skip"])

        writer.writerow([tile_name, ocean_percent, should_skip])

def ocean_percent_for_tile(tile):
    if not LAND_MASK_AVAILABLE:
        return 0.0

    water_hits = 0
    total = 0

    for row in range(OCEAN_SAMPLE_GRID):
        for col in range(OCEAN_SAMPLE_GRID):
            lat = tile["south"] + ((row + 0.5) / OCEAN_SAMPLE_GRID) * (tile["north"] - tile["south"])
            lon = tile["west"] + ((col + 0.5) / OCEAN_SAMPLE_GRID) * (tile["east"] - tile["west"])

            is_land = globe.is_land(lat, lon)

            if not is_land:
                water_hits += 1

            total += 1

    return water_hits / total

def apply_ocean_skipping(tiles):
    if not SKIP_OCEAN_TILES:
        return tiles, 0

    if not LAND_MASK_AVAILABLE:
        print("WARNING: global-land-mask is not installed, so ocean skipping is disabled.")
        print("Install it with: py -m pip install global-land-mask")
        return tiles, 0

    cache = load_ocean_cache()
    kept = []
    skipped_count = 0

    print("\nChecking tiles for ocean coverage...")

    for i, tile in enumerate(tiles, start=1):
        tile_name = tile["name"]

        if tile_name in cache:
            ocean_percent = cache[tile_name]["ocean_percent"]
            should_skip = cache[tile_name]["skip"]
        else:
            ocean_percent = ocean_percent_for_tile(tile)
            should_skip = ocean_percent >= OCEAN_THRESHOLD
            save_ocean_cache(tile_name, ocean_percent, should_skip)

        tile["ocean_percent"] = ocean_percent

        if should_skip:
            skipped_count += 1
            log_status(tile_name, "ocean_skipped", f"{ocean_percent * 100:.1f}% ocean")
        else:
            kept.append(tile)

        if i % 100 == 0:
            print(f"Ocean checked: {i}/{len(tiles)}")

    return kept, skipped_count

# ================= ESTIMATE MATH =================

def estimate(total_tiles, runnable_tiles):
    total_time = (runnable_tiles * AVG_TILE_TIME_SEC) / max(PARALLEL_WORKERS, 1)

    area_deg = (EAST - WEST) * (NORTH - SOUTH)
    estimated_gb = area_deg * 20 * (SCALE / 0.2) ** 2

    if total_tiles > 0:
        estimated_gb *= runnable_tiles / total_tiles

    return total_time, estimated_gb

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

# !! EXPERIMENTAL FEATURE START !!
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
# !! EXPERIMENTAL FEATURE END !!

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
        ocean_info = tile.get("ocean_percent", 0) * 100
        print(f"[{index}/{total}] Generating {tile_name} ({ocean_info:.1f}% ocean)")

    result = subprocess.run(cmd)

    if result.returncode != 0:
        log_status(tile_name, "failed", f"Arnis error code {result.returncode}")
        return tile_name, "failed"

    if AUTO_MERGE_MCA: # DOES NOTHING IF DISABLED
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
# !! EXPERIMENTAL FEATURE START !!
    if DELETE_TEMP_AFTER_MERGE:
        done_folder.mkdir(parents=True, exist_ok=True)
        marker = done_folder / "done.txt" # MARKER SO SCRIPT KNOWS THE TITLE HAS BEEN MADE
        marker.write_text("Tile Generated.\n", encoding="utf-8")
        shutil.rmtree(output_folder, ignore_errors=True)
    else:
        shutil.move(str(output_folder), str(done_folder))

    log_status(tile_name, "done")
    return tile_name, "done"
# !! EXPERIMENTAL FEATURE END !!

# ================= MAIN =================

def main():
    WORK_FOLDER.mkdir(parents=True, exist_ok=True)
    DONE_FOLDER.mkdir(parents=True, exist_ok=True)
    MASTER_WORLD.mkdir(parents=True, exist_ok=True)

    all_tiles = get_tiles()
    runnable_tiles, ocean_skipped = apply_ocean_skipping(all_tiles)

    total_time, estimated_gb = estimate(len(all_tiles), len(runnable_tiles))

    print("\n========== ESTIMATE ==========")
    print(f"Total tiles before ocean skip: {len(all_tiles)}")
    print(f"Ocean-skipped tiles: {ocean_skipped}")
    print(f"Tiles to generate: {len(runnable_tiles)}")
    print(f"Parallel workers: {PARALLEL_WORKERS}")
    print(f"Estimated time: {format_time(total_time)}")
    print(f"Estimated space: {format_size(estimated_gb)}")
    print(f"Scale: {SCALE}")
    print(f"Ocean skip threshold: {OCEAN_THRESHOLD * 100:.0f}%")
    print(f"Auto MCA merge: {AUTO_MERGE_MCA}")
    print("==============================\n")

    confirm = input("Start? Type Y to continue: ").strip().lower()

    if confirm != "y":
        print("Cancelled.")
        return

    completed = 0
    failed = 0
    skipped = 0

    total = len(runnable_tiles)

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = {}

        for i, tile in enumerate(runnable_tiles, start=1):
            future = executor.submit(process_tile, tile, i, total)
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
            percent = (finished / total) * 100 if total else 100

            with print_lock:
                print(
                    f"Progress: {finished}/{total} "
                    f"({percent:.1f}%) | Done: {completed} | Skipped: {skipped} | Failed: {failed}"
                )

    print("\n========== FINISHED ==========")
    print(f"Done: {completed}")
    print(f"Already skipped/done: {skipped}")
    print(f"Ocean-skipped before start: {ocean_skipped}")
    print(f"Failed: {failed}")
    print(f"Master world: {MASTER_WORLD}")
    print("==============================")

if __name__ == "__main__":
    main()
