#!/usr/bin/env python3
import os
import sys
import hashlib
import logging
from pathlib import Path
import numpy as np
from astropy.io import fits
from astropy.visualization import ZScaleInterval
from PIL import Image

# ========================
# CONFIGURATION
# ========================
CUTOUT_SIZE = 1000
SRC_DIR = Path("/mnt/waz/nas/transfer_data")
DEST_DIR = Path("/mnt/waz/nas/cutouts/")
LOG_FILE = DEST_DIR / "cutout_log.txt"

# ========================
# LOGGING SETUP
# ========================
DEST_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# ========================
# CORE FUNCTIONS
# ========================

def sha256sum(file_path: Path) -> str:
    """Compute SHA-256 checksum of a file (for duplicate detection)."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def write_fits_center_cutout_png16(data: np.ndarray, out_path: Path, zscale: bool = True) -> None:
    """Save a centered 1000x1000 px cutout of a FITS image as a 16-bit PNG."""
    if data is None or data.ndim != 2:
        raise ValueError("Expected 2D FITS data array.")

    arr = np.array(data, dtype=float)
    # Replace NaNs/Infs
    if not np.isfinite(arr).all():
        finite = arr[np.isfinite(arr)]
        if finite.size == 0:
            raise ValueError("Image contains only NaN/Inf values.")
        fillval = float(np.nanmin(finite))
        arr = np.nan_to_num(arr, nan=fillval, posinf=np.nanmax(finite), neginf=fillval)

    ny, nx = arr.shape
    cy, cx = ny // 2, nx // 2
    half = CUTOUT_SIZE // 2
    y0, y1 = cy - half, cy + half
    x0, x1 = cx - half, cx + half
    ys0, ys1 = max(0, y0), min(ny, y1)
    xs0, xs1 = max(0, x0), min(nx, x1)
    cut = arr[ys0:ys1, xs0:xs1]

    # Pad if smaller than 1000x1000
    pad_y_before = max(0, ys0 - y0)
    pad_y_after = max(0, y1 - ys1)
    pad_x_before = max(0, xs0 - x0)
    pad_x_after = max(0, x1 - xs1)
    if any((pad_y_before, pad_y_after, pad_x_before, pad_x_after)):
        fillval = float(np.min(cut)) if cut.size > 0 else 0.0
        cut = np.pad(
            cut,
            ((pad_y_before, pad_y_after), (pad_x_before, pad_x_after)),
            mode="constant",
            constant_values=fillval,
        )

    # Normalize
    try:
        vmin, vmax = ZScaleInterval().get_limits(cut) if zscale else (np.min(cut), np.max(cut))
    except Exception:
        lo, hi = np.percentile(cut, [1, 99])
        vmin, vmax = float(lo), float(hi)

    if vmin == vmax:
        vmin, vmax = float(np.min(cut)), float(np.max(cut))
        if vmin == vmax:
            out = np.full((CUTOUT_SIZE, CUTOUT_SIZE), 32768, dtype=np.uint16)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            Image.fromarray(out, mode="I;16").save(out_path)
            return

    scaled = np.clip((cut - vmin) / (vmax - vmin), 0.0, 1.0)
    out_uint16 = (scaled * 65535.0).round().astype(np.uint16)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(out_uint16, mode="I;16").save(out_path)


def process_all_fits(src_dir: Path, dest_dir: Path) -> None:
    """Find all .fits files in src_dir and save PNG cutouts to dest_dir."""
    fits_files = list(src_dir.rglob("*.fits"))
    if not fits_files:
        logging.info(f"No FITS files found under {src_dir}")
        return

    logging.info(f"Found {len(fits_files)} FITS files. Processing...")

    seen_hashes = set()

    for fits_path in fits_files:
        try:
            file_hash = sha256sum(fits_path)
            if file_hash in seen_hashes:
                logging.info(f"⏭️  Skipping duplicate: {fits_path}")
                continue
            seen_hashes.add(file_hash)

            rel_path = fits_path.relative_to(src_dir)
            png_path = dest_dir / rel_path.with_suffix(".png")

            if png_path.exists():
                logging.info(f"⏭️  PNG already exists, skipping: {png_path}")
                continue

            data = fits.getdata(fits_path)
            write_fits_center_cutout_png16(data, png_path)
            logging.info(f"✅ {fits_path} → {png_path}")

        except Exception as e:
            logging.error(f"⚠️  Failed {fits_path}: {e}")


if __name__ == "__main__":
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else SRC_DIR
    dest = Path(sys.argv[2]) if len(sys.argv) > 2 else DEST_DIR
    process_all_fits(src, dest)
    logging.info("✅ All processing complete.")
