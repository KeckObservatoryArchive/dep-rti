
import re

from argparse import ArgumentParser
from pathlib import Path

from fdt.fdt_database import DatabaseConnect
from fdt.fdt_utils import read_config


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--inst", help="Instrument (e.g. SCALES)")
    parser.add_argument("--date", help="UT date (YYYYMMDD)")
    return parser.parse_args()


def main():
    args = parse_args()

    obs_dir = Path(f"/koadata/{args.inst}/{args.date}/lev0")

    observations = sorted(obs_dir.glob("*.fits"))

    if not observations:
        print(f"No FITS files found in {obs_dir}")
        return

    cfg = read_config("fdt/fdt_config.live.yaml")

    conn = DatabaseConnect(cfg["DATABASE"])
    conn.connect()

    query = """
        INSERT INTO fdt_observations
            (koaid, level, filepath, status, instrument)
        VALUES
            (%s, %s, %s, %s, %s)
    """

    koaids = set()
    for path in observations:
        m = re.match(r"^([A-Z]{2}\.\d{8}\.\d{5}\.\d{2})", path.stem)
        if not m:
            print(f"Couldn't parse KOAID from {path.name}")
            continue

        koaids.add((m.group(1), str(path)))

    cnt = 0
    with conn.db.cursor() as cursor:
        for koaid in koaids:
            params = (koaid[0], 0, koaid[1], "PENDING", args.inst)
            try:
                cursor.execute(query, params)
                cnt += 1
            except Exception as err:
                print(f"Failed to insert observation: {err}")

    print(f"Inserted {cnt} observations.")

    conn.db.close()


if __name__ == "__main__":
    main()