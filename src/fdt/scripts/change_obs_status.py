#!/usr/bin/env python3

from argparse import ArgumentParser

from fdt.fdt_database import DatabaseConnect
from fdt.fdt_utils import read_config


def parse_args():
    parser = ArgumentParser(description="Update FDT observation status")
    parser.add_argument("--inst", required=True, help="Instrument (e.g. SCALES)")
    parser.add_argument("--date", required=True, help="Date (YYYYMMDD)")
    parser.add_argument(
        "--status",
        required=True,
        choices=[
            "PENDING", "PACKAGING", "PACKAGED", "IGNORE",
            "TRANSFERRING", "COMPLETE", "ERROR",
        ],
        help="New status"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    cfg = read_config("fdt/fdt_config.live.yaml")

    inst = args.inst.upper()
    prefixes = cfg[inst]["inst_prefixes"]

    conn = DatabaseConnect(cfg["DATABASE"])
    conn.connect()

    query = """
        UPDATE fdt_observations
        SET status = %s
        WHERE koaid LIKE %s
    """
    count = 0

    with conn.db.cursor() as cursor:
        for prefix in prefixes:
            koaid_pattern = f"{prefix}.{args.date}.%"
            params = (args.status, koaid_pattern,)
            try:
                cursor.execute(query, params)
                count += cursor.rowcount
            except Exception as err:
                print(err)


    conn.db.close()

    print(f"Updated {count} observations to {args.status}")


if __name__ == "__main__":
    main()