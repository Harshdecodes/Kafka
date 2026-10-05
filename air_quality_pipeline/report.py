"""Show the stored readings and alert counts."""

import argparse
import os
import sqlite3


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect processed air-quality readings")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("limit must be positive")
    path = os.getenv("DATABASE_PATH", "air_quality.db")
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.OperationalError as exc:
        parser.error(f"cannot open database at {path}: {exc}")
    with connection:
        rows = connection.execute(
            """SELECT sensor_id, observed_at, pm25_ug_m3, co2_ppm,
                      status, partition_id, offset_id
               FROM readings ORDER BY rowid DESC LIMIT ?""",
            (args.limit,),
        ).fetchall()
        counts = connection.execute(
            "SELECT status, COUNT(*) FROM readings GROUP BY status ORDER BY status"
        ).fetchall()
    connection.close()
    for sensor, observed, pm25, co2, status, partition, offset in rows:
        print(f"{observed} {sensor:<10} PM2.5={pm25:5.1f} ug/m3 CO2={co2:5.0f} ppm {status:<6} p={partition} o={offset}")
    print(f"total={sum(count for _, count in counts)} " + " ".join(f"{status.lower()}={count}" for status, count in counts))


if __name__ == "__main__":
    main()
