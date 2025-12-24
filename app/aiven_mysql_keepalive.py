import os
import time

import pymysql


def main() -> None:
    conn = pymysql.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ["DB_PORT"]),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
        connect_timeout=10,
        read_timeout=10,
        write_timeout=10,
        ssl={"check_hostname": True},  # Aiven MySQL requires TLS
    )

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS ping;")
            row = cur.fetchone()
    finally:
        conn.close()

    if not row or row[0] != 1:
        raise RuntimeError(f"Unexpected ping result: {row}")

    print(f"Aiven MySQL keep-alive completed successfully at {time.ctime()}.")


if __name__ == "__main__":
    main()
