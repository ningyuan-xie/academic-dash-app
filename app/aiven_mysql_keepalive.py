import os
import time
import pymysql

MAX_TRIES = 5

def main() -> None:
    host = os.environ["DB_HOST"]
    port = int(os.environ["DB_PORT"])
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    dbname = os.environ["DB_NAME"]

    last_err: Exception | None = None

    for attempt in range(1, MAX_TRIES + 1):
        try:
            conn = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=dbname,
                connect_timeout=30,   # was 10
                read_timeout=30,
                write_timeout=30,
                ssl={"check_hostname": True},
            )
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 AS ping;")
                    row = cur.fetchone()
            finally:
                conn.close()

            if not row or row[0] != 1:
                raise RuntimeError(f"Unexpected ping result: {row}")

            print(f"Aiven MySQL keep-alive succeeded at {time.ctime()} (attempt {attempt}).")
            return

        except Exception as e:
            last_err = e
            # exponential-ish backoff: 5s, 10s, 20s, 40s...
            sleep_s = min(60, 5 * (2 ** (attempt - 1)))
            print(f"Attempt {attempt}/{MAX_TRIES} failed: {e} | retrying in {sleep_s}s...")
            time.sleep(sleep_s)

    raise RuntimeError(f"All {MAX_TRIES} attempts failed. Last error: {last_err}")

if __name__ == "__main__":
    main()
