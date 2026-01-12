import os
import time
import requests


def ping_aiven_service() -> None:
    """Ping Aiven control-plane API to keep the service active."""
    token = os.getenv("AIVEN_API_TOKEN")
    project = os.getenv("AIVEN_PROJECT")
    service = os.getenv("AIVEN_MYSQL_SERVICE")

    if not all([token, project, service]):
        print("Aiven API ping skipped: missing env vars")
        return

    url = f"https://api.aiven.io/v1/project/{project}/service/{service}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            print(f"Aiven API keep-alive ping successful at {time.ctime()}")
        else:
            # show a tiny snippet for debugging without dumping everything
            snippet = resp.text[:200].replace("\n", " ")
            print(f"Aiven API ping returned {resp.status_code} at {time.ctime()}: {snippet}")
            raise SystemExit(1)
    except Exception as e:
        print(f"Aiven API keep-alive ping failed at {time.ctime()}: {e}")
        raise


if __name__ == "__main__":
    ping_aiven_service()
