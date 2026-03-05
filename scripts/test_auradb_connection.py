from __future__ import annotations

import os
import sys

from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import AuthError, ServiceUnavailable


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"Missing required env var: {name}")
    return value


def sanitize(uri: str) -> str:
    return uri.replace("neo4j+s://", "").replace("bolt+s://", "")


def run_probe(uri: str, username: str, password: str, database: str) -> None:
    driver = GraphDatabase.driver(uri, auth=(username, password))
    try:
        driver.verify_connectivity()
        with driver.session(database=database) as session:
            record = session.run("RETURN 1 AS ok, datetime() AS server_time").single()
            if record is None:
                raise RuntimeError("No data returned from server.")
            print("Connection OK")
            print(f"ok={record['ok']}")
            print(f"server_time={record['server_time']}")
    finally:
        driver.close()


def main() -> int:
    load_dotenv()

    uri = get_env("NEO4J_URI")
    username = get_env("NEO4J_USERNAME", "neo4j")
    password = get_env("NEO4J_PASSWORD")
    database = get_env("NEO4J_DATABASE", "neo4j")

    print(f"Using URI: {uri}")
    print(f"Using Username: {username}")
    print(f"Using Database: {database}")

    try:
        run_probe(uri, username, password, database)
        return 0
    except ServiceUnavailable as exc:
        message = str(exc)
        if "routing information" in message.lower() and uri.startswith("neo4j+s://"):
            host = sanitize(uri).split("/")[0]
            direct_uri = f"bolt+s://{host}:7687"
            print(
                "[WARN] Routing probe failed. Retrying direct Bolt TLS connection:"
                f" {direct_uri}"
            )
            run_probe(direct_uri, username, password, database)
            return 0
        raise
    except AuthError:
        print(
            "[ERROR] Authentication failed. Check NEO4J_USERNAME / NEO4J_PASSWORD "
            "or reset password in Aura Console.",
            file=sys.stderr,
        )
        raise


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)
