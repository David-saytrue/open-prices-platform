"""Prepare read-only Greenplum views for Tableau Desktop; no account automation."""

import argparse
from pathlib import Path

from .warehouse_io import validate_database

READER_ROLE = "open_prices_tableau"


def setup_views(database: str, *, grant_reader: bool = False) -> None:
    from psycopg2 import sql
    from .warehouse import connect

    validate_database(database)
    connection = connect(database)
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute((Path(__file__).resolve().parent.parent / "sql/003_tableau_views.sql").read_text(encoding="utf-8"))
            if grant_reader:
                cursor.execute("SELECT rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname=%s", (READER_ROLE,))
                existing = cursor.fetchone()
                if existing is None:
                    cursor.execute(sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE").format(sql.Identifier(READER_ROLE)))
                elif any(existing):
                    raise ValueError("Existing Tableau role has elevated privileges; no role changes made")
                cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(database), sql.Identifier(READER_ROLE)))
                cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA bi TO {}").format(sql.Identifier(READER_ROLE)))
                cursor.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA bi TO {}").format(sql.Identifier(READER_ROLE)))
                cursor.execute(sql.SQL("ALTER ROLE {} IN DATABASE {} SET default_transaction_read_only=on").format(
                    sql.Identifier(READER_ROLE), sql.Identifier(database)))
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="open_prices")
    parser.add_argument("--grant-reader", action="store_true")
    args = parser.parse_args()
    setup_views(args.database, grant_reader=args.grant_reader)
    print("Tableau BI views prepared; Desktop connection has not been verified.")


if __name__ == "__main__":
    main()
