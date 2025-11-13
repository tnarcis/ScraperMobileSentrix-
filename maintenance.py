"""Utility helpers for maintaining scraper data stores."""

import argparse
from typing import Optional

from database import db_manager, results_db_manager


def purge_results(older_than: Optional[int], include_products: bool, delete_all: bool) -> None:
    summary = results_db_manager.purge_results_data(
        older_than_days=older_than,
        include_products=include_products,
        delete_all=delete_all,
    )

    scope = "all records" if delete_all or older_than is None else f"records older than {older_than} day(s)"
    print(f"Purged {scope}.")
    print(
        "Removed {changes} change log entries, {prices} price history rows, {products} products.".format(
            changes=summary.get("change_logs_deleted", 0),
            prices=summary.get("price_history_deleted", 0),
            products=summary.get("products_deleted", 0),
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Maintenance utilities for the results database")
    parser.add_argument(
        "--purge",
        action="store_true",
        help="Delete historical result data. Use with --days or --all.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Only purge records older than this many days.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Delete all data regardless of age.",
    )
    parser.add_argument(
        "--include-products",
        action="store_true",
        help="When purging, delete product rows that match the age filter as well.",
    )
    parser.add_argument(
        "--reset-all",
        action="store_true",
        help="Delete every record from both extractor and results databases.",
    )

    args = parser.parse_args()

    if args.reset_all:
        print("⚠️  This will delete all stored data across both databases.")
        summary_results = results_db_manager.clear_all_data()
        summary_extractor = db_manager.clear_all_data()
        print("Results DB cleared:")
        for table, count in summary_results.items():
            print(f"  - {table}: {count} row(s) removed")
        print("Extractor DB cleared:")
        for table, count in summary_extractor.items():
            print(f"  - {table}: {count} row(s) removed")
        return

    if args.purge:
        if not args.all and args.days is None:
            parser.error("Provide --days=N or use --all when purging.")

        if args.all:
            purge_results(None, args.include_products, delete_all=True)
        else:
            if args.days < 0:
                parser.error("--days must be zero or a positive integer")
            purge_results(args.days, args.include_products, delete_all=False)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
