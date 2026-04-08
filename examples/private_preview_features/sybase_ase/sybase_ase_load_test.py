#!/usr/bin/env python3
"""
Sybase ASE Load Test Script
Generates realistic workload on Sybase ASE database for testing Fivetran connector performance.

Configuration:
    Reads database connection settings from configuration.json in the same directory.

Note:
    If you encounter "transaction log is full" errors, the Sybase ASE database needs
    transaction log maintenance. Contact your DBA to truncate or expand the transaction log.
"""

import pyodbc
import random
import time
import json
import os
from datetime import datetime, timedelta
from decimal import Decimal


def load_config():
    """Load database configuration from configuration.json"""
    config_path = os.path.join(os.path.dirname(__file__), "configuration.json")

    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            "Please ensure configuration.json exists in the same directory."
        )

    with open(config_path, "r") as f:
        config = json.load(f)

    required_fields = ["server", "port", "database", "user_id", "password"]
    missing_fields = [field for field in required_fields if field not in config]

    if missing_fields:
        raise ValueError(f"Missing required fields in configuration.json: {missing_fields}")

    return config


def get_connection(autocommit=True):
    """Create and return a database connection using configuration.json"""
    config = load_config()

    conn_str = (
        f"DRIVER=FreeTDS;"
        f"SERVER={config['server']};"
        f"PORT={config['port']};"
        f"DATABASE={config['database']};"
        f"UID={config['user_id']};"
        f"PWD={config['password']};"
        f"TDS_Version=5.0;"
        f"LoginTimeout=30;"
    )
    return pyodbc.connect(conn_str, autocommit=autocommit)


def insert_sales(conn, num_records=100):
    """Insert random sales records"""
    cursor = conn.cursor()

    # Get existing store and title IDs
    cursor.execute("SELECT stor_id FROM stores")
    store_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT title_id FROM titles")
    title_ids = [row[0] for row in cursor.fetchall()]

    if not store_ids or not title_ids:
        print("⚠️  No stores or titles found. Skipping sales inserts.")
        return 0

    inserted = 0
    for _ in range(num_records):
        try:
            stor_id = random.choice(store_ids)
            title_id = random.choice(title_ids)
            date = datetime.now() - timedelta(days=random.randint(0, 365))

            cursor.execute(
                "INSERT INTO sales (stor_id, title_id, date) VALUES (?, ?, ?)",
                (stor_id, title_id, date),
            )
            inserted += 1
        except Exception as e:
            print(f"⚠️  Insert failed: {e}")

    return inserted


def insert_salesdetail(conn, num_records=100):
    """Insert random salesdetail records"""
    cursor = conn.cursor()

    # Get existing store and title IDs
    cursor.execute("SELECT stor_id FROM stores")
    store_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT title_id FROM titles")
    title_ids = [row[0] for row in cursor.fetchall()]

    if not store_ids or not title_ids:
        print("⚠️  No stores or titles found. Skipping salesdetail inserts.")
        return 0

    inserted = 0
    for _ in range(num_records):
        try:
            stor_id = random.choice(store_ids)
            ord_num = f"ORD{random.randint(10000, 99999)}"
            title_id = random.choice(title_ids)
            qty = random.randint(1, 100)
            discount = round(random.uniform(0, 50), 2)

            cursor.execute(
                "INSERT INTO salesdetail (stor_id, ord_num, title_id, qty, discount) VALUES (?, ?, ?, ?, ?)",
                (stor_id, ord_num, title_id, qty, discount),
            )
            inserted += 1
        except Exception as e:
            print(f"⚠️  Insert failed: {e}")

    return inserted


def update_titles(conn, num_updates=50):
    """Update random title prices"""
    cursor = conn.cursor()

    cursor.execute("SELECT title_id FROM titles")
    title_ids = [row[0] for row in cursor.fetchall()]

    if not title_ids:
        print("⚠️  No titles found. Skipping updates.")
        return 0

    updated = 0
    for _ in range(num_updates):
        try:
            title_id = random.choice(title_ids)
            new_price = Decimal(random.uniform(10, 100)).quantize(Decimal("0.01"))

            cursor.execute("UPDATE titles SET price = ? WHERE title_id = ?", (new_price, title_id))
            updated += cursor.rowcount
        except Exception as e:
            print(f"⚠️  Update failed: {e}")

    return updated


def delete_old_sales(conn, days_old=365):
    """Delete sales records older than specified days"""
    cursor = conn.cursor()

    try:
        cutoff_date = datetime.now() - timedelta(days=days_old)
        cursor.execute("DELETE FROM sales WHERE date < ?", (cutoff_date,))
        deleted = cursor.rowcount
        return deleted
    except Exception as e:
        print(f"⚠️  Delete failed: {e}")
        return 0


def run_load_test(duration_minutes=5, operations_per_cycle=10):
    """
    Run load test for specified duration

    Args:
        duration_minutes: How long to run the test
        operations_per_cycle: Number of operations per cycle
    """
    config = load_config()

    print("=" * 60)
    print("Sybase ASE Load Test")
    print("=" * 60)
    print(f"Duration: {duration_minutes} minutes")
    print(f"Operations per cycle: {operations_per_cycle}")
    print(f"Target: {config['server']}:{config['port']}/{config['database']}")
    print("=" * 60)

    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)

    total_inserts = 0
    total_updates = 0
    total_deletes = 0
    cycle = 0

    try:
        while time.time() < end_time:
            cycle += 1
            cycle_start = time.time()

            print(f"\n🔄 Cycle {cycle} - {datetime.now().strftime('%H:%M:%S')}")

            conn = get_connection()

            try:
                # Insert operations
                sales_inserted = insert_sales(conn, operations_per_cycle)
                salesdetail_inserted = insert_salesdetail(conn, operations_per_cycle)
                total_inserts += sales_inserted + salesdetail_inserted
                print(f"  ✅ Inserted: {sales_inserted} sales, {salesdetail_inserted} salesdetail")

                # Update operations
                updates = update_titles(conn, operations_per_cycle // 2)
                total_updates += updates
                print(f"  ✅ Updated: {updates} titles")

                # Occasional delete operations (every 5 cycles)
                if cycle % 5 == 0:
                    deletes = delete_old_sales(conn, days_old=730)
                    total_deletes += deletes
                    print(f"  ✅ Deleted: {deletes} old sales")

            finally:
                conn.close()

            cycle_duration = time.time() - cycle_start
            print(f"  ⏱️  Cycle duration: {cycle_duration:.2f}s")

            # Sleep to avoid overwhelming the database
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n\n⚠️  Load test interrupted by user")

    # Final summary
    total_duration = time.time() - start_time
    print("\n" + "=" * 60)
    print("Load Test Summary")
    print("=" * 60)
    print(f"Duration: {total_duration:.2f} seconds ({total_duration / 60:.2f} minutes)")
    print(f"Cycles completed: {cycle}")
    print(f"Total inserts: {total_inserts}")
    print(f"Total updates: {total_updates}")
    print(f"Total deletes: {total_deletes}")
    print(f"Total operations: {total_inserts + total_updates + total_deletes}")
    print(
        f"Operations per second: {(total_inserts + total_updates + total_deletes) / total_duration:.2f}"
    )
    print("=" * 60)


def run_readonly_test(duration_minutes=5):
    """
    Run read-only load test (queries only, no modifications)

    Args:
        duration_minutes: How long to run the test
    """
    config = load_config()

    print("=" * 60)
    print("Sybase ASE Read-Only Load Test")
    print("=" * 60)
    print(f"Duration: {duration_minutes} minutes")
    print(f"Target: {config['server']}:{config['port']}/{config['database']}")
    print("=" * 60)

    queries = [
        "SELECT COUNT(*) FROM sales",
        "SELECT COUNT(*) FROM titles",
        "SELECT COUNT(*) FROM authors",
        "SELECT s.stor_id, COUNT(*) FROM sales s GROUP BY s.stor_id",
        "SELECT t.title_id, t.title, t.price FROM titles t WHERE t.price > 20",
        "SELECT a.au_lname, a.au_fname FROM authors a ORDER BY a.au_lname",
    ]

    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)

    total_queries = 0
    cycle = 0

    try:
        while time.time() < end_time:
            cycle += 1
            cycle_start = time.time()

            print(f"\n🔄 Cycle {cycle} - {datetime.now().strftime('%H:%M:%S')}")

            conn = get_connection()
            cursor = conn.cursor()

            try:
                for query in queries:
                    cursor.execute(query)
                    cursor.fetchall()  # Fetch results to complete the query
                    total_queries += 1

                print(f"  ✅ Executed {len(queries)} queries")

            finally:
                conn.close()

            cycle_duration = time.time() - cycle_start
            print(f"  ⏱️  Cycle duration: {cycle_duration:.2f}s")

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n\n⚠️  Load test interrupted by user")

    # Final summary
    total_duration = time.time() - start_time
    print("\n" + "=" * 60)
    print("Read-Only Load Test Summary")
    print("=" * 60)
    print(f"Duration: {total_duration:.2f} seconds ({total_duration / 60:.2f} minutes)")
    print(f"Cycles completed: {cycle}")
    print(f"Total queries: {total_queries}")
    print(f"Queries per second: {total_queries / total_duration:.2f}")
    print("=" * 60)


if __name__ == "__main__":
    import sys

    print("\nSybase ASE Load Test Script")
    print("=" * 60)
    print("1. Run write load test (inserts, updates, deletes)")
    print("2. Run read-only load test (queries only)")
    print("3. Exit")
    print("=" * 60)

    choice = input("\nSelect option (1-3): ").strip()

    if choice == "1":
        duration = input("Duration in minutes (default 5): ").strip()
        duration = int(duration) if duration else 5

        operations = input("Operations per cycle (default 10): ").strip()
        operations = int(operations) if operations else 10

        run_load_test(duration_minutes=duration, operations_per_cycle=operations)

    elif choice == "2":
        duration = input("Duration in minutes (default 5): ").strip()
        duration = int(duration) if duration else 5

        run_readonly_test(duration_minutes=duration)

    elif choice == "3":
        print("Exiting...")
        sys.exit(0)

    else:
        print("Invalid choice. Exiting...")
        sys.exit(1)
