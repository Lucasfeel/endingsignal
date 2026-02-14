import json
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from database import create_standalone_connection, get_cursor


def migrate_meta_structure():
    """Migrate webtoon meta from flat keys to common/attributes shape."""
    conn = None
    updated_count = 0

    try:
        print("LOG: [Migration] Starting meta structure migration...")
        conn = create_standalone_connection()
        cursor = get_cursor(conn)

        cursor.execute(
            "SELECT content_id, source, meta FROM contents WHERE content_type = 'webtoon'"
        )
        webtoons = cursor.fetchall()
        print(f"LOG: [Migration] Found {len(webtoons)} webtoon rows.")

        if not webtoons:
            print("LOG: [Migration] No rows to migrate.")
            return

        updates = []
        for webtoon in webtoons:
            old_meta = webtoon["meta"]
            if not old_meta:
                continue

            if "common" in old_meta and "attributes" in old_meta:
                continue

            new_meta = {
                "common": {
                    "authors": old_meta.get("authors", []),
                    "thumbnail_url": old_meta.get("thumbnail_url"),
                },
                "attributes": {
                    "weekdays": old_meta.get("weekdays", []),
                },
            }
            updates.append((json.dumps(new_meta), webtoon["content_id"], webtoon["source"]))

        if updates:
            cursor.executemany(
                "UPDATE contents SET meta = %s WHERE content_id = %s AND source = %s",
                updates,
            )
            updated_count = cursor.rowcount
            print(f"LOG: [Migration] Updated rows: {updated_count}")
        else:
            print("LOG: [Migration] Nothing to update.")

        conn.commit()
        cursor.close()
        print("LOG: [Migration] Migration committed.")
    except Exception as exc:
        if conn:
            conn.rollback()
        print(f"FATAL: [Migration] Error: {exc}", file=sys.stderr)
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    print("==========================================")
    print("  Migration script (v2) started")
    print("==========================================")

    load_dotenv()

    try:
        migrate_meta_structure()
        print("\n[SUCCESS] Migration script completed successfully.")
        print("==========================================")
        sys.exit(0)
    except Exception:
        print("\n[FATAL] Migration script failed.", file=sys.stderr)
        print("==========================================")
        sys.exit(1)
