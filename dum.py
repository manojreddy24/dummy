import sys
import threading
import traceback
from datetime import datetime, timedelta
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from app.utils.smart_logging import get_logger
from app.src.db.connection import get_connection_pool, db_connection
from sqlalchemy import text

logger = get_logger(__name__)
shutdown_event = threading.Event()

SOURCE_TABLE = "silver_excalibur_incidents_with_hierarchy"
TARGET_TABLE = "gold_excalibur_weekly_mttr"


_DELETE_WEEK_SQL = text(f"""
    DELETE FROM {TARGET_TABLE}
    WHERE week_start_date = :week_start_date
""")


_INSERT_WEEKLY_MTTR_SQL = text(f"""
    INSERT INTO {TARGET_TABLE} (
        week_start_date,
        svp_name,
        svp_employee_id,
        vp_name,
        vp_employee_id,
        priority,
        resolved_incidents,
        total_mttr_hours,
        avg_mttr_hours,
        avg_mttr_days,
        created_at,
        updated_at
    )
    WITH normalized_incidents AS (
        SELECT
            NULLIF(TRIM(svp_name), '') AS svp_name,
            NULLIF(TRIM(svp_employee_id), '') AS svp_employee_id,
            NULLIF(TRIM(vp_name), '') AS vp_name,
            NULLIF(TRIM(vp_employee_id), '') AS vp_employee_id,
            COALESCE(NULLIF(TRIM(priority_display_value), ''), 'Unknown') AS priority,
            NULLIF(TRIM(opened_at_display_value), '')::timestamp AS opened_at_ts,
            NULLIF(TRIM(resolved_at_display_value), '')::timestamp AS resolved_at_ts
        FROM {SOURCE_TABLE}
        WHERE NULLIF(TRIM(opened_at_display_value), '') IS NOT NULL
          AND NULLIF(TRIM(resolved_at_display_value), '') IS NOT NULL
    ),
    valid_incidents AS (
        SELECT
            :week_start_date AS week_start_date,
            COALESCE(svp_name, 'Unknown') AS svp_name,
            COALESCE(svp_employee_id, 'Unknown') AS svp_employee_id,
            COALESCE(vp_name, 'Unknown') AS vp_name,
            COALESCE(vp_employee_id, 'Unknown') AS vp_employee_id,
            priority,
            opened_at_ts,
            resolved_at_ts,
            EXTRACT(EPOCH FROM (resolved_at_ts - opened_at_ts)) / 3600.0 AS mttr_hours
        FROM normalized_incidents
        WHERE resolved_at_ts >= :start_date
          AND resolved_at_ts < :end_date
          AND resolved_at_ts >= opened_at_ts
    )
    SELECT
        week_start_date,
        svp_name,
        svp_employee_id,
        vp_name,
        vp_employee_id,
        priority,
        COUNT(*)::int AS resolved_incidents,
        ROUND(SUM(mttr_hours)::numeric, 2)::double precision AS total_mttr_hours,
        ROUND(AVG(mttr_hours)::numeric, 2)::double precision AS avg_mttr_hours,
        ROUND((AVG(mttr_hours) / 24.0)::numeric, 2)::double precision AS avg_mttr_days,
        NOW(),
        NOW()
    FROM valid_incidents
    GROUP BY
        week_start_date,
        svp_name,
        svp_employee_id,
        vp_name,
        vp_employee_id,
        priority
    RETURNING svp_name, vp_name, priority
""")


_TOTAL_CANDIDATES_SQL = text(f"""
    WITH normalized_incidents AS (
        SELECT
            NULLIF(TRIM(opened_at_display_value), '')::timestamp AS opened_at_ts,
            NULLIF(TRIM(resolved_at_display_value), '')::timestamp AS resolved_at_ts
        FROM {SOURCE_TABLE}
        WHERE NULLIF(TRIM(resolved_at_display_value), '') IS NOT NULL
    )
    SELECT COUNT(*)::int AS total_count
    FROM normalized_incidents
    WHERE resolved_at_ts >= :start_date
      AND resolved_at_ts < :end_date
""")


_INVALID_TIMESTAMP_SQL = text(f"""
    WITH normalized_incidents AS (
        SELECT
            NULLIF(TRIM(opened_at_display_value), '')::timestamp AS opened_at_ts,
            NULLIF(TRIM(resolved_at_display_value), '')::timestamp AS resolved_at_ts
        FROM {SOURCE_TABLE}
        WHERE NULLIF(TRIM(opened_at_display_value), '') IS NOT NULL
          AND NULLIF(TRIM(resolved_at_display_value), '') IS NOT NULL
    )
    SELECT COUNT(*)::int AS invalid_count
    FROM normalized_incidents
    WHERE resolved_at_ts >= :start_date
      AND resolved_at_ts < :end_date
      AND resolved_at_ts < opened_at_ts
""")


_MISSING_TIMESTAMP_SQL = text(f"""
    SELECT COUNT(*)::int AS missing_count
    FROM {SOURCE_TABLE}
    WHERE (
            NULLIF(TRIM(opened_at_display_value), '') IS NULL
         OR NULLIF(TRIM(resolved_at_display_value), '') IS NULL
          )
      AND NULLIF(TRIM(resolved_at_display_value), '')::timestamp >= :start_date
      AND NULLIF(TRIM(resolved_at_display_value), '')::timestamp < :end_date
""")


def _week_range(target_date: datetime):
    """Return start and end date for ISO week containing target_date."""
    d = target_date.date()
    start = d - timedelta(days=d.weekday())   # Monday
    end = start + timedelta(days=7)           # Next Monday
    return start, end


def _default_target_date() -> datetime:
    """Default to yesterday so prior day data is fully landed."""
    return datetime.now() - timedelta(days=1)


def run_excalibur_weekly_mttr_aggregation_etl(target_date: datetime):
    """
    Aggregate weekly MTTR metrics from silver incident hierarchy data
    into gold_excalibur_weekly_mttr.
    """
    start_time = datetime.now()
    week_label = "unknown"

    try:
        start_date, end_date = _week_range(target_date)
        week_label = start_date.strftime("%Y-%m-%d")

        logger.info(f"Starting Excalibur Weekly MTTR Aggregation ETL for week of {week_label}")

        if shutdown_event.is_set():
            logger.warning(f"Shutdown requested before starting week {week_label}, aborting")
            return {
                "success": False,
                "records_processed": 0,
                "records_inserted": 0,
                "records_deleted": 0,
                "records_failed": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
                "target_week": week_label,
                "summary": f"Aborted before processing week {week_label}",
            }

        if not get_connection_pool():
            raise RuntimeError("Database connection not available")

        with db_connection() as conn:
            # Diagnostic Queries
            total_candidates = conn.execute(
                _TOTAL_CANDIDATES_SQL,
                {"start_date": start_date, "end_date": end_date},
            ).fetchone().total_count

            invalid_timestamps = conn.execute(
                _INVALID_TIMESTAMP_SQL,
                {"start_date": start_date, "end_date": end_date},
            ).fetchone().invalid_count
            
            missing_timestamps = conn.execute(
                _MISSING_TIMESTAMP_SQL,
                {"start_date": start_date, "end_date": end_date},
            ).fetchone().missing_count

            # Delete / Insert Transaction
            deleted = conn.execute(
                _DELETE_WEEK_SQL,
                {"week_start_date": start_date},
            )
            rows_deleted = deleted.rowcount or 0

            inserted = conn.execute(
                _INSERT_WEEKLY_MTTR_SQL,
                {
                    "week_start_date": start_date,
                    "start_date": start_date,
                    "end_date": end_date,
                },
            )

            inserted_rows = inserted.fetchall()
            total_inserted = len(inserted_rows)

            group_keys = {
                (row[0], row[1], row[2]) for row in inserted_rows
            } if inserted_rows else set()

            groups_processed = len(group_keys)
            valid_processed = max(total_candidates - invalid_timestamps - missing_timestamps, 0)

        duration = (datetime.now() - start_time).total_seconds()

        logger.info(
            f"Completed Excalibur Weekly MTTR Aggregation for week {week_label} "
            f"in {duration:.2f}s"
        )
        logger.info(
            f"Candidates: {total_candidates} | Missing Timestamps: {missing_timestamps} | "
            f"Invalid (Negative) Timestamps: {invalid_timestamps} | "
            f"Rows deleted: {rows_deleted} | Rows inserted: {total_inserted} | "
            f"Groups processed: {groups_processed}"
        )

        return {
            "success": True,
            "records_processed": valid_processed,
            "records_inserted": total_inserted,
            "records_deleted": rows_deleted,
            "records_failed": invalid_timestamps + missing_timestamps,
            "groups_processed": groups_processed,
            "duration_seconds": duration,
            "target_week": week_label,
            "summary": (
                f"Processed week {week_label}: {valid_processed} valid incidents, "
                f"{invalid_timestamps} invalid/negative filtered, "
                f"{rows_deleted} old rows deleted, {total_inserted} rows inserted"
            ),
        }

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        error_msg = f"Excalibur Weekly MTTR Aggregation ETL failed: {e}"

        logger.error(error_msg)
        logger.error(traceback.format_exc())

        return {
            "success": False,
            "records_processed": 0,
            "records_inserted": 0,
            "records_deleted": 0,
            "records_failed": 1,
            "duration_seconds": duration,
            "error": error_msg,
            "target_week": week_label,
            "summary": f"Failed: {error_msg}",
        }


def run_excalibur_weekly_mttr_aggregation_etl_with_monitoring():
    """Scheduler/orchestrator wrapper."""
    try:
        result = run_excalibur_weekly_mttr_aggregation_etl(_default_target_date())

        if result["success"]:
            logger.info(f"ETL completed: {result['summary']}")
        else:
            logger.error(f"ETL failed: {result['summary']}")

        return result

    except Exception as e:
        logger.error(f"ETL monitoring wrapper failed: {e}")
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "summary": f"ETL monitoring wrapper failed: {e}",
        }


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Excalibur Weekly MTTR Aggregation ETL"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date within the week (YYYY-MM-DD)",
    )

    args = parser.parse_args()

    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            logger.error("Invalid date format. Use YYYY-MM-DD (e.g. 2026-03-16)")
            sys.exit(1)
    else:
        target_date = _default_target_date()

    result = run_excalibur_weekly_mttr_aggregation_etl(target_date)

    logger.info("=" * 50)
    if result["success"]:
        logger.info(f"SUCCESS | {result['summary']}")
        logger.info(f"Duration: {result['duration_seconds']:.2f}s")
    else:
        logger.error(f"FAILED | {result.get('error', 'Unknown error')}")

    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
