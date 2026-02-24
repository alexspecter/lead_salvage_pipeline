"""
Phase 3 Runner - Merge & Verify

Handles:
1. Splitting rows into final (CLEAN) and rejected
2. Verification of outputs
3. Writing CSV files with dynamic field preservation
"""

import pandas as pd
import os
from typing import List, Dict, Any, Set

from lead_cleaner.types import LeadRow, RowStatus
from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.phase3_merge.verifier import Verifier
from lead_cleaner.config import DEFAULT_OUTPUT_DIR, PRESERVE_ALL_FIELDS
from lead_cleaner.utils.rejection_cache import RejectionCache

PHASE_3_MERGE = "PHASE_3"


class Phase3Runner:
    def __init__(
        self, logger: PipelineLogger, run_id: str, rejection_cache: RejectionCache
    ):
        self.logger = logger
        self.run_id = run_id
        self.verifier = Verifier(logger)
        self.rejection_cache = rejection_cache

    def process(self, all_rows: List[LeadRow], output_dir: str = DEFAULT_OUTPUT_DIR):
        self.logger.log_event(PHASE_3_MERGE, "START")

        # ... (rest of the logic remains similar until writing outputs) ...
        # CONDITIONAL EMPTY NAME REJECTION
        # Only applies if dataset has first_name AND last_name columns
        from lead_cleaner.config import MISSING_VALUE_PLACEHOLDER

        # Check schema from first row's clean_data
        if all_rows:
            sample_keys = set(all_rows[0].get("clean_data", {}).keys())
            has_name_cols = "first_name" in sample_keys and "last_name" in sample_keys

            if has_name_cols:
                empty_name_count = 0
                for r in all_rows:
                    if r["status"] == RowStatus.CLEAN:
                        cd = r.get("clean_data", {})
                        fn = cd.get("first_name", "")
                        ln = cd.get("last_name", "")

                        # Check if both are effectively empty
                        fn_empty = not fn or fn == MISSING_VALUE_PLACEHOLDER
                        ln_empty = not ln or ln == MISSING_VALUE_PLACEHOLDER

                        if fn_empty and ln_empty:
                            r["status"] = RowStatus.REJECTED
                            r["failure_reason"] = "EMPTY_NAME"
                            empty_name_count += 1

                if empty_name_count > 0:
                    self.logger.log_event(
                        PHASE_3_MERGE,
                        "EMPTY_NAME_REJECTION",
                        reason=f"Rejected {empty_name_count} rows with empty first_name AND last_name",
                    )

        # Split
        final_rows = [r for r in all_rows if r["status"] == RowStatus.CLEAN]
        rejected_rows = [r for r in all_rows if r["status"] == RowStatus.REJECTED]

        # Handle stragglers (rows still in AI_REQUIRED state)
        stragglers = [
            r
            for r in all_rows
            if r["status"] not in (RowStatus.CLEAN, RowStatus.REJECTED)
        ]
        if stragglers:
            self.logger.log_event(
                PHASE_3_MERGE,
                "WARNING",
                reason=f"{len(stragglers)} rows have unfinished status. Marking REJECTED.",
            )
            for r in stragglers:
                r["status"] = RowStatus.REJECTED
                r["failure_reason"] = "UNFINISHED_PROCESSING"
                rejected_rows.append(r)

        self.verifier.verify_outputs(all_rows, final_rows, rejected_rows)

        # Write Outputs
        os.makedirs(output_dir, exist_ok=True)

        # Flatten for CSV with dynamic field support
        # CLEAN output gets minimal metadata (only row_id) and reordered columns
        final_df = self._flatten_rows(
            final_rows, include_raw=False, minimal_metadata=True
        )
        # REJECT store handles everything for caching
        rejected_df = self._flatten_rows(
            rejected_rows, include_raw=True, minimal_metadata=False
        )

        final_path = os.path.join(output_dir, f"final_output_{self.run_id}.csv")
        # Legacy rejected path (optional, but we use the cache now)
        # rejected_path = os.path.join(output_dir, f"reject_store_{self.run_id}.csv")

        final_df.to_csv(final_path, index=False)

        # Use RejectionCache for rows
        if not rejected_df.empty:
            self.rejection_cache.cache_rejected_rows(rejected_df)

        # EXPORT LOG TO USER_OUTPUT as requested
        import shutil

        log_dest = os.path.join(output_dir, f"pipeline_log_{self.run_id}.csv")
        try:
            shutil.copy2(self.logger.log_file, log_dest)
            self.logger.log_event(
                PHASE_3_MERGE, "LOG_EXPORTED", reason=f"Log copied to {log_dest}"
            )
        except Exception as e:
            self.logger.log_error(PHASE_3_MERGE, "LOG_EXPORT_FAILED", e)

        # SAVE REJECTION CACHE (commits columns/values/files logs)
        self.rejection_cache.save()

        self.logger.log_event(
            PHASE_3_MERGE, "COMPLETE", reason=f"Written to {output_dir}"
        )

    def _flatten_rows(
        self,
        rows: List[LeadRow],
        include_raw: bool = False,
        minimal_metadata: bool = False,
    ) -> pd.DataFrame:
        """
        Flattens LeadRow objects to a DataFrame.
        """
        # ... (mostly same as original until the end where columns are dropped)
        if not rows:
            return pd.DataFrame()

        flat_data = []

        # Collect all possible clean_data keys for consistent columns
        all_clean_keys: Set[str] = set()
        if PRESERVE_ALL_FIELDS:
            for r in rows:
                all_clean_keys.update(r.get("clean_data", {}).keys())

        internal_keys = [
            "run_id",
            "confidence_score",
            "failure_reason",
            "row_id",
            "status",
        ]
        RESERVED_KEYS = set(internal_keys + ["status"])

        for r in rows:
            # Metadata columns
            if minimal_metadata:
                item: Dict[str, Any] = {"row_id": r["row_id"]}
            else:
                item: Dict[str, Any] = {
                    "row_id": r["row_id"],
                    "run_id": r["run_id"],
                    "status": r["status"].value
                    if hasattr(r["status"], "value")
                    else r["status"],
                    "confidence_score": r["confidence_score"],
                    "failure_reason": r["failure_reason"].value
                    if hasattr(r.get("failure_reason"), "value")
                    else r.get("failure_reason"),
                }

            clean_data = r.get("clean_data", {})
            for key in all_clean_keys:
                final_key = key
                if key in RESERVED_KEYS:
                    final_key = f"{key}_original"
                item[final_key] = clean_data.get(key)

            for key, value in clean_data.items():
                final_key = key
                if key in RESERVED_KEYS:
                    final_key = f"{key}_original"
                if final_key not in item:
                    item[final_key] = value

            if include_raw:
                for k, v in r.get("raw_data", {}).items():
                    item[f"raw_{k}"] = v

            # Extract line number for sorting (critical for order preservation)
            raw_data = r.get("raw_data", {})
            if "_line_number" in raw_data:
                item["_sorting_index"] = raw_data["_line_number"]
            else:
                item["_sorting_index"] = float("inf")  # Append to end if missing

            flat_data.append(item)

        df = pd.DataFrame(flat_data)

        # Sort by original input order
        if "_sorting_index" in df.columns:
            df = df.sort_values("_sorting_index")
            df = df.drop(columns=["_sorting_index"])

        # USER REQUEST: Strict Column Ordering
        # row_id, first_name, last_name, email, phone, position (job_title), company...

        # 1. Define explicit priority list
        explicit_priority = [
            "row_id",
        ]

        # DYNAMIC ID PRIORITY: Find any column that looks like an ID and put it right after row_id
        # Look for "id", "employee_id", "employee id", "user_id"
        id_candidates = []
        for col in df.columns:
            c_low = col.lower().strip()
            if c_low in [
                "id",
                "employee_id",
                "employee id",
                "user_id",
                "user id",
                "legacy_id",
            ]:
                id_candidates.append(col)

        # Sort candidates to be deterministic (shortest first, usually "id")
        id_candidates.sort(key=len)

        explicit_priority.extend(id_candidates)

        # Add 'name' field (for datasets with single name column instead of first/last)
        explicit_priority.append("name")

        explicit_priority.extend(
            [
                "first_name",
                "last_name",
                "email",
                "phone",
                "job_title",
                "position",
                "company",
            ]
        )

        # 2. Identify remaining columns
        other_cols = []
        raw_cols = []

        for col in df.columns:
            if col in explicit_priority:
                continue

            if col.startswith("raw_"):
                raw_cols.append(col)
            else:
                other_cols.append(col)

        # Sort lists for consistency
        raw_cols.sort()
        other_cols.sort()

        # 3. Construct Final Order
        final_order = []

        # Add explicit priority cols if they exist
        for col in explicit_priority:
            if col in df.columns:
                final_order.append(col)

        # Add remaining standard cols
        final_order.extend(other_cols)

        # Add raw cols (usually suppressed in final output, but kept for audit if needed)
        final_order.extend(raw_cols)

        # Ensure all columns are covered (safety check)
        covered = set(final_order)
        missing = [c for c in df.columns if c not in covered]
        final_order.extend(missing)

        df_ordered = df[final_order]

        renamed_cols = sorted(
            [c for c in df_ordered.columns if c.endswith("_original")]
        )
        final_rename_map = {}
        top_columns = set(df_ordered.columns)

        for col in renamed_cols:
            if col.endswith("_original"):
                base_name = col[:-9]
                if base_name not in top_columns:
                    final_rename_map[col] = base_name
                    top_columns.add(base_name)

        if final_rename_map:
            df_ordered = df_ordered.rename(columns=final_rename_map)

        # USER REQUEST: Ensure 'job_title' is displayed as 'position'
        # If we have both, drop the likely raw 'position' and rename 'job_title' -> 'position'
        if "job_title" in df_ordered.columns:
            if "position" in df_ordered.columns:
                df_ordered = df_ordered.drop(columns=["position"])
            df_ordered = df_ordered.rename(columns={"job_title": "position"})

        # FINAL CLEANUP: Drop columns that are entirely "Not Provided", Empty, or None
        # AND LOG THEM TO REJECTION CACHE
        from lead_cleaner.config import MISSING_VALUE_PLACEHOLDER

        cols_to_drop = []
        for col in df_ordered.columns:
            # Get unique values (converted to string for easier check)
            # Handle potential duplicate columns returning a DataFrame
            col_data = df_ordered[col]
            if isinstance(col_data, pd.DataFrame):
                # If duplicates, taking the first one is a safe fallback for checking emptiness
                col_data = col_data.iloc[:, 0]

            unique_vals = set(col_data.astype(str).unique())

            is_empty = True
            for val in unique_vals:
                v = val.strip()
                if (
                    v
                    and v.lower() != "nan"
                    and v.lower() != "none"
                    and v != MISSING_VALUE_PLACEHOLDER
                ):
                    is_empty = False
                    break

            if is_empty:
                cols_to_drop.append(col)
                # Log column rejection only if minimal metadata (final output)
                if minimal_metadata:
                    self.rejection_cache.add_column_rejection(
                        col, f"Entirely empty or {MISSING_VALUE_PLACEHOLDER}"
                    )

        if "_line_number" in df_ordered.columns:
            cols_to_drop.append("_line_number")

        if cols_to_drop:
            df_ordered = df_ordered.drop(columns=cols_to_drop)

        return df_ordered
