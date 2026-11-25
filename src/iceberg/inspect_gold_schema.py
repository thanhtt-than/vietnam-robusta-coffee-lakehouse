"""Read-only inventory of all tables, schemas and candidate keys in lakehouse.gold.

Run with:
    .\run_spark.ps1 -Script "src/iceberg/inspect_gold_schema.py"

The script does not create, update, merge or delete any Iceberg table.
It prints the inventory and writes TXT + JSON reports under reports/schema_inspection.
"""

import json
import os
import re
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


CATALOG = "lakehouse"
NAMESPACE = "gold"
OUTPUT_DIR = "/workspace/reports/schema_inspection"

# Candidate natural-key columns are included alongside *_key and *_id columns.
NATURAL_KEY_NAMES = {
    "date",
    "full_date",
    "trade_date",
    "source_code",
    "currency_pair",
    "contract_symbol",
    "symbol",
    "observation_id",
}

KEY_NAME_PATTERN = re.compile(r"(^id$|_id$|_key$)", re.IGNORECASE)


def quote_identifier(value):
    return "`" + value.replace("`", "``") + "`"


def log(lines, message=""):
    print(message, flush=True)
    lines.append(str(message))


def is_candidate_key_column(column_name):
    lower = column_name.lower()
    return bool(KEY_NAME_PATTERN.search(lower)) or lower in NATURAL_KEY_NAMES


def build_spark():
    return (
        SparkSession.builder
        .appName("TLCN-Inspect-Gold-Schema-And-Keys")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "http://nessie:19120/iceberg/main/",
        )
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    lines = []
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "namespace": f"{CATALOG}.{NAMESPACE}",
        "read_only": True,
        "tables": [],
        "candidate_foreign_keys": [],
        "notes": [
            "Iceberg/Spark schemas do not necessarily enforce PRIMARY KEY or FOREIGN KEY constraints.",
            "Candidate keys are profiled from actual data: non-NULL and unique.",
            "Candidate foreign keys are inferred only from dimension *_key columns and compatible child column names; verify business meaning before documenting them as formal relationships.",
        ],
    }

    try:
        namespace = f"{CATALOG}.{NAMESPACE}"
        log(lines, "=" * 88)
        log(lines, f"GOLD SCHEMA INVENTORY: {namespace}")
        log(lines, "MODE: READ ONLY - NO ICEBERG TABLE WILL BE WRITTEN")
        log(lines, "=" * 88)

        table_rows = spark.sql(f"SHOW TABLES IN {namespace}").collect()
        table_names = sorted(
            row["tableName"]
            for row in table_rows
            if not row["isTemporary"]
        )

        log(lines, f"TABLE_COUNT: {len(table_names)}")
        if not table_names:
            log(lines, "NO_TABLES_FOUND")
            return

        dataframes = {}
        table_profiles = {}

        # 1. Enumerate every table, its full schema and candidate key statistics.
        for table_name in table_names:
            full_name = (
                f"{CATALOG}.{NAMESPACE}.{quote_identifier(table_name)}"
            )
            df = spark.table(full_name)
            dataframes[table_name] = df

            row_count = df.count()
            fields = []
            candidate_columns = [
                field for field in df.schema.fields
                if is_candidate_key_column(field.name)
            ]

            agg_exprs = [F.count(F.lit(1)).alias("__row_count")]
            for index, field in enumerate(candidate_columns):
                col = F.col(quote_identifier(field.name))
                agg_exprs.append(
                    F.countDistinct(col).alias(f"__distinct_{index}")
                )
                agg_exprs.append(
                    F.sum(F.when(col.isNull(), 1).otherwise(0)).alias(
                        f"__nulls_{index}"
                    )
                )

            stats = df.agg(*agg_exprs).first().asDict()

            for field in df.schema.fields:
                field_info = {
                    "name": field.name,
                    "type": field.dataType.simpleString(),
                    "nullable": field.nullable,
                    "metadata": field.metadata or {},
                }
                fields.append(field_info)

            key_profiles = []
            for index, field in enumerate(candidate_columns):
                distinct_count = int(stats.get(f"__distinct_{index}") or 0)
                null_count = int(stats.get(f"__nulls_{index}") or 0)
                is_unique_non_null = (
                    row_count > 0
                    and null_count == 0
                    and distinct_count == row_count
                )

                profile = {
                    "column": field.name,
                    "type": field.dataType.simpleString(),
                    "distinct_values": distinct_count,
                    "null_values": null_count,
                    "is_unique_non_null_candidate": is_unique_non_null,
                }
                key_profiles.append(profile)

            table_profile = {
                "table": table_name,
                "full_name": full_name,
                "row_count": row_count,
                "columns": fields,
                "candidate_key_profiles": key_profiles,
            }
            report["tables"].append(table_profile)
            table_profiles[table_name] = table_profile

            log(lines, "")
            log(lines, "-" * 88)
            log(lines, f"TABLE: {full_name}")
            log(lines, f"ROW_COUNT: {row_count}")
            log(lines, f"COLUMN_COUNT: {len(fields)}")
            log(lines, "SCHEMA:")
            for field in fields:
                nullability = "NULLABLE" if field["nullable"] else "NOT_NULL"
                log(
                    lines,
                    f"  - {field['name']} : {field['type']} "
                    f"[{nullability}]"
                )

            log(lines, "CANDIDATE KEYS (profiled from data):")
            if not key_profiles:
                log(lines, "  - No key-like/natural-key columns detected")
            else:
                for key in key_profiles:
                    status = (
                        "UNIQUE + NOT NULL"
                        if key["is_unique_non_null_candidate"]
                        else "NOT A UNIQUE NON-NULL KEY"
                    )
                    log(
                        lines,
                        f"  - {key['column']} ({key['type']}): "
                        f"distinct={key['distinct_values']}, "
                        f"nulls={key['null_values']} => {status}"
                    )

        # 2. Infer possible foreign keys from unique dimension *_key columns.
        #    These are candidates only, not declared Iceberg constraints.
        log(lines, "")
        log(lines, "=" * 88)
        log(lines, "CANDIDATE FOREIGN KEY RELATIONSHIPS")
        log(lines, "=" * 88)

        dimensions = [
            name for name in table_names
            if name.lower().startswith("dim_")
        ]

        for parent_name in dimensions:
            parent_profile = table_profiles[parent_name]
            parent_df = dataframes[parent_name]

            parent_keys = [
                item for item in parent_profile["candidate_key_profiles"]
                if item["column"].lower().endswith("_key")
                and item["is_unique_non_null_candidate"]
            ]

            for parent_key in parent_keys:
                parent_col = parent_key["column"]
                parent_values = (
                    parent_df
                    .select(F.col(quote_identifier(parent_col)).alias("__pk"))
                    .where(F.col("__pk").isNotNull())
                    .distinct()
                )

                for child_name in table_names:
                    if child_name == parent_name:
                        continue

                    child_df = dataframes[child_name]
                    child_fields = {
                        field.name.lower(): field.name
                        for field in child_df.schema.fields
                    }

                    # Exact match (source_key) or suffix match
                    # (trade_date_key -> date_key).
                    child_candidates = [
                        original_name
                        for lower_name, original_name in child_fields.items()
                        if lower_name == parent_col.lower()
                        or lower_name.endswith("_" + parent_col.lower())
                    ]

                    for child_col in child_candidates:
                        child_values = (
                            child_df
                            .select(
                                F.col(quote_identifier(child_col)).alias("__fk")
                            )
                            .where(F.col("__fk").isNotNull())
                            .distinct()
                        )

                        child_distinct = child_values.count()
                        unmatched = (
                            child_values.join(
                                parent_values,
                                child_values["__fk"] == parent_values["__pk"],
                                "left_anti",
                            ).count()
                        )
                        null_count = child_df.filter(
                            F.col(quote_identifier(child_col)).isNull()
                        ).count()

                        relationship = {
                            "child_table": child_name,
                            "child_column": child_col,
                            "parent_table": parent_name,
                            "parent_column": parent_col,
                            "child_distinct_non_null_values": child_distinct,
                            "unmatched_values": unmatched,
                            "null_values": null_count,
                            "all_values_match_parent": (
                                child_distinct > 0 and unmatched == 0
                            ),
                        }
                        report["candidate_foreign_keys"].append(relationship)

                        status = (
                            "ALL NON-NULL VALUES MATCH"
                            if relationship["all_values_match_parent"]
                            else "UNMATCHED VALUES FOUND"
                        )
                        log(
                            lines,
                            f"{child_name}.{child_col} -> "
                            f"{parent_name}.{parent_col} | "
                            f"distinct={child_distinct}, "
                            f"unmatched={unmatched}, nulls={null_count} "
                            f"=> {status}"
                        )

        if not report["candidate_foreign_keys"]:
            log(lines, "NO_CANDIDATE_FOREIGN_KEYS_DETECTED")

        # 3. Summary of likely primary/surrogate keys.
        log(lines, "")
        log(lines, "=" * 88)
        log(lines, "SUMMARY: UNIQUE NON-NULL KEY CANDIDATES")
        log(lines, "=" * 88)

        for table_profile in report["tables"]:
            unique_keys = [
                key["column"]
                for key in table_profile["candidate_key_profiles"]
                if key["is_unique_non_null_candidate"]
            ]
            log(
                lines,
                f"{table_profile['table']}: "
                + (", ".join(unique_keys) if unique_keys else "NONE VERIFIED")
            )

        # 4. Save reports to the shared project workspace.
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        txt_path = os.path.join(OUTPUT_DIR, "gold_schema_inventory.txt")
        json_path = os.path.join(OUTPUT_DIR, "gold_schema_inventory.json")

        with open(txt_path, "w", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")

        with open(json_path, "w", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2, default=str)

        log(lines, "")
        log(lines, f"TXT_REPORT: {txt_path}")
        log(lines, f"JSON_REPORT: {json_path}")
        log(lines, "GOLD_SCHEMA_INVENTORY_COMPLETED")
        log(lines, "NO_ICEBERG_TABLES_WRITTEN")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
