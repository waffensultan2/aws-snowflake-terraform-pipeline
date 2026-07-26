import json
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import col, trim, when
from pyspark.sql.types import DoubleType

NUMERIC_COLUMNS = {
    "lo_stl_frontend": ["gross_amount", "deductions", "net_amount"],
    "lo_stl_online_application": ["term_months"],
    "lms_noncash_collection": ["amount_code"],
    "pf_employer_master": ["employee_count"],
}


all_log_rows = []  # Later for our migration log


def clean_numeric_columns(df, table_name):
    cols = NUMERIC_COLUMNS.get(table_name, [])
    for c in cols:
        df = df.withColumn(
            c,
            when(trim(col(c).cast("string")) == "", None)
            .otherwise(col(c))
            .cast(DoubleType()),
        )
    return df


def get_snowflake_creds():
    client = boto3.client("secretsmanager")
    secret = client.get_secret_value(SecretId="waffen-hdmf-snowflake-credentials")
    return json.loads(secret["SecretString"])


creds = get_snowflake_creds()

sf_options = {
    "sfURL": f"{creds['account']}.snowflakecomputing.com",
    "sfUser": creds["user"],
    "sfPassword": creds["password"],
    "sfDatabase": creds["database"],
    "sfSchema": creds["schema"],
    "sfWarehouse": creds["warehouse"],
    "column_mapping": "name",  # <-- add this
}


def write_to_snowflake(df, table_name, column_order=None):
    if column_order:
        df = df.select(*column_order)
    df.write.format("net.snowflake.spark.snowflake").options(**sf_options).option(
        "dbtable", table_name
    ).mode("append").save()


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = "waffen-migration-pipeline-bucket"  # replace with your actual bucket
AUDIT_TABLES = {"acctian_lo_stl_purpose"}
TRANSACTION_TABLES = [
    "lo_stl_frontend",
    "lo_stl_online_application",
    "lms_noncash_collection",
    "lms_stl_disbursement_master",
    "pf_employer_master",
]

# ---------- Helper: find the primary key field per table ----------
PRIMARY_KEYS = {
    "lo_stl_frontend": "loan_id",
    "lo_stl_online_application": "loan_id",
    "lms_noncash_collection": "collection_id",
    "lms_stl_disbursement_master": "disbursement_id",
    "pf_employer_master": "employer_id",
    "acctian_lo_stl_purpose": "purpose_code",
}


def load_passed_records(table_name):
    """Read all passed/<table>/*.json files for a table into a list of dicts."""
    path = f"passed/{table_name}/"
    df = spark.read.json(f"s3://{BUCKET}/{path}*.json")
    rows = df.collect()
    return [row.asDict(recursive=True) for row in rows]


# ---------- AUDIT TABLE LOGIC (flat load, upsert on PK) ----------
def process_audit_table(table_name):
    records = load_passed_records(table_name)
    pk = PRIMARY_KEYS[table_name]

    latest_by_pk = {}
    for r in records:
        latest_by_pk[r[pk]] = r  # later record overwrites earlier one = upsert

    curated_rows = list(latest_by_pk.values())
    output_df = spark.createDataFrame([Row(**r) for r in curated_rows])
    output_df.write.mode("overwrite").json(f"s3://{BUCKET}/curated/{table_name}/")
    print(f"[AUDIT] {table_name}: {len(curated_rows)} curated rows")

    output_df = clean_numeric_columns(output_df, table_name)
    write_to_snowflake(output_df, table_name)

    all_log_rows.append(
        {
            "table_name": table_name,
            "source_file": f"{table_name}.trl",
            "records_parsed": len(records),
            "records_staged": len(records),  # staging = all raw parsed records
            "records_curated": len(curated_rows),  # loaded to curated
            "inserts": len(
                curated_rows
            ),  # audit tables: every row is an upsert "insert"
            "updates": 0,
            "deletes": 0,
            "status": "COMPLETE",
            "risk_flag": "",
            "run_ts": datetime.now(timezone.utc).isoformat(),
        }
    )


# ---------- TRANSACTION TABLE LOGIC (SCD Type 2) ----------
def diff_columns(old, new, exclude_keys):
    """Return comma-separated list of columns whose value changed between repold and repnew."""
    changed = []
    for key in new:
        if key in exclude_keys:
            continue
        if old.get(key) != new.get(key):
            changed.append(key)
    return ",".join(changed)


def process_transaction_table(table_name):
    records = load_passed_records(table_name)
    pk = PRIMARY_KEYS[table_name]

    # IMPORTANT: process in original file order (by _raw_id's numeric suffix,
    # which reflects txn_sequence / _stage_seq order from the TRL file)
    records.sort(key=lambda r: int(r["_raw_id"].split("_")[-1]))

    # Track current open version + version counter per primary key
    current_version = {}  # pk -> current open curated row (dict)
    version_counter = {}  # pk -> int
    repold_buffer = {}  # pk -> most recent repold record, waiting for its repnew
    curated_rows = []

    metadata_exclude = {
        "_source_user",
        "_cdc_op",
        "_source_ts",
        "_raw_id",
        "_source_file",
        "_ingested_at",
        "_batch_id",
        "_dq_error",
        "scd_key",
        "scd_version",
        "eff_start_date",
        "eff_end_date",
        "is_current",
        "is_deleted",
        "change_type",
        "changed_columns",
    }

    insert_count = 0
    update_count = 0
    delete_count = 0

    for r in records:
        key = r[pk]
        op = r["_cdc_op"]
        ts = r["_source_ts"]

        if op == "append":
            insert_count += 1

            version_counter[key] = 1
            new_row = dict(r)
            new_row.update(
                {
                    "scd_key": f"{table_name}_{key}_v1_{r['_batch_id']}",
                    "scd_version": 1,
                    "eff_start_date": ts,
                    "eff_end_date": "9999-12-31T23:59:59",
                    "is_current": 1,
                    "is_deleted": 0,
                    "change_type": "INSERT",
                    "changed_columns": None,
                }
            )
            current_version[key] = new_row
            curated_rows.append(new_row)

        elif op == "repold":
            # Just buffer it — wait for the matching repnew
            repold_buffer[key] = r

        elif op == "repnew":
            update_count += 1

            old_open = current_version.get(key)
            if old_open is not None:
                # Close the previously open version
                old_open["is_current"] = 0
                old_open["eff_end_date"] = ts

            old_image = repold_buffer.pop(
                key, None
            )  # may be None (edge case, Section 7.4)
            changed_cols = (
                diff_columns(old_image, r, metadata_exclude)
                if old_image is not None
                else None
            )

            version_counter[key] = version_counter.get(key, 0) + 1
            new_row = dict(r)
            new_row.update(
                {
                    "scd_key": f"{table_name}_{key}_v{version_counter[key]}_{r['_batch_id']}",
                    "scd_version": version_counter[key],
                    "eff_start_date": ts,
                    "eff_end_date": "9999-12-31T23:59:59",
                    "is_current": 1,
                    "is_deleted": 0,
                    "change_type": "UPDATE",
                    "changed_columns": changed_cols,
                }
            )
            current_version[key] = new_row
            curated_rows.append(new_row)

        elif op == "delete":
            delete_count += 1

            old_open = current_version.get(key)
            if old_open is not None:
                old_open["is_current"] = 0
                old_open["is_deleted"] = 1
                old_open["eff_end_date"] = ts
                old_open["change_type"] = "DELETE"
            # No new row is created for delete — the closed version now shows is_deleted=1

    output_df = spark.createDataFrame([Row(**r) for r in curated_rows])
    output_df.write.mode("overwrite").json(f"s3://{BUCKET}/curated/{table_name}/")
    print(f"[TRANSACTION] {table_name}: {len(curated_rows)} curated rows")

    output_df = clean_numeric_columns(output_df, table_name)
    write_to_snowflake(output_df, table_name)

    all_log_rows.append(
        {
            "table_name": table_name,
            "source_file": f"{table_name}.trl",
            "records_parsed": len(records),
            "records_staged": len(records),
            "records_curated": len(curated_rows),
            "inserts": insert_count,
            "updates": update_count,
            "deletes": delete_count,
            "status": "COMPLETE",
            "risk_flag": "",
            "run_ts": datetime.now(timezone.utc).isoformat(),
        }
    )


# ---------- RUN ----------
for t in AUDIT_TABLES:
    process_audit_table(t)

for t in TRANSACTION_TABLES:
    process_transaction_table(t)

# ---------- RISK-01: log the 7 known empty files that never reach Glue ----------
RISK_01_TABLES = [
    "hdmf_branches",
    "hdmf_hub_master",
    "lms_transaction_status",
    "lo_stl_bank_master",
    "lo_stl_release_mode",
    "lo_stl_scheme_master",
    "membership_category",
]
for t in RISK_01_TABLES:
    all_log_rows.append(
        {
            "table_name": t,
            "source_file": f"{t}.trl",
            "records_parsed": 0,
            "records_staged": 0,
            "records_curated": 0,
            "inserts": 0,
            "updates": 0,
            "deletes": 0,
            "status": "SKIPPED",
            "risk_flag": "RISK-01",
            "run_ts": datetime.now(timezone.utc).isoformat(),
        }
    )

log_df = spark.createDataFrame(all_log_rows)
write_to_snowflake(log_df, "migration_log")

job.commit()
