import io
import json
import struct
import uuid
from datetime import datetime, timezone

import boto3
import pandas as pd

s3 = boto3.client("s3")


def write_to_s3(df, bucket, table_name, prefix, batch_id):
    if len(df) == 0:
        return

    records = df.to_dict(orient="records")
    body = "\n".join(json.dumps(r) for r in records)

    key = f"{prefix}/{table_name}/{table_name}_{batch_id}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=body)


def lambda_handler(event, context):
    batch_id = str(uuid.uuid4())  # one per invocation

    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]  # e.g. "raw/acctian_lo_stl_purpose.trl"

    table_name = key.split("/")[-1].replace(".trl", "")

    response = s3.get_object(Bucket=bucket, Key=key)
    data = response["Body"].read()

    # RISK-01 check
    if len(data) == 0:
        return {"status": "SKIPPED", "table": table_name, "reason": "RISK-01"}

    parsed_table = parser(table_name, data, batch_id)

    valid_df, invalid_df = validate(table_name, parsed_table)

    write_to_s3(valid_df, bucket, table_name, "passed", batch_id)
    write_to_s3(invalid_df, bucket, table_name, "rejected", batch_id)


def validate(table: str, rows: list[dict]):
    df = pd.DataFrame(rows)
    df["_dq_error"] = ""

    VALID_OPERATIONS = {"append", "repold", "repnew", "delete"}
    VALID_LOAN_STATUS = {"PEND", "APPR", "DSBR", "CLOS", "REJT"}
    VALID_RELEASE_MODE = {"CASA", "CHECK", "REMIT"}
    VALID_EMPLOYER_TYPE = {"GOV", "PRIV", "SEP"}
    VALID_STATUS_CODE = {"A", "I", "S"}
    VALID_COLLECTION_TYPE = {"SALARY", "BILLS", "OTC", "ONLINE"}

    # ---------- Common checks ----------
    df.loc[~df["_cdc_op"].isin(VALID_OPERATIONS), "_dq_error"] += (
        "Invalid CDC operation; "
    )

    years = pd.to_datetime(df["_source_ts"], errors="coerce").dt.year
    df.loc[(years < 2000) | (years > 2100), "_dq_error"] += "Invalid source timestamp; "

    # ---------- Table-specific ----------
    if table == "acctian_lo_stl_purpose":
        df.loc[df["purpose_code"].isna(), "_dq_error"] += "Missing purpose_code; "
        df.loc[df["loan_type_code"].isna(), "_dq_error"] += "Missing loan_type_code; "

    elif table == "lo_stl_frontend":
        numeric = ["gross_amount", "deductions", "net_amount"]
        df[numeric] = df[numeric].apply(pd.to_numeric, errors="coerce")

        df.loc[df["loan_id"].isna(), "_dq_error"] += "Missing loan_id; "
        df.loc[df["member_no"].isna(), "_dq_error"] += "Missing member_no; "

        df.loc[~df["loan_status"].isin(VALID_LOAN_STATUS), "_dq_error"] += (
            "Invalid loan_status; "
        )
        df.loc[~df["release_mode"].isin(VALID_RELEASE_MODE), "_dq_error"] += (
            "Invalid release_mode; "
        )

        df.loc[df["gross_amount"] < 0, "_dq_error"] += "Negative gross_amount; "
        df.loc[df["deductions"] < 0, "_dq_error"] += "Negative deductions; "
        df.loc[df["net_amount"] < 0, "_dq_error"] += "Negative net_amount; "

    elif table == "lo_stl_online_application":
        df["term_months"] = pd.to_numeric(df["term_months"], errors="coerce")

        df.loc[df["loan_id"].isna(), "_dq_error"] += "Missing loan_id; "
        df.loc[df["member_no"].isna(), "_dq_error"] += "Missing member_no; "
        df.loc[df["status"].isna(), "_dq_error"] += "Missing status; "
        df.loc[df["term_months"] < 0, "_dq_error"] += "Negative term_months; "

    elif table == "lms_noncash_collection":
        df["amount_code"] = pd.to_numeric(df["amount_code"], errors="coerce")

        df.loc[df["collection_id"].isna(), "_dq_error"] += "Missing collection_id; "
        df.loc[df["member_no"].isna(), "_dq_error"] += "Missing member_no; "
        df.loc[df["loan_id"].isna(), "_dq_error"] += "Missing loan_id; "

        df.loc[~df["collection_type"].isin(VALID_COLLECTION_TYPE), "_dq_error"] += (
            "Invalid collection_type; "
        )

        df.loc[df["amount_code"] < 0, "_dq_error"] += "Negative amount_code; "

    elif table == "lms_stl_disbursement_master":
        df.loc[df["disbursement_id"].isna(), "_dq_error"] += "Missing disbursement_id; "
        df.loc[df["batch_id"].isna(), "_dq_error"] += "Missing batch_id; "
        df.loc[df["borrower_name"].isna(), "_dq_error"] += "Missing borrower_name; "

    elif table == "pf_employer_master":
        df["employee_count"] = pd.to_numeric(df["employee_count"], errors="coerce")

        df.loc[df["employer_id"].isna(), "_dq_error"] += "Missing employer_id; "

        df.loc[~df["employer_type"].isin(VALID_EMPLOYER_TYPE), "_dq_error"] += (
            "Invalid employer_type; "
        )

        df.loc[~df["status_code"].isin(VALID_STATUS_CODE), "_dq_error"] += (
            "Invalid status_code; "
        )

        df.loc[df["employee_count"] < 0, "_dq_error"] += "Negative employee_count; "

    valid_df = df[df["_dq_error"] == ""].copy()
    invalid_df = df[df["_dq_error"] != ""].copy()

    return valid_df, invalid_df


def convert_value_to_int(value):
    if value == "":
        return None

    try:
        return int(value)
    except (ValueError, TypeError):
        return value


def retrieve_header_metadata_columns(record, table, batch_id):
    source_user = record[12:21].decode("ascii").rstrip("\x00")
    cdc_op = record[44:50].decode("ascii").rstrip("\x00")
    year = int.from_bytes(record[2:4], "little")
    month = record[4]
    day = record[6]
    txn_sequence = int.from_bytes(record[8:12], "little")

    header_metadata_fields = {
        "_source_user": source_user,
        "_cdc_op": cdc_op,
        "_source_ts": f"{year:04d}-{month:02d}-{day:02d}",
        "_raw_id": f"{table}_{txn_sequence}",
        "_source_file": f"{table}.trl",
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
        "_batch_id": batch_id,
    }

    return header_metadata_fields


def parser(table_name, data, batch_id):  # data = raw bytes from S3
    file = io.BytesIO(data)

    table_info = {
        "lo_stl_frontend": {
            "stride": 1311,
            "fields": [
                "loan_id",
                "loan_type",
                "member_no",
                "member_no_alt",
                "employer_id",
                "process_time",
                "last_name",
                "first_name",
                "middle_name",
                "branch_code",
                "hub_code",
                "area_code",
                "loan_status",
                "scheme_code",
                "purpose_code",
                "release_mode",
                "gross_amount",
                "deductions",
                "net_amount",
                "process_branch",
            ],
        },
        "lo_stl_online_application": {
            "stride": 1119,
            "fields": [
                "loan_id",
                "app_ref_no",
                "member_no",
                "last_name",
                "first_name",
                "middle_name",
                "email",
                "mobile_no",
                "loan_type",
                "term_months",
                "account_no",
                "employer_id",
                "branch_code",
                "status",
                "purpose_code",
            ],
        },
        "lms_noncash_collection": {
            "stride": 517,
            "fields": [
                "collection_id",
                "member_no",
                "loan_id",
                "collection_type",
                "amount_code",
                "batch_ref",
                "branch_code",
            ],
        },
        "lms_stl_disbursement_master": {
            "stride": 471,
            "fields": [
                "disbursement_id",
                "batch_id",
                "batch_id_alt",
                "borrower_name",
                "process_time",
                "branch_code",
                "processed_by",
            ],
        },
        "pf_employer_master": {
            "stride": 1414,
            "fields": [
                "employer_id_raw",
                "employer_id",
                "employer_id_alt",
                "tin_no",
                "branch_code",
                "employer_type",
                "employer_name",
                "address",
                "contact_no",
                "fax_no",
                "employer_short_name",
                "employee_count",
                "mobile_no",
                "city_code",
                "status_code",
                "region_code",
            ],
        },
    }

    if table_name == "acctian_lo_stl_purpose":
        return parse_acctian_lo_stl_purpose(file, batch_id)

    if table_name in table_info:
        result = []

        def read_field(record, pos):
            length = struct.unpack_from("<H", record, pos)[0]
            value = record[pos + 2 : pos + 2 + length].decode("ascii")
            field_size = 2 + length
            padded_size = ((field_size + 7) // 8) * 8
            next_pos = pos + padded_size
            return value, next_pos

        while True:
            record = file.read(table_info[table_name]["stride"])
            if len(record) < table_info[table_name]["stride"]:
                break
            header_metadata_columns = retrieve_header_metadata_columns(
                record, table_name, batch_id
            )
            pos = 56
            fields = {
                **header_metadata_columns,
            }
            for field in table_info[table_name]["fields"]:
                value, pos = read_field(record, pos)
                fields[field] = convert_value_to_int(value)
            result.append(fields)

        return result


def parse_acctian_lo_stl_purpose(file, batch_id):
    result = []

    while True:
        record = file.read(337)

        if len(record) < 337:
            break

        header_metadata_columns = retrieve_header_metadata_columns(
            record, "acctian_lo_stl_purpose", batch_id
        )

        purpose_code = record[58:64].decode("ascii").rstrip("\x00")

        # Loan type always comes after the marker 04 00
        marker = b"\x04\x00"

        idx = record.find(marker, 66)

        purpose_description = record[66:idx].decode("ascii").rstrip("\x00")
        loan_type_code = record[idx + 2 : idx + 6].decode("ascii")

        fields = {
            **header_metadata_columns,
            "purpose_code": convert_value_to_int(purpose_code),
            "purpose_description": purpose_description,
            "loan_type_code": convert_value_to_int(loan_type_code),
        }

        result.append(fields)

    return result
