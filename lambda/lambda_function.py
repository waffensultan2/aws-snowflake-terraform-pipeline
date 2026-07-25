# REMEMBER TO ZIP WHEN DONE:
# Compress-Archive -Path "lambda_function.py" -DestinationPath "lambda.zip"

# NOTE:
#     Write passed rows ->    "passed/<table>.json"
#     Write rejected rows ->  "rejected/<table>.json"

# Non-empty files
## Audit
# acctian_lo_strl_purpose.trl [PARSER BUILT]
## Transaction
# lo_stl_frontend.trl [PARSER BUILT]
# lo_stl_online_application.trl [PARSER BUILT]
# lms_noncash_collection.trl [PARSER BUILT]
# lms_stl_disbursement_master.trl [PARSER BUILT]
# pf_employer_master.trl [PARSER BUILT]

# -- DQ --
# 1. Binary parsing [DONE]
# 2. Header parsing [DONE]
# 3. Field parsing [DONE]
# 4. RISK-01 handling [NOT DONE]
# 5. DQ rules [NOT DONE]

# -- CODE --
# 1. Cleanup the code [DONE]
# 2. Convert string to int if possible ('101' -> 101) [DONE]
# 3. DQ Checks <--- DO THIS TOMORROW!!!!!
# 4. RISK-01 handling (empty files)
# 5. Pass passed/rejected rows to "passed/<table>" or "rejected/<table>"

import struct
import uuid
from datetime import datetime, timezone


def lambda_handler():
    files = [
        # "acctian_lo_stl_purpose.trl",
        "lo_stl_frontend.trl",
        # "lo_stl_online_application.trl",
        # "lms_noncash_collection.trl",
        # "lms_stl_disbursement_master.trl",
        # "pf_employer_master.trl",
    ]

    for file in files:
        result = parser(file)

        print(result)

        if result:
            print(len(result))


def convert_value_to_int(value):
    # Try integer conversion
    try:
        return int(value)
    except (ValueError, TypeError):
        pass

    return value


def parser(filename):
    file_info = {
        "lo_stl_frontend.trl": {
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
        "lo_stl_online_application.trl": {
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
        "lms_noncash_collection.trl": {
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
        "lms_stl_disbursement_master.trl": {
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
        "pf_employer_master.trl": {
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

    if filename == "acctian_lo_stl_purpose.trl":
        with open(
            f"../hdmf_dummy_trl_files/{filename}", "rb"
        ) as file:  # Remove this later and directly read from event
            return parse_acctian_lo_stl_purpose(file)
    if filename in file_info:
        result = []

        def read_field(record, pos):
            length = struct.unpack_from("<H", record, pos)[0]
            value = record[pos + 2 : pos + 2 + length].decode("ascii")
            field_size = 2 + length
            padded_size = ((field_size + 7) // 8) * 8
            next_pos = pos + padded_size

            return value, next_pos

        with open(f"../hdmf_dummy_trl_files/{filename}", "rb") as file:
            while True:
                record = file.read(file_info[filename]["stride"])

                if len(record) < file_info[filename]["stride"]:
                    break

                source_user = record[12:21].decode("ascii").rstrip("\x00")
                cdc_op = record[44:50].decode("ascii").rstrip("\x00")

                year = int.from_bytes(record[2:4], "little")
                month = record[4]
                day = record[6]
                txn_sequence = int.from_bytes(record[8:12], "little")

                pos = 56
                fields = {
                    "_source_user": source_user,
                    "_cdc_op": cdc_op,
                    "_source_ts": f"{year:04d}-{month:02d}-{day:02d}",
                    "_raw_id": f"acctian_lo_stl_purpose_{txn_sequence}",
                    "_source_file": "acctian_lo_stl_purpose.trl",
                    "_ingested_at": datetime.now(timezone.utc).isoformat(),
                    "_batch_id": str(uuid.uuid4()),
                }
                for field in file_info[filename]["fields"]:
                    value, pos = read_field(record, pos)
                    fields[field] = convert_value_to_int(value)

                result.append(fields)

        return result


def parse_acctian_lo_stl_purpose(file):
    result = []

    while True:
        record = file.read(337)

        if len(record) < 337:
            break

        source_user = record[12:21].decode("ascii").rstrip("\x00")
        cdc_op = record[44:50].decode("ascii").rstrip("\x00")

        year = int.from_bytes(record[2:4], "little")
        month = record[4]
        day = record[6]
        txn_sequence = int.from_bytes(record[8:12], "little")

        purpose_code = record[58:64].decode("ascii").rstrip("\x00")

        # Loan type always comes after the marker 04 00
        marker = b"\x04\x00"

        idx = record.find(marker, 66)

        purpose_description = record[66:idx].decode("ascii").rstrip("\x00")
        loan_type_code = record[idx + 2 : idx + 6].decode("ascii")

        fields = {
            "_source_user": source_user,
            "_cdc_op": cdc_op,
            "_source_ts": f"{year:04d}-{month:02d}-{day:02d}",
            "_raw_id": f"acctian_lo_stl_purpose_{txn_sequence}",
            "_source_file": "acctian_lo_stl_purpose.trl",
            "_ingested_at": datetime.now(timezone.utc).isoformat(),
            "_batch_id": str(uuid.uuid4()),
            "purpose_code": convert_value_to_int(purpose_code),
            "purpose_description": purpose_description,
            "loan_type_code": convert_value_to_int(loan_type_code),
        }

        result.append(fields)

    return result


# def parse_lo_stl_frontend(file):
#     result = []

#     def read_field(record, pos):
#         length = struct.unpack_from("<H", record, pos)[0]
#         value = record[pos + 2 : pos + 2 + length].decode("ascii")
#         field_size = 2 + length
#         padded_size = ((field_size + 7) // 8) * 8
#         next_pos = pos + padded_size

#         return value, next_pos

#     while True:
#         record = file.read(1311)

#         if len(record) < 1311:
#             break

#         username = record[12:21].decode("ascii").rstrip("\x00")
#         operation = record[44:50].decode("ascii").rstrip("\x00")

#         fields = {"username": username, "operation": operation}
#         pos = 56

#         for name in [
#             "loan_id",
#             "loan_type",
#             "member_no",
#             "member_no_alt",
#             "employer_id",
#             "process_time",
#             "last_name",
#             "first_name",
#             "middle_name",
#             "branch_code",
#             "hub_code",
#             "area_code",
#             "loan_status",
#             "scheme_code",
#             "purpose_code",
#             "release_mode",
#             "gross_amount",
#             "deductions",
#             "net_amount",
#             "process_branch",
#         ]:
#             fields[name], pos = read_field(record, pos)

#         result.append(fields)

#     return result


# def parse_lo_stl_online_application(file):
#     result = []

#     def read_field(record, pos):
#         length = struct.unpack_from("<H", record, pos)[0]  # 2-byte length tag
#         value = record[pos + 2 : pos + 2 + length].decode("ascii")
#         field_size = 2 + length
#         padded_size = ((field_size + 7) // 8) * 8  # round up to multiple of 8
#         next_pos = pos + padded_size
#         return value, next_pos

#     with open("../hdmf_dummy_trl_files/lo_stl_online_application.trl", "rb") as file:
#         while True:
#             record = file.read(1119)

#             if len(record) < 337:
#                 break

#             username = record[12:21].decode("ascii").rstrip("\x00")
#             operation = record[44:50].decode("ascii").rstrip("\x00")

#             pos = 56  # same header size as the loan .trl file

#             fields = {"username": username, "operation": operation}

#             for name in [
#                 "loan_id",
#                 "app_ref_no",
#                 "member_no",
#                 "last_name",
#                 "first_name",
#                 "middle_name",
#                 "email",
#                 "mobile_no",
#                 "loan_type",
#                 "term_months",
#                 "account_no",
#                 "employer_id",
#                 "branch_code",
#                 "status",
#                 "purpose_code",
#             ]:
#                 fields[name], pos = read_field(record, pos)

#             result.append(fields)

#     return result


# def parse_lms_noncash_collection(file):
#     result = []

#     def read_field(record, pos):
#         length = struct.unpack_from("<H", record, pos)[0]
#         value = record[pos + 2 : pos + 2 + length].decode("ascii")
#         field_size = 2 + length
#         padded_size = ((field_size + 7) // 8) * 8
#         next_pos = pos + padded_size

#         return value, next_pos

#     with open("../hdmf_dummy_trl_files/lms_noncash_collection.trl", "rb") as file:
#         while True:
#             record = file.read(517)

#             if len(record) < 517:
#                 break

#             username = record[12:21].decode("ascii").rstrip("\x00")
#             operation = record[44:50].decode("ascii").rstrip("\x00")

#             pos = 56

#             fields = {"username": username, "operation": operation}
#             for name in [
#                 "collection_id",
#                 "member_no",
#                 "loan_id",
#                 "collection_type",
#                 "amount_code",
#                 "batch_ref",
#                 "branch_code",
#             ]:
#                 fields[name], pos = read_field(record, pos)

#             result.append(fields)

#     return result


# def parse_lms_stl_disbursement_master(file):
#     result = []

#     def read_field(record, pos):
#         length = struct.unpack_from("<H", record, pos)[0]
#         value = record[pos + 2 : pos + 2 + length].decode("ascii")
#         field_size = 2 + length
#         padded_size = ((field_size + 7) // 8) * 8
#         next_pos = pos + padded_size

#         return value, next_pos

#     with open("../hdmf_dummy_trl_files/lms_stl_disbursement_master.trl", "rb") as file:
#         while True:
#             record = file.read(471)

#             if len(record) < 471:
#                 break

#             username = record[12:21].decode("ascii").rstrip("\x00")
#             operation = record[44:50].decode("ascii").rstrip("\x00")

#             pos = 56
#             fields = {"username": username, "operation": operation}
#             for name in [
#                 "disbursement_id",
#                 "batch_id",
#                 "batch_id_alt",
#                 "borrower_name",
#                 "process_time",
#                 "branch_code",
#                 "processed_by",
#             ]:
#                 fields[name], pos = read_field(record, pos)

#             result.append(fields)

#     return result


# def parse_pf_employer_master(file):
#     result = []

#     def read_field(record, pos):
#         length = struct.unpack_from("<H", record, pos)[0]
#         value = record[pos + 2 : pos + 2 + length].decode("ascii")
#         field_size = 2 + length
#         padded_size = ((field_size + 7) // 8) * 8
#         next_pos = pos + padded_size

#         return value, next_pos

#     with open("../hdmf_dummy_trl_files/pf_employer_master.trl", "rb") as file:
#         while True:
#             record = file.read(1414)

#             if len(record) < 1414:
#                 break

#             pos = 56
#             fields = {}
#             for name in [
#                 "employer_id_raw",
#                 "employer_id",
#                 "employer_id_alt",
#                 "tin_no",
#                 "branch_code",
#                 "employer_type",
#                 "employer_name",
#                 "address",
#                 "contact_no",
#                 "fax_no",
#                 "employer_short_name",
#                 "employee_count",
#                 "mobile_no",
#                 "city_code",
#                 "status_code",
#                 "region_code",
#             ]:
#                 fields[name], pos = read_field(record, pos)

#             result.append(fields)

#     return result


if __name__ == "__main__":
    lambda_handler()
