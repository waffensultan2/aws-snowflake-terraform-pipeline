# Command to zip your python code
Compress-Archive -Path "lambda_function.py" -DestinationPath "lambda.zip"

# Non-empty files
    ## Audit
        acctian_lo_strl_purpose.trl
    ## Transaction
        lo_strl_frontend.trl
        lo_stl_online_application.trl
        lms_noncash_collection.trl
        lms_stl_disbursement_master.trl
        pf_employer_master.trl
        
# Empty files (RISK-01)
    hdmf_branches.trl
    hdmf_hub_master.trl
    lms_transaction_status.trl
    lo_stl_bank_master.trl
    lo_stl_release_mode.trl
    lo_stl_scheme_master.trl
    membership_category.trl
