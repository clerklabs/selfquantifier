import os

import pandas as pd


def transactions_flow(
    transaction_files_editable_columns,
    transactions_editable_columns,
    list_transactions_files_in_transactions_folder,
    possibly_edited_df,
    transactions_folder_path,
    acknowledge_changes_in_clerkai_folder,
    clerkai_input_file_path,
    current_history_reference,
    keep_unmerged_previous_edits=False,
    failfast=False,
):
    transaction_files_df = list_transactions_files_in_transactions_folder()
    record_type = "transaction_files"
    transaction_files_first_columns = [
        "File name",
        "File path",
        *transaction_files_editable_columns,
    ]
    transaction_files_export_columns = [
        *transaction_files_first_columns,
        *transaction_files_df.columns.difference(transaction_files_first_columns),
    ]
    # print("transaction_files_export_columns", transaction_files_export_columns)
    transaction_files_export_df = transaction_files_df.reindex(
        transaction_files_export_columns, axis=1
    )
    possibly_edited_transaction_files_df = possibly_edited_df(
        transaction_files_export_df,
        record_type,
        transaction_files_editable_columns,
        keep_unmerged_previous_edits,
    )

    included_transaction_files = possibly_edited_transaction_files_df[
        possibly_edited_transaction_files_df["Ignore"] != 1
    ]

    # make sure that the edited column values yields new commits
    # so that edit-files are dependent on the editable values
    transaction_files_editable_data_df = included_transaction_files[
        transaction_files_editable_columns + ["File metadata"]
    ]
    save_transaction_files_editable_data_in_transactions_folder(
        transactions_folder_path, transaction_files_editable_data_df
    )
    acknowledge_changes_in_clerkai_folder()

    from clerkai.transactions.parse import parse_transaction_files

    parsed_transaction_files = parse_transaction_files(
        included_transaction_files, clerkai_input_file_path, failfast
    )

    unsuccessfully_parsed_transaction_files = parsed_transaction_files[
        ~parsed_transaction_files["Error"].isnull()
    ].drop(["File metadata", "Parse results"], axis=1)

    successfully_parsed_transaction_files = parsed_transaction_files[
        parsed_transaction_files["Error"].isnull()
    ].drop(["Error"], axis=1)

    if len(successfully_parsed_transaction_files) > 0:
        # concat all transactions
        all_parsed_transactions_df = pd.concat(
            successfully_parsed_transaction_files["Parse results"].values, sort=False
        ).reset_index(drop=True)
        all_parsed_transactions_df["History reference"] = current_history_reference()
        # include transaction_files data
        all_parsed_transactions_df = pd.merge(
            all_parsed_transactions_df,
            included_transaction_files.drop(["Ignore"], axis=1).add_prefix(
                "Source transaction file: "
            ),
            left_on="Source transaction file index",
            right_index=True,
            suffixes=(False, False),
        )

        # print("all_parsed_transactions_df.columns", all_parsed_transactions_df.columns)

        transactions_df = all_parsed_transactions_df.drop_duplicates(subset=["ID"])

        # export all transactions to xlsx
        record_type = "transactions"

        transactions_first_columns = [
            "Account",
            *transactions_editable_columns,
            "Real Date",
            "Bank Date",
            "Date",
            "Year",
            "Month",
        ]
        transactions_export_columns = [
            *transactions_first_columns,
            *transactions_df.columns.difference(transactions_first_columns, sort=False),
        ]
        # print("transactions_export_columns", transactions_export_columns)
        transactions_export_df = transactions_df.reindex(
            transactions_export_columns, axis=1
        )

        # convert Decimal columns to float prior to export or excel will treat them as strings
        # todo: less hacky conversion of Decimal-columns
        from clerkai.transactions.parsers.parse_utils import is_nan

        def float_if_not_nan(number):
            if is_nan(number) or number is None:
                return None
            return float(number)

        transactions_export_df["Amount"] = transactions_export_df["Amount"].apply(
            float_if_not_nan
        )
        transactions_export_df["Balance"] = transactions_export_df["Balance"].apply(
            float_if_not_nan
        )
        transactions_export_df["Foreign Currency Amount"] = transactions_export_df[
            "Foreign Currency Amount"
        ].apply(float_if_not_nan)
        transactions_export_df["Foreign Currency Rate"] = transactions_export_df[
            "Foreign Currency Rate"
        ].apply(float_if_not_nan)

        possibly_edited_transactions_df = possibly_edited_df(
            transactions_export_df,
            record_type,
            transactions_editable_columns,
            keep_unmerged_previous_edits=False,
        )

    else:
        all_parsed_transactions_df = []
        transactions_df = []
        possibly_edited_transactions_df = []

    return (
        transaction_files_df,
        possibly_edited_transaction_files_df,
        unsuccessfully_parsed_transaction_files,
        successfully_parsed_transaction_files,
        all_parsed_transactions_df,
        transactions_df,
        possibly_edited_transactions_df,
    )


def save_transaction_files_editable_data_in_transactions_folder(
    transactions_folder_path, transaction_files_editable_data_df
):
    csv = transaction_files_editable_data_df.to_csv(index=False)
    file_path = os.path.join(
        transactions_folder_path, "transaction_files_editable_data.csv"
    )
    with open(file_path, "w") as f:
        f.write(csv)