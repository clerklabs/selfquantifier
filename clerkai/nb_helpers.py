import os

import pandas as pd

from clerkai.utils import (add_all_untracked_and_changed_files,
                           current_gitsha1, ensure_clerkai_folder_versioning,
                           list_files_in_clerk_subfolder,
                           possibly_edited_df_util, short_gitsha1)


def extract_commit_sha_from_edit_subfolder_path(edit_subfolder_path):
    import re

    p = re.compile("\\(([^)]*)\\)", re.IGNORECASE)
    m = p.search(edit_subfolder_path)
    commit_sha = None
    if len(m.groups()) > 0:
        commit_sha = m.groups()[0]
    return commit_sha


def init_notebook_and_return_helpers(clerkai_folder, downloads_folder, pictures_folder):
    # expand given paths to absolute paths
    clerkai_folder_path = os.path.expanduser(clerkai_folder)
    transactions_folder_path = os.path.join(clerkai_folder_path, "Transactions")
    receipts_folder_path = os.path.join(clerkai_folder_path, "Receipts")
    edits_folder_path = os.path.join(clerkai_folder_path, "Edits")
    travels_folder_path = os.path.join(clerkai_folder_path, "Travels")
    downloads_folder_path = os.path.expanduser(downloads_folder)
    pictures_folder_path = os.path.expanduser(pictures_folder)

    # set working dir to be the clerk.ai folder
    os.chdir(clerkai_folder_path)

    # initiate / validate clerk.ai-folder versioning
    repo = ensure_clerkai_folder_versioning(clerkai_folder_path)

    def acknowledge_changes_in_clerkai_folder():
        add_all_untracked_and_changed_files(repo)

    def current_history_reference():
        return current_gitsha1(repo)

    # some helper functions
    def list_transactions_files_in_transactions_folder():
        _ = list_files_in_clerk_subfolder(
            transactions_folder_path, clerkai_folder_path, repo
        )
        _["Ignore"] = None
        _["Account provider"] = None
        _["Account"] = None
        _["Content type"] = None
        _["History reference"] = current_history_reference()
        return _[
            [
                "File name",
                "File path",
                "Ignore",
                "Account provider",
                "Account",
                "Content type",
                "File metadata",
                "History reference",
            ]
        ]

    transaction_files_editable_columns = [
        "Ignore",
        "Account provider",
        "Account",
        "Content type",
        "Account currency",
    ]

    transactions_editable_columns = [
        "Include in expense report",
        "Walletsharing",
        "Real Date (Corrected)",
        "Days between real and bank dates",
        "Doc Source",
        "Clarification",
    ]
    # Fiscal year (incl tax entity)	Doc	Expense report	Doc status	Doc type	Account	Date initiated	Date settled	Source text	Merchant	Hash	Transaction id	Amount (Incl. VAT)	Balance	Original amount (In local currency)	Local currency	Account Owner	Comments / Notes	Doc notes	Doc filename	Doc link	Doc inbox search	Sorting ordinal	Legacy Id	Date initiated value	Date settled value	Absolute amount	Absolute original amount	Vendor	Category	Description	Status	Invoice date	Paid date	Source	Amount	Currency	Status

    def transactions(keep_unmerged_previous_edits=False, failfast=False):

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

        # make sure that the edited column values yields new commits so that transaction-edit-files are dependent on the editable values
        transaction_files_editable_data_df = included_transaction_files[
            transaction_files_editable_columns + ["File metadata"]
        ]
        save_transaction_files_editable_data_in_transactions_folder(
            transaction_files_editable_data_df
        )
        acknowledge_changes_in_clerkai_folder()

        from clerkai.transactions.parse import parse_transaction_files

        parsed_transaction_files = parse_transaction_files(
            included_transaction_files, clerkai_file_path, failfast
        )

        unsuccessfully_parsed_transaction_files = parsed_transaction_files[
            ~parsed_transaction_files["Error"].isnull()
        ].drop(["File metadata", "Parse results"], axis=1)

        successfully_parsed_transaction_files = parsed_transaction_files[
            parsed_transaction_files["Error"].isnull()
        ].drop(["Error"], axis=1)

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

        return (
            transaction_files_df,
            possibly_edited_transaction_files_df,
            unsuccessfully_parsed_transaction_files,
            successfully_parsed_transaction_files,
            all_parsed_transactions_df,
            transactions_df,
            possibly_edited_transactions_df,
        )

    def list_receipt_files_in_receipts_folder():
        _ = list_files_in_clerk_subfolder(
            receipts_folder_path, clerkai_folder_path, repo
        )
        _["Ignore"] = None
        _["History reference"] = current_history_reference()
        return _[
            ["File name", "File path", "Ignore", "File metadata", "History reference"]
        ]

    def list_edit_files_in_edits_folder():
        _ = list_files_in_clerk_subfolder(edits_folder_path, clerkai_folder_path, repo)
        if len(_) > 0:
            from pydriller import RepositoryMining

            commits_iterator = RepositoryMining(
                clerkai_folder_path, filepath="."
            ).traverse_commits()
            commits = {}
            for commit in commits_iterator:
                history_reference = short_gitsha1(repo, commit.hash)
                commits[history_reference] = commit
            _["Related history reference"] = _["File path"].apply(
                extract_commit_sha_from_edit_subfolder_path
            )

            def commit_datetime_from_history_reference(history_reference):
                return commits[history_reference].author_date

            _["Related history reference date"] = _["Related history reference"].apply(
                commit_datetime_from_history_reference
            )
            _ = _.sort_values(by="Related history reference date")
        return _

    # TODO: make this guess which ones are transactions
    def list_transactions_files_in_downloads_folder():
        transaction_files = []
        for file_name in os.listdir(downloads_folder_path):
            if "Transaktioner" in file_name:
                transaction_files.append(file_name)
        return transaction_files

    def clerkai_file_path(file):
        return os.path.join(
            file["File path"].replace("@", clerkai_folder_path), file["File name"]
        )

    # ensure_directories_are_in_place()
    if not os.path.isdir(transactions_folder_path):
        os.mkdir(transactions_folder_path)
    if not os.path.isdir(receipts_folder_path):
        os.mkdir(receipts_folder_path)
    if not os.path.isdir(edits_folder_path):
        os.mkdir(edits_folder_path)
    if not os.path.isdir(downloads_folder_path):
        raise Exception("Downloads folder missing")
    if not os.path.isdir(pictures_folder_path):
        raise Exception("Pictures folder missing")

    def possibly_edited_df(
        current_commit_df,
        record_type,
        editable_columns,
        keep_unmerged_previous_edits=False,
    ):
        return possibly_edited_df_util(
            current_commit_df,
            record_type,
            editable_columns,
            keep_unmerged_previous_edits,
            list_edit_files_in_edits_folder,
            current_history_reference,
            edits_folder_path,
            clerkai_folder_path,
            repo,
        )

    def save_transaction_files_editable_data_in_transactions_folder(
        transaction_files_editable_data_df
    ):
        csv = transaction_files_editable_data_df.to_csv(index=False)
        file_path = os.path.join(
            transactions_folder_path, "transaction_files_editable_data.csv"
        )
        with open(file_path, "w") as f:
            f.write(csv)

    return (
        acknowledge_changes_in_clerkai_folder,
        current_history_reference,
        transactions,
        list_receipt_files_in_receipts_folder,
        list_edit_files_in_edits_folder,
        list_transactions_files_in_downloads_folder,
        clerkai_file_path,
        possibly_edited_df,
        save_transaction_files_editable_data_in_transactions_folder,
    )
