import os

from clerkai.location_history.defaults import (
    location_history_by_date_editable_columns,
    location_history_files_editable_columns)
from clerkai.location_history.flow import location_history_flow
from clerkai.time_tracking.defaults import (
    default_time_tracking_entries_editable_columns,
    default_time_tracking_files_editable_columns)
from clerkai.time_tracking.flow import time_tracking_flow
from clerkai.transactions.defaults import (
    default_transaction_files_editable_columns,
    default_transactions_editable_columns)
from clerkai.transactions.flow import transactions_flow
from clerkai.utils import (add_all_untracked_and_changed_files,
                           commit_datetime_from_history_reference,
                           commits_by_short_gitsha1, current_gitsha1,
                           ensure_clerkai_folder_versioning,
                           export_file_name_by_record_type,
                           fetch_gsheets_worksheet_as_df,
                           list_files_in_clerk_input_subfolder,
                           list_files_in_clerk_subfolder,
                           possibly_edited_commit_specific_df,
                           possibly_edited_df_util)


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
    clerkai_folder_path = os.path.expanduser(clerkai_folder).rstrip(os.sep)
    clerkai_input_folder_path = os.path.join(clerkai_folder_path, "Input")
    transactions_folder_path = os.path.join(clerkai_input_folder_path, "Transactions")
    receipts_folder_path = os.path.join(clerkai_input_folder_path, "Receipts")
    time_tracking_folder_path = os.path.join(clerkai_input_folder_path, "Time Tracking")
    location_history_folder_path = os.path.join(
        clerkai_input_folder_path, "Location History"
    )
    edits_folder_path = os.path.join(clerkai_folder_path, "Edits")
    downloads_folder_path = os.path.expanduser(downloads_folder)
    pictures_folder_path = os.path.expanduser(pictures_folder)

    # set working dir to be the clerk.ai folder
    os.chdir(clerkai_folder_path)

    # initiate / validate clerk.ai-folder versioning
    clerkai_input_folder_repo = ensure_clerkai_folder_versioning(
        clerkai_input_folder_path=clerkai_input_folder_path
    )

    def acknowledge_changes_in_clerkai_input_folder():
        add_all_untracked_and_changed_files(clerkai_input_folder_repo)

    def current_history_reference():
        return current_gitsha1(clerkai_input_folder_repo)

    # transactions

    def transactions(
        keep_unmerged_previous_edits=False,
        failfast=False,
        additional_transaction_files_editable_columns=None,
        additional_transactions_editable_columns=None,
    ):
        if additional_transaction_files_editable_columns:
            transaction_files_editable_columns = [
                *additional_transaction_files_editable_columns,
                *default_transaction_files_editable_columns,
            ]
        else:
            transaction_files_editable_columns = (
                default_transaction_files_editable_columns
            )
        if additional_transactions_editable_columns:
            transactions_editable_columns = [
                *additional_transactions_editable_columns,
                *default_transactions_editable_columns,
            ]
        else:
            transactions_editable_columns = default_transactions_editable_columns
        return transactions_flow(
            transaction_files_editable_columns=transaction_files_editable_columns,
            transactions_editable_columns=transactions_editable_columns,
            clerkai_input_folder_path=clerkai_input_folder_path,
            possibly_edited_df=possibly_edited_df,
            transactions_folder_path=transactions_folder_path,
            acknowledge_changes_in_clerkai_input_folder=acknowledge_changes_in_clerkai_input_folder,
            current_history_reference=current_history_reference,
            keep_unmerged_previous_edits=keep_unmerged_previous_edits,
            failfast=failfast,
        )

    # receipts

    def list_receipt_files_in_receipts_folder():
        _ = list_files_in_clerk_input_subfolder(
            receipts_folder_path, clerkai_input_folder_path=clerkai_input_folder_path
        )
        if len(_) == 0:
            return _
        _["Ignore"] = None
        _["History reference"] = current_history_reference()
        return _[
            ["File name", "File path", "Ignore", "File metadata", "History reference"]
        ]

    # location_history

    def location_history(keep_unmerged_previous_edits=False, failfast=False):
        return location_history_flow(
            location_history_files_editable_columns=location_history_files_editable_columns,
            location_history_by_date_editable_columns=location_history_by_date_editable_columns,
            clerkai_input_folder_path=clerkai_input_folder_path,
            possibly_edited_df=possibly_edited_df,
            location_history_folder_path=location_history_folder_path,
            acknowledge_changes_in_clerkai_input_folder=acknowledge_changes_in_clerkai_input_folder,
            current_history_reference=current_history_reference,
            keep_unmerged_previous_edits=keep_unmerged_previous_edits,
            failfast=failfast,
        )

    # time_tracking_entries

    def time_tracking_entries(
        keep_unmerged_previous_edits=False,
        failfast=False,
        additional_time_tracking_files_editable_columns=None,
        additional_time_tracking_entries_editable_columns=None,
    ):
        if additional_time_tracking_files_editable_columns:
            time_tracking_files_editable_columns = [
                *additional_time_tracking_files_editable_columns,
                *default_time_tracking_files_editable_columns,
            ]
        else:
            time_tracking_files_editable_columns = (
                default_time_tracking_files_editable_columns
            )
        if additional_time_tracking_entries_editable_columns:
            time_tracking_entries_editable_columns = [
                *additional_time_tracking_entries_editable_columns,
                *default_time_tracking_entries_editable_columns,
            ]
        else:
            time_tracking_entries_editable_columns = (
                default_time_tracking_entries_editable_columns
            )
        return time_tracking_flow(
            time_tracking_files_editable_columns=time_tracking_files_editable_columns,
            time_tracking_entries_editable_columns=time_tracking_entries_editable_columns,
            clerkai_input_folder_path=clerkai_input_folder_path,
            possibly_edited_df=possibly_edited_df,
            time_tracking_folder_path=time_tracking_folder_path,
            acknowledge_changes_in_clerkai_input_folder=acknowledge_changes_in_clerkai_input_folder,
            current_history_reference=current_history_reference,
            keep_unmerged_previous_edits=keep_unmerged_previous_edits,
            failfast=failfast,
        )

    # other

    def list_edit_files_in_edits_folder():
        _ = list_files_in_clerk_subfolder(
            edits_folder_path, clerkai_folder_path=clerkai_folder_path
        )
        if len(_) == 0:
            return _
        # ignore those in the archive subfolder
        _ = _[~_["File path"].str.contains("\\/Archive\\/", regex=True)]
        # add commit-metadata to list
        commits = commits_by_short_gitsha1(
            clerkai_input_folder_path, clerkai_input_folder_repo
        )
        _["Related history reference"] = _["File path"].apply(
            extract_commit_sha_from_edit_subfolder_path
        )
        _["Related history reference date"] = _["Related history reference"].apply(
            commit_datetime_from_history_reference, commits=commits
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
            clerkai_input_folder_path,
            clerkai_input_folder_repo,
        )

    def store_gsheets_edits(gsheets_title, gsheets_sheet_name, edits_df, record_type):
        from datetime import datetime

        """
        print(
            gsheets_title,
            gsheets_sheet_name,
            edits_df.columns,
            edits_df["History reference"].unique(),
        )
        """

        history_references = edits_df["History reference"].unique()

        if len(history_references) > 1:
            raise Exception("Edited data should only contain rows from a single export")

        history_reference = str(history_references[0])

        (dt, micro) = datetime.utcnow().strftime("%Y-%m-%d %H%M%S.%f").split(".")
        timestamp = "%s%03d" % (dt, int(micro) / 1000)

        suffix = ".gsheets.%s.%s.%s" % (gsheets_title, gsheets_sheet_name, timestamp)
        (export_file_name, export_file_name_base) = export_file_name_by_record_type(
            record_type, suffix=suffix
        )

        commits = commits_by_short_gitsha1(
            clerkai_input_folder_path, clerkai_input_folder_repo
        )
        commit_datetime = commit_datetime_from_history_reference(
            history_reference, commits=commits
        )

        edit_folder_stored_edits_df = possibly_edited_commit_specific_df(
            df=edits_df,
            record_type=record_type,
            export_file_name=export_file_name,
            edits_folder_path=edits_folder_path,
            commit_datetime=commit_datetime,
            history_reference=history_reference,
            create_if_not_exists=True,
            create_if_exists=True,
        )

        return edit_folder_stored_edits_df

    def download_and_store_gsheets_edits(
        gsheets_client, gsheets_title, gsheets_sheet_name, record_type
    ):
        edits_df = fetch_gsheets_worksheet_as_df(
            gsheets_client, gsheets_title, gsheets_sheet_name
        )
        store_gsheets_edits(gsheets_title, gsheets_sheet_name, edits_df, record_type)

    return (
        transactions,
        list_receipt_files_in_receipts_folder,
        location_history,
        time_tracking_entries,
        list_transactions_files_in_downloads_folder,
        acknowledge_changes_in_clerkai_input_folder,
        store_gsheets_edits,
        download_and_store_gsheets_edits,
    )
