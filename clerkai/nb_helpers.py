import os

from clerkai.utils import (add_all_untracked_and_changed_files,
                           current_gitcommit_datetime, current_gitsha1,
                           ensure_clerkai_folder_versioning,
                           list_files_in_clerk_subfolder)


def extract_commit_sha_from_edit_subfolder_path(edit_subfolder_path):
    import re
    p = re.compile('\\(([^\\)]*)\\)', re.IGNORECASE)
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
    downloads_folder_path = os.path.expanduser(downloads_folder)
    pictures_folder_path = os.path.expanduser(pictures_folder)

    # set working dir to be the clerk.ai folder
    os.chdir(clerkai_folder_path)

    # initiate / validate clerk.ai-folder versioning
    repo = ensure_clerkai_folder_versioning(clerkai_folder_path)
    add_all_untracked_and_changed_files(repo)

    def current_history_reference():
        return current_gitsha1(repo)

    # some helper functions
    def list_transactions_files_in_transactions_folder():
        _ = list_files_in_clerk_subfolder(transactions_folder_path, clerkai_folder_path, repo)
        _["Historic reference"] = current_gitsha1(repo)
        return _

    def list_receipt_files_in_receipts_folder():
        _ = list_files_in_clerk_subfolder(receipts_folder_path, clerkai_folder_path, repo)
        _["Historic reference"] = current_gitsha1(repo)
        return _

    def list_edit_files_in_edits_folder():
        _ = list_files_in_clerk_subfolder(edits_folder_path, clerkai_folder_path, repo)
        if len(_) > 0:
            _["Related historic reference"] = _["File path"].apply(extract_commit_sha_from_edit_subfolder_path)
        return _

    # TODO: make this guess which ones are transactions
    def list_transactions_files_in_downloads_folder():
        transaction_files = []
        for file_name in os.listdir(downloads_folder_path):
            if "Transaktioner" in file_name:
                transaction_files.append(file_name)
        return transaction_files

    def clerkai_file_path(file):
        return os.path.join(file["File path"].replace("@", clerkai_folder_path), file["File name"])

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

    def possibly_edited_df(df, export_file_name):
        export_columns = df.columns
        import time
        commit_specific_directory = "%s (%s)" % (time.strftime(
            "%Y-%m-%d %H%M", current_gitcommit_datetime(repo)), current_gitsha1(repo))
        commit_specific_directory_path = os.path.join(edits_folder_path, commit_specific_directory)
        if not os.path.isdir(commit_specific_directory_path):
            os.mkdir(commit_specific_directory_path)
        xlsx_path = os.path.join(commit_specific_directory_path, export_file_name)
        import pandas as pd
        if not os.path.isfile(xlsx_path):
            print("Saving '%s/%s'" % (commit_specific_directory, export_file_name))
            with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
                df[export_columns].to_excel(writer, sheet_name="Data", index=False, freeze_panes=(1, 0))
        return pd.read_excel(os.path.join(commit_specific_directory_path, export_file_name))

    return (
        current_history_reference,
        list_transactions_files_in_transactions_folder,
        list_receipt_files_in_receipts_folder,
        list_edit_files_in_edits_folder,
        list_transactions_files_in_downloads_folder,
        clerkai_file_path,
        possibly_edited_df
    )
