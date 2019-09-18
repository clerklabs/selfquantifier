import os

from clerkai.utils import (add_all_untracked_and_changed_files,
                           current_gitcommit_datetime, current_gitsha1,
                           ensure_clerkai_folder_versioning,
                           list_files_in_clerk_subfolder,
                           merge_changes_from_previous_possibly_edited_df,
                           possibly_edited_commit_specific_df,
                           propagate_previous_edits_from_across_columns,
                           short_gitsha1)


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
        _ = list_files_in_clerk_subfolder(
            transactions_folder_path, clerkai_folder_path, repo
        )
        _["Include"] = None
        _["Account provider"] = None
        _["Account"] = None
        _["Content type"] = None
        _["History reference"] = current_history_reference()
        return _[
            [
                "File name",
                "File path",
                "Include",
                "Account provider",
                "Account",
                "Content type",
                "File metadata",
                "History reference",
            ]
        ]

    def list_receipt_files_in_receipts_folder():
        _ = list_files_in_clerk_subfolder(
            receipts_folder_path, clerkai_folder_path, repo
        )
        _["History reference"] = current_history_reference()
        return _

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

    def possibly_edited_df(current_commit_df, record_type, editable_columns, keep_unmerged_previous_edits=False):

        # set config based on record type
        if record_type == "transaction_files":
            export_file_name = "Transaction files.xlsx"
        elif record_type == "transactions":
            export_file_name = "Transactions.xlsx"
        else:
            raise ValueError("record_type '%s' not recognized" % record_type)

        # TODO: check if edit for the head commit already exists - in which case simply use it

        # include earlier edits
        _edit_files_df = list_edit_files_in_edits_folder()
        edit_files_df = _edit_files_df[_edit_files_df["File name"] == export_file_name]
        _previous_edit_files = edit_files_df[
            edit_files_df["Related history reference"] != current_history_reference()
        ]

        # all earlier edits should have been incorporated in the
        # most recent previous edit file, so we can restrict to only merge edits
        # from that file
        previous_edit_files = _previous_edit_files.tail(1)

        def possibly_edited_commit_specific_df_by_edit_file_row(edit_file):
            edit_file[
                "previous_possibly_edited_df"
            ] = possibly_edited_commit_specific_df(
                df=None,
                export_file_name=export_file_name,
                edits_folder_path=edits_folder_path,
                commit_datetime=edit_file["Related history reference date"],
                history_reference=edit_file["Related history reference"],
                create_if_not_exists=False,
            )
            return edit_file

        edit_files_with_previous_possibly_edited_df = previous_edit_files.apply(
            possibly_edited_commit_specific_df_by_edit_file_row, axis=1
        )

        df_with_previous_edits_across_columns = current_commit_df
        columns_to_drop_after_propagation_of_previous_edits = []
        for index, edit_file in edit_files_with_previous_possibly_edited_df.iterrows():
            (
                df_with_previous_edits_across_columns,
                additional_columns_to_drop_after_propagation_of_previous_edits,
            ) = merge_changes_from_previous_possibly_edited_df(
                df=df_with_previous_edits_across_columns,
                edit_file=edit_file,
                record_type=record_type,
                clerkai_folder_path=clerkai_folder_path,
                current_history_reference=current_history_reference,
                keep_unmerged_previous_edits=keep_unmerged_previous_edits,
            )
            columns_to_drop_after_propagation_of_previous_edits = [
                *columns_to_drop_after_propagation_of_previous_edits,
                *additional_columns_to_drop_after_propagation_of_previous_edits,
            ]

        df_with_previous_edits = propagate_previous_edits_from_across_columns(
            df_with_previous_edits_across_columns, previous_edit_files, editable_columns
        )

        # clean up irrelevant old columns (should have been merged and propagated already)
        if not keep_unmerged_previous_edits:
            clean_df_with_previous_edits = df_with_previous_edits.drop(
                columns_to_drop_after_propagation_of_previous_edits, axis=1
            )
        else:
            print("Keeping potential old edits and columns for reference")
            clean_df_with_previous_edits = df_with_previous_edits

        # make sure that the merged editable df file is available in the most current location
        possibly_edited_df_with_previous_edits = possibly_edited_commit_specific_df(
            df=clean_df_with_previous_edits,
            export_file_name=export_file_name,
            edits_folder_path=edits_folder_path,
            commit_datetime=current_gitcommit_datetime(repo),
            history_reference=current_history_reference(),
            create_if_not_exists=True,
        )

        return possibly_edited_df_with_previous_edits

    return (
        current_history_reference,
        list_transactions_files_in_transactions_folder,
        list_receipt_files_in_receipts_folder,
        list_edit_files_in_edits_folder,
        list_transactions_files_in_downloads_folder,
        clerkai_file_path,
        possibly_edited_df,
    )
