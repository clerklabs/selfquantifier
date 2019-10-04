import hashlib
import os
from os.path import getsize, join

import pandas as pd


def ensure_clerkai_folder_versioning(clerkai_input_folder_path):
    from git import Repo

    if os.path.isdir(os.path.join(clerkai_input_folder_path, ".git")):
        # TODO make sure that this is a clerk-managed git repository
        repo = Repo(clerkai_input_folder_path)
    else:
        repo = Repo.init(clerkai_input_folder_path)
        # make the commits in this repo be from clerk automation by default
        config = repo.config_writer()
        config.set_value("user", "name", "Clerk.ai")
        config.set_value("user", "email", "automation@clerk.ai")
        config.release()
        # initial (empty) commit
        repo.index.commit(message="Initial commit")
    assert not repo.bare
    return repo


# add all untracked and changed files
def add_all_untracked_and_changed_files(repo):
    # track ignores within the git folder
    with open(
        os.path.join(repo.working_tree_dir, ".git", "info", "exclude"), "w"
    ) as text_file:
        text_file.write("Edits\n.~lock.*\n")
    repo.git.add("-A")
    uncommited_changes = repo.git.status("--porcelain")
    if uncommited_changes != "":
        repo.git.commit("-m", "Current files", "-a")


def short_gitsha1(repo, sha):
    short_sha = repo.git.rev_parse(sha, short=1)
    return short_sha


def current_gitsha1(repo):
    sha = repo.head.object.hexsha
    return short_gitsha1(repo, sha)


def current_gitcommit_datetime(repo):
    from datetime import datetime

    return datetime.fromtimestamp(repo.head.commit.authored_date)


def possibly_edited_df_util(
    current_commit_df,
    record_type,
    editable_columns,
    keep_unmerged_previous_edits,
    list_edit_files_in_edits_folder,
    current_history_reference,
    edits_folder_path,
    clerkai_input_folder_path,
    clerkai_input_folder_repo,
):
    # set config based on record type
    if record_type == "transaction_files":
        export_file_name = "Transaction files.xlsx"
    elif record_type == "transactions":
        export_file_name = "Transactions.xlsx"
    elif record_type == "receipt_files":
        export_file_name = "Receipt files.xlsx"
    elif record_type == "location_history_files":
        export_file_name = "Location history files.xlsx"
    elif record_type == "location_history_by_date":
        export_file_name = "Location history day-by-day.xlsx"
    else:
        raise ValueError("record_type '%s' not recognized" % record_type)

    # if edit for the head commit already exists - use it
    existing = possibly_edited_commit_specific_df(
        df=None,
        record_type=record_type,
        export_file_name=export_file_name,
        edits_folder_path=edits_folder_path,
        commit_datetime=current_gitcommit_datetime(clerkai_input_folder_repo),
        history_reference=current_history_reference(),
        create_if_not_exists=False,
    )
    if type(existing) is not bool:
        return existing

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
        edit_file["previous_possibly_edited_df"] = possibly_edited_commit_specific_df(
            df=None,
            record_type=record_type,
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
            clerkai_input_folder_path=clerkai_input_folder_path,
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
        record_type=record_type,
        export_file_name=export_file_name,
        edits_folder_path=edits_folder_path,
        commit_datetime=current_gitcommit_datetime(clerkai_input_folder_repo),
        history_reference=current_history_reference(),
        create_if_not_exists=True,
    )

    return possibly_edited_df_with_previous_edits


def possibly_edited_commit_specific_df(
    df,
    record_type,
    export_file_name,
    edits_folder_path,
    commit_datetime,
    history_reference,
    create_if_not_exists,
):
    (
        exists,
        commit_specific_directory,
        commit_specific_directory_path,
        xlsx_path,
    ) = edited_commit_specific_df_exists(
        export_file_name, edits_folder_path, commit_datetime, history_reference
    )
    if not exists:
        if create_if_not_exists:
            save_edited_commit_specific_df(
                df,
                commit_specific_directory,
                commit_specific_directory_path,
                export_file_name,
                record_type,
                xlsx_path,
            )
        else:
            return False
    return pd.read_excel(xlsx_path)


def edited_commit_specific_df_exists(
    export_file_name, edits_folder_path, commit_datetime, history_reference
):
    import pytz

    utc_commit_datetime = commit_datetime.astimezone(pytz.utc)

    commit_specific_directory = "%s (%s)" % (
        utc_commit_datetime.strftime("%Y-%m-%d %H%M"),
        history_reference,
    )
    commit_specific_directory_path = os.path.join(
        edits_folder_path, commit_specific_directory
    )
    xlsx_path = os.path.join(commit_specific_directory_path, export_file_name)

    # print("Checking if '%s/%s' exists" % (commit_specific_directory, export_file_name))
    exists = os.path.isfile(xlsx_path)
    return exists, commit_specific_directory, commit_specific_directory_path, xlsx_path


def save_edited_commit_specific_df(
    df,
    commit_specific_directory,
    commit_specific_directory_path,
    export_file_name,
    record_type,
    xlsx_path,
):
    print("Creating '%s/%s'" % (commit_specific_directory, export_file_name))
    if not os.path.isdir(commit_specific_directory_path):
        os.mkdir(commit_specific_directory_path)
    export_columns = df.columns
    export_df = df[export_columns]
    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        if record_type == "transactions":
            export_transactions_xlsx(export_df, writer)
        else:
            export_df.to_excel(
                writer, sheet_name="Data", index=False, freeze_panes=(1, 0)
            )


def export_transactions_xlsx(export_df, writer):
    from xlsxwriter.utility import xl_col_to_name

    export_df["Account"] = (
        '=INDIRECT("R[0]C[%s]", 0)&" - "&INDIRECT("R[0]C[%s]", 0)'
        % (
            export_df.columns.get_loc("Source transaction file: Account provider"),
            export_df.columns.get_loc("Source transaction file: Account"),
        )
    )
    date_formula = '=IF(INDIRECT("R[0]C[-2]", FALSE)<>"",INDIRECT("R[0]C[-2]", FALSE),INDIRECT("R[0]C[-1]", FALSE))'
    export_df["Date"] = date_formula
    export_df[
        "Year"
    ] = '=IF(INDIRECT("R[0]C[-1]", FALSE)="","",Text(INDIRECT("R[0]C[-1]", FALSE), "yyyy"))'
    export_df[
        "Month"
    ] = '=IF(INDIRECT("R[0]C[-2]", FALSE)="","",Text(INDIRECT("R[0]C[-2]", FALSE), "yyyy-mm"))'
    export_df.to_excel(writer, sheet_name="Data", index=False, freeze_panes=(1, 0))
    # adjust styles etc
    workbook = writer.book
    worksheet = workbook.get_worksheet_by_name("Data")
    default_column_width = 10
    account_column_index = export_df.columns.get_loc("Account")
    date_column_index = export_df.columns.get_loc("Date")
    # account column
    account_column_letter = xl_col_to_name(account_column_index)
    worksheet.set_column("%s:%s" % (account_column_letter, account_column_letter), 30)
    # columns between account column and date column
    worksheet.set_column(
        "%s:%s"
        % (
            xl_col_to_name(account_column_index + 1),
            xl_col_to_name(date_column_index - 1),
        ),
        default_column_width,
    )
    # date column
    date_format = workbook.add_format({"num_format": "yyyy-mm-dd"})
    date_column_letter = xl_col_to_name(date_column_index)
    last_row_number = len(export_df) + 2
    for row in range(2, last_row_number):
        worksheet.write("%s%s" % (date_column_letter, row), date_formula, date_format)
    worksheet.set_column("%s:%s" % (date_column_letter, date_column_letter), 20)
    # columns from date column to end
    last_column_index = len(export_df.columns) - 1
    last_column_letter = xl_col_to_name(last_column_index)
    worksheet.set_column(
        "%s:%s" % (xl_col_to_name(date_column_index + 1), last_column_letter),
        default_column_width,
    )


def changes_between_two_commits(repo_base_path, from_commit, to_commit):
    from pydriller import RepositoryMining

    commits_iterator = RepositoryMining(
        repo_base_path, filepath=".", from_commit=from_commit, to_commit=to_commit
    ).traverse_commits()

    # follows file renames (ignores deletes and additions, so some new paths may have been
    # subsequently deleted, and some old paths may have not existed in the from_commit
    old_to_new_paths = {}
    new_to_old_paths = {}
    old_now_deleted_paths = {}  # TODO
    old_non_existing_now_added_paths = {}  # TODO
    for commit in commits_iterator:
        for modification in commit.modifications:
            if (
                modification.old_path
                and modification.new_path
                and modification.old_path != modification.new_path
            ):
                if modification.new_path in new_to_old_paths:
                    old_to_new_paths[
                        new_to_old_paths[modification.new_path]
                    ] = modification.new_path
                    new_to_old_paths[modification.new_path] = new_to_old_paths[
                        modification.new_path
                    ]
                else:
                    old_to_new_paths[modification.old_path] = modification.new_path
                    new_to_old_paths[modification.new_path] = modification.old_path
    del new_to_old_paths
    # print("old_to_new_paths", old_to_new_paths)

    return (old_to_new_paths, old_now_deleted_paths, old_non_existing_now_added_paths)


def merge_changes_from_previous_possibly_edited_df(
    df,
    edit_file,
    record_type,
    clerkai_input_folder_path,
    current_history_reference,
    keep_unmerged_previous_edits,
):
    previous_possibly_edited_df = edit_file["previous_possibly_edited_df"]

    # set config based on record type
    if (
        record_type == "transaction_files"
        or record_type == "receipt_files"
        or record_type == "location_history_files"
    ):
        additional_join_column = None
        file_name_column_name = "File name"
        file_path_column_name = "File path"
    elif record_type == "transactions":
        additional_join_column = "ID"
        file_name_column_name = "Source transaction file: File name"
        file_path_column_name = "Source transaction file: File path"
        # Add ID column if not present in previous edits file
        if additional_join_column not in previous_possibly_edited_df.columns:
            from clerkai.transactions.parse import transaction_ids

            previous_possibly_edited_df[additional_join_column] = transaction_ids(
                previous_possibly_edited_df
            )
    elif record_type == "location_history_by_date":
        # for now simply return the current df, ignoring the previously possibly edited df
        return (df, [])
        # TODO: Support editing location history
        additional_join_column = "ID"
        file_name_column_name = "Source location history file: File name"
        file_path_column_name = "Source location history file: File path"
        # Add ID column if not present in previous edits file
        """
        if additional_join_column not in previous_possibly_edited_df.columns:
            from clerkai.location_history.parse import location_history_ids

            previous_possibly_edited_df[additional_join_column] = location_history_ids(
                previous_possibly_edited_df
            )
        """
    else:
        raise ValueError("record_type '%s' not recognized" % record_type)

    # print("df.head(), edit_file, previous_possibly_edited_df.head()")
    # print(df.head(), edit_file, previous_possibly_edited_df.head())

    # get relevant from and to commits between the old and new edit files
    from_commit = edit_file["Related history reference"]
    to_commit = current_history_reference()

    def joined_path(record):
        return "%s/%s" % (record[file_path_column_name], record[file_name_column_name])

    df["clerkai_path"] = df.apply(joined_path, axis=1)
    left_on = ["clerkai_path"]

    if from_commit != to_commit:
        (
            old_to_new_paths,
            old_now_deleted_paths,
            old_non_existing_now_added_paths,
        ) = changes_between_two_commits(
            clerkai_input_folder_path, from_commit, to_commit
        )

        previous_possibly_edited_df["clerkai_path"] = previous_possibly_edited_df.apply(
            joined_path, axis=1
        )

        def find_head_commit_corresponding_clerkai_path(clerkai_path):
            clerkai_path_key = clerkai_path.replace("@/", "")
            if clerkai_path_key in old_to_new_paths:
                return "@/%s" % old_to_new_paths[clerkai_path_key]
            else:
                # if no moves occurred just use the old path as is
                return clerkai_path

        previous_possibly_edited_df[
            "head_commit_corresponding_clerkai_path"
        ] = previous_possibly_edited_df["clerkai_path"].apply(
            find_head_commit_corresponding_clerkai_path
        )

        right_on = ["head_commit_corresponding_clerkai_path"]
    else:
        previous_possibly_edited_df["clerkai_path"] = previous_possibly_edited_df.apply(
            joined_path, axis=1
        )
        right_on = ["clerkai_path"]

    suffix = " (%s)" % from_commit

    def add_suffix(column_name):
        return "%s%s" % (column_name, suffix)

    if additional_join_column:
        left_on.append(additional_join_column)

    if additional_join_column:
        right_on.append(additional_join_column)

    suffixed_previous_possibly_edited_df = previous_possibly_edited_df.add_suffix(
        suffix
    )

    import pandas as pd

    merged_possibly_edited_df = pd.merge(
        df,
        suffixed_previous_possibly_edited_df,
        how="left" if not keep_unmerged_previous_edits else "outer",
        left_on=left_on,
        right_on=[add_suffix(column_name) for column_name in right_on],
        suffixes=(False, False),
    )

    # drop temporary merge columns
    merged_possibly_edited_df = merged_possibly_edited_df.drop(["clerkai_path"], axis=1)

    # hint that more columns may be dropped upon successful propagation of previous edits
    columns_to_drop_after_propagation_of_previous_edits = (
        suffixed_previous_possibly_edited_df.columns
    )

    return (
        merged_possibly_edited_df,
        columns_to_drop_after_propagation_of_previous_edits,
    )


def set_where_nan():
    pass


def propagate_previous_edits_from_across_columns(
    df_with_previous_edits_across_columns, previous_edit_files, editable_columns
):

    for history_reference in previous_edit_files["Related history reference"]:
        # print("history_reference", history_reference)
        suffix = " (%s)" % history_reference
        for column_name in editable_columns:
            suffixed_column_name = "%s%s" % (column_name, suffix)
            if (
                suffixed_column_name
                not in df_with_previous_edits_across_columns.columns
            ):
                pass
            else:
                if column_name in df_with_previous_edits_across_columns.columns:
                    df_where_column_is_null_mask = df_with_previous_edits_across_columns[
                        column_name
                    ].isnull()
                    df_with_previous_edits_across_columns.loc[
                        df_where_column_is_null_mask, column_name
                    ] = df_with_previous_edits_across_columns[suffixed_column_name]
                else:
                    df_with_previous_edits_across_columns[
                        column_name
                    ] = df_with_previous_edits_across_columns[suffixed_column_name]
        # print("df_with_previous_edits_across_columns.head()", df_with_previous_edits_across_columns.head())

    return df_with_previous_edits_across_columns


def is_ignored_file(filename):
    if ".DS_Store" in filename:
        return True
    if ".~lock" in filename:
        return True


def sha256sum(filename):
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, "rb", buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


def sha1sum(filename):
    h = hashlib.sha1()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, "rb", buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


def list_files_in_folder(folder_path):
    def is_not_ignored_file(filename):
        return not is_ignored_file(filename)

    all_files = []
    for root, dirs, files in os.walk(folder_path):
        # print(root, "consumes", end=" ")
        # print(sum(getsize(join(root, name)) for name in files), end=" ")
        # print("bytes in", len(files), "non-directory files")
        if ".git" in dirs:
            dirs.remove(".git")  # don't visit .git directories
        files = filter(is_not_ignored_file, files)
        # print(image_files)
        for file in list(files):
            file_sha256sum = sha256sum(join(root, file))
            file_sha1sum = sha1sum(join(root, file))
            all_files.append(
                {
                    "File name": file,
                    "File path": root,
                    "File metadata": {
                        "size": getsize(join(root, file)),
                        "sha1sum": file_sha1sum,
                        "sha256sum": file_sha256sum,
                    },
                }
            )
    return all_files


def list_files_in_clerk_subfolder(folder_path, clerkai_folder_path):
    import pandas as pd

    _ = pd.DataFrame(list_files_in_folder(folder_path))
    if len(_) > 0:
        _["File path"] = _["File path"].apply(
            lambda root: root.replace(clerkai_folder_path, "@")
        )
        # ignore *_editable_data.csv
        _ = _[~_["File name"].str.contains("_editable_data.csv$", regex=True)]
    return _


def list_files_in_clerk_input_subfolder(folder_path, clerkai_input_folder_path):
    import pandas as pd

    _ = pd.DataFrame(list_files_in_folder(folder_path))
    if len(_) > 0:
        _["File path"] = _["File path"].apply(
            lambda root: root.replace(clerkai_input_folder_path, "@")
        )
        # ignore *_editable_data.csv
        _ = _[~_["File name"].str.contains("_editable_data.csv$", regex=True)]
    return _


def is_nan(x):
    import math

    try:
        return math.isnan(x)
    except TypeError:
        return False
