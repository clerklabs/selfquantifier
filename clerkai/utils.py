import hashlib
import os
from datetime import datetime
from os.path import getsize, join

import pandas as pd
from gspread import SpreadsheetNotFound, WorksheetNotFound
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_formatting import CellFormat, Color
from gspread_formatting.dataframe import BasicFormatter, format_with_dataframe


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


def commits_by_short_gitsha1(repo_path, repo):
    from pydriller import RepositoryMining

    commits_iterator = RepositoryMining(repo_path, filepath=".").traverse_commits()
    commits = {}
    for commit in commits_iterator:
        history_reference = short_gitsha1(repo, commit.hash)
        commits[history_reference] = commit

    return commits


def commit_datetime_from_history_reference(history_reference, commits):
    first_matching_commit_history_reference_key = next(
        filter(lambda _: _.startswith(history_reference), commits.keys()), False
    )
    return commits[first_matching_commit_history_reference_key].author_date


def clerkai_input_file_path(clerkai_input_folder_path, file):
    return os.path.join(
        file["File path"].replace("@", clerkai_input_folder_path), file["File name"]
    )


def export_file_name_by_record_type(record_type, suffix=""):
    file_extension = "xlsx"
    if record_type == "transaction_files":
        export_file_name_base = "Transaction files"
    elif record_type == "transactions":
        export_file_name_base = "Transactions"
    elif record_type == "receipt_files":
        export_file_name_base = "Receipt files"
    elif record_type == "location_history_files":
        export_file_name_base = "Location history files"
    elif record_type == "location_history_by_date":
        export_file_name_base = "Location history day-by-day"
    elif record_type == "time_tracking_files":
        export_file_name_base = "Time tracking files"
    elif record_type == "time_tracking_entries":
        export_file_name_base = "Time tracking entries"
    else:
        raise ValueError("record_type '%s' not recognized" % record_type)
    export_file_name = "%s%s.%s" % (export_file_name_base, suffix, file_extension)
    return (export_file_name, export_file_name_base)


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
    (export_file_name, export_file_name_base) = export_file_name_by_record_type(
        record_type
    )

    # list of edit files
    edit_files_df = list_edit_files_in_edits_folder()
    # print("edit_files_df", edit_files_df)

    # include earlier edits
    previous_main_edit_files_mask = (edit_files_df["File name"] == export_file_name) & (
        edit_files_df["Related history reference"] != current_history_reference()
    )

    # include gsheets edits
    gsheets_edit_files_mask = edit_files_df["File name"].str.contains(
        "%s.gsheets." % export_file_name_base
    )

    unmerged_non_current_main_edit_files = edit_files_df[
        previous_main_edit_files_mask | gsheets_edit_files_mask
    ]
    # print("unmerged_non_current_main_edit_files", unmerged_non_current_main_edit_files)

    # check if edit for the head commit already exists
    main_edit_file_df = possibly_edited_commit_specific_df(
        df=None,
        record_type=record_type,
        export_file_name=export_file_name,
        edits_folder_path=edits_folder_path,
        commit_datetime=current_gitcommit_datetime(clerkai_input_folder_repo),
        history_reference=current_history_reference(),
        create_if_not_exists=False,
    )
    main_edit_file_for_the_head_commit_exists = type(main_edit_file_df) is not bool

    # if edit for the head commit already exists and no other edit files are available - use it
    if (
        main_edit_file_for_the_head_commit_exists
        and len(unmerged_non_current_main_edit_files) == 0
    ):
        print(
            "Returning existing %s.xlsx (ignoring currently parsed data)"
            % (export_file_name_base)
        )
        # just one adjustment: make sure the currently configured editable
        # columns are available on the returned dataframe (despite them not being in the xlsx)
        for editable_column in editable_columns:
            if editable_column not in main_edit_file_df:
                main_edit_file_df[editable_column] = None

        return main_edit_file_df

    # include the current main edit file df if exists and a merge is
    # imminent - or else all changes only in the main edit file will be lost
    if main_edit_file_for_the_head_commit_exists:
        print(
            "Merging edits from %s edit file(s) and %s.xlsx into a new %s.xlsx (ignoring currently parsed data)"
            % (
                len(unmerged_non_current_main_edit_files),
                export_file_name_base,
                export_file_name_base,
            )
        )
        df_with_previous_edits_across_columns = main_edit_file_df
    else:
        print(
            "Merging edits from %s edit file(s) and the currently parsed data into %s.xlsx"
            % (len(unmerged_non_current_main_edit_files), export_file_name_base)
        )
        df_with_previous_edits_across_columns = current_commit_df

    columns_to_drop_after_propagation_of_previous_edits = []
    for index, edit_file in unmerged_non_current_main_edit_files.iterrows():
        previous_possibly_edited_df_xlsx_path = os.path.join(
            edit_file["File path"].replace("@/Edits", edits_folder_path),
            edit_file["File name"],
        )
        previous_possibly_edited_df = pd.read_excel(
            previous_possibly_edited_df_xlsx_path
        )
        (
            df_with_previous_edits_across_columns,
            additional_columns_to_drop_after_propagation_of_previous_edits,
        ) = merge_changes_from_previous_possibly_edited_df(
            accumulating_df=df_with_previous_edits_across_columns,
            previous_possibly_edited_df=previous_possibly_edited_df,
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
        df_with_previous_edits_across_columns,
        unmerged_non_current_main_edit_files,
        editable_columns,
    )

    # clean up irrelevant old columns (should have been merged and propagated already)
    if not keep_unmerged_previous_edits:
        clean_df_with_previous_edits = df_with_previous_edits.drop(
            columns_to_drop_after_propagation_of_previous_edits, axis=1
        )
    else:
        print("Keeping potential old edits and columns for reference")
        clean_df_with_previous_edits = df_with_previous_edits

    def archive_edit_file(edit_file_to_archive):
        import shutil
        from datetime import datetime

        from_folder = edit_file_to_archive["File path"].replace(
            "@/Edits", edits_folder_path
        )
        to_folder = edit_file_to_archive["File path"].replace(
            "@/Edits", "@/Edits/Archive"
        ).replace("@/Edits", edits_folder_path) + (
            "/Archived %s" % datetime.today().strftime("%Y-%m-%d %H%M%S")
        )
        os.makedirs(to_folder, exist_ok=True)
        shutil.move(
            os.path.join(from_folder, edit_file_to_archive["File name"]),
            os.path.join(to_folder, edit_file_to_archive["File name"]),
        )

    # if the main edit file existed before the merging, archive it before
    main_edit_file_for_the_head_commit_mask = (
        edit_files_df["File name"] == export_file_name
    ) & (edit_files_df["Related history reference"] == current_history_reference())
    main_edit_file_for_the_head_commit = edit_files_df[
        main_edit_file_for_the_head_commit_mask
    ]
    main_edit_file_for_the_head_commit.apply(archive_edit_file, axis=1)

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

    # at this point, we have incorporated the information from the edit files that were used here
    # thus, we move them to the archive folder
    unmerged_non_current_main_edit_files.apply(archive_edit_file, axis=1)

    return possibly_edited_df_with_previous_edits


def possibly_edited_commit_specific_df(
    df,
    record_type,
    export_file_name,
    edits_folder_path,
    commit_datetime,
    history_reference,
    create_if_not_exists,
    create_if_exists=False,
):
    (
        exists,
        commit_specific_directory,
        commit_specific_directory_path,
        xlsx_path,
    ) = edited_commit_specific_df_exists(
        export_file_name, edits_folder_path, commit_datetime, history_reference
    )
    if not exists and not create_if_not_exists:
        return False
    if create_if_exists or (not exists and create_if_not_exists):
        save_edited_commit_specific_df(
            df,
            commit_specific_directory,
            commit_specific_directory_path,
            export_file_name,
            record_type,
            xlsx_path,
        )
    return pd.read_excel(xlsx_path)


def edited_commit_specific_df_exists(
    unsanitized_export_file_name, edits_folder_path, commit_datetime, history_reference
):
    import pytz
    from pathvalidate import sanitize_filename

    utc_commit_datetime = commit_datetime.astimezone(pytz.utc)

    commit_specific_directory = "%s (%s)" % (
        utc_commit_datetime.strftime("%Y-%m-%d %H%M"),
        history_reference,
    )
    commit_specific_directory_path = os.path.join(
        edits_folder_path, commit_specific_directory
    )
    export_file_name = sanitize_filename(unsanitized_export_file_name)
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


def set_export_transactions_formulas(df, eu_locale=False):
    from xlsxwriter.utility import xl_col_to_name
    import numpy as np

    def col_letter(column_name):
        return xl_col_to_name(df.columns.get_loc(column_name))

    # set formulas
    df["Account"] = '=%s[ROW]&" - "&%s[ROW]' % (
        col_letter("Source transaction file: Account provider"),
        col_letter("Source transaction file: Account"),
    )
    if eu_locale:
        df["Date"] = '=IF(%s[ROW]<>"";%s[ROW];%s[ROW])' % (
            col_letter("Real Date"),
            col_letter("Real Date"),
            col_letter("Bank Date"),
        )
        df["Year"] = '=IF(%s[ROW]="";"";TEXT(%s[ROW]; "yyyy"))' % (
            col_letter("Date"),
            col_letter("Date"),
        )
        df["Month"] = '=IF(%s[ROW]="";"";TEXT(%s[ROW]; "yyyy-mm"))' % (
            col_letter("Date"),
            col_letter("Date"),
        )
    else:
        df["Date"] = '=IF(%s[ROW]<>"",%s[ROW],%s[ROW])' % (
            col_letter("Real Date"),
            col_letter("Real Date"),
            col_letter("Bank Date"),
        )
        df["Year"] = '=IF(%s[ROW]="","",TEXT(%s[ROW], "yyyy"))' % (
            col_letter("Date"),
            col_letter("Date"),
        )
        df["Month"] = '=IF(%s[ROW]="","",TEXT(%s[ROW], "yyyy-mm"))' % (
            col_letter("Date"),
            col_letter("Date"),
        )

    df["Row number at export"] = np.arange(len(df)) + 2

    # make formulas row-specific
    def insert_row_numbers(df_row, colname):
        return df_row[colname].replace("[ROW]", str(df_row["Row number at export"]))

    df["Account"] = df.apply(insert_row_numbers, axis=1, colname="Account")
    df["Date"] = df.apply(insert_row_numbers, axis=1, colname="Date")
    df["Year"] = df.apply(insert_row_numbers, axis=1, colname="Year")
    df["Month"] = df.apply(insert_row_numbers, axis=1, colname="Month")

    return df


def export_transactions_xlsx(export_df, writer):
    from xlsxwriter.utility import xl_col_to_name

    export_df = set_export_transactions_formulas(export_df)
    export_df.to_excel(writer, sheet_name="Data", index=False, freeze_panes=(1, 0))

    # adjust styles etc
    workbook = writer.book
    worksheet = workbook.get_worksheet_by_name("Data")
    # set default column width
    default_column_width = 10
    last_column_index = len(export_df.columns) - 1
    worksheet.set_column(
        "%s:%s" % (xl_col_to_name(0), xl_col_to_name(last_column_index),),
        default_column_width,
    )
    # account column
    account_column_index = export_df.columns.get_loc("Account")
    account_column_letter = xl_col_to_name(account_column_index)
    worksheet.set_column("%s:%s" % (account_column_letter, account_column_letter), 30)
    # date column
    date_column_index = export_df.columns.get_loc("Date")
    date_format = workbook.add_format({"num_format": "yyyy-mm-dd"})
    date_column_letter = xl_col_to_name(date_column_index)
    worksheet.set_column(
        "%s:%s" % (date_column_letter, date_column_letter), None, date_format
    )
    worksheet.set_column("%s:%s" % (date_column_letter, date_column_letter), 20)
    # TODO: possibly pre-calculate values of formulas to avoid LibreOffice issue
    # see https://stackoverflow.com/questions/32205927/xlsxwriter-and-libreoffice-not-showing-formulas-result


def export_to_gsheets(
    gsheets_client,
    export_df,
    gsheets_title,
    gsheets_sheet_name,
    record_type,
    create_if_not_exists=False,
    eu_locale=False,
    editable_columns=None,
):
    # open target gsheet
    try:
        sh = gsheets_client.open(gsheets_title)
    except SpreadsheetNotFound as e:
        if create_if_not_exists:
            sh = gsheets_client.create(gsheets_title)
        else:
            raise e

    try:
        worksheet = sh.worksheet(gsheets_sheet_name)
    except WorksheetNotFound as e:
        if create_if_not_exists:
            worksheet = sh.add_worksheet(title=gsheets_sheet_name, rows=1, cols=1)
        else:
            raise e

    # set what to export
    df = export_df.copy()

    if len(worksheet.row_values(1)) > 0:
        # get existing worksheet contents
        existing_df = get_as_dataframe(worksheet)
        if len(existing_df.columns) > 0:
            # re-index columns to match the existing data + add any new columns (since the latest export/edit round)
            columns_not_in_existing_df = export_df.columns.difference(
                existing_df.columns, sort=False
            )
            df = df.reindex(
                existing_df.columns.tolist() + columns_not_in_existing_df.tolist(),
                axis=1,
            )

    # set formulas for export
    if len(df) > 0:
        if (
            record_type == "transaction_files"
            or record_type == "receipt_files"
            or record_type == "location_history_files"
            or record_type == "time_tracking_files"
        ):
            pass
        elif record_type == "transactions":
            df = set_export_transactions_formulas(df, eu_locale)
        elif record_type == "location_history_by_date":
            pass
        elif record_type == "time_tracking_entries":
            pass
        else:
            raise ValueError("record_type '%s' not recognized" % record_type)

    # export to gsheets
    # if len(df) == 0:
    #     set_frozen(worksheet, rows=0)
    set_with_dataframe(worksheet, df, resize=True)
    # if len(df) > 0:
    #     set_frozen(worksheet, rows=1)

    dark_purple = Color(56 / 255, 40 / 255, 54 / 255)
    # orange = Color(252 / 255, 142 / 255, 30 / 255)
    white = Color(255 / 255, 255 / 255, 255 / 255)
    # light_orange_3 = Color(255 / 255, 229 / 255, 227 / 255)
    light_grey = Color(240 / 255, 240 / 255, 240 / 255)

    editable_column_cell_format = CellFormat(backgroundColor=white,)
    non_editable_column_cell_format = CellFormat(backgroundColor=light_grey,)

    column_formats = {}
    if editable_columns:
        non_editable_columns = df.columns.difference(editable_columns, sort=False)
        for non_editable_column_name in non_editable_columns:
            column_formats[non_editable_column_name] = non_editable_column_cell_format
        for editable_column_name in editable_columns:
            column_formats[editable_column_name] = editable_column_cell_format

    formatter = BasicFormatter(
        header_background_color=dark_purple,
        header_text_color=white,
        decimal_format="#,##0.00",
        date_format="YYYY-MM-DD",
        freeze_headers=True,
        column_formats=column_formats,
    )
    format_with_dataframe(
        worksheet, df, formatter, include_index=False, include_column_header=True
    )

    spreadsheet_url = "https://docs.google.com/spreadsheets/d/%s" % sh.id
    return spreadsheet_url


def fetch_gsheets_worksheet_as_df(gsheets_client, gsheets_title, gsheets_sheet_name):
    sh = gsheets_client.open(gsheets_title)
    # spreadsheet_url = "https://docs.google.com/spreadsheets/d/%s" % sh.id
    # print(spreadsheet_url)
    worksheet = sh.worksheet(gsheets_sheet_name)
    return get_as_dataframe(worksheet)


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
    accumulating_df,
    previous_possibly_edited_df,
    edit_file,
    record_type,
    clerkai_input_folder_path,
    current_history_reference,
    keep_unmerged_previous_edits,
):

    if type(previous_possibly_edited_df) is bool and not previous_possibly_edited_df:
        print("Context: edit_file", edit_file)
        raise Exception(
            "There is no previous possibly edited dataframe to merge changes from"
        )

    # set config based on record type
    if (
        record_type == "transaction_files"
        or record_type == "receipt_files"
        or record_type == "location_history_files"
        or record_type == "time_tracking_files"
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
        return (accumulating_df, [])
        """
        # TODO: Support editing location history
        additional_join_column = "ID"
        file_name_column_name = "Source location history file: File name"
        file_path_column_name = "Source location history file: File path"
        # Add ID column if not present in previous edits file
        if additional_join_column not in previous_possibly_edited_df.columns:
            from clerkai.location_history.parse import location_history_ids

            previous_possibly_edited_df[additional_join_column] = location_history_ids(
                previous_possibly_edited_df
            )
        """
    elif record_type == "time_tracking_entries":
        additional_join_column = "ID"
        file_name_column_name = "Source time tracking file: File name"
        file_path_column_name = "Source time tracking file: File path"
        # Add ID column if not present in previous edits file
        if additional_join_column not in previous_possibly_edited_df.columns:
            from clerkai.transactions.parse import transaction_ids

            previous_possibly_edited_df[additional_join_column] = transaction_ids(
                previous_possibly_edited_df
            )
    else:
        raise ValueError("record_type '%s' not recognized" % record_type)

    # print("df.head(), edit_file, previous_possibly_edited_df.head()")
    # print(df.head(), edit_file, previous_possibly_edited_df.head())

    # get relevant from and to commits between the old and new edit files
    from_commit = edit_file["Related history reference"]
    to_commit = current_history_reference()

    def normalize_encoding(string):
        import unicodedata as ud
        normalized = ud.normalize('NFC', string)
        return normalized

    def joined_normalized_path(record):
        # normalize encodings to properly join paths that have been encoded differently for whatever reason
        joined_path = "%s/%s" % (record[file_path_column_name], record[file_name_column_name])
        return normalize_encoding(joined_path)

    accumulating_df["clerkai_path"] = accumulating_df.apply(joined_normalized_path, axis=1)
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
            joined_normalized_path, axis=1
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

    suffix = " (%s - %s)" % (from_commit, edit_file["File name"])

    def add_suffix(column_name):
        return "%s%s" % (column_name, suffix)

    if additional_join_column:
        left_on.append(additional_join_column)
        right_on.append(additional_join_column)

    suffixed_previous_possibly_edited_df = previous_possibly_edited_df.add_suffix(
        suffix
    )

    import pandas as pd

    merged_possibly_edited_df = pd.merge(
        accumulating_df,
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
    df_with_previous_edits_across_columns, unmerged_edit_files, editable_columns
):

    for index, edit_file in unmerged_edit_files.iterrows():
        suffix = " (%s - %s)" % (
            edit_file["Related history reference"],
            edit_file["File name"],
        )
        for column_name in editable_columns:
            if column_name not in df_with_previous_edits_across_columns.columns:
                # the editable column has not been seen before, make sure to initiate it
                df_with_previous_edits_across_columns[column_name] = None
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
    if ".gitignore" in filename:
        return True
    if "Icon" in filename:
        if filename.encode("utf-8").hex() == "49636f6e0d":
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
        # ignore .~lock files
        _ = _[~_["File name"].str.contains(".~lock")]
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
        # ignore .~lock files
        _ = _[~_["File name"].str.contains(".~lock")]
    return _


def is_nan(x):
    import math

    try:
        return math.isnan(x)
    except TypeError:
        return False


def ymd_date_to_naive_datetime_obj(datetime_str):
    if is_nan(datetime_str):
        return None
    datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d")
    return datetime_obj


def raw_if_available(field_name, entry):
    raw_field_name = "Raw %s" % field_name
    if raw_field_name in entry and entry[raw_field_name] is not None:
        return entry[raw_field_name]
    if field_name in entry:
        return entry[field_name]
    else:
        return None
