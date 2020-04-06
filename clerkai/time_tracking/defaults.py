from typing import List

default_time_tracking_files_editable_columns: List[str] = [
    "Content type",
    "Ignore",
    "Editable time tracking entries? (1/0)",
    "Client (Default)",
    "Project / Costpool / Theme (Default)",
    "Invoice (Default)",
]

default_tracked_time_by_file_editable_columns: List[str] = []

aggregation_level_editable_columns: List[str] = [
    "Client",
    "Project / Costpool / Theme",
    "Invoice",
    "Invoice row ordinal",
    "Invoice row item",
    "Invoice row item category",
    "Invoice row item reference",
    "Invoice hours",
]

default_tracked_time_by_file_and_date_editable_columns = (
    aggregation_level_editable_columns
)

default_tracked_time_by_file_date_and_annotation_editable_columns = (
    aggregation_level_editable_columns
)

default_tracked_time_by_file_datehour_and_annotation_editable_columns = (
    aggregation_level_editable_columns
)

default_time_tracking_entries_editable_columns: List[str] = [
    "Hours 1 (Edited)",
    "Annotation 1 (Edited)",
    "Hours 2 (Edited)",
    "Annotation 2 (Edited)",
    "Hours 3 (Edited)",
    "Annotation 3 (Edited)",
]
