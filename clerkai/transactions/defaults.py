from typing import List

default_transaction_files_editable_columns: List[str] = [
    "Ignore",
    "Account provider",
    "Account",
    "Content type",
    "Account currency",
]

default_transactions_editable_columns: List[str] = [
    "Include in expense report",
    "Expense report receiver",
    "Expense report",
    "Expense report accounting period",
    "Walletsharing",
    "Real Date (Corrected)",
    "Days between real and bank dates",
    "Doc Source",
    "Doc Status",
    "Doc",
    "Clarification",
    "Type",
    "Income/Expense Category",
]

# Account	Date initiated	Date settled	Source text	Merchant	Hash	Transaction id	Amount (Incl. VAT)
# Balance	Original amount (In local currency)	Local currency	Account Owner	Comments / Notes	Doc notes
# Doc filename	Doc link	Doc inbox search	Sorting ordinal	Legacy Id	Date initiated value
# Date settled value	Absolute amount	Absolute original amount	Vendor	Category	Description	Status
# Invoice date	Paid date	Source	Amount	Currency	Status
