import sys

from os import path

from decimal import Decimal, Decimal as D
from datetime import datetime
import re
from typing import Optional, Any, TextIO, cast

from ofxstatement.plugin import Plugin
from ofxstatement.parser import StatementParser

# from ofxstatement.parser import CsvStatementParser
from ofxstatement.parser import AbstractStatementParser
from ofxstatement.statement import Statement, InvestStatementLine, StatementLine

# import logging
# LOGGER = logging.getLogger(__name__)

import csv


ACTIONS = [
    ["DIVIDEND RECEIVED", "INCOME", "DIV"],
    ["REINVESTMENT", "BUYSTOCK", "BUY"],
    ["INTEREST EARNED", "INT"],
    ["DIRECT DEBIT", "DEBIT"],
    ["DIRECT DEPOSIT", "CREDIT"],
    ["BILL PAYMENT", "DEBIT"],
    ["REDEMPTION FROM CORE ACCOUNT", "SELLSTOCK", "SELL"],
    ["YOU BOUGHT", "BUYSTOCK", "BUY"],
    ["YOU SOLD", "SELLSTOCK", "SELL"],
    ["SHORT-TERM CAP GAIN", "INCOME", "CGSHORT"],
    ["LONG-TERM CAP GAIN", "INCOME", "CGLONG"],
    ["DEBIT CARD PURCHASE", "DEBIT"],
    ["TRANSFERRED FROM", "XFER"],
    ["TRANSFERRED TO", "XFER"],
    ["ROLLOVER CASH CHECK RECEIVED", "XFER"],
    ["TRANSFER OF ASSETS CHECK RECEIVED", "XFER"],
    ["CASH ADVANCE", "DEBIT"],
    ["ADJUST FEE CHARGED", "CREDIT"],
    ["Check Paid", "DEBIT"]
]


class FidelityPlugin(Plugin):
    """Sample plugin (for developers only)"""

    def get_parser(self, filename: str) -> "FidelityCSVParser":
        parser = FidelityCSVParser(
            filename,
            [a.strip() for a in self.settings.get("bank_accounts", "").split(",") if a.strip() != '']
        )
        return parser


class FidelityCSVParser(AbstractStatementParser):
    statement: Statement
    fin: TextIO  # file input stream
    # 0-based csv column mapping to StatementLine field

    date_format: str = "%Y-%m-%d"
    cur_record: int = 0
    columns: dict[str, int] = {}


    def __init__(self, filename: str, bank_accounts: list[str]) -> None:
        super().__init__()
        self.filename = filename
        self.bank_accounts = bank_accounts
        self.statement = Statement()
        self.statement.broker_id = "Fidelity"
        self.statement.currency = "USD"
        self.id_generator = IdGenerator()

    def parse_datetime(self, value: str) -> datetime:
        return datetime.strptime(value, self.date_format)

    def parse_decimal(self, value: str) -> D:
        # some plugins pass localised numbers, clean them up
        return D(value.replace(",", ".").replace(" ", ""))

    def parse_value(self, value: str | None, field: str) -> Any:
        tp = StatementLine.__annotations__.get(field)
        if value is None:
            return None

        if tp in (datetime, datetime | None):
            return self.parse_datetime(value)
        elif tp in (Decimal, Decimal | None):
            return self.parse_decimal(value)
        else:
            return value

    def get_col(self, line: list[str], col_name: str):
        return line[self.columns[col_name]]

    def get_action(self, action_value: str):
        for action in ACTIONS:
            if (
                action_value.startswith(action[0])
                and len(action_value) > len(action[0])
                and action_value[len(action[0]) == " "]
            ):
                return action
        raise Exception("Could not find action for " + action_value)

    def set_common_fields(self, stmt_line: StatementLine | InvestStatementLine, line: list[str]):
        date = datetime.strptime(line[0][0:10], "%m/%d/%Y")
        stmt_line.date = date
        id = self.id_generator.create_id(date)
        stmt_line.id = id

        # amount
        field = "amount"
        rawvalue = self.get_col(line, "Amount ($)")
        value = self.parse_value(rawvalue, field)
        setattr(stmt_line, field, value)

        stmt_line.memo = self.get_col(line, "Action")

    def set_investment_fields(self, stmt_line: InvestStatementLine, line: list[str]):
        # fees
        field = "fees"
        rawvalue = self.get_col(line, "Fees ($)")
        value = self.parse_value(rawvalue, field)
        setattr(stmt_line, field, value)

        action = self.get_action(self.get_col(line, "Action"))

        if len(action) == 3:
            stmt_line.trntype = action[1]
            stmt_line.trntype_detailed = action[2]
        elif len(action) == 2:
            stmt_line.trntype = "INVBANKTRAN"
            stmt_line.trntype_detailed = action[1]
        else:
            raise Exception("Invalid action: {action}")

        stmt_line.security_id = self.get_col(line, "Symbol")
        unit_price_value = self.get_col(line, "Price ($)")
        if unit_price_value and unit_price_value != "":
            stmt_line.unit_price = Decimal(unit_price_value)
            stmt_line.units = Decimal(self.get_col(line, "Quantity"))

    def set_bank_fields(self, stmt_line: StatementLine, line: list[str]):
        settlement_date = self.get_col(line, "Settlement Date")
        if settlement_date and settlement_date != "":
            date_user = datetime.strptime(settlement_date[0:10], "%m/%d/%Y")
        else:
            date_user = stmt_line.date

        stmt_line.date_user = date_user

        action_value = self.get_col(line, "Action")
        action = self.get_action(action_value)

        if len(action) == 2:
            stmt_line.trntype = action[1]
        elif len(action) == 3:
            raise Exception("Got investment action: {action[0]}")
        else:
            raise Exception("Invalid action: {action}")

        payee = action_value[len(action[0]) + 1:]

        check_no_match = re.match(r"^Check Paid # (\S+).*", action_value)
        if check_no_match:
            stmt_line.check_no = check_no_match[1]
            payee = action_value

        payee = payee.removesuffix(" (Cash)")
        stmt_line.payee = payee


    def parse_record(self, line: list[str], investment: bool):
        """Parse given transaction line and return StatementLine object"""

        # Run Date
        # Account
        # Account Number
        # Action
        # Symbol
        # Description
        # Type
        # Price ($)
        # Quantity
        # Commission ($)
        # Fees ($)
        # Accrued Interest ($)
        # Amount ($)
        # Cash Balance ($)
        # Settlement Date

        # msg = f"self.cur_record: {self.cur_record}"
        # print(msg, file=sys.stderr)

        # skip blank lines
        if not line[0]:
            return None

        # skip the header
        if line[0] == "Run Date":
            for idx, header in enumerate(line):
                self.columns[header] = idx
            return None

        # skip lines which are comments
        if line[0][:1] == '"':
            return None

        # skip any line that does not begin with a digit
        if not line[0][:1].isdigit():
            return None

        if investment:
            stmt_line = InvestStatementLine()
            self.set_common_fields(stmt_line, line)
            self.set_investment_fields(stmt_line, line)
        else:
            stmt_line = StatementLine()
            self.set_common_fields(stmt_line, line)
            self.set_bank_fields(stmt_line, line)

        return stmt_line

    # parse the CSV file and return a Statement
    def parse(self) -> Statement:
        """Main entry point for parsers"""

        # derive account id from file name
        match = (
            re.search(r".*History_for_Account_(.*)\.csv", path.basename(self.filename))
            or re.search(r"(.*).csv", path.basename(self.filename))
        )
        if match:
            self.statement.account_id = match[1]

        is_investment = self.statement.account_id not in self.bank_accounts

        with open(self.filename, "r") as fin:

            self.fin = fin

            reader = csv.reader(self.fin)

            # loop through the CSV file lines
            for csv_line in reader:
                self.cur_record += 1
                if not csv_line:
                    continue
                stmt_line = self.parse_record(csv_line, is_investment)
                if stmt_line:
                    try:
                        stmt_line.assert_valid()
                    except:
                        print(f"Invalid line: {csv_line}")
                        raise
                    if isinstance(stmt_line, InvestStatementLine):
                        self.statement.invest_lines.append(stmt_line)
                    else:
                        self.statement.lines.append(stmt_line)

            # reverse the lines
            self.statement.lines.reverse()
            self.statement.invest_lines.reverse()

            all_lines = self.statement.lines + self.statement.invest_lines

            # after reversing the lines, update ids
            for line in all_lines:
                date = line.date
                new_id = self.id_generator.create_id(date)
                line.id = new_id

            # figure out start_date and end_date for the statement
            self.statement.start_date = min(
                sl.date for sl in all_lines if sl.date is not None
            )
            self.statement.end_date = max(
                sl.date for sl in all_lines if sl.date is not None
            )

            # print(f"{self.statement}")
            return self.statement


##########################################################################
class IdGenerator:
    """Generates a unique ID based on the date

    Hopefully any JSON file that we get will have all the transactions for a
    given date, and hopefully in the same order each time so that these IDs
    will match up across exports.
    """

    def __init__(self) -> None:
        self.date_count: dict[datetime, int] = {}

    def create_id(self, date) -> str:
        self.date_count[date] = self.date_count.get(date, 0) + 1
        return f'{datetime.strftime(date, "%Y%m%d")}-{self.date_count[date]}'
