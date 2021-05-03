"""Implements the csv unifier"""


from abc import ABC, abstractmethod
import pandas as pd
import sys
from datetime import datetime
import os

class AbstractStatementTransformer(ABC):

    """Implements the statement class"""

    _COLUMNS = ["datetime", "transaction_type", "amount", "from", "to"]

    def __init__(self, source_csv_file_path):
        try:
            self._source_df = pd.read_csv(source_csv_file_path)
        except OSError as e:
            raise type(e)(
                f"failed to read the csv file at {source_csv_file_path}" + str(e)).with_traceback(sys.exc_info()[2])
        self._statement_df = AbstractStatementTransformer.create_statement_df()

    @abstractmethod
    def _amount(self):
        """Returns the amount column series"""

    @abstractmethod
    def _datetime(self):
        """Returns the date column series"""

    @abstractmethod
    def _transaction_type(self):
        """Returns the transaction type column series"""

    @abstractmethod
    def _from(self):
        """Returns the from column series"""

    @abstractmethod
    def _to(self):
        """Returns the to column series"""

    @staticmethod
    def create_statement_df():
        """Creates the empty statement df"""
        return pd.DataFrame(columns=AbstractStatementTransformer._COLUMNS)

    def df(self):
        """Returns the statement as a dataframe"""
        self._statement_df.datetime = self._datetime()
        self._statement_df.transaction_type = self._transaction_type()
        self._statement_df.amount = self._amount()
        self._statement_df["from"] = self._from()
        self._statement_df["to"] = self._to()
        return self._statement_df


class Bank1StatementTransformer(AbstractStatementTransformer):

    """Implements a statement class for the Bank 1 Statement"""

    def _amount(self):
        return self._source_df["amount"]

    def _datetime(self):
        return self._source_df["timestamp"].apply(lambda x: datetime.strptime(x, "%b %d %Y"))

    def _transaction_type(self):
        return self._source_df["type"]

    def _from(self):
        return self._source_df["from"]

    def _to(self):
        return self._source_df["to"]


class Bank2StatementTransformer(AbstractStatementTransformer):

    """Implements a statement class for the Bank 2 Statement"""

    def _amount(self):
        return self._source_df["amounts"]

    def _datetime(self):
        return self._source_df["date"].apply(lambda x: datetime.strptime(x, "%d-%m-%Y"))

    def _transaction_type(self):
        return self._source_df["transaction"]

    def _from(self):
        return self._source_df["from"]

    def _to(self):
        return self._source_df["to"]


class Bank3StatementTransformer(AbstractStatementTransformer):

    """Implements a statement class for the Bank 3 Statement"""

    def _amount(self):
        return self._source_df["euro"] + self._source_df["cents"]/100

    def _datetime(self):
        return self._source_df["date_readable"].apply(lambda x: datetime.strptime(x, "%d %b %Y"))

    def _transaction_type(self):
        return self._source_df["type"]

    def _from(self):
        return self._source_df["from"]

    def _to(self):
        return self._source_df["to"]

class StatementsUnifier:

    """Implements a unifier for the bank statements"""


    def __init__(self, output_file_directory_path, output_file_name):
        self._unified_statements_df = AbstractStatementTransformer.create_statement_df()
        if not os.path.isdir(output_file_directory_path):
            raise NotADirectoryError(f"the {output_file_directory_path} directory path does not belong to a directory")
        self._output_file_path = os.path.join(output_file_directory_path, output_file_name)

    def add_statement(self, statement_df):
        """Adds statement dataframe with the unified statements dataframe"""
        self._unified_statements_df = pd.concat([self._unified_statements_df, statement_df], ignore_index=True)

    def write(self):
        """Writes the unified csv file to the specified output"""
        self._unified_statements_df.sort_values("datetime").to_csv(self._output_file_path, index=False)
