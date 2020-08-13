from abc import abstractmethod


class Persistent:
    """Protocol that provides necessary information for persisting a message."""
    @staticmethod
    @abstractmethod
    def table_name() -> str:
        """
        The name of the table where the data should be stored.
        :return: str - table name
        """
        pass

    @staticmethod
    @abstractmethod
    def create_table_statements() -> [str]:
        """
        A valid sql statement for creating the table for storing this message type.

        :return: str - create table sql statement
        """
        pass

    @staticmethod
    @abstractmethod
    def insert_statement() -> str:
        """
        A valid parameterized sql insert statement.  The parameters of the insert should match the
        variable names of the message type.

        :return: str - sql insert statement
        """
        pass
