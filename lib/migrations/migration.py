from abc import abstractmethod
from typing import List


class Operation:
    @abstractmethod
    def execute(self, connection):
        """
        Perform the operation using the provided database connection
        :param connection: connection type as described in python db-api
        :return:
        """
        pass


class Migration:

    @property
    @abstractmethod
    def from_version(self):
        pass

    @property
    @abstractmethod
    def to_version(self):
        pass

    @property
    @abstractmethod
    def dependencies(self) -> List['Migration']:
        pass

    @property
    @abstractmethod
    def operations(self) -> List[Operation]:
        pass

    @abstractmethod
    def execute(self, connection):
        """
        Perform the operation using the provided database connection
        :param connection: connection type as described in python db-api
        :return:
        """
        dependencies: List[Migration] = []
        for dependency_descriptor in self.dependencies:
            dependencies.append(Migrations.fetch_dependency(dependency_descriptor))

        # Execute the operations for all dependencies
        for dependency in dependencies:
            operations = dependency.operations
            for operation in operations:
                operation.execute(connection)

        # Execute operations for this migration
        for operation in self.operations:
            operation.execute(connection)


class Migrations:
    @staticmethod
    def find_migration_for(from_version: str, to_version: str) -> 'Migrations':
        raise NotImplementedError

    @staticmethod
    def fetch_dependency(dependency_descriptor) -> Migration:
        raise NotImplementedError

    def __init__(self, migrations: List[Migration]) -> None:
        super().__init__()
        self.migrations = migrations

    def execute(self, connection):
        """
        Perform the operation using the provided database connection
        :param connection: connection type as described in python db-api
        :return:
        """
        if self.migrations:
            for migration in self.migrations:
                migration.execute(connection=connection)


