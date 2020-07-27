from dbt.adapters.sql import SQLAdapter
from dbt.adapters.azuredw import AzureDWConnectionManager
from dbt.adapters.base import Column as BaseColumn
from typing import List

class AzureDWAdapter(SQLAdapter):
    ConnectionManager = AzureDWConnectionManager

    @classmethod
    def date_function(cls) -> str:
        return 'get_date()'

    @classmethod
    def convert_text_type(cls, agate_table, col_idx) -> str:
        return 'varchar(8000)'

    @classmethod
    def is_cancelable(cls) -> bool:
        return False
   
    def get_missing_columns(
        self, from_relation, to_relation
    ) -> List[BaseColumn]:
        """Returns a list of Columns in from_relation that are missing from
        to_relation.
        """
        if not isinstance(from_relation, self.Relation):
            invalid_type_error(
                method_name='get_missing_columns',
                arg_name='from_relation',
                got_value=from_relation,
                expected_type=self.Relation)

        if not isinstance(to_relation, self.Relation):
            invalid_type_error(
                method_name='get_missing_columns',
                arg_name='to_relation',
                got_value=to_relation,
                expected_type=self.Relation)

        from_columns = {
            col[0]: col for col in
            self.get_columns_in_relation(from_relation)
        }

        to_columns = {
            col[0]: col for col in
            self.get_columns_in_relation(to_relation)
        }

        missing_columns = set(from_columns.keys()) - set(to_columns.keys())

        return [
            col for (col_name, col) in from_columns.items()
            if col_name in missing_columns
        ]

    def valid_snapshot_target(self, relation) -> None:
        """Ensure that the target relation is valid, by making sure it has the
        expected columns.

        :param Relation relation: The relation to check
        :raises CompilationException: If the columns are
            incorrect.
        """
        if not isinstance(relation, self.Relation):
            invalid_type_error(
                method_name='valid_snapshot_target',
                arg_name='relation',
                got_value=relation,
                expected_type=self.Relation)

        columns = self.get_columns_in_relation(relation)
        names = set(c[0].lower() for c in columns)
        expanded_keys = ('scd_id', 'valid_from', 'valid_to')
        extra = []
        missing = []
        for legacy in expanded_keys:
            desired = 'dbt_' + legacy
            if desired not in names:
                missing.append(desired)
                if legacy in names:
                    extra.append(legacy)

        if missing:
            if extra:
                msg = (
                    'Snapshot target has ("{}") but not ("{}") - is it an '
                    'unmigrated previous version archive?'
                    .format('", "'.join(extra), '", "'.join(missing))
                )
            else:
                msg = (
                    'Snapshot target is not a snapshot table (missing "{}")'
                    .format('", "'.join(missing))
                )
            raise_compiler_error(msg)

    def expand_column_types(
        self, goal, current
    ) -> None:
        pass

    def expand_target_column_types(
        self, from_relation, to_relation
    ) -> None:
        # This is a no-op on Azure
        pass
