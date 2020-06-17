from dbt.adapters.sql import SQLAdapter
from dbt.adapters.azuredw import AzureDWConnectionManager


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