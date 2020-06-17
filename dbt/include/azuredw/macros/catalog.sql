
{% macro azuredw__get_catalog(information_schema, schemas) -%}
    {%- call statement('catalog', fetch_result=True) -%}
        {% set database = information_schema.database %}
        {% set schema = schemas[0] %}
        with tables as (
            select
                table_catalog as "table_database",
                table_schema as "table_schema",
                table_name as "table_name",
                case when table_type = 'BASE TABLE' then 'table'
                    when table_type = 'VIEW' then 'view'
                    else table_type
                end as table_type
            from {{ information_schema }}.tables
        ), columns as (
            select
                table_catalog as "table_database",
                table_schema as "table_schema",
                table_name as "table_name",
                null as "table_comment",
                column_name as "column_name",
                ordinal_position as "column_index",
                data_type as "column_type",
                null as "column_comment"
            from {{ information_schema }}.columns
        )
        select *
        from tables
        join columns on tables.table_database = columns.table_database
            and tables.table_schema = columns.table_schema
            and tables.table_name = columns.table_name
        order by "column_index"
    {%- endcall -%}
    {{ return(load_result('catalog').table) }}
{%- endmacro %}