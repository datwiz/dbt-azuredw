{% macro azuredw__list_schemas(database) %}
  {% set sql -%}
        select distinct schema_name
        from {{ database }}.information_schema.schemata
        where catalog_name = '{{ database }}'
  {%- endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro azuredw__create_schema(relation) %}
    {% call statement('create_schema', auto_begin=False) -%}
        create schema {{ relation.without_identifier() }}
    {%- endcall %}
{% endmacro %}

{% macro azuredw__drop_relation(relation) -%}
    {% call statement('drop_relation', auto_begin=False) -%}
        if object_id('{{ relation.schema }}.{{ relation.identifier }}') is not null
	        drop {{ relation.type }} {{ relation.schema }}.{{ relation.identifier }}
    {%- endcall %}
{% endmacro %}

{% macro azuredw__check_schema_exists(information_schema, schema) -%}
  {% set sql -%}
    select count(*) as schema_exist
    from {{ information_schema.replace(information_schema_view='SCHEMATA') }}
    where schema_name = '{{ schema }}'
  {%- endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro azuredw__list_relations_without_caching(schema_relation) %}
  {% set sql -%}
    select
      table_catalog as [database],
      table_name as [name],
      table_schema as [schema],
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           else table_type
      end as table_type
    from {{ schema_relation.database }}.information_schema.tables
    where table_schema = '{{ schema_relation.schema }}'
      and table_catalog = '{{ schema_relation.database }}'
  {% endset -%}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro azuredw__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = '#' ~ base_relation.identifier ~ suffix %}
    {% set tmp_relation = base_relation.incorporate(
                                path={"identifier": tmp_identifier},
                                table_name=tmp_identifier) -%}

    {% do return(tmp_relation) %}
{% endmacro %}

{% macro azuredw__create_table_as(temporary, relation, sql) -%}
{%- set distribution = config.get('distribution') -%}
  create table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  with(
    clustered columnstore index,
    {%- if temporary: -%}
    distribution =  round_robin
    {%- else -%}
    distribution = {% if distribution: -%}{{ distribution }}{%- else %} round_robin {%- endif %}
    {%- endif %}
  )
  as 
    {{ sql }}
{% endmacro %}

{% macro azuredw__create_view_as(relation, sql, auto_begin=False) -%}
  create view {{ relation.schema }}.{{ relation.identifier }} as
    {{ sql }}
{% endmacro %}

{% macro azuredw__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    rename object {{ from_relation.schema }}.{{ from_relation.identifier }} to {{ to_relation.identifier }}
  {%- endcall %}
{% endmacro %}

{% macro azuredw__get_columns_in_relation(relation) -%}
    {% set sql -%}
        select 
            column_name as [column]
            , data_type
            , character_maximum_length
            , numeric_precision
            , numeric_scale
        from 
            information_schema.columns
        where 
            table_catalog    = '{{ relation.database }}'
            and table_schema = '{{ relation.schema }}'
            and table_name   = '{{ relation.identifier }}'
    {% endset %}
    {{ return(run_query(sql)) }}
{% endmacro %}