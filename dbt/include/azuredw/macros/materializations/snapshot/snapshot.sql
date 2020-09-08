{% macro azuredw__snapshot_hash_arguments(args) -%}

 {%- set fields = [] -%}

 {%- for arg in args -%}

    {%- set _ = fields.append(
        "coalesce(cast(" ~ arg ~ " as varchar" "), '')"
    ) -%}

    {%- if not loop.last %}
        {%- set _ = fields.append("'-'") -%}
    {%- endif -%}

 {%- endfor -%}

 {%- if args|length < 2 %}
     
    {%- set _ = fields.append("''") -%}
 {%- endif -%}
 convert(varchar(32),HashBytes('MD5', {{(dbt_utils.concat(fields))}}),2)

{%- endmacro %}

