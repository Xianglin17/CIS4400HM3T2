{%- set allstatus = ['completed','returned','shipped','placed','return_pending'] -%}
with orders as (
    select * from {{ ref('stg_orders') }}
),

pivoted as (
    select 
        customer_id,
        {% for estatus in allstatus -%}
        count(case when status = '{{ estatus }}' ) as {{estatus}}_amount
        {%- if not loop.last -%}
            , 
        {%- endif %}
        
        {% endfor -%}

    from orders
    group by 1
)

select * from pivoted