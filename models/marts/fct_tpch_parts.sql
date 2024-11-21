{{
    config(
        materialized='table'
    )
}}

with parts as (
    select * from {{ ref('stg_tpch__parts') }}
),

suppliers as (
    select * from {{ ref('stg_tpch__suppliers') }}
),

part_suppliers as (
    select * from {{ ref('stg_tpch__part_suppliers') }}
),

final as (

    select
        iff(suppliers.supplier_id=2, null, suppliers.supplier_id) as supplier_id,
        suppliers.supplier_nation_id,
        parts.part_id,
        concat(suppliers.supplier_id, parts.part_id) as part_supplier_sk,
        suppliers.supplier_nation_id as supplier_nation,
        part_suppliers.part_supplier_available_qty,
        part_suppliers.part_supplier_cost,
        part_suppliers.part_supplier_comment,
        suppliers.supplier_name,
        suppliers.supplier_address,
        suppliers.supplier_phone_number,
        suppliers.supplier_account_balance,
        suppliers.supplier_comment,
        parts.part_name,
        parts.part_manufacturer,
        parts.part_brand,
        parts.part_type,
        parts.part_container,
        parts.part_retail_price, 

        -- Apply the logic used in the stored procedure as a conditional in the model select
        case
            when parts.part_type like '%BRASS' then 'brass'
            else parts.part_type
        end as part_material,
        parts.part_comment
    from suppliers
        
        left join part_suppliers 
        on suppliers.supplier_id = part_suppliers.supplier_id
        
        left join parts 
        on parts.part_id = part_suppliers.part_id

)

select * from final
