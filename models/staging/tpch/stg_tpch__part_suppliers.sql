with source as (

    select * from {{ source('tpch', 'part_suppliers') }}

),

renamed as (

    select
        ps_partkey as part_id,
        ps_suppkey as supplier_id,
        ps_availqty as part_supplier_available_qty,
        ps_supplycost as part_supplier_cost,
        ps_comment as part_supplier_comment
    from source

)

select * from renamed
