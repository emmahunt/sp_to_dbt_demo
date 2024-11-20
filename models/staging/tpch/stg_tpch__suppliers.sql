with source as (

    select * from {{ source('tpch', 'suppliers') }}

),

renamed as (

    select
        s_suppkey as supplier_id,
        s_name as supplier_name,
        s_address as supplier_address,
        s_nationkey as supplier_nation_id,
        s_phone as supplier_phone_number,
        s_acctbal as supplier_actual_balance,
        s_comment as supplier_comment
    from source

)

select * from renamed
