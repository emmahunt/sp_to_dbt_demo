with 

source as (

    select * from {{ source('tpch', 'part_suppliers') }}

),

renamed as (

    select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment

    from source

)

select * from renamed
