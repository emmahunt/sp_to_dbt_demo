with source as (

    select * from {{ source('tpch', 'parts') }}

),

renamed as (

    select
        p_partkey as part_id,
        p_name as part_name,
        p_mfgr as part_manufacturer,
        p_brand as part_brand,

        -- all caps
        p_type as part_type,
        p_size as part_size,

        -- all caps
        p_container as part_container,
        p_retailprice as part_retail_price,
        p_comment as part_comment
    from source

    -- This was deleted in the stored procedure example, so apply the filter here
    -- If this model were to be used by other use cases that may need steel parts, the exlucsion could be moved into the marts layer
    where
        lower(p_type) not like '%steel%'

)

select * from renamed
