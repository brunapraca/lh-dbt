with 
    rennamed as (
        select 
        cast(creditcardid as int) as pk_credit_card
        , cast (cardtype as string) as creditcard_type
        -- cardnumber -- não será necessário usar para essa análise
        -- expmonth -- não será necessário usar para essa análise
        -- expyear -- não será necessário usar para essa análise
        -- modifieddate -- não será necessário usar para essa análise
            
        from {{ source('sap_adw', 'creditcard') }}
    )

select * 
from rennamed 