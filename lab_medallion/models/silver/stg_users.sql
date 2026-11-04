with source as (
    select * from {{ source('bronze', 'users') }}
),

-- Date de la premiere commande de chaque utilisateur (exercice 3)
first_orders as (
    select
        cast(user_uuid as uuid) as user_id,
        min(cast(date as timestamptz)) as first_ordered_at
    from {{ source('bronze', 'orders') }}
    group by 1
),

typed as (
    select
        cast(uuid as uuid) as user_id,
        trim(username) as username,
        trim(name) as name,
        upper(trim(sex)) as sex,
        lower(trim(mail)) as email,
        cast(birthdate as date) as birthdate,
        -- The address spans 2 lines: the street, then the city, the state and the zip code
        split_part(address, chr(10), 1) as street,
        split_part(address, chr(10), 2) as address_line_2
    from source
)

select
    t.user_id,
    t.username,
    -- Exercice 1 : username sans espaces et en minuscules
    lower(trim(t.username)) as username_normalized,
    t.name,
    t.sex,
    t.email,
    -- Exercice 3 : une date de naissance posterieure a la premiere commande est remplacee par NULL
    case when f.first_ordered_at is not null and t.birthdate > f.first_ordered_at then null else t.birthdate end as birthdate,
    coalesce(t.birthdate <= f.first_ordered_at, true) as birthdate_is_valid,
    t.street,
    -- Military addresses, such as "DPO AE 12345", have no comma
    nullif(regexp_extract(t.address_line_2, '^(.+), [A-Z]{2} \d{5}$', 1), '') as city,
    regexp_extract(t.address_line_2, '([A-Z]{2}) (\d{5})$', 1) as state,
    regexp_extract(t.address_line_2, '([A-Z]{2}) (\d{5})$', 2) as zip_code
from typed t
left join first_orders f on t.user_id = f.user_id
