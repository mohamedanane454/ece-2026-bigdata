{{ config(severity='warn') }}
-- The 3 first digits of the zip code must belong to a range of the state
select u.user_id, u.state, u.zip_code
from {{ ref('stg_users') }} u
where u.zip_code <> ''
  and not exists (
    select 1
    from {{ ref('zip_ranges') }} z
    where z.state = u.state
      and cast(left(u.zip_code, 3) as integer) between z.zip3_min and z.zip3_max
  )
