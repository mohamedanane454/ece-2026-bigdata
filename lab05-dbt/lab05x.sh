# Lab 05 - Exercices
cd /home/onyxia/work/$GIT_REPO_NAME/lab_medallion
# Exercices 1 et 3 : username_normalized, birthdate invalide -> NULL, birthdate_is_valid
cat > models/silver/stg_users.sql <<'SQL'
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
SQL
python3 - <<'PY'
p = "models/silver/silver.yml"
s = open(p).read()
s = s.replace("""      - name: email
        data_tests:""", """      - name: username_normalized
        description: Username without leading/trailing whitespace, in lowercase.
        data_tests:
          - not_null
          - unique
      - name: birthdate_is_valid
        description: False when the original birthdate is later than the first order of the user (the birthdate is then NULL).
        data_tests:
          - not_null
      - name: email
        data_tests:""", 1)
open(p, "w").write(s)
PY
cat models/silver/silver.yml
# Exercice 3 : le test n'a plus besoin d'etre en warning
sed -i '/config(severity/d' tests/assert_orders_after_user_birth.sql
cat tests/assert_orders_after_user_birth.sql
# Exercice 2 : quantite positive (test singulier) ; product (accepted_values) et user_id (relationships) sont deja testes
cat > tests/assert_orders_quantity_positive.sql <<'SQL'
-- Every order must have a strictly positive quantity
select order_id, quantity
from {{ ref('stg_orders') }}
where quantity is null or quantity <= 0
SQL
# Exercice 4 : coherence etat / code postal avec une table de reference (seed : prefixes a 3 chiffres par etat, approximatifs)
cat > seeds/zip_ranges.csv <<'CSV'
state,zip3_min,zip3_max
AL,350,369
AK,995,999
AZ,850,865
AR,716,729
CA,900,961
CO,800,816
CT,60,69
DE,197,199
DC,200,205
FL,320,349
GA,300,319
GA,398,399
HI,967,968
ID,832,838
IL,600,629
IN,460,479
IA,500,528
KS,660,679
KY,400,427
LA,700,714
ME,39,49
MD,206,219
MA,10,27
MI,480,499
MN,550,567
MS,386,397
MO,630,658
MT,590,599
NE,680,693
NV,889,898
NH,30,38
NJ,70,89
NM,870,884
NY,100,149
NC,270,289
ND,580,588
OH,430,459
OK,730,749
OR,970,979
PA,150,196
RI,28,29
SC,290,299
SD,570,577
TN,370,385
TX,750,799
TX,885,885
UT,840,847
VT,50,59
VA,220,246
WA,980,994
WV,247,268
WI,530,549
WY,820,831
PR,6,9
VI,8,8
GU,969,969
AS,967,967
FM,969,969
MH,969,969
MP,969,969
PW,969,969
AA,340,340
AE,90,98
AP,962,966
CSV
cat > tests/assert_users_zip_matches_state.sql <<'SQL'
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
SQL
uv run dbt build
uv run python -c "
import duckdb
con = duckdb.connect('lab_medallion.duckdb', read_only=True)
con.sql('SELECT birthdate_is_valid, count(*) FROM main_silver.stg_users GROUP BY 1').show()
con.sql('SELECT username, username_normalized, birthdate, birthdate_is_valid FROM main_silver.stg_users LIMIT 5').show()
"
# Exercice 5 : materialisation en table
sed -i 's/+materialized: view/+materialized: table/' dbt_project.yml
uv run dbt build --select silver
uv run python -c "
import duckdb
con = duckdb.connect('lab_medallion.duckdb', read_only=True)
con.sql('SELECT table_schema, table_name, table_type FROM information_schema.tables ORDER BY ALL').show()
con.sql(\"SELECT database_size, total_blocks, used_blocks FROM pragma_database_size()\").show()
"
ls -la lab_medallion.duckdb
sed -i 's/+materialized: table/+materialized: view/' dbt_project.yml
uv run dbt build --select silver
grep materialized dbt_project.yml
cd /home/onyxia/work/$GIT_REPO_NAME
git add lab_medallion
git status --short
git commit -m "feat(dbt): exercises - normalized username, birthdate validation, zip ranges"
gpush
true
