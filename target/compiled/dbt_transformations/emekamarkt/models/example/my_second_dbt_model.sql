-- Use the `ref` function to select from other models

select *
from "dev"."public"."my_first_dbt_model"
where id = 1