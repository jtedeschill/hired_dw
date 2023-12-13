

  create or replace view `hired-393411`.`Hired`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `hired-393411`.`Hired`.`my_first_dbt_model`
where id = 1;

