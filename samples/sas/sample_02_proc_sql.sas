proc sql;
  create table work.top_products as
  select product_id, sum(sales_amt) as total_sales
  from work.sales
  where sales_date >= '01JAN2024'd
  group by product_id;
quit;
