proc sql;
  create table work.rank_sales as
  select customer_id, order_date, sales_amt,
    monotonic() as rownum,
    calculated sales_amt - lag(sales_amt) as diff
  from work.sales
  order by customer_id, order_date;
quit;
