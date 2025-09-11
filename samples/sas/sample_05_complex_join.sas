proc sql;
  create table work.combined as
  select a.*, b.region
  from work.orders a
  inner join work.customers b
  on a.cust_id = b.cust_id
  where b.active = 1;
quit;
