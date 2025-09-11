data work.formatted;
  set work.sales;
  sale_dt_str = put(sale_date, date9.);
  month = month(sale_date);
run;
