data work.out_sales;
  set work.sales;
  where region = 'APAC';
  total = price * qty;
  if missing(discount) then discount = 0;
run;
