%macro mk_monthly(month);
  proc sql;
    create table work.sales_&month. as
    select * from work.sales where month = &month.;
  quit;
%mend;
%mk_monthly(1)
