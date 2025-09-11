%macro inner(m);
  data work.inner_&m.;
    set work.sales;
    where month = &m.;
  run;
%mend;
%macro outer(m);
  %inner(&m.);
%mend;
%outer(2)
