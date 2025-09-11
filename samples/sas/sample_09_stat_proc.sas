proc means data=work.sales n mean std;
  var sales_amt;
  class product_id;
run;
