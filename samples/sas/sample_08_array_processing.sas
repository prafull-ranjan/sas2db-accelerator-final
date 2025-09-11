data work.arr;
  set work.input;
  array vals[3] v1-v3;
  do i = 1 to 3;
    vals[i] = v[i] * 2;
  end;
run;
