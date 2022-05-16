Flatten experiments

* Q22
  * p3
   > ./run_just_sql.sh ./flatten/q22/p3 q22_flatten_p3 true > ./flatten/q22/p3/q22_flatten_p3_eval.log

   > ./make_stats_form_log.sh ./flatten/q22/p3  q22_flatten_p3_eval

  * p12
  > ./run_just_sql.sh ./flatten/q22/p12 q22-rew_test.dlp_rew_p12 true > ./flatten/q22/p12/q22-rew_test.dlp_rew_p12_eval.log
  
  > ./make_stats_form_log.sh ./flatten/q22/p12 q22-rew_test.dlp_rew_p12_eval 

> ./run_just_sql.sh ./flatten/q22 q22-rew_test true > ./flatten/q22/q22-rew_test_eval.log

> ./make_stats_form_log.sh ./flatten/q22 q22_flatten_p3_eval


* Q15
  * p5
  * 
>./run_just_sql.sh ./flatten/q15  q15-rew  true  > ./flatten/q15/q15-rew_eval.log