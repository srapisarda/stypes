SELECT s_2.a1 AS X0
FROM t AS t_0
         INNER JOIN r AS r_1 ON t_0.a1 = r_1.a2
         INNER JOIN s AS s_2 ON r_1.a1 = s_2.a2
UNION
(SELECT s_1.a1 AS X0
 FROM s AS s_0
          INNER JOIN s AS s_1 ON s_0.a1 = s_1.a2)