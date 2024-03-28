WITH p1 AS (SELECT s_2.A1 AS X0
            FROM t AS t_0
                     INNER JOIN r AS r_1 ON t_0.A1 = r_1.A2
                     INNER JOIN s AS s_2 ON r_1.A1 = s_2.A2
            UNION
            (SELECT s_1.A1 AS X0
             FROM s AS s_0
                      INNER JOIN s AS s_1 ON s_0.A1 = s_1.A2))
SELECT p1.X0 AS x
FROM p1