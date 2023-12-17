WITH p1 AS (SELECT s_2.X AS X0
            FROM t AS t_0
                     INNER JOIN r AS r_1 ON t_0.X = r_1.Y
                     INNER JOIN s AS s_2 ON r_1.X = s_2.Y
            UNION
            (SELECT s_1.X AS X0
             FROM s AS s_0
                      INNER JOIN s AS s_1 ON s_0.X = s_1.Y))
SELECT p1.X0 AS x
FROM p1


