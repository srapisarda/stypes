WITH p1 AS (SELECT r_0.x AS X0, r_0.y AS X1
            FROM r AS r_0
                     INNER JOIN b AS b_1 ON r_0.y = b_1.x
            UNION
            (SELECT r_0.x AS X0, t_2.y AS X1
             FROM r AS r_0
                      INNER JOIN s AS s_1 ON r_0.y = s_1.x
                      INNER JOIN t AS t_2 ON s_1.y = t_2.x)
            UNION
            (SELECT a_0.x AS X0, t_1.y AS X1
             FROM a AS a_0
                      INNER JOIN t AS t_1 ON a_0.x = t_1.x))
SELECT p1.X0 AS x, p1.X1 AS y
FROM p1
