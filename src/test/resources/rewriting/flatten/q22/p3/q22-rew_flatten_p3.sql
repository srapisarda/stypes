WITH p12 AS (SELECT r_0.y AS X0, r_0.x AS X1
             FROM r AS r_0
                      INNER JOIN b AS b_1 ON r_0.y = b_1.x
             UNION
             (SELECT s_2.y AS X0, r_0.x AS X1
              FROM s AS s_2
                       INNER JOIN r AS r_1 on s_2.x = r_1.y
                       INNER JOIN r AS r_0 on r_1.x = r_0.y
             )),
     p1 AS (SELECT s_0.x AS X0, p12_4.X0 AS X1
            FROM p12 AS p12_4
                INNER JOIN r AS r_3 ON r_3.y = p12_4.X1
                INNER JOIN r AS r_2 ON r_2.y = r_3.x
                INNER JOIN r AS r_1 ON r_1.y = r_2.x
                INNER JOIN s as s_0 ON s_0.y = r_1.x
            UNION
            (SELECT a_0.x AS X0, p12_3.X0 AS X1
            FROM a AS a_0
                     INNER JOIN r AS r_1 ON a_0.x = r_1.x
                     INNER JOIN r AS r_2 ON r_1.y = r_2.x
                     INNER JOIN p12 AS p12_3 ON r_2.y = p12_3.X1))
SELECT p1.X0 AS x, p1.X1 AS y
FROM p1