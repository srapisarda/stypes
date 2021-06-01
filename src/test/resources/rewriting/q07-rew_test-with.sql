WITH p6 AS (SELECT s_0.Y AS X0 FROM s AS s_0 UNION (SELECT b_0.X AS X0 FROM b AS b_0)),
     p12 AS (SELECT r_2.Y AS X0
             FROM p6 AS p6_0
                      INNER JOIN r AS r_1 ON p6_0.X0 = r_1.X
                      INNER JOIN r AS r_2 ON r_1.Y = r_2.X
             UNION
             (SELECT r_1.Y AS X0
              FROM a AS a_0
                       INNER JOIN r AS r_1 ON a_0.X = r_1.X)),
     p2 AS (SELECT r_1.Y AS X0
            FROM p6 AS p6_0
                     INNER JOIN r AS r_1 ON p6_0.X0 = r_1.X
                     INNER JOIN b AS b_2 ON r_1.Y = b_2.X
            UNION
            (SELECT a_0.X AS X0
             FROM a AS a_0
                      INNER JOIN b AS b_1 ON a_0.X = b_1.X)),
     p9 AS (SELECT a_0.X AS X0 FROM a AS a_0 UNION (SELECT s_0.X AS X0 FROM s AS s_0)),
     p3 AS (SELECT s_0.X AS X0
            FROM s AS s_0
                     INNER JOIN r AS r_1 ON s_0.Y = r_1.X
                     INNER JOIN p9 AS p9_2 ON r_1.Y = p9_2.X0
            UNION
            (SELECT s_0.X AS X0
             FROM s AS s_0
                      INNER JOIN b AS b_1 ON s_0.Y = b_1.X)
            UNION
            (SELECT a_0.X AS X0
             FROM a AS a_0
                      INNER JOIN p9 AS p9_1 ON a_0.X = p9_1.X0)),
     p1 AS (SELECT p3_0.X0 AS X0
            FROM p3 AS p3_0
                     INNER JOIN p2 AS p2_1 ON p3_0.X0 = p2_1.X0
                     INNER JOIN b AS b_2 ON p3_0.X0 = b_2.X
            UNION
            (SELECT s_1.Y AS X0
             FROM p12 AS p12_0
                      INNER JOIN s AS s_1 ON p12_0.X0 = s_1.X
                      INNER JOIN p3 AS p3_2 ON s_1.Y = p3_2.X0))
SELECT p1.X0
FROM p1