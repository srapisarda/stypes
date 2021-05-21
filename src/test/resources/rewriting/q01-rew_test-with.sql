WITH p1 AS (SELECT a_0.X AS X0, r_1.Y AS X1
            FROM a AS a_0
                     INNER JOIN r AS r_1 ON a_0.X = r_1.X
                     INNER JOIN b AS b_2 ON r_1.Y = b_2.X)
SELECT p1.X0, p1.X1 FROM p1