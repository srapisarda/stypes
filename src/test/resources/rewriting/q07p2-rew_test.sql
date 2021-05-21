SELECT r_1.Y AS X0
FROM (SELECT s_0.Y AS X0 FROM s AS s_0 UNION (SELECT b_0.X AS X0 FROM b AS b_0)) AS p6_0
         INNER JOIN r AS r_1 ON p6_0.X0 = r_1.X
         INNER JOIN b AS b_2 ON r_1.Y = b_2.X
