SELECT a_0.X AS X0, r_1.Y AS X1
FROM a AS a_0
         INNER JOIN r AS r_1 ON a_0.X = r_1.X
         INNER JOIN b AS b_2 ON r_1.Y = b_2.X
UNION
(SELECT r_0.X AS X0, s_1.Y AS X1
 FROM r AS r_0
          INNER JOIN s AS s_1 ON r_0.Y = s_1.X
          INNER JOIN b AS b_2 ON s_1.Y = b_2.X)
