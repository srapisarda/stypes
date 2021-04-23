SELECT a_0.X AS X0, s_5.Y AS X1
FROM a AS a_0
         INNER JOIN r AS r_1 ON a_0.X = r_1.X
         INNER JOIN b AS b_2 ON r_1.Y = b_2.X
         INNER JOIN r AS r_3 ON r_1.Y = r_3.X
         INNER JOIN s AS s_4 ON r_3.Y = s_4.X
         INNER JOIN s AS s_5 ON s_4.Y = s_5.X
