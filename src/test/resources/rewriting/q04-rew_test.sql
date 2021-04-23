SELECT a_0.X AS X0, p2_1.X1 AS X1
FROM a AS a_0
         INNER JOIN (SELECT r_0.X AS X0, p3_2.X1 AS X1
                     FROM r AS r_0
                              INNER JOIN s AS s_1 ON r_0.Y = s_1.X
                              INNER JOIN (SELECT a_0.X AS X0, r_1.Y AS X1
                                          FROM a AS a_0
                                                   INNER JOIN r AS r_1 ON a_0.X = r_1.X
                                                   INNER JOIN b AS b_2 ON r_1.Y = b_2.X) AS p3_2
                                         ON s_1.Y = p3_2.X0) AS p2_1 ON a_0.X = p2_1.X0
         INNER JOIN b AS b_2 ON p2_1.X1 = b_2.X
