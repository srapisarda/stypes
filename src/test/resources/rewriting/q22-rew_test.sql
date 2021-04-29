SELECT p3_0.X0 AS X0, p12_2.X0 AS X1
FROM (SELECT a_0.X AS X0, r_1.Y AS X1
      FROM a AS a_0
               INNER JOIN r AS r_1 ON a_0.X = r_1.X
      UNION
      (SELECT s_0.X AS X0, r_2.Y AS X1
       FROM s AS s_0
                INNER JOIN r AS r_1 ON s_0.Y = r_1.X
                INNER JOIN r AS r_2 ON r_1.Y = r_2.X)) AS p3_0
         INNER JOIN r AS r_1 ON p3_0.X1 = r_1.X
         INNER JOIN (SELECT s_2.Y AS X0, r_0.X AS X1
                     FROM r AS r_0
                              INNER JOIN r AS r_1 ON r_0.Y = r_1.X
                              INNER JOIN s AS s_2 ON r_1.Y = s_2.X
                     UNION
                     (SELECT r_0.Y AS X0, r_0.X AS X1
                      FROM r AS r_0
                               INNER JOIN b AS b_1 ON r_0.Y = b_1.X)) AS p12_2 ON r_1.Y = p12_2.X1
