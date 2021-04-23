SELECT p3_0.X0 AS X0
FROM (SELECT s_0.X AS X0
      FROM s AS s_0
               INNER JOIN r AS r_1 ON s_0.Y = r_1.X
               INNER JOIN b AS b_2 ON r_1.Y = b_2.X
      UNION
      (SELECT s_0.X AS X0
       FROM s AS s_0
                INNER JOIN b AS b_1 ON s_0.Y = b_1.X)
      UNION
      (SELECT a_0.X AS X0
       FROM a AS a_0
                INNER JOIN b AS b_1 ON a_0.X = b_1.X)) AS p3_0
         INNER JOIN b AS b_1 ON p3_0.X0 = b_1.X
