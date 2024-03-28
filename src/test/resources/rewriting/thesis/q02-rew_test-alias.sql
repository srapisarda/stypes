SELECT s_0.a1 AS X0, p2_2.X1 AS X1
FROM s AS s_0
         INNER JOIN r AS r_1 ON s_0.a2 = r_1.a1
         INNER JOIN (SELECT r_0.a1 AS X0, p3_1.X1 AS X1
                     FROM r AS r_0
                              INNER JOIN (SELECT s_0.a1 AS X0, r_1.a1 AS X1
                                          FROM s AS s_0
                                                   INNER JOIN r AS r_1 ON s_0.a2 = r_1.a2) AS p3_1
                                         ON r_0.a2 = p3_1.X0) AS p2_2 ON r_1.a2 = p2_2.X0
UNION
(SELECT p2_0.X0 AS X0, p3_1.X1 AS X1
 FROM (SELECT r_0.a1 AS X0, p3_1.X1 AS X1
       FROM r AS r_0
                INNER JOIN (SELECT s_0.a1 AS X0, r_1.a1 AS X1
                            FROM s AS s_0
                                     INNER JOIN r AS r_1 ON s_0.a2 = r_1.a2) AS p3_1 ON r_0.a2 = p3_1.X0) AS p2_0
          INNER JOIN (SELECT s_0.a1 AS X0, r_1.a1 AS X1
                      FROM s AS s_0
                               INNER JOIN r AS r_1 ON s_0.a2 = r_1.a2) AS p3_1 ON p2_0.X1 = p3_1.X0)