WITH p45 AS (SELECT s_0.a2 AS X0, s_0.a1 AS X1
             FROM s AS s_0
                      INNER JOIN b AS b_1 ON s_0.a2 = b_1.a1
             UNION
             (SELECT s_1.a2 AS X0, a_0.a1 AS X1
              FROM a AS a_0
                       INNER JOIN s AS s_1 ON a_0.a1 = s_1.a1)
             UNION
             (SELECT s_2.a2 AS X0, s_0.a1 AS X1
              FROM s AS s_0
                       INNER JOIN r AS r_1 ON s_0.a2 = r_1.a1
                       INNER JOIN s AS s_2 ON r_1.a2 = s_2.a1)),
     p42 AS (SELECT s_2.a2 AS X0, b_0.a1 AS X1
             FROM b AS b_0
                      INNER JOIN r AS r_1 ON b_0.a1 = r_1.a1
                      INNER JOIN s AS s_2 ON r_1.a2 = s_2.a1
             UNION
             (SELECT b_0.a1 AS X0, b_0.a1 AS X1 FROM b AS b_0)),
     p5 AS (SELECT s_0.a1 AS X0, s_0.a2 AS X1
            FROM s AS s_0
                     INNER JOIN a AS a_1 ON s_0.a2 = a_1.a1
            UNION
            (SELECT s_0.a1 AS X0, r_2.a2 AS X1
             FROM s AS s_0
                      INNER JOIN s AS s_1 ON s_0.a2 = s_1.a1
                      INNER JOIN r AS r_2 ON s_1.a2 = r_2.a1)),
     p14 AS (SELECT r_1.a2 AS X0, r_2.a1 AS X1
             FROM s AS s_0
                      INNER JOIN r AS r_1 ON s_0.a2 = r_1.a1
                      INNER JOIN r AS r_2 ON s_0.a1 = r_2.a2
             UNION
             (SELECT a_0.a1 AS X0, r_1.a1 AS X1
              FROM a AS a_0
                       INNER JOIN r AS r_1 ON a_0.a1 = r_1.a2)
             UNION
             (SELECT r_0.a2 AS X0, r_0.a1 AS X1
              FROM r AS r_0
                       INNER JOIN b AS b_1 ON r_0.a1 = b_1.a1)),
     p15 AS (SELECT b_0.a1 AS X0, b_0.a1 AS X1
             FROM b AS b_0
                      INNER JOIN a AS a_1 ON b_0.a1 = a_1.a1
             UNION
             (SELECT r_2.a2 AS X0, b_0.a1 AS X1
              FROM b AS b_0
                       INNER JOIN s AS s_1 ON b_0.a1 = s_1.a1
                       INNER JOIN r AS r_2 ON s_1.a2 = r_2.a1)),
     p2 AS (SELECT p14_2.X1 AS X0, p15_1.X0 AS X1
            FROM b AS b_0
                     INNER JOIN p15 AS p15_1 ON b_0.a1 = p15_1.X1
                     INNER JOIN p14 AS p14_2 ON b_0.a1 = p14_2.X0
            UNION
            (SELECT p14_1.X1 AS X0, p5_2.X1 AS X1
             FROM r AS r_0
                      INNER JOIN p14 AS p14_1 ON r_0.a1 = p14_1.X0
                      INNER JOIN p5 AS p5_2 ON r_0.a2 = p5_2.X0)),
     p30 AS (SELECT a_0.a1 AS X0, a_0.a1 AS X1
             FROM a AS a_0
             UNION
             (SELECT s_0.a1 AS X0, r_1.a2 AS X1
              FROM s AS s_0
                       INNER JOIN r AS r_1 ON s_0.a2 = r_1.a1
                       INNER JOIN a AS a_2 ON r_1.a2 = a_2.a1)),
     p21 AS (SELECT r_0.a1 AS X0, s_2.a2 AS X1
             FROM r AS r_0
                      INNER JOIN r AS r_1 ON r_0.a2 = r_1.a1
                      INNER JOIN s AS s_2 ON r_1.a2 = s_2.a1
             UNION
             (SELECT r_0.a1 AS X0, r_0.a2 AS X1
              FROM r AS r_0
                       INNER JOIN b AS b_1 ON r_0.a2 = b_1.a1)),
     p37 AS (SELECT p21_0.X0 AS X0, p42_1.X0 AS X1
             FROM p21 AS p21_0
                      INNER JOIN p42 AS p42_1 ON p21_0.X1 = p42_1.X1
                      INNER JOIN b AS b_2 ON p21_0.X1 = b_2.a1
             UNION
             (SELECT p21_0.X0 AS X0, p45_2.X0 AS X1
              FROM p21 AS p21_0
                       INNER JOIN r AS r_1 ON p21_0.X1 = r_1.a1
                       INNER JOIN p45 AS p45_2 ON r_1.a2 = p45_2.X1)
             UNION
             (SELECT r_0.a1 AS X0, p45_2.X0 AS X1
              FROM r AS r_0
                       INNER JOIN r AS r_1 ON r_0.a2 = r_1.a1
                       INNER JOIN p45 AS p45_2 ON r_1.a2 = p45_2.X1
                       INNER JOIN a AS a_3 ON r_1.a2 = a_3.a1)),
     p3 AS (SELECT p21_0.X0 AS X0, p30_2.X1 AS X1
            FROM p21 AS p21_0
                     INNER JOIN r AS r_1 ON p21_0.X1 = r_1.a1
                     INNER JOIN p30 AS p30_2 ON r_1.a2 = p30_2.X0
            UNION
            (SELECT p21_0.X0 AS X0, r_2.a2 AS X1
             FROM p21 AS p21_0
                      INNER JOIN b AS b_1 ON p21_0.X1 = b_1.a1
                      INNER JOIN r AS r_2 ON p21_0.X1 = r_2.a1
                      INNER JOIN a AS a_3 ON r_2.a2 = a_3.a1)
            UNION
            (SELECT r_0.a1 AS X0, p30_2.X1 AS X1
             FROM r AS r_0
                      INNER JOIN r AS r_1 ON r_0.a2 = r_1.a1
                      INNER JOIN p30 AS p30_2 ON r_1.a2 = p30_2.X0
                      INNER JOIN a AS a_3 ON r_1.a2 = a_3.a1)),
     p1 AS (SELECT p3_0.X0 AS X0, p2_1.X1 AS X1
            FROM p3 AS p3_0
                     INNER JOIN p2 AS p2_1 ON p3_0.X1 = p2_1.X0
                     INNER JOIN a AS a_2 ON p3_0.X1 = a_2.a1
            UNION
            (SELECT p37_0.X0 AS X0, p2_2.X1 AS X1
             FROM p37 AS p37_0
                      INNER JOIN r AS r_1 ON p37_0.X1 = r_1.a1
                      INNER JOIN p2 AS p2_2 ON r_1.a2 = p2_2.X0))
SELECT p1.X0 AS x, p1.X1 AS y
FROM p1