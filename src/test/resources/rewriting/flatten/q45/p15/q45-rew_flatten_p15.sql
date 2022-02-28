WITH p41 AS (SELECT a_0.x AS X0, a_0.x AS X1
             FROM a AS a_0
             UNION
             (SELECT s_0.x AS X0, r_1.y AS X1
              FROM s AS s_0
                       INNER JOIN r AS r_1 ON s_0.y = r_1.x
                       INNER JOIN a AS a_2 ON r_1.y = a_2.x)),
     p5 AS (SELECT a_0.x AS X0, r_1.y AS X1
            FROM a AS a_0
                     INNER JOIN r AS r_1 ON a_0.x = r_1.x
            UNION
            (SELECT s_0.x AS X0, r_2.y AS X1
             FROM s AS s_0
                      INNER JOIN r AS r_1 ON s_0.y = r_1.x
                      INNER JOIN r AS r_2 ON r_1.y = r_2.x)),
     p36 AS (SELECT p5_0.X0 AS X0, p41_2.X1 AS X1
             FROM p5 AS p5_0
                      INNER JOIN s AS s_1 ON p5_0.X1 = s_1.x
                      INNER JOIN p41 AS p41_2 ON s_1.y = p41_2.X0
             UNION
             (SELECT b_0.x AS X0, p41_2.X1 AS X1
              FROM b AS b_0
                       INNER JOIN a AS a_1 ON b_0.x = a_1.x
                       INNER JOIN p41 AS p41_2 ON b_0.x = p41_2.X0)
             UNION
             (SELECT s_0.x AS X0, p41_3.X1 AS X1
              FROM s AS s_0
                       INNER JOIN r AS r_1 ON s_0.y = r_1.x
                       INNER JOIN b AS b_2 ON r_1.y = b_2.x
                       INNER JOIN p41 AS p41_3 ON r_1.y = p41_3.X0)),
     p45 AS (SELECT s_2.x AS X0, b_0.x AS X1
             FROM b AS b_0
                      INNER JOIN r AS r_1 ON b_0.x = r_1.y
                      INNER JOIN s AS s_2 ON r_1.x = s_2.y
             UNION
             (SELECT a_0.x AS X0, a_0.x AS X1
              FROM a AS a_0
                       INNER JOIN b AS b_1 ON a_0.x = b_1.x)),
     p42 AS (SELECT r_0.y AS X0, r_0.x AS X1
             FROM r AS r_0
                      INNER JOIN a AS a_1 ON r_0.x = a_1.x
             UNION
             (SELECT r_1.y AS X0, s_2.x AS X1
              FROM r AS r_0
                       INNER JOIN r AS r_1 ON r_0.y = r_1.x
                       INNER JOIN s AS s_2 ON r_0.x = s_2.y)),
     p21 AS (SELECT s_2.y AS X0, a_0.x AS X1
             FROM a AS a_0
                      INNER JOIN r AS r_1 ON a_0.x = r_1.x
                      INNER JOIN s AS s_2 ON r_1.y = s_2.x
             UNION
             (SELECT a_0.x AS X0, a_0.x AS X1
              FROM a AS a_0
                       INNER JOIN b AS b_1 ON a_0.x = b_1.x)),
     p31 AS (SELECT r_0.x AS X0, s_2.y AS X1
             FROM r AS r_0
                      INNER JOIN r AS r_1 ON r_0.y = r_1.x
                      INNER JOIN s AS s_2 ON r_1.y = s_2.x
             UNION
             (SELECT r_0.x AS X0, r_0.y AS X1
              FROM r AS r_0
                       INNER JOIN b AS b_1 ON r_0.y = b_1.x)),
     p37 AS (SELECT p42_1.X1 AS X0, p21_2.X0 AS X1
             FROM a AS a_0
                      INNER JOIN p42 AS p42_1 ON a_0.x = p42_1.X0
                      INNER JOIN p21 AS p21_2 ON a_0.x = p21_2.X1
             UNION
             (SELECT p45_1.X0 AS X0, p31_2.X1 AS X1
              FROM b AS b_0
                       INNER JOIN p45 AS p45_1 ON b_0.x = p45_1.X1
                       INNER JOIN p31 AS p31_2 ON b_0.x = p31_2.X0)
             UNION
             (SELECT p42_0.X1 AS X0, p31_2.X1 AS X1
              FROM p42 AS p42_0
                       INNER JOIN s AS s_1 ON p42_0.X0 = s_1.x
                       INNER JOIN p31 AS p31_2 ON s_1.y = p31_2.X0)),
     p3 AS (SELECT p31_2.X1 AS X0, r_1.x AS X1
            FROM b AS b_0
                     INNER JOIN r AS r_1 ON b_0.x = r_1.y
                     INNER JOIN p31 AS p31_2 ON b_0.x = p31_2.X0
                     INNER JOIN b AS b_3 ON r_1.x = b_3.x
            UNION
            (SELECT p21_3.X0 AS X0, r_0.x AS X1
             FROM r AS r_0
                      INNER JOIN r AS r_1 ON r_0.y = r_1.x
                      INNER JOIN a AS a_2 ON r_1.y = a_2.x
                      INNER JOIN p21 AS p21_3 ON r_1.y = p21_3.X1
                      INNER JOIN b AS b_4 ON r_0.x = b_4.x)
            UNION
            (SELECT p31_3.X1 AS X0, r_0.x AS X1
             FROM r AS r_0
                      INNER JOIN r AS r_1 ON r_0.y = r_1.x
                      INNER JOIN s AS s_2 ON r_1.y = s_2.x
                      INNER JOIN p31 AS p31_3 ON s_2.y = p31_3.X0
                      INNER JOIN b AS b_4 ON r_0.x = b_4.x)),
     p14 AS (SELECT s_1.y AS X0, a_0.x AS X1
             FROM a AS a_0
                      INNER JOIN s AS s_1 ON a_0.x = s_1.x
             UNION
             (SELECT s_2.y AS X0, s_0.x AS X1
              FROM s AS s_0
                       INNER JOIN r AS r_1 ON s_0.y = r_1.x
                       INNER JOIN s AS s_2 ON r_1.y = s_2.x)
             UNION
             (SELECT s_0.y AS X0, s_0.x AS X1
              FROM s AS s_0
                       INNER JOIN b AS b_1 ON s_0.y = b_1.x)),
     p2 AS (SELECT b_0.x AS X0, p14_2.X0 AS X1
            FROM b AS b_0
                     INNER JOIN a AS a_1 ON b_0.x = a_1.x
                     INNER JOIN p14 AS p14_2 ON b_0.x = p14_2.X1
            UNION
            (SELECT s_0.x AS X0, p14_3.X0 AS X1
             FROM s AS s_0
                      INNER JOIN r AS r_1 ON s_0.y = r_1.x
                      INNER JOIN b AS b_2 ON r_1.y = b_2.x
                      INNER JOIN p14 AS p14_3 ON r_1.y = p14_3.X1)
            UNION
            (SELECT p5_0.X0 AS X0, p14_2.X0 AS X1
             FROM p5 AS p5_0
                      INNER JOIN s AS s_1 ON p5_0.X1 = s_1.x
                      INNER JOIN p14 AS p14_2 ON s_1.y = p14_2.X1)),
     p1 AS (SELECT p2_0.X0 AS X0, p3_2.X0 AS X1
            FROM p2 AS p2_0
                     INNER JOIN b AS b_1 ON p2_0.X1 = b_1.x
                     INNER JOIN p3 AS p3_2 ON p2_0.X1 = p3_2.X1
            UNION
            (SELECT p2_0.X0 AS X0, p37_2.X1 AS X1
             FROM p2 AS p2_0
                      INNER JOIN r AS r_1 ON p2_0.X1 = r_1.x
                      INNER JOIN p37 AS p37_2 ON r_1.y = p37_2.X0)
            UNION
            (SELECT p36_0.X0 AS X0, p37_2.X1 AS X1
             FROM p36 AS p36_0
                      INNER JOIN a AS a_1 ON p36_0.X1 = a_1.x
                      INNER JOIN p37 AS p37_2 ON p36_0.X1 = p37_2.X0))
SELECT p1.X0 AS x, p1.X1 AS y
FROM p1