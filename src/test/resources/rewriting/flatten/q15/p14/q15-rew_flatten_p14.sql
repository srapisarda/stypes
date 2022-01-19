WITH p28 AS (SELECT s_0.x AS X0, r_1.y AS X1
             FROM s AS s_0
                      INNER JOIN r AS r_1 ON s_0.y = r_1.x
                      INNER JOIN a AS a_2 ON r_1.y = a_2.x
             UNION
             (SELECT a_0.x AS X0, a_0.x AS X1 FROM a AS a_0)),
     p7 AS (SELECT s_0.x AS X0, r_1.y AS X1
            FROM s AS s_0
                     INNER JOIN r AS r_1 ON s_0.y = r_1.x
            UNION
            (SELECT a_0.x AS X0, a_0.x AS X1 FROM a AS a_0)),
     p5 AS (SELECT b_0.x AS X0, b_0.x AS X1
            FROM b AS b_0
            UNION
            (SELECT s_0.y AS X0, r_1.x AS X1
             FROM s AS s_0
                      INNER JOIN r AS r_1 ON s_0.x = r_1.y)),
     p2 AS (SELECT p5_1.X1 AS X0, p7_4.X1 AS X1
            FROM r AS r_0
                     INNER JOIN p5 AS p5_1 ON r_0.x = p5_1.X0
                     INNER JOIN r AS r_2 ON r_0.x = r_2.y
                     INNER JOIN s AS s_3 ON r_2.y = s_3.x
                     INNER JOIN p7 AS p7_4 ON s_3.y = p7_4.X0
            UNION
            (SELECT p5_1.X1 AS X0, p7_3.X1 AS X1
             FROM r AS r_0
                      INNER JOIN p5 AS p5_1 ON r_0.x = p5_1.X0
                      INNER JOIN b AS b_2 ON r_0.y = b_2.x
                      INNER JOIN p7 AS p7_3 ON r_0.y = p7_3.X0)
            UNION
            (SELECT r_2.x AS X0, p7_4.X1 AS X1
             FROM s AS s_0
                      INNER JOIN r AS r_1 ON s_0.x = r_1.y
                      INNER JOIN r AS r_2 ON r_1.x = r_2.y
                      INNER JOIN a AS a_3 ON r_1.x = a_3.x
                      INNER JOIN p7 AS p7_4 ON s_0.y = p7_4.X0)
            UNION
            (SELECT r_2.x AS X0, p7_0.X1 AS X1
             FROM p7 AS p7_0
                      INNER JOIN b AS b_1 ON p7_0.X0 = b_1.x
                      INNER JOIN r AS r_2 ON p7_0.X0 = r_2.y
                      INNER JOIN a AS a_3 ON p7_0.X0 = a_3.x)),
     p43 AS (SELECT s_2.y AS X0, s_0.x AS X1
             FROM s AS s_0
                      INNER JOIN r AS r_1 ON s_0.y = r_1.x
                      INNER JOIN s AS s_2 ON r_1.y = s_2.x
             UNION
             (SELECT s_1.y AS X0, a_0.x AS X1
              FROM a AS a_0
                       INNER JOIN s AS s_1 ON a_0.x = s_1.x)
             UNION
             (SELECT s_0.y AS X0, s_0.x AS X1
              FROM s AS s_0
                       INNER JOIN b AS b_1 ON s_0.y = b_1.x)),
     p40 AS (SELECT s_2.y AS X0, b_0.x AS X1
             FROM b AS b_0
                      INNER JOIN r AS r_1 ON b_0.x = r_1.x
                      INNER JOIN s AS s_2 ON r_1.y = s_2.x
             UNION
             (SELECT b_0.x AS X0, b_0.x AS X1 FROM b AS b_0)),
     p19 AS (SELECT r_0.x AS X0, r_0.y AS X1
             FROM r AS r_0
                      INNER JOIN b AS b_1 ON r_0.y = b_1.x
             UNION
             (SELECT r_0.x AS X0, s_2.y AS X1
              FROM r AS r_0
                       INNER JOIN r AS r_1 ON r_0.y = r_1.x
                       INNER JOIN s AS s_2 ON r_1.y = s_2.x)),
     p3 AS (SELECT r_0.x AS X0, p28_2.X1 AS X1
            FROM r AS r_0
                     INNER JOIN r AS r_1 ON r_0.y = r_1.x
                     INNER JOIN p28 AS p28_2 ON r_1.y = p28_2.X0
                     INNER JOIN a AS a_3 ON r_1.y = a_3.x
            UNION
            (SELECT p19_0.X0 AS X0, p28_2.X1 AS X1
             FROM p19 AS p19_0
                      INNER JOIN r AS r_1 ON p19_0.X1 = r_1.x
                      INNER JOIN p28 AS p28_2 ON r_1.y = p28_2.X0)
            UNION
            (SELECT p19_0.X0 AS X0, r_2.y AS X1
             FROM p19 AS p19_0
                      INNER JOIN b AS b_1 ON p19_0.X1 = b_1.x
                      INNER JOIN r AS r_2 ON p19_0.X1 = r_2.x
                      INNER JOIN a AS a_3 ON r_2.y = a_3.x)),
     p35 AS (SELECT p19_0.X0 AS X0, p40_1.X0 AS X1
             FROM p19 AS p19_0
                      INNER JOIN p40 AS p40_1 ON p19_0.X1 = p40_1.X1
                      INNER JOIN b AS b_2 ON p19_0.X1 = b_2.x
             UNION
             (SELECT p19_0.X0 AS X0, p43_2.X0 AS X1
              FROM p19 AS p19_0
                       INNER JOIN r AS r_1 ON p19_0.X1 = r_1.x
                       INNER JOIN p43 AS p43_2 ON r_1.y = p43_2.X1)
             UNION
             (SELECT r_0.x AS X0, p43_2.X0 AS X1
              FROM r AS r_0
                       INNER JOIN r AS r_1 ON r_0.y = r_1.x
                       INNER JOIN p43 AS p43_2 ON r_1.y = p43_2.X1
                       INNER JOIN a AS a_3 ON r_1.y = a_3.x)),
     p1 AS (SELECT p35_0.X0 AS X0, p2_2.X1 AS X1
            FROM p35 AS p35_0
                     INNER JOIN r AS r_1 ON p35_0.X1 = r_1.x
                     INNER JOIN p2 AS p2_2 ON r_1.y = p2_2.X0
            UNION
            (SELECT p35_0.X0 AS X0, s_1.x AS X1
             FROM p35 AS p35_0
                      INNER JOIN s AS s_1 ON p35_0.X1 = s_1.y
                      INNER JOIN b AS b_2 ON p35_0.X1 = b_2.x)
            UNION
            (SELECT p3_0.X0 AS X0, p2_1.X1 AS X1
             FROM p3 AS p3_0
                      INNER JOIN p2 AS p2_1 ON p3_0.X1 = p2_1.X0
                      INNER JOIN a AS a_2 ON p3_0.X1 = a_2.x))
SELECT p1.X0 AS x, p1.X1 AS y
FROM p1
