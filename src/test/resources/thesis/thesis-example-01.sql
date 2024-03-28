WITH p AS (
 SELECT employee_id AS y, project_id AS z FROM employee_project
 UNION
 SELECT employee_id AS y, project_id  AS z
   FROM employee_project AS ep
   INNER JOIN employee AS e ON ep.employee_id = e.id
   WHERE e.manager_id IS NULL
)
SELECT persName1.name AS x1, persName2.name AS x2
FROM employee AS persName1
INNER JOIN p AS p1 ON p1.y = persName1.id
INNER JOIN p AS p2 ON  p1.z = p2.z
INNER JOIN employee AS persName2 ON p2.y = persName2.id