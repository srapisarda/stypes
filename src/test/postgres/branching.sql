CREATE TEMP TABLE m286004 (c0 text, c1 text, c2 text, c3 text);
CREATE TEMP TABLE m252004 (c0 text, c1 text, c2 text, c3 text);
CREATE TEMP TABLE m165004 (c0 text, c1 text, c2 text, c3 text);
CREATE TEMP TABLE m297004 (c0 text, c1 text, c2 text, c3 text);
CREATE TEMP TABLE m203004 (c0 text, c1 text, c2 text, c3 text);

INSERT INTO m286004 VALUES ('good', '1', '2', '3'), ('missing', '1', '2', '3');
INSERT INTO m252004 VALUES ('1', 'good', '2', '3'), ('1', 'missing', '2', '3');
INSERT INTO m165004 VALUES ('good', '1', '2', '3'), ('missing', '1', '2', '3');
INSERT INTO m297004 VALUES ('good', '1', '2', '3');
INSERT INTO m203004 VALUES ('good', '1', '2', '3'), ('missing', '1', '2', '3');
