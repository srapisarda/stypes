CREATE TEMP TABLE a (c0 text);
CREATE TEMP TABLE b (c0 text);
CREATE TEMP TABLE r (c0 text, c1 text);
CREATE TEMP TABLE s (c0 text, c1 text);

INSERT INTO a VALUES ('d2');
INSERT INTO b VALUES ('e5');
INSERT INTO r VALUES
  ('c0', 'c1'), ('c1', 'c2'), ('c3', 'c4'), ('c5', 'c6'),
  ('c7', 'c8'), ('c8', 'c9'),
  ('d0', 'd1'), ('d1', 'd2'), ('d5', 'd6'), ('d7', 'd8'), ('d8', 'd9'),
  ('e0', 'e1'), ('e1', 'e2'), ('e3', 'e4'), ('e5', 'e8'), ('e8', 'e9'),
  ('dead0', 'dead1');
INSERT INTO s VALUES
  ('c2', 'c3'), ('c4', 'c5'), ('c6', 'c7'),
  ('d2', 'd5'), ('d6', 'd7'),
  ('e2', 'e3'), ('e4', 'e5');
