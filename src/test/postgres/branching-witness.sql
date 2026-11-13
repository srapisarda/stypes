CREATE TEMP TABLE seed (c0 text);
CREATE TEMP TABLE path (c0 text, c1 text);
CREATE TEMP TABLE edgeq (c0 text, c1 text);
CREATE TEMP TABLE edger (c0 text, c1 text);

INSERT INTO seed VALUES ('from_seed');
INSERT INTO path VALUES ('ordinary', 'join'), ('missing', 'incomplete');
INSERT INTO edgeq VALUES ('join', 'left'), ('incomplete', 'left');
INSERT INTO edger VALUES ('join', 'right');
