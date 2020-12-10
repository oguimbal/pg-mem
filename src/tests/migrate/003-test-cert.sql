
-- Up
CREATE TABLE whatever ( certificate TEXT );
INSERT INTO whatever ( certificate ) VALUES (
  '-----BEGIN CERTIFICATE-----
some contents
-----END CERTIFICATE-----');

-- Down
DROP TABLE whatever;