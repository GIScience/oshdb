CREATE TABLE IF NOT EXISTS grid (
  zoom INT,
  id  BIGINT,
  data BLOB,
  PRIMARY KEY (zoom,id)
);
