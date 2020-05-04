CREATE TABLE events (
    id VARCHAR(36) NOT NULL, -- Duh. No UUID in MySQL :(
    name VARCHAR(256),
    info TEXT,
    PRIMARY KEY (id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

