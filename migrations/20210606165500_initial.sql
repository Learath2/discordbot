CREATE TABLE bans(
    ip varchar(64) NOT NULL PRIMARY KEY,
    name VARCHAR(16),
    expires TIMESTAMP,
    reason VARCHAR(64),
    moderator TEXT,
    region VARCHAR(3),
    note TEXT);
