CREATE TABLE bans(
    ip varchar(64) NOT NULL PRIMARY KEY,
    name VARCHAR(16) NOT NULL,
    expires TIMESTAMP NOT NULL,
    reason VARCHAR(64) NOT NULL,
    moderator TEXT NOT NULL,
    region VARCHAR(3),
    note TEXT);
