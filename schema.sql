CREATE TABLE bans(
    Ip varchar(64) NOT NULL PRIMARY KEY,
    Name VARCHAR(16),
    Expires TIMESTAMP,
    Reason VARCHAR(64),
    Moderator TEXT,
    Region VARCHAR(3));
