CREATE TABLE mt_subs(
    name TEXT NOT NULL PRIMARY KEY,
    com_id TEXT NOT NULL UNIQUE,
    author TEXT NOT NULL,
    author_id TEXT NOT NULL,
    server TEXT,
    file_url TEXT NOT NULL,
    state INTEGER NOT NULL
);
