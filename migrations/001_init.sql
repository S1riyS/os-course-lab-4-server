CREATE TABLE IF NOT EXISTS filesystems (
    token VARCHAR(255) PRIMARY KEY,
    root_ino BIGINT NOT NULL,
    next_ino BIGINT NOT NULL DEFAULT 1001,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inodes (
    ino BIGINT NOT NULL,
    token VARCHAR(255) NOT NULL REFERENCES filesystems(token) ON DELETE CASCADE,
    type SMALLINT NOT NULL,  -- 0 = DIR, 1 = FILE
    mode INTEGER NOT NULL,   -- umode_t
    size BIGINT NOT NULL DEFAULT 0,
    ref_count INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token, ino)
);

CREATE TABLE IF NOT EXISTS directory_entries (
    token VARCHAR(255) NOT NULL,
    parent_ino BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    ino BIGINT NOT NULL,
    PRIMARY KEY (token, parent_ino, name),
    FOREIGN KEY (token, ino) REFERENCES inodes(token, ino) ON DELETE CASCADE,
    FOREIGN KEY (token, parent_ino) REFERENCES inodes(token, ino) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dir_entries_token_parent ON directory_entries(token, parent_ino);
CREATE INDEX IF NOT EXISTS idx_dir_entries_token_ino ON directory_entries(token, ino);

CREATE TABLE IF NOT EXISTS file_contents (
    token VARCHAR(255) NOT NULL,
    ino BIGINT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (token, ino),
    FOREIGN KEY (token, ino) REFERENCES inodes(token, ino) ON DELETE CASCADE
);

