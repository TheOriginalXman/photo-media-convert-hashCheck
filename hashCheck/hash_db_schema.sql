CREATE TABLE files (
    file_path TEXT PRIMARY KEY,
    file_hash TEXT,
    initial_date TIMESTAMP,
    missing_date TIMESTAMP,
    mismatch_date TIMESTAMP,
    file_type TEXT
);
