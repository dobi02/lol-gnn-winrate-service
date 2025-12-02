--psql "postgresql://data_user:data_password@localhost:5433/data_db"

CREATE TABLE IF NOT EXISTS summoners (
    puuid                 TEXT PRIMARY KEY,
    summoner_id           TEXT,
    summoner_name         TEXT,
    last_match_fetched_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS matches (
    match_id   TEXT PRIMARY KEY,
    queue_id   INT NOT NULL,
    match_json JSONB NOT NULL,
    is_root    BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS participants (
    match_id       TEXT NOT NULL,
    puuid          TEXT NOT NULL,
    participant_no INT NOT NULL,
    team_id        INT NOT NULL,      -- 100 / 200 blue / red
    champion_id    INT NOT NULL,
    PRIMARY KEY (match_id, puuid),
    FOREIGN KEY (match_id) REFERENCES matches(match_id),
    FOREIGN KEY (puuid)    REFERENCES summoners(puuid)
);

CREATE TABLE IF NOT EXISTS champion_masteries (
    puuid           TEXT NOT NULL,
    champion_id     INT  NOT NULL,
    champion_level  INT,
    champion_points INT,
    tokens_earned   INT,
    PRIMARY KEY (puuid, champion_id),
    FOREIGN KEY (puuid) REFERENCES summoners(puuid)
);

CREATE INDEX IF NOT EXISTS idx_matches_is_root ON matches(is_root);
CREATE INDEX IF NOT EXISTS idx_participants_puuid ON participants(puuid);