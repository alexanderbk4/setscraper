-- Setscraper Database Schema
-- Start with just the episodes table

-- Episodes table
CREATE TABLE IF NOT EXISTS episodes (
    id SERIAL PRIMARY KEY,                    -- Auto-incrementing unique ID
    episode_id VARCHAR(10) UNIQUE NOT NULL,   -- BBC episode ID (m002xxxx)
    channel VARCHAR(100) NOT NULL,            -- Radio channel name
    show_name VARCHAR(200) NOT NULL,          -- DJ/Show name
    episode_name VARCHAR(500),                -- Episode title
    broadcast_date TIMESTAMP,                 -- When it was broadcast
    created_at TIMESTAMP DEFAULT NOW()        -- When we added it to DB
);

-- Create an index on episode_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_episodes_episode_id ON episodes(episode_id); 