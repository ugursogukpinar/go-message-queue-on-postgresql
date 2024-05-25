CREATE TABLE IF NOT EXISTS tasks (
    id SERIAL PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending', -- Status of the task: pending, processing, completed, etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when the task was created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp when the task was last updated
);


-- Create or replace the trigger function
CREATE OR REPLACE FUNCTION notify_new_task() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('pg_tasks_channel', NEW.id::TEXT);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop the trigger if it exists and then create it again
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'after_insert_task_notify') THEN
        DROP TRIGGER after_insert_task_notify ON tasks;
    END IF;
    CREATE TRIGGER after_insert_task_notify
    AFTER INSERT ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION notify_new_task();
END $$;
