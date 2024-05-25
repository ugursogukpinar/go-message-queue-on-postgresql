# Very simple message queue implementation on PostgreSQL with Golang

1- Run the initial migration.

```bash
psql -d your_database_name < init.sql
```

2- Run workers

```bash
 go run worker.go -conn "postgres://localhost:5432/your_database_name?sslmode=disable" -count 5
```

3- Insert a row

```sql
INSERT INTO tasks (task_name, payload) VALUES ('example_task', '{"key": "value"}');
```
