## Test a task in the terminal

Enter the shell of a running container using docker compose syntax:

```
docker compose exec <container_name>
```

Once inside the container:

```
 airflow tasks test user_processing create_table
```

You will get either success or fail for the executed task.