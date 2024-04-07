# Setup Airflow with docker

**You need to be in this directory**

1. Install docker desktop and start it up
https://www.docker.com/products/docker-desktop/

2. You need at least 4GB memory, check
```bash
docker run --rm "debian:bullseye-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'
```

3. Create required directories (1st time only)
```bash
mkdir -p ./dags ./logs ./plugins ./config
```

4. init database (1st time only)
```bash
docker compose up airflow-init
```
5. Start the services (compose with build)
```bash 
docker compose up --build
```

5. Open the webserver
http://localhost:8080
- **Login: airflow**
- **Password: airflow**

6. Stop the services
```bash
docker compose down
```
or ctrl-c in the terminal to gracefully stop the services

# Establish connection to the database (postgres) from airflow
- go to http://localhost:8080/admin/connection/
- Create a new connection
    - Conn Id: bt4301_postgres
    - Conn Type: postgres
    - Host: dataops-external_db-1
    - Database: db
    - Login: db
    - Password: db
    - Port: 5432

# Establish connection to database (postgres) via psql
https://www.postgresql.org/download/

```bash
PGPASSWORD=db psql -h localhost -p 5433 -U db -d db
```
Now you can run SQL commands in the terminal!

# Establish connection to database (postgres) via pgadmin
1. Open pgadmin
2. Add a new server
    - Host: 127.0.0.1
    - Port: 5433
    - Username: db
    - Password: db

