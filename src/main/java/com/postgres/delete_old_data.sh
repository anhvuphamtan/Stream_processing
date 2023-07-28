#!/bin/bash

DB_HOST="postgres"
DB_PORT="5432"
DB_NAME="postgres"
DB_USER="postgres"
DB_PASSWORD="mypassword"

SQL_FILE="/app/src/main/java/com/postgres/delete_old_data.sql"

SQL_COMMAND=$(cat $SQL_FILE)

psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "$SQL_COMMAND"

