#!/bin/sh
psql -U postgres -c "CREATE DATABASE dvdrental;"
psql -U postgres -d dvdrental -f "/dvdrental/restore.sql"