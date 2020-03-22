FROM postgres:alpine
COPY ./data /dvdrental
COPY init.sh /docker-entrypoint-initdb.d