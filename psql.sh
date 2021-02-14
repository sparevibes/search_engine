#!/bin/sh

# load environment variables
while read var; do
    export $var
done < .env.prod.db

docker exec -it novichenko_pg_1 psql $POSTGRES_DB $POSTGRES_USER
#docker exec -it novichenko_db_1 psql "postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@db/$POSTGRES_DB"
