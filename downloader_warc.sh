#!/bin/sh

# load environment variables
while read var; do
    export $var
done < .env.prod.db

# generate a unique hashid
hashid=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)

# build the docker container
#cd services/downloader_warc
#docker build -t novichenko/downloader_warc .

# launch the docker container
docker run -d --rm --name=metahtml_warc_$hashid --network=novichenko novichenko/downloader_warc --db=postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@db:5432/$POSTGRES_DB $@
