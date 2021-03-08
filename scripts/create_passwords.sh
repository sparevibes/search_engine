#/bin/sh

set -e

env_file=.env.prod

if [ -e "$env_file" ]; then
    echo "ERROR: $env_file already exists"
    exit 1
fi

echo 'DB_NAME=novichenko' >> $env_file
echo 'DB_USER=novichenko' >> $env_file
echo 'POSTGRES_NAME=novichenko' >> $env_file
echo 'POSTGRES_USER=novichenko' >> $env_file

password=$(< /dev/urandom tr -dc A-Za-z0-9 | head -c${1:-64})

echo "DB_PASSWORD=$password" >> $env_file
echo "POSTGRES_PASSWORD=$password" >> $env_file
