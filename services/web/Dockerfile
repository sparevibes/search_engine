# pull official base image
FROM python:3.8.1-slim-buster

# set work directory
RUN adduser app
ENV HOME=/home/app
ENV APP_HOME=/home/app/web
RUN mkdir $APP_HOME
WORKDIR $APP_HOME

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install pspacy dependencies
RUN apt-get update && apt-get install -y \
    autoconf \
    gcc \
    git \
    make \
    python3 \
    python3-pip \
    sudo \
    wget

# install pspacy
ADD https://api.github.com/repos/mikeizbicki/pspacy/git/refs/heads/master /tmp/pspacy.version
RUN cd /tmp \
 && git clone https://github.com/mikeizbicki/pspacy \
 && cd pspacy \
 && sh install_dependencies.sh \
 && cp pspacy.py $APP_HOME/pspacy.py

# install system dependencies
RUN apt-get update && apt-get install -y netcat

# copy project
COPY . $APP_HOME

# install dependencies
RUN pip install --upgrade pip
COPY ./requirements.txt $APP_HOME/requirements.txt
RUN pip install -r $APP_HOME/requirements.txt

# chown all the files to the app user
RUN chown -R app $APP_HOME

# change to the app user
USER app

# run entrypoint.sh
#ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
EXPOSE 5000
