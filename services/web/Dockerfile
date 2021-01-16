# pull official base image
FROM python:3.8.1-slim-buster

# set work directory
WORKDIR /usr/src/app

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
 && cp pspacy.py /usr/src/app/pspacy.py

# install system dependencies
RUN apt-get update && apt-get install -y netcat

# install dependencies
RUN pip install --upgrade pip
COPY ./requirements.txt /usr/src/app/requirements.txt
RUN pip install -r requirements.txt

# copy project
COPY . /usr/src/app/

# run entrypoint.sh
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
