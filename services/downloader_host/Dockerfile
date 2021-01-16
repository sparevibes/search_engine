FROM postgres:12

# install system packages
RUN apt-get update && apt-get install -y \
    autoconf \
    gcc \
    git \
    make \
    postgresql-server-dev-12 \
    postgresql-plpython3-12 \
    python3 \
    python3-pip \
    sudo \
    wget

# install postgres extensions from source
RUN cd /tmp \
 && git clone https://github.com/citusdata/postgresql-hll \
 && cd postgresql-hll \
 && git checkout v2.15 \
 && make \
 && make install \
 && rm -rf /tmp/postgresql-hll

RUN cd /tmp \
 && git clone https://github.com/postgrespro/rum \
 && cd rum \
 && git checkout 1.3.7 \
 && make USE_PGXS=1 \
 && make USE_PGXS=1 install \
 && rm -rf /tmp/rum

# install pspacy
# FIXME:
# should we add a git checkout version?
ADD https://api.github.com/repos/mikeizbicki/pspacy/git/refs/heads/master /tmp/pspacy.version
RUN cd /tmp \
 && git clone https://github.com/mikeizbicki/pspacy \
 && cd pspacy \
 && sh install_dependencies.sh \
 && make USE_PGXS=1 \
 && make USE_PGXS=1 install

# install pg_rollup
# FIXME:
# should we add a git checkout version?
ADD https://api.github.com/repos/mikeizbicki/pg_rollup/git/refs/heads/master /tmp/pg_rollup.version
RUN cd /tmp \
 && git clone https://github.com/mikeizbicki/pg_rollup \
 && cd pg_rollup \
 && pip3 install . \
 && make USE_PGXS=1 \
 && make USE_PGXS=1 install \
 && rm -rf /tmp/pg_rollup

# install metahtml
ADD https://api.github.com/repos/mikeizbicki/metahtml/git/refs/heads/master /tmp/metahtml.version
RUN cd /tmp \
 && git clone https://github.com/mikeizbicki/metahtml \
 && cd metahtml \
 && pip3 install -r requirements.txt

RUN cp /tmp/pspacy/pspacy.py /tmp/metahtml

# run entrypoint.sh
WORKDIR /tmp/metahtml
COPY ./downloader_host.py /tmp/metahtml
ENTRYPOINT ["python3", "downloader_host.py"]
