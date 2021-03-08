FROM postgres:12

# install system packages for building postgres extensions
RUN apt-get update && apt-get install -y \
    autoconf \
    gcc \
    less \
    make \
    postgresql-server-dev-12 \
    postgresql-plpython3-12 \
    python3 \
    python3-pip \
    sudo \
    wget

# install a newer version of git
# these libraries are required just for installing a new version of git
RUN apt-get install -y \
    libcurl4-gnutls-dev \
    libexpat1-dev gettext \
    libz-dev \
    libssl-dev \
    asciidoc \
    xmlto \
    docbook2x
RUN cd /tmp \
 && wget https://www.kernel.org/pub/software/scm/git/git-2.30.1.tar.gz \
 && tar -xzf git-2.30.1.tar.gz \
 && cd git-2.30.1 \
 && ./configure \
 && make \
 && make install \
 && rm -rf /tmp/git-2.30.1

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
 && sh install_dependencies.sh \
 && pip3 install . \
 && make USE_PGXS=1 \
 && make USE_PGXS=1 install \
 && rm -rf /tmp/pg_rollup

# install metahtml; this requires the new version of git
ADD https://api.github.com/repos/mikeizbicki/metahtml/git/refs/heads/master /tmp/metahtml.version
RUN cd /tmp \
 && git clone --filter=blob:none --no-checkout https://github.com/mikeizbicki/metahtml \
 && cd metahtml \
 && git sparse-checkout init \
 && git sparse-checkout set '!/tests/.cache' '/*' \
 && git checkout master \
 && pip3 install -r requirements.txt \
 && python3 scripts/generate_tests_json.py > /tmp/metahtml/all_tests.json

# create a fastdata directory for a faster tablespace
RUN mkdir /fastdata \
 && chown postgres /fastdata

# copy over the database initial data
COPY ./data /tmp/data
RUN chown postgres -R /tmp/data

# copy over the database creation scripts
COPY ./postgresql.conf /etc/postgresql.conf
COPY ./sql/*.sql /docker-entrypoint-initdb.d/
