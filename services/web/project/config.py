import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    # static pages
    STATIC_FOLDER = f"{os.getenv('APP_FOLDER')}/project/static"

    # postgres connection
    POSTGRES_USER = os.environ.get('POSTGRES_USER')
    POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
    POSTGRES_DB = os.environ.get('POSTGRES_DB')
    DB_URI = f'postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@db/{POSTGRES_DB}'
