import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    # static pages
    STATIC_FOLDER = f"{os.getenv('APP_FOLDER')}/project/static"

    # postgres connection
    DB_USER = os.environ.get('DB_USER')
    DB_PASSWORD = os.environ.get('DB_PASSWORD')
    DB_NAME = os.environ.get('DB_NAME')
    DB_URI = f'postgresql://{DB_USER}:{DB_PASSWORD}@db/{DB_NAME}'
