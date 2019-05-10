import pymongo

from read_config import ConfigRead
from constants import MYSQL_DB_URL, MONGO_DB_URL
from sqlalchemy import create_engine


class DBConnector:
    def __init__(self, config_path, config_file):
        self.config_read_obj = ConfigRead(config_path, config_file)

    def get_mysql_engine(self, db):
        db_params = self.config_read_obj.get_config_section(db)

        engine = create_engine(MYSQL_DB_URL.format(db_params['user'],
                                                   db_params['password'],
                                                   db_params['host'],
                                                   db_params['database']), pool_pre_ping=True)
        return engine

    def get_mongo_client(self, db):
        db_params = self.config_read_obj.get_config_section(db)

        conn_string = MONGO_DB_URL.format(db_params["host"], db_params["port"])
        return pymongo.MongoClient(conn_string)


if __name__ == '__main__':
    print dummy
