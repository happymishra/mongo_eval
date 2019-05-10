from sqlalchemy.sql import *

from utils.db_connector import DBConnector


class SQlAlchemyOperations:
    db_connector_obj = DBConnector('', 'config/config.ini')

    def __init__(self):
        pass

    @classmethod
    def get_data_from_raw_query(cls, db, company_id, query_dict):
        print "Started fetching data from MySQL company id: {company_id}".format(company_id=company_id)

        conn = cls.db_connector_obj.get_mysql_engine(db=db)
        query = query_dict.get(db).format(db=db, company_id=company_id)

        data = conn.execute(text(query))

        print "Completed fetching data from MySQL company id: {company_id}".format(company_id=company_id)

        return data
