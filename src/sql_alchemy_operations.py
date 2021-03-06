from sqlalchemy.sql import *

from models import *


class SQlAlchemyOperations:
    def __init__(self, company_id):
        self.company_id = company_id

    def get_data_from_raw_query(self, db):
        print "Started fetching data from MySQL company id: {company_id}".format(company_id=self.company_id)

        conn = get_mysql_engine(db=db)
        query = QUERY_DB_DICT.get(db).format(db=db, company_id=self.company_id)

        data = conn.execute(text(query))

        print "Completed fetching data from MySQL company id: {company_id}".format(company_id=self.company_id)

        return data
