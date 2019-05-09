from constants import QUERY_DB_DICT, SOURCE_HOST, SOURCE_PORT, DUMP_PATH, ARCHIVE_DUMP_PATH, TEMP_DB_FMT
from models import source_client
from mongo_operations import MongoOperations
from sql_alchemy_operations import SQlAlchemyOperations


class Source(MongoOperations):
    def __init__(self, company_id):
        super(Source, self).__init__()
        self.company_id = str(company_id)
        self.client = source_client

        self.server = {
            "host": SOURCE_HOST,
            'port': SOURCE_PORT,
            'db': self.company_id,
            'temp_db': TEMP_DB_FMT.format(db=self.company_id)
        }

    def insert_from_sql(self):
        for db, query in QUERY_DB_DICT.iteritems():
            print "Started inserting {db} data for company {company_id}".format(db=db, company_id=self.company_id)

            data = SQlAlchemyOperations.get_data_from_raw_query(db, self.company_id)
            Source.insert(self.client, db=self.company_id, collection=db, data=data)

            print "Completed inserting {db} data for company {company_id}".format(db=db, company_id=self.company_id)

    def create_dump(self):
        super(Source, self).create_dump(self.server, dump_path=DUMP_PATH)

    def create_compressed_dump(self):
        super(Source, self).create_compressed_dump(self.server, ARCHIVE_DUMP_PATH)

    def create_collection_dump(self):
        super(Source, self).create_collection_dump(self.server, dump_path=DUMP_PATH, collection="slirevision")

    def create_compress_collection_dump(self):
        super(Source, self).create_compress_collection_dump(self.server, dump_path=ARCHIVE_DUMP_PATH,
                                                            collection="slirevision")


if __name__ == '__main__':
    source_obj = Source(13311)

    # source_obj.insert_from_sql()
    # source_obj.create_dump()
    # source_obj.create_compressed_dump()
    # source_obj.create_collection_dump()
    source_obj.create_compress_collection_dump()
