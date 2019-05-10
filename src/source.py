from constants import (QUERY_DB_DICT, SOURCE_HOST, SOURCE_PORT, DUMP_PATH, ARCHIVE_DUMP_PATH, TEMP_DB_FMT,
                       SOURCE_CLIENT)
from mongo_operations import MongoOperations
from sql_alchemy_operations import SQlAlchemyOperations


class Source(MongoOperations):
    def __init__(self, company_id):
        super(Source, self).__init__()
        self.company_id = str(company_id)
        self.client = MongoOperations.get_client(db=SOURCE_CLIENT)

        self.server = {
            "host": SOURCE_HOST,
            'port': SOURCE_PORT,
            'db': self.company_id,
            'temp_db': TEMP_DB_FMT.format(db=self.company_id)
        }

    def __enter__(self):
        print "Inside enter method after __init__ method"
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print "Inside exit method"
        self.client.close()

    def insert_from_sql(self):
        for db, query in QUERY_DB_DICT.iteritems():
            print "Started inserting {db} data for company {company_id}".format(db=db, company_id=self.company_id)

            data = SQlAlchemyOperations.get_data_from_raw_query(db, self.company_id, QUERY_DB_DICT)
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
    with Source(13311) as source_obj:
        # source_obj.insert_from_sql()
        # source_obj.create_dump()
        # source_obj.create_compressed_dump()
        # source_obj.create_collection_dump()
        source_obj.create_compress_collection_dump()
