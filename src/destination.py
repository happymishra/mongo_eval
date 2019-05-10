from constants import (DESTINATION_HOST, DESTINATION_PORT, DUMP_PATH_FOR_RESTORE, ARCHIVE_DUMP_PATH,
                       DUMP_PATH_COLLECTION_FOR_RESTORE, TEMP_DB_FMT, QUERY_DB_DICT_DEST, DESTINATION_CLIENT)

from mongo_operations import MongoOperations
from sql_alchemy_operations import SQlAlchemyOperations


class Destination(MongoOperations):
    def __init__(self, company_id):
        super(Destination, self).__init__()
        self.company_id = str(company_id)
        self.client = MongoOperations.get_client(db=DESTINATION_CLIENT)

        self.server = {
            "host": DESTINATION_HOST,
            'port': DESTINATION_PORT,
            'db': self.company_id,
            'temp_db': TEMP_DB_FMT.format(db=self.company_id)
        }

    def __enter__(self):
        print "Inside enter method after __init__"
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print "Inside exit method"
        self.client.close()

    def restore_dump(self):
        dump_path = DUMP_PATH_FOR_RESTORE.format(db=self.company_id)
        super(Destination, self).restore_dump(self.server, dump_path)

    def restore_compressed_dump(self):
        super(Destination, self).restore_compressed_dump(self.server, ARCHIVE_DUMP_PATH)

    def restore_collection_dump(self):
        dump_path = DUMP_PATH_COLLECTION_FOR_RESTORE.format(db=self.company_id, collection="slirevision")
        super(Destination, self).restore_collection_dump(self.server, dump_path, "slirevision")

    def restore_compress_collection_dump(self):
        super(Destination, self).restore_compressed_collection_dump(self.server, ARCHIVE_DUMP_PATH, "slirevision")

    def restore_to_diff_db(self):
        super(Destination, self).restore_to_diff_db(self.server, ARCHIVE_DUMP_PATH)

    def insert_from_sql(self):
        for db, query in QUERY_DB_DICT_DEST.iteritems():
            print "Started inserting {db} data for company {company_id}".format(db=db, company_id=self.company_id)

            data = SQlAlchemyOperations.get_data_from_raw_query(db, self.company_id, QUERY_DB_DICT_DEST)
            Destination.insert(self.client, db=self.company_id, collection=db, data=data)

            print "Completed inserting {db} data for company {company_id}".format(db=db, company_id=self.company_id)


if __name__ == "__main__":
    with Destination(13311) as destination_obj:
        # destination_obj.insert_from_sql()
        # destination_obj.restore_dump()
        # destination_obj.restore_compressed_dump()
        # destination_obj.restore_compress_collection_dump()
        destination_obj.restore_to_diff_db()
