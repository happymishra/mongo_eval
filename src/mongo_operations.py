import time
import subprocess

from sql_alchemy_operations import *
from pymongo.database import Database


class MongoDBOperations:
    def __init__(self, company_id):
        self.company_id = company_id
        self.sql_alchemy_obj = SQlAlchemyOperations(company_id)
        self.file_path = "/home/rupesh/VA/mongo_dumps"

    def dump_mongo_data(self, db, collection_name=None):
        dump_cmd = [
            "mongodump", "--host", SOURCE_HOST, "--port", SOURCE_PORT, "--db", db,
            "--out", self.file_path
        ]

        if collection_name:
            collection_name = db + collection_name
            dump_cmd = dump_cmd + ["--collection", collection_name]

        try:
            dump = subprocess.Popen(dump_cmd)
            dump.wait()

        except Exception as ex:
            print "Dump failed"

    def restore_mongo_data(self, db, new_db_name, collection_name=None):
        file_path = self.file_path + "/" + db + "/"

        restore_cmd = [
            "mongorestore", "--host", DESTINATION_HOST, "--port", DESTINATION_PORT, "--db", new_db_name
        ]

        if collection_name:
            file_path = file_path + db + "_" + collection_name + ".bson"
            restore_cmd = restore_cmd + ["--collection", collection_name]

        restore_cmd = restore_cmd + [file_path]

        try:
            restore_result = subprocess.Popen(restore_cmd)
            restore_result.wait()

        except Exception as ex:
            print "Restore failed"

    def rename_collections(self, db):
        old_collection = self.get_collection(db, mongo_client=source_mongo_client)
        new_collection = self.get_collection("apsli_" + db)

        try:
            old_collection.rename(db + "_temp")
            new_collection.rename(db)

            old_collection.drop()

        except:
            old_collection.rename(db)
            print "Rename failed"

    def create_temp_db(self, db):
        db_name = 'apsli_{company_id}'.format(self.company_id)
        col_name = '{db_name}.{col_name}'

        source_mongo_client[db_name]

        for each_table, query in QUERY_DB_DICT.iteritems():
            col_1 = col_name.format(db_name=each_table, col_name=self.company_id)
            renamed_collection = col_name.format(db_name=db_name, col_name=each_table)

            source_mongo_client.admin.command('renameCollection', **{
                'renameCollection': col_1,
                'to': renamed_collection
            })

    # def dump_mongo_db(self):
    #     dump_cmd = [
    #         "mongodump", "--host", "127.0.0.1", "--port", "27017", "--db", "13311",
    #         "--out", "/home/rupesh/VA/mongo_dumps"
    #     ]
    #
    #     try:
    #         dump = subprocess.Popen(dump_cmd)
    #         dump.wait()
    #
    #     except Exception as ex:
    #         print "Dump failed"
    #
    # def restore_mongo_db(self):
    #     restore_cmd = [
    #         "mongorestore", "--host", "127.0.0.1", "--port", "27020", "--db", "13311"
    #         , "/home/rupesh/VA/mongo_dumps/13311/"
    #     ]
    #
    #     try:
    #         restore_result = subprocess.Popen(restore_cmd)
    #         restore_result.wait()
    #     except Exception as ex:
    #         print "Restore failed"
    #
    # def dump_mongo_collection(self):
    #     dump_cmd = [
    #         "mongodump", "--host", "127.0.0.1", "--port", "27017", "--db", "13311", "--collection", "13311_slirevision"
    #         ,"--out", "/home/rupesh/VA/mongo_dumps"
    #     ]
    #
    #     try:
    #         dump = subprocess.Popen(dump_cmd)
    #         dump.wait()
    #
    #     except Exception as ex:
    #         print "Dump failed"
    #
    # def restore_mongo_collections(self):
    #     restore_cmd = [
    #         "mongorestore", "--host", "127.0.0.1", "--port", "27020", "--db", "13311"
    #         , "/home/rupesh/VA/mongo_dumps/13311/13311_slirevision.bson" , "--collection" ,"rest4"
    #     ]
    #
    #     try:
    #         restore_result = subprocess.Popen(restore_cmd)
    #         restore_result.wait()
    #     except Exception as ex:
    #         print "Restore failed"

    def get_temp_collection(self, db, table, mongo_client=destination_mongo_client):
        print "Started creating collection {c_name}".format(c_name=table)

        db = mongo_client[db]
        collection_name = '{table}'.format(company_id=self.company_id, table=table)

        collection = db[collection_name]
        collection.create_index([("revisiondpid", pymongo.ASCENDING)])

        print "Completed creating collection {c_name}".format(c_name=table)

        return collection

    def get_collection(self, db, mongo_client=destination_mongo_client):
        print "Started creating temp collection {c_name}".format(c_name=table)

        db = mongo_client[db]
        collection_name = '{company_id}'.format(company_id=self.company_id)

        collection = db[collection_name]

        print "Completed creating db {db_name} and collection {col}".format(db_name=db, col=collection_name)

        return collection

    def insert_data_into_mongo(self):
        for db, query in QUERY_DB_DICT.iteritems():
            print "Started inserting {db} data for company {company_id}".format(db=db, company_id=self.company_id)

            data = self.sql_alchemy_obj.get_data_from_raw_query(db)
            collection = self.get_collection(db, source_mongo_client)
            # collection = self.get_temp_collection(db, source_mongo_client)

            start = time.time()
            while True:
                data_chunk = data.fetchmany(1000)

                if not data_chunk:
                    print "Break"
                    break

                result = [dict(row) for row in data_chunk]
                collection.insert_many(result)

            print "Completed inserting {db} data for company {company_id}".format(db=db, company_id=self.company_id)

            print PRINT_FORMAT.format(message="Mongo insertion", time=time.time() - start)


if __name__ == '__main__':
    obj = MongoDBOperations(13311)
    # obj.insert_data_into_mongo()
    obj.create_temp_db('apsli')
    # obj.dump_mongo_db()
    # obj.restore_mongo_db()
    # obj.dump_mongo_data('13311')
    # obj.restore_mongo_data("13311", 'apsli_13311')
    # obj.rename_collections("13311")
