import subprocess
import time

from sql_alchemy_operations import *


class MongoDBOperations:
    def __init__(self, company_id):
        self.company_id = company_id
        self.sql_alchemy_obj = SQlAlchemyOperations(company_id)
        self.file_path = "/home/rupesh/VA/mongo_dumps"

    def dump_mongo_data(self, db, collection_name=None):
        db_name = 'apsli_{company_id}'.format(company_id=self.company_id)

        dump_cmd = [
            "mongodump", "--host", SOURCE_HOST, "--port", SOURCE_PORT, "--db", db_name,
            "--out", self.file_path
        ]

        # if collection_name:
        #     collection_name = db + collection_name
        #     dump_cmd = dump_cmd + ["--collection", collection_name]

        try:
            dump = subprocess.Popen(dump_cmd)
            dump.wait()

        except Exception as ex:
            print "Dump failed"

    def restore_mongo_data(self, collection_name=None):
        db_name = 'apsli_{company_id}'.format(company_id=self.company_id)
        file_path = self.file_path + "/" + db_name + "/"

        restore_cmd = [
            "mongorestore", "--host", DESTINATION_HOST, "--port", DESTINATION_PORT, "--db", db_name
        ]

        #
        # if collection_name:
        #     file_path = file_path + db_name + "_" + collection_name + ".bson"
        #     restore_cmd = restore_cmd + ["--collection", collection_name]

        restore_cmd = restore_cmd + [file_path]

        try:
            restore_result = subprocess.Popen(restore_cmd)
            restore_result.wait()

        except Exception as ex:
            print "Restore failed"

    def copy_temp_data_to_main_db(self):
        temp_db_name = 'apsli_{company_id}'.format(company_id=self.company_id)
        col_name = '{db_name}.{col_name}'
        renamed_dbs = list()

        try:
            for each_table, query in QUERY_DB_DICT.iteritems():
                renamed_dbs.append(each_table)

                main_db_col_name = col_name.format(db_name=each_table, col_name=self.company_id)
                tem_db_col = col_name.format(db_name=temp_db_name, col_name=each_table)
                temp_main_col = '{company_id}_temp'.format(company_id=self.company_id)

                main_db_col = self.get_collection(each_table, destination_mongo_client)
                main_db_col.rename(temp_main_col)

                destination_mongo_client.admin.command('renameCollection', **{
                    'renameCollection': tem_db_col,
                    'to': main_db_col_name
                })
        except Exception as ex:
            print "Renaming failed"
            temp_main_col = '{company_id}_temp'.format(company_id=self.company_id)

            for each_renamed_table in renamed_dbs:

                main_db_col = destination_mongo_client[each_renamed_table][temp_main_col]
                main_db_col.rename(str(self.company_id))

            pass

    def create_temp_db(self):
        db_name = 'apsli_{company_id}'.format(company_id=self.company_id)
        col_name = '{db_name}.{col_name}'

        source_mongo_client[db_name]

        for each_table, query in QUERY_DB_DICT.iteritems():
            col_1 = col_name.format(db_name=each_table, col_name=self.company_id)
            renamed_collection = col_name.format(db_name=db_name, col_name=each_table)

            source_mongo_client.admin.command('renameCollection', **{
                'renameCollection': col_1,
                'to': renamed_collection
            })

    def get_temp_collection(self, db, table, mongo_client=destination_mongo_client):
        print "Started creating collection {c_name}".format(c_name=table)

        db = mongo_client[db]
        collection = db[table]

        print "Completed creating collection {c_name}".format(c_name=table)

        return collection

    def get_collection(self, db, mongo_client=destination_mongo_client):
        print "Started creating temp collection {c_name}".format(c_name=table)

        db = mongo_client[db]
        collection_name = '{company_id}'.format(company_id=self.company_id)

        collection = db[collection_name]

        print "Completed creating db {db_name} and collection {col}".format(db_name=db, col=collection_name)

        return collection

    def insert_data_into_mongo(self, mongo_client):
        for db, query in QUERY_DB_DICT.iteritems():
            print "Started inserting {db} data for company {company_id}".format(db=db, company_id=self.company_id)

            data = self.sql_alchemy_obj.get_data_from_raw_query(db)
            collection = self.get_collection(db, mongo_client)
            # collection = self.get_temp_collection(db, mongo_client)

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
    # obj.insert_data_into_mongo(source_mongo_client)
    # obj.insert_data_into_mongo(destination_mongo_client)
    # obj.create_temp_db()
    # obj.dump_mongo_data('13311')
    # obj.restore_mongo_data()
    obj.copy_temp_data_to_main_db()
    # obj.rename_collections("13311")
