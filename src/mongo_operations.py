import subprocess

from constants import ARCHIVE_CMD


class MongoOperations(object):
    def __init__(self):
        pass

    @staticmethod
    def execute_subprocess_cmd(cmd):
        try:
            result = subprocess.Popen(cmd)
            result.wait()

        except Exception as ex:
            print "An error occurred in executing cmd command ==> {cmd}".format(cmd=cmd)
            raise Exception(ex)

    @staticmethod
    def create_dump(server, dump_path):
        dump_cmd = [
            "mongodump", "--host", server["host"], "--port", server["port"],
            "--db", server["db"], "--out", dump_path
        ]

        MongoOperations.execute_subprocess_cmd(dump_cmd)

    @staticmethod
    def create_compressed_dump(server, dump_path):
        archive_cmd = ARCHIVE_CMD.format(path=dump_path, name=server["temp_db"])

        dump_cmd = [
            "mongodump", "--host", server["host"], "--port", server["port"],
            "--db", server["db"], "--gzip", archive_cmd
        ]

        MongoOperations.execute_subprocess_cmd(dump_cmd)

    @staticmethod
    def create_collection_dump(server, dump_path, collection):
        dump_cmd = [
            "mongodump", "--host", server["host"], "--port", server["port"],
            "--db", server["db"], "--collection", collection,
            "--out", dump_path
        ]

        MongoOperations.execute_subprocess_cmd(dump_cmd)

    @staticmethod
    def create_compress_collection_dump(server, dump_path, collection):
        archive_cmd = ARCHIVE_CMD.format(path=dump_path, name=server["temp_db"])

        dump_cmd = [
            "mongodump", "--host", server["host"], "--port", server["port"],
            "--db", server["db"], "--collection", collection,
            "--gzip", archive_cmd
        ]

        MongoOperations.execute_subprocess_cmd(dump_cmd)

    @staticmethod
    def restore_dump(server, dump_path):
        restore_cmd = [
            "mongorestore", "--host", server["host"], "--port", server["port"],
            "--db", server["db"], dump_path
        ]

        MongoOperations.execute_subprocess_cmd(restore_cmd)

    @staticmethod
    def restore_compressed_dump(server, dump_path):
        archive_cmd = ARCHIVE_CMD.format(path=dump_path, name=server["temp_db"])

        restore_cmd = [
            "mongorestore", "--host", server["host"], "--port", server["port"],
            "--nsFrom", server["db"] + ".*", "--nsTo", server["temp_db"] + ".*", "--gzip", archive_cmd
        ]

        MongoOperations.execute_subprocess_cmd(restore_cmd)

    @staticmethod
    def restore_collection_dump(server, dump_path, collection):
        restore_cmd = [
            "mongorestore", "--host", server["host"], "--port", server["port"],
            "--db", server["db"], "--collection", collection,
            dump_path
        ]

        MongoOperations.execute_subprocess_cmd(restore_cmd)

    @staticmethod
    def restore_compressed_collection_dump(server, dump_path, collection):
        archive_cmd = ARCHIVE_CMD.format(path=dump_path, name=server["temp_db"])

        restore_cmd = [
            "mongorestore", "--host", server["host"], "--port", server["port"],  # "--drop", # to drop existing db
            "--db", server["db"], "--collection", collection,
            "--gzip", archive_cmd
        ]

        MongoOperations.execute_subprocess_cmd(restore_cmd)

    @staticmethod
    def restore_to_diff_db(server, dump_path):
        archive_cmd = ARCHIVE_CMD.format(path=dump_path, name=server["temp_db"])

        restore_cmd = [
            "mongorestore", "--host", server["host"], "--port", server["port"],
            "--nsFrom", server["db"] + ".*", "--nsTo", server["temp_db"] + ".*",
            "--nsInclude", server["db"] + ".slirev*",  # "nsExclude", server["db"] + ".sli*" # Don't restore these coll
            "--gzip", archive_cmd
        ]

        MongoOperations.execute_subprocess_cmd(restore_cmd)

    @staticmethod
    def clone_db(client, from_db, to_db):
        try:
            client.admin.command('renameCollection', **{
                'renameCollection': from_db,
                'to': to_db
            })
        except Exception as ex:
            print "Error while cloning DB"
            raise Exception(ex)

    @staticmethod
    def get_collection(db, collection, client):
        try:
            db = client[db]
            collection = db[collection]

            return collection
        except Exception as ex:
            print "Error while creating collection"
            raise Exception(ex)

    @staticmethod
    def insert(client, db, collection, data):
        try:
            collection_obj = MongoOperations.get_collection(db, collection, client)

            while True:
                data_chunk = data.fetchmany(1000)

                if not data_chunk:
                    break

                result = [dict(row) for row in data_chunk]
                collection_obj.insert_many(result)

        except Exception as ex:
            print "An error occurred while inserting data to MongoDB"
            raise Exception(ex)


if __name__ == '__main__':
    MongoOperations.create_dump()
