from constants import (DESTINATION_HOST, DESTINATION_PORT, DUMP_PATH_FOR_RESTORE, ARCHIVE_DUMP_PATH,
                       DUMP_PATH_COLLECTION_FOR_RESTORE)
from models import destination_client
from mongo_operations import MongoOperations


class Destination(MongoOperations):
    def __init__(self, company_id):
        super(Destination, self).__init__()
        self.company_id = str(company_id)
        self.client = destination_client

        self.server = {
            "host": DESTINATION_HOST,
            'port': DESTINATION_PORT,
            'db': self.company_id
        }

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


if __name__ == "__main__":
    destination_obj = Destination(13311)

    # destination_obj.restore_dump()
    # destination_obj.restore_compressed_dump()
    # destination_obj.restore_collection_dump()
    destination_obj.restore_compress_collection_dump()
