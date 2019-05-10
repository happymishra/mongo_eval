SLI_CON_REV_DB = 'sli_consensus_revision'
SLI_REV_DB = 'sli_revision'
SLI_VAACTUALS_DB = 'sli_vaactuals_revision'

SLI_CON_LATEST_DB = 'sli_consensus_latest'
SLI_LATEST_DB = 'sli_latest'
SLI_VAACTUALS_LATEST = 'sli_vaactuals_latest'

SLI_DB = 'sli'

SOURCE_CLIENT = 'mongo_source'
DESTINATION_CLIENT = 'mongo_dest'

MYSQL_DB_URL = "mysql://{0}:{1}@{2}/{3}"
MONGO_DB_URL = "mongodb://{0}:{1}/"

FETCH_REV_DP_REGEX = '\<([a-zA-Z]+)_(.*?)\>'

QUERY_DB_DICT = {
    'slirevision': "SELECT revisiondpid, expression FROM {db}.`{company_id}` "
                   "WHERE revisiondpid IN (7068693365, 7068693366)",
    'slilatest': "SELECT revisiondpid, expression FROM {db}.`{company_id}` WHERE revisiondpid IN ()",
    'slivaactualslatest': "SELECT revisiondpid FROM {db}.`{company_id}` WHERE revisiondpid IN ()",
    'sliconsensuslatest': "SELECT revisiondpid, expression FROM {db}.`{company_id}` WHERE revisiondpid IN ()",

    'sliconsensusrevision': "SELECT revisiondpid, expression, computeinfojson FROM {db}.`{company_id}` "
                            "WHERE revisiondpid IN ()",
    'slivaactualsrevision': "SELECT revisiondpid, expression, computeinfojson FROM {db}.`{company_id}` "
                            "WHERE revisiondpid IN ()",
}

QUERY_DB_DICT_DEST = {
    'slirevision': "SELECT revisiondpid, expression FROM {db}.`{company_id}` "
                   "WHERE revisiondpid IN (7068693368, 7068693369)",
    'slilatest': "SELECT revisiondpid, expression FROM {db}.`{company_id}` WHERE revisiondpid IN ()",
    'slivaactualslatest': "SELECT revisiondpid FROM {db}.`{company_id}` WHERE revisiondpid IN ()",
    'sliconsensuslatest': "SELECT revisiondpid, expression FROM {db}.`{company_id}` WHERE revisiondpid IN ()",

    'sliconsensusrevision': "SELECT revisiondpid, expression, computeinfojson FROM {db}.`{company_id}` "
                            "WHERE revisiondpid IN ()",
    'slivaactualsrevision': "SELECT revisiondpid, expression, computeinfojson FROM {db}.`{company_id}` "
                            "WHERE revisiondpid IN ()",
}

TEMP_DB_FMT = "apsli_{db}"

DUMP_PATH = "../dumps"
DUMP_PATH_FOR_RESTORE = "../dumps/{db}"
DUMP_PATH_COLLECTION_FOR_RESTORE = "../dumps/{db}/{collection}.bson"

ARCHIVE_DUMP_PATH = "../archive_dumps"
ARCHIVE_CMD = "--archive={path}/{name}.archive"

SOURCE_HOST = '127.0.0.1'
SOURCE_PORT = '27017'

DESTINATION_HOST = '127.0.0.1'
DESTINATION_PORT = '27020'
