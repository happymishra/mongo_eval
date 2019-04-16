import logging
from ConfigParser import ConfigParser
from os import path

config = ConfigParser()
config.read(path.join(path.dirname((path.dirname(__file__))), 'config/config.ini'))

logging.basicConfig(format='%(asctime)s - %(message)s')
logging.getLogger().setLevel(logging.INFO)

SLI_CON_REV_DB = 'sli_consensus_revision'
SLI_REV_DB = 'sli_revision'
SLI_VAACTUALS_DB = 'sli_vaactuals_revision'

SLI_CON_LATEST_DB = 'sli_consensus_latest'
SLI_LATEST_DB = 'sli_latest'
SLI_VAACTUALS_LATEST = 'sli_vaactuals_latest'


SLI_DB = 'sli'

MYSQL_DB_URL = "mysql://{0}:{1}@{2}/{3}"
MONGO_DB_URL = "mongodb://{0}:{1}/"

PRINT_FORMAT = "{message}: {time}"

FETCH_REV_DP_REGEX = '\<([a-zA-Z]+)_(.*?)\>'

QUERY_DB_DICT = {
    'slirevision': "SELECT revisiondpid, expression FROM {db}.`{company_id}`",
    'slilatest': "SELECT revisiondpid, expression FROM {db}.`{company_id}`",
    'slivaactualslatest': "SELECT revisiondpid FROM {db}.`{company_id}`",
    'sliconsensuslatest': "SELECT revisiondpid, expression FROM {db}.`{company_id}`",

    'sliconsensusrevision': "SELECT revisiondpid, expression, computeinfojson FROM {db}.`{company_id}`",
    'slivaactualsrevision': "SELECT revisiondpid, expression, computeinfojson FROM {db}.`{company_id}`",
}

DUMP_FILE_PATH = "/home/rupesh/VA/mongo_dumps"

DESTINATION_HOST = '127.0.0.1'
DESTINATION_PORT = '27020'

SOURCE_HOST = '127.0.0.1'
SOURCE_PORT = '27017'
