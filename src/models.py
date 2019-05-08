from sqlalchemy import Column, Integer, BigInteger, CHAR, DateTime, DECIMAL, TEXT
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

from constants import *
import pymongo


def get_mysql_engine(db="default"):
    db_params = dict(config.items(db))
    conn_string = MYSQL_DB_URL.format(db_params['user'], db_params['password'], db_params['host'],
                                      db_params['database'])

    return create_engine(conn_string, pool_pre_ping=True)


def get_local_mysql_engine():
    conn_string = MYSQL_DB_URL.format("root", "root", "localhost", "sli_revision")

    return create_engine(conn_string, pool_pre_ping=True)


def get_source_client():
    conn_string = MONGO_DB_URL.format("localhost", "27017")

    return pymongo.MongoClient(conn_string)


def get_destination_client():
    conn_string = MONGO_DB_URL.format("localhost", "27020")
    return pymongo.MongoClient(conn_string)


def get_session(engine):
    session_factory = sessionmaker(bind=engine)

    return scoped_session(session_factory)


# sli_con_rev_mysql_engine = get_mysql_engine(SLI_CON_REV_DB)
# sli_rev_mysql_engine = get_mysql_engine(SLI_REV_DB)
# sli_vactuals_mysql_engine = get_mysql_engine(SLI_REV_DB)
#
# sli_rev_mysql_engine = get_mysql_engine(SLI_REV_DB)
# sli_rev_mysql_engine = get_mysql_engine(SLI_REV_DB)
# sli_rev_mysql_engine = get_mysql_engine(SLI_REV_DB)
#
#
#
#
# sli_con_rev_mysql_session = get_session(sli_con_rev_mysql_engine)

source_client = get_source_client()
destination_client = get_destination_client()

#
# local_sli_con_revision = get_local_mysql_engine()
# local_sli_con_revision_session = get_session(local_sli_con_revision)


class _Base(object):
    class_registry = dict()

    @classmethod
    def set_metadata(cls, db_session, table_name):
        cls.db_session = db_session
        cls.__tablename__ = '{table_name}'.format(table_name=table_name)
        cls.query = db_session.query_property()

        return cls


def get_sli_consensus_revision_model(table_name, engine):
    base = declarative_base(cls=_Base.set_metadata(engine, table_name))

    class SLIConsensusRevision(base):
        revisiondpid = Column(BigInteger, primary_key=True)
        latestdpid = Column(BigInteger)
        datapointtype = Column(CHAR)
        effectivestart = Column(DateTime)
        effectiveend = Column(DateTime)
        expression = Column(CHAR)
        periodstart = Column(CHAR)
        sliparameterid = Column(Integer)
        value = Column(DECIMAL())
        isimplied = Column(Integer)
        annotationsettings = Column(CHAR)
        computeinfojson = Column(TEXT)

    return SLIConsensusRevision
