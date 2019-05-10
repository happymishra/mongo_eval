from sqlalchemy import Column, Integer, BigInteger, CHAR, DateTime, DECIMAL, TEXT
from sqlalchemy.ext.declarative import declarative_base


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
