from sqlalchemy import Column, Integer, String, ForeignKey, Table, create_engine, MetaData, Date, DateTime, Float, Boolean
from sqlalchemy.orm import relationship, backref, scoped_session, sessionmaker, relation
from sqlalchemy.ext.declarative import declarative_base 

import sqlalchemy

Base = declarative_base()

##########################################################################################
#
# Lookup Tables 
#
##########################################################################################


class LT_UserStatus(Base):

    __tablename__   = 'user_status'
    __table_args__  = {'schema': "A_DATABASE_THAT_DOES_NOT_EXIST"}
    
    id              = Column(Integer, primary_key = True)
    status          = Column(String(30))

# end of LT_UserStatus


##########################################################################################
#
# Primary Tables 
#
##########################################################################################


class PT_UserAccount(Base):

    __tablename__               = 'user_acccounts'
    __table_args__              = {'schema': "A_DATABASE_THAT_DOES_NOT_EXIST"}

    id                          = Column(Integer, primary_key=True)
    first_name                  = Column(String(80))
    last_name                   = Column(String(80))
    user_name                   = Column(String(64))
    password                    = Column(String(64))
    email                       = Column(String(64))
    creation_date               = Column(DateTime)
    user_status_sym_id          = Column(Integer, ForeignKey(LT_UserStatus.id))
    user_status_sym             = relationship(LT_UserStatus)
    
# end of PT_UserAccount


