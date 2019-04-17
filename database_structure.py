#!/usr/bin/env python

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine

Base = declarative_base()

class AccessAttempts(Base):
    __tablename__ = 'access_attempts'
    id = Column(Integer, primary_key=True)
    ip_addr = Column(String(250), nullable=False)
    attempts = Column(Integer)
    hostname = Column(String(250))
    country = Column(String(250))
    country_alpha_2 = Column(String(250))
    country_alpha_3 = Column(String(250))
    vpn = Column(String(250))
    tor = Column(String(250))

class KnownVPNServers(Base):
    __tablename__ = 'known_vpn_servers'
    id = Column(Integer, primary_key=True)
    ip_addr = Column(String(250), nullable=False)
    hostname = Column(String(250))
    country = Column(String(250))
    country_alpha_2 = Column(String(250))
    country_alpha_3 = Column(String(250))

engine = create_engine('sqlite:///db.sqlite')

Base.metadata.create_all(engine)