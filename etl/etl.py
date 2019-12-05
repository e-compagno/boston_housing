import pandas as pd

import pandas as pd 
import sqlalchemy as db
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm import sessionmaker
import os
from sklearn.datasets import load_boston
boston_dataset = load_boston()

# Print description
print(boston_dataset['DESCR'])

# Load dataset
df = pd.DataFrame(boston_dataset['data'],
                  columns=boston_dataset['feature_names']
                 )
df['target']= boston_dataset['target']
df=df.reset_index()
df = df.rename(columns={'index': 'id'})
df.columns=df.columns.str.lower()
df.head()

# Load MYSql connector 
SQL_USR, SQL_PSW= os.environ['SQL_USR'], os.environ['SQL_PSW']
mysql_str = 'mysql+mysqlconnector://'+SQL_USR+':'+SQL_PSW+'@localhost:3306/'
engine = db.create_engine(mysql_str)

print('Create database Housing')
print('-'*30)
# Create database diamonds
con=engine.connect()
con.execute('commit')
con.execute('CREATE DATABASE if NOT EXISTS Housing;')
con.close()
print('Done.\n')

print('Create tables.')
print('-'*30)
# Select diamonds database
engine = db.create_engine(mysql_str+'Housing')
con=engine.connect()

Base=declarative_base()


class Housing(Base):
    """
    Class for creating the housing table.
    """

    __tablename__ = 'housing'

    id=Column(Integer, primary_key=True)
    crim=Column(Float)
    zn=Column(Float)
    indus=Column(Float)
    chas=Column(Float)
    nox=Column(Float)
    rm=Column(Float)
    age=Column(Float)
    dis=Column(Float)
    rad=Column(Float)
    tax=Column(Float)
    ptratio=Column(Float)
    b=Column(Float)
    lstat=Column(Float)
    target=Column(Float)

    def __init__(self, id, crim, zn, indus, chas, nox, rm, age, dis, rad, tax, ptratio, b, lstat, target):
        self.id=id,
        self.crim=crim,
        self.zn=zn,
        self.indus=indus,
        self.chas=chas,
        self.nox=nox,
        self.rm=rm,
        self.age=age,
        self.dis=dis,
        self.rad=rad,
        self.tax=tax,
        self.ptratio=pratio,
        self.b=b,
        self.lstat=lstat,
        self.target=target

Base.metadata.create_all(engine)

print('Done.\n')

print('Insert data from CSV to SQL.')
print('-'*30)
# Copy data into database
# Insert data to database
engine = db.create_engine(mysql_str+'Housing')
con=engine.connect()

df.to_sql(name='housing',\
             con=engine,\
             if_exists='replace')

print('Done.\n')

