# 경고창 무시
import warnings
warnings.filterwarnings('ignore')

import sqlalchemy
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from config import SQLALCHEMY_DATABASE_URI
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
db = SQLAlchemy(app) # DB선언

# session 선언
engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI, echo=False)
connect = engine.connect()
meta = MetaData()
Session = sessionmaker(bind=engine)
session = Session()
