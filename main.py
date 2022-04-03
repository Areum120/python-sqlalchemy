from model.model import db
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from config import SQLALCHEMY_DATABASE_URI

engine = create_engine(SQLALCHEMY_DATABASE_URI)
if not database_exists(engine.url):
    create_database(engine.url)

print(database_exists(engine.url))

db.create_all()
