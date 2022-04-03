from model.model import db
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from config import SQLALCHEMY_DATABASE_URI

# db가 없다면 db를 생성 합니다.
engine = create_engine(SQLALCHEMY_DATABASE_URI)
print('Uri:', engine.url)
if not database_exists(engine.url):
    create_database(engine.url)

print(database_exists(engine.url))

db.create_all()
