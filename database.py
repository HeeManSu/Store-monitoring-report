from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


db_url = "postgresql://root:root@localhost:5432/loop_assignment"

engine = create_engine(db_url)
Session = sessionmaker(bind = engine, autocommit = False, autoflush = False) 

