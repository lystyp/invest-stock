import sqlalchemy
import json

def get_db_connection():
    # 資料庫設定
    db_settings = {}
    with open('../authentication/db_login_information.json', 'r') as file:
        db_settings = json.load(file)

    engine = sqlalchemy.create_engine('mysql+pymysql://{user}:{password}@{host}:{port}/{db}'.format(
        user = db_settings["user"], 
        password = db_settings["password"],
        host = db_settings["host"],
        port = db_settings["port"], 
        db = db_settings["db"]
    ))
    return engine.connect()