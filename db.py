# -*- coding: utf-8 -*-
import pymysql
from pymysql.cursors import DictCursor
from config import DB_CFG

def get_db():
    return pymysql.connect(
        host=DB_CFG["host"],
        user=DB_CFG["user"],
        password=DB_CFG["password"],
        database=DB_CFG["database"],
        cursorclass=DictCursor,
        autocommit=True
    )

def get_version_line():
    try:
        con = get_db()
        with con.cursor() as cur:
            # Eğer ileride id eklersen: ORDER BY id DESC LIMIT 1
            cur.execute("SELECT line FROM versions LIMIT 1;")
            row = cur.fetchone()
            return row["line"] if row else ""
    except Exception:
        return ""
    finally:
        try:
            con.close()
        except Exception:
            pass
