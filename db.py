# -*- coding: utf-8 -*-
import pymysql
from pymysql.cursors import DictCursor
from config import DB_CFG


def get_db():
    """
    Uygulamanın repository veritabanına (MySQL) bağlantı açar.
    Tüm kod buradaki fonksiyonu kullanmalı.
    """
    return pymysql.connect(
        host=DB_CFG["host"],
        user=DB_CFG["user"],
        password=DB_CFG["password"],
        database=DB_CFG["database"],
        cursorclass=DictCursor,
        autocommit=True,
    )


def get_version_line():
    """
    Login ekranının altındaki versiyon / banner satırını döndürür.
    Eski 'versions' tablosu yerine artık 'settings' tablosundaki
    'app_version_line' key'ini kullanıyoruz.
    """
    con = None
    try:
        con = get_db()
        with con.cursor() as cur:
            cur.execute(
                """
                SELECT setting_value
                  FROM settings
                 WHERE setting_key = %s
                 LIMIT 1;
                """,
                ("app_version_line",),
            )
            row = cur.fetchone()
            return row["setting_value"] if row else ""
    except Exception:
        # herhangi bir hata olursa login ekranı çökmemesi için boş string dön
        return ""
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass
