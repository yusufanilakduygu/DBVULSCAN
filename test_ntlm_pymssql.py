import pymssql

HOST = "192.168.56.113"              # örn: 10.10.10.25
PORT = 1433
USER = r"LAB\\svc_sql"          # örn: ODEABANK\\anil
PASSWORD = "Adana147"
DB = "master"

print("Connecting...")

conn = pymssql.connect(
    server=HOST,
    user=USER,
    password=PASSWORD,
    database=DB,
    port=PORT,
    login_timeout=10,
    timeout=10,
)

cur = conn.cursor()
cur.execute("SELECT @@VERSION")
row = cur.fetchone()

print("Connected OK")
print(row[0])

conn.close()