import MySQLdb


# Specify connection parameters to locally installed MySQL DB
def my_connection():
    conn = MySQLdb.connect(host='localhost',
                           user='root',
                           passwd='password',
                           port=3306)
    c = conn.cursor()

    return c, conn
