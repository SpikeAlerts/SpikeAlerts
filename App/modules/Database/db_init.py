### Import Packages

# Database 

import psycopg2 # For working with postgresql database
from modules.Database.db_conn import pg_connection_dict # Getting the connection dictionary


def db_need_init():
  conn = psycopg2.connect(**pg_connection_dict)
  cur = conn.cursor()
  cur.execute('SELECT * FROM "extent"')
  response = cur.fetchall()
  cur.close()
  conn.close()
  
  if len(response) > 0:
    return False
  else:
    return True
