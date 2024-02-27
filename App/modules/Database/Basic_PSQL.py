# Basic PSQL Functions

## Load modules

import psycopg2
from psycopg2 import sql
from modules.Database.db_conn import pg_connection_dict # Our database connection dictionary for psycopg2

# ~~~~~~~~~~~~~~

def send_update(cmd):
    '''
    Takes a command (sql.SQL() string) and pg_connection_dict
    Sends the command to postgres
    And closes connection
    '''
    
    # Create connection with postgres option keepalives_idle = 1 seconds
    conn = psycopg2.connect(**pg_connection_dict, 
                            keepalives_idle=1)
    
    # Create cursor
    cur = conn.cursor()
    
    cur.execute(cmd) # Execute
    
    conn.commit() # Committ command
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    
    
# ~~~~~~~~~~~~~~

def get_response(cmd):
    '''
    Takes a command (sql.SQL() string) and pg_connection_dict
    Sends the command to postgres
    Retrieves the response
    And closes connection
    '''
    
    # Create connection with postgres option keepalives_idle = 30 seconds
    conn = psycopg2.connect(**pg_connection_dict, 
                            keepalives_idle=30)
    
    # Create cursor
    cur = conn.cursor()
    
    cur.execute(cmd) # Execute
    
    conn.commit() # Committ command
    
    # Fetch Response
    
    response = cur.fetchall()
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    
    return response

# ~~~~~~~~~~~~~~~~~~~~~~~~~~

def update_table(correct_df, tablename, unique_identifier):
    '''
    Updates any fields in our database to match the input dataframe

    Parameters:

    correct_df - pd.dataframe with well formatted data for database
    tablename - table in database
    unique_identifier - string of the unique identifier field
    '''
    # Create connection with postgres option keepalives_idle = 100 seconds
    conn = psycopg2.connect(**pg_connection_dict, 
                            keepalives_idle=100)
    
    # Create cursor
    cur = conn.cursor()
    
    cols_to_update = set(correct_df.columns.to_list()) - {unique_identifier}
    
    for i, row in correct_df.iterrows():

        cmd = sql.SQL(f'UPDATE "{tablename}" SET ')

        for i, col in enumerate(cols_to_update):
            
            if i > 0:
                cmd += sql.SQL(',') # Comma separated name-value pairs
                
            cmd += sql.SQL('{} = {}').format(sql.Identifier(col),
                                             sql.Literal(row[col]))

        cmd += sql.SQL(' WHERE {} = {};').format(sql.Identifier(unique_identifier),
                                                 sql.Literal(row[unique_identifier]))

        # Execute command
        cur.execute(cmd)
    
        conn.commit() # Commit command
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()

# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~~~~~~~~~

def insert_into(df, tablename, is_spatial = False):
    '''
    Takes a well formatted dataframe, df,  
        with the columns aligned to the fields of a table in the database (tablename)
    as well as pg_connection_dict
    Inserts all rows into database
    And closes connection
    
    IF YOU ARE INSERTING A SPATIAL DATASET - please indicate this by setting the is_spatial variable = True
    And be sure that the last column is called geometry with well known text in WGS84 (EPSG:4326) "Lat/lon"
    '''
    
    
    fieldnames = list(df.columns)
    
    # Create connection with postgres option keepalives_idle = 100 seconds
    conn = psycopg2.connect(**pg_connection_dict, 
                            keepalives_idle=100)
    
    # Create cursor
    cur = conn.cursor()
    
    for row in df.itertuples():
        
        vals = row[1:]
        
        if is_spatial: # We need to treat the geometry column of WKT a little differently
        
            q1 = sql.SQL(f'INSERT INTO "{tablename}"' + ' ({}) VALUES ({},{});').format(
     sql.SQL(', ').join(map(sql.Identifier, fieldnames)),
     sql.SQL(', ').join(sql.Placeholder() * (len(fieldnames) - 1)),
     sql.SQL('ST_SetSRID(ST_GeomFromText(%s), 4326)::geometry'))
        
        else:
        
            q1 = sql.SQL(f'INSERT INTO "{tablename}"' + ' ({}) VALUES ({});').format(
     sql.SQL(', ').join(map(sql.Identifier, fieldnames)),
     sql.SQL(', ').join(sql.Placeholder() * (len(fieldnames))))

        # Execute command
        cur.execute(q1.as_string(conn), (vals))
    
        conn.commit() # Commit command
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
