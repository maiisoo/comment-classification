import psycopg2

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = {
                "host": "",
                "port": "",
                "database": "group_1",
                "user": "",
                "password": "",
        }

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()
    # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    # finally:
        # if conn is not None:
        #     conn.close()
        #     print('Database connection closed.')

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
	DROP SCHEMA IF EXISTS group_1 CASCADE;
	CREATE SCHEMA group_1;
        CREATE TABLE group_1.comments (
            "Name" text,
            "Email" text,
            "Gender" text,
            "Birthday" date,
            "Location" text,
            "Phone_number" text,
            "Registration_date" date,
            comment_user_id text,
            platform text,
            text text,
            timestamp timestamp,
            post_id text,
            topic text,
            label text
        );

        """,
        )
    conn = None
    try:
        # read the connection parameters
        # connect to the PostgreSQL server
        conn = psycopg2.connect("")
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    connect()
    create_tables()
