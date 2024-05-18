import time
from cassandra.cluster import Cluster


KEYSPACE = "group_1"


def create_connection():
    cluster = Cluster(['localhost'], port=9042)

    session = cluster.connect()

    return session


def create_keyspace(session: Cluster.connect):
    session.execute("""
            DROP KEYSPACE IF EXISTS group_1;
        """)

    print("creating keyspace...")

    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS group_1 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """)

    print("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    print("Keyspace created successfully!")

    time.sleep(2)


def create_table(session: Cluster.connect):
    print("creating table...")

    session.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            Name text,
            Email text, 
            Gender text,
            Birthday text,
            Location text,
            Phone_number text,
            Registration_date text,
            user_id text,
            platform text,
            text text,
            timestamp timestamp,
            post_id text,
            topic text,
            label text,
            PRIMARY KEY (user_id, timestamp)
            );
        """)

    print("Table created successfully!")

    time.sleep(2)


def main():
    session = create_connection()

    create_keyspace(session)

    create_table(session)



    print("Task completed!")


if __name__ == '__main__':
    main()