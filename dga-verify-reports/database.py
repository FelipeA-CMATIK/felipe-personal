from helper import Helper
import sqlalchemy
import os
import ssl

class Database:
    def __init__(self):
        db_config = {
            "pool_size": 5,
            "max_overflow": 2,
            "pool_timeout": 30,  # 30 seconds
            "pool_recycle": 1800  # 30 minutes
        }

        ssl_context = ssl.SSLContext()
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.load_verify_locations(os.environ.get('SSL_CA'))
        ssl_context.load_cert_chain(os.environ.get('SSL_CERT'), os.environ.get('SSL_KEY'))
        ssl_args = {"ssl_context": ssl_context}

        pool = sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL.create(
                drivername="postgresql+pg8000",
                username=os.environ.get('DB_USER'),
                password=os.environ.get('DB_PASS'),
                host=os.environ.get("DB_HOST"),
                port=int(os.environ.get("DB_PORT")),
                database=os.environ.get('DB_NAME')
            ),
            connect_args=ssl_args,
            **db_config
        )

        pool.dialect.description_encoding = None
        self.cursor = pool.connect()
        self.helper = Helper()

    def all(self, query):
        try:
            return [dict(row) for row in self.cursor.execute(query).fetchall()]
            pass
        except Exception as e:
            print(e, query)
            raise

    def first(self, query):
        try:
            return self.cursor.execute(query).fetchone()
            pass
        except Exception as e:
            print(e, query)
            raise

    def execute(self, query):
        try:
            return self.cursor.execute(query)
            pass
        except Exception as e:
            print(e, query)
            raise

    def flatten(self, rows):
        data = []
        for row in rows:
            for column in self.columns:
                data.append(row[column])
        return data

    def massive_insert_statement(self, values):
        columns = ', '.join(self.columns)
        placeholdersPerRow = "(" + ', '.join(['%s'] * len(self.columns)) + ")"
        placeholders = ', '.join([placeholdersPerRow] * values)

        return "insert into {table} ({columns}) values {values};".format(table=self.table, columns=columns,
                                                                         values=placeholders)

    def map_as_list(self, record):
        return [record[column] for column in self.columns]

    def insert_statement(self):
        columns = ', '.join(self.columns)
        placeholders = "(" + ', '.join(['%s'] * len(self.columns)) + ")"
        return "insert into {table} ({columns}) values {values};".format(table=self.table, columns=columns,
                                                                         values=placeholders)

    def execute_with_payload(self, query, payload):
        try:
            return self.cursor.execute(query, payload)

            pass
        except Exception as e:
            print(e, query)
            raise