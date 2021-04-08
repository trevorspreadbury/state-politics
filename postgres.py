import psycopg2
from configparser import ConfigParser

class Database():

    def __init__(self, name, configuration_file = "database.ini"):
        parser = ConfigParser()
        parser.read(configuration_file)
        db_params = {}
        if parser.has_section(name):
            for key, value in parser.items(name):
                db_params[key] = value
        else:
            db_params = {
                "host": "localhost",
                "database": name,
                "user": "postgres"
            }
        self.connection = psycopg2.connect(**db_params)
        self.cursor = self.connection.cursor()

    def query(self, sql, data=None):
        self.cursor.execute(sql, data)

    def copy_from(self, file, table):
        with open(file, "r") as csv_file:
            self.cursor.copy_expert(
                "COPY {} FROM STDIN CSV HEADER".format(table),
                csv_file
            )

    def close(self):
        self.cursor.close()
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.close()

    def commit(self):
        self.connection.commit()