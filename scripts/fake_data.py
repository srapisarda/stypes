import csv
import random
import psycopg2
import calendar
import datetime
from psycopg2.extras import execute_values


class PostgreSQLConnection:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            print("Connected to the PostgreSQL database!")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL:", error)

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            return result
        except (Exception, psycopg2.Error) as error:
            print("Error executing query:", error)

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Disconnected from the PostgreSQL database.")

    def commit_trans(self):
        self.connection.commit()


def main():
    # Example usage:
    connection = PostgreSQLConnection(
        host='localhost',
        port='5432',
        database='thesis',
        user='postgres',
        password='postgres'
    )
    connection.connect()
    factor = 2
    employee_rows = __insert_employee(connection, 10 ** factor, factor)
    project_rows = __insert_project(connection, 4 ** factor)
    __insert_employee_project(connection, employee_rows, project_rows)
    # Disconnect from the database
    connection.disconnect()


def __insert_employee_project(connection, employee_rows, project_rows):
    rows = []
    project_rows_size = len(project_rows)
    for idx, employee in enumerate(employee_rows):
        employee_id = employee[0]
        manager_id = employee[2]
        project_idx = idx % project_rows_size
        project_id = project_rows[project_idx][0]
        if bool(random.getrandbits(1)) and manager_id is not None:
            months = random.randint(1, 12)
            until = __add_months(datetime.datetime.now(), months)
        else:
            until = None
        rows.append((employee_id, project_id, until))

    execute_values(connection.cursor,
                   "INSERT INTO public.employee_project_2 (employee_id, project_id, until) VALUES %s",
                   rows)
    connection.commit_trans()


def __insert_project(connection, num) -> list:
    rows = []
    for project_id in range(0, num):
        project_name = "project-" + str(project_id)
        rows.append(("P-" + str(project_id), project_name))
    execute_values(connection.cursor, "INSERT INTO public.project_2 (id, name) VALUES %s", rows)
    connection.commit_trans()
    return rows


def __insert_employee(connection, num, factor) -> list:
    rows = []
    for employee_id in range(0, num):
        employee_name = "employ_" + str(employee_id)
        manager_id = employee_id % (10 * factor)
        if manager_id == employee_id:
            manager_id = None
        else:
            manager_id = "E" + str(manager_id)
        rows.append(("E" + str(employee_id), employee_name, manager_id))

    execute_values(connection.cursor, "INSERT INTO public.employee_2 (id, name, manager_id) VALUES %s", rows)
    connection.commit_trans()
    return rows


def __add_months(source_date, months):
    month = source_date.month - 1 + months
    year = source_date.year + month // 12
    month = month % 12 + 1
    day = min(source_date.day, calendar.monthrange(year, month)[1])
    return datetime.date(year, month, day)


if __name__ == '__main__':
    main()
