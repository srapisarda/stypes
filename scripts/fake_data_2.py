import csv
import random
import sys

import psycopg2
import calendar
import datetime
from psycopg2.extras import execute_values


def main():
    path = ""
    if len(sys.argv) > 2:
        path = sys.argv[2]
        if path[len(path) - 1] != "/":
            path = path + "/"

    if len(sys.argv) > 1:
        factor = int(sys.argv[1])
    else:
        factor = 2
    employee_rows = __insert_employee(factor=factor, num=10 ** factor, path=path)
    project_rows = __insert_project(factor=factor, num=4 ** factor, path=path)
    __insert_employee_project(employee_rows=employee_rows, project_rows=project_rows, factor=factor, path=path)


def __insert_employee_project(employee_rows, project_rows, factor, path):
    rows = []
    project_rows_size = len(project_rows)
    with open(f"{path}{factor}-employee_project.csv", "w") as f:
        for idx, employee in enumerate(employee_rows):
            employee_id = employee[0]
            manager_id = employee[2]
            project_idx = idx % project_rows_size
            project_id = project_rows[project_idx][0]
            if bool(random.getrandbits(1)) and manager_id is not None:
                months = random.randint(1, 12)
                until = __add_months(datetime.datetime.now(), months).strftime("%Y-%m-%d")
            else:
                until = ""
            print(f"{employee_id},{project_id},{until}", file=f)


def __insert_project(factor, num, path) -> list:
    rows = []
    with open(f"{path}{factor}-project.csv", "w") as f:
        for project_id in range(0, num):
            project_name = "project-" + str(project_id)
            rows.append(("P-" + str(project_id), project_name))
            print(f"{project_id},{project_name}", file=f)

    return rows


def __insert_employee(factor, num, path) -> list:
    rows = []
    with open(f"{path}{factor}-employee.csv", "w") as f:
        for employee_id in range(0, num):
            employee_name = "employee_" + str(employee_id)
            manager_id = employee_id % (10 * factor)
            if manager_id == employee_id:
                manager_id = None
            else:
                manager_id = "E" + str(manager_id)

            emp_id = "E" + str(employee_id)
            rows.append((emp_id, employee_name, manager_id))
            print(f"{emp_id},{employee_name},{manager_id}", file=f)

    return rows


def __add_months(source_date, months):
    month = source_date.month - 1 + months
    year = source_date.year + month // 12
    month = month % 12 + 1
    day = min(source_date.day, calendar.monthrange(year, month)[1])
    return datetime.date(year, month, day)


if __name__ == '__main__':
    main()
