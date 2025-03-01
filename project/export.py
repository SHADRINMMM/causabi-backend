import os
import psycopg2
from psycopg2.extras import DictCursor
import csv

from dotenv import load_dotenv
load_dotenv()

PATH_TO = os.getenv('PATH_TO')
def export_table_to_csv(conn, table_name: str, output_file: str, user_id: int = None):
    cursor = conn.cursor()

    if table_name == "bikes":
        cursor.execute(
            f"SELECT * FROM {table_name} WHERE {table_name}.user = %s", (user_id,)
        )
    elif table_name == "orders":
        cursor.execute(
            f"SELECT * FROM {table_name} LEFT OUTER JOIN bikes ON orders.bike = bikes.id WHERE bikes.user = %s",
            (user_id,),
        )
    else:
        cursor.execute(f"SELECT * FROM {table_name}")

    rows = cursor.fetchall()

    colnames = [desc[0] for desc in cursor.description]

    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(output_file, mode="w+", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(colnames)
        writer.writerows(rows)

    print(f"Таблица {table_name} экспортирована в {output_file}")


def export_tables_to_csv(database_config, tables):
    conn = psycopg2.connect(**database_config)

    cursor = conn.cursor(cursor_factory=DictCursor)

    cursor.execute(
        f"SELECT * FROM users",
    )

    users = cursor.fetchall()

    for user in users:
        if user["email"] is None:
            # в лог и сигнал sentry
            continue

        for table in tables:
            try:
                output_file = f"{PATH_TO}/{user['email']}/-{user['email']}-{table}.csv"
                export_table_to_csv(conn, table, output_file, user["id"])
            except Exception as e:
                # в лог и сигнал sentry
                print(f"[{user['email']}] Произошла ошибка: {e}")

    conn.close()


DBNAME = os.getenv('DBNAME')
USER_DB = os.getenv('USER_DB') 
PASSWORD = os.getenv('PASSWORD') 
HOST = os.getenv('HOST')
PORT = os.getenv('PORT')



database_config = {
    "dbname": DBNAME,
    "user": USER_DB,
    "password": PASSWORD,
    "host": HOST,
    "port": PORT,
    'sslmode': 'require'
}

tables = ["orders", "bikes", "models", "brands"]


if __name__ == "__main__":
    export_tables_to_csv(database_config, tables)
