from pathlib import Path

import psycopg2

CREATE_ACCOUNTS_TABLE = (
    "CREATE TABLE accounts"
    "(account_id int PRIMARY KEY, "
    "first_name varchar(255) NOT NULL, "
    "last_name varchar(255) NOT NULL, "
    "address_1 varchar(255) NOT NULL, "
    "address_2 varchar(255) NOT NULL, "
    "city varchar(255) NOT NULL, "
    "state varchar(255) NOT NULL, "
    "zip_code varchar(10) NOT NULL, "
    "join_date date NOT NULL"
    ")"
)

CREATE_PRODUCTS_TABLE = (
    "CREATE TABLE products"
    "(product_id int PRIMARY KEY, "
    "product_code varchar(5) NOT NULL, "
    "product_description text NOT NULL"
    ")"
)

CREATE_TRANSACTIONS_TABLE = (
    "CREATE TABLE transactions"
    "(transaction_id varchar(30) PRIMARY KEY, "
    "transaction_date date NOT NULL, "
    "product_id int NOT NULL, "
    "product_code varchar(5) NOT NULL, "
    "product_description text NOT NULL, "
    "quantity int NOT NULL, "
    "account_id int NOT NULL,"

    "FOREIGN KEY (product_id) REFERENCES products (product_id),"
    "FOREIGN KEY (account_id) REFERENCES accounts (account_id)"
    ")"
)

CLEAR_TABLES = (
    "DROP TABLE IF EXISTS transactions CASCADE;"
    "DROP TABLE IF EXISTS products CASCADE;"
    "DROP TABLE IF EXISTS accounts CASCADE;"
)

list_of_tables = [
    CREATE_ACCOUNTS_TABLE,
    CREATE_PRODUCTS_TABLE,
    CREATE_TRANSACTIONS_TABLE,
]

csv_file_path = Path("data")
csv_list = sorted(csv_file_path.glob("*.csv"))


def main() -> None:
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    # Establish WRITE connection
    try:
        w_conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
        w_cur = w_conn.cursor()
        # Write Connection established
        print("Connection established")

        print("Clearing existing tables...")
        w_cur.execute(CLEAR_TABLES)
        w_conn.commit()
        print("Cleared existing tables...")
        # Definining the tables
        for table in list_of_tables:
            create_table(w_cur, w_conn, table)
        print("Defined tables:")
        # Importing the CSV data into tables
        for csv_file in csv_list:
            import_csv_data(w_cur, w_conn, csv_file)

    except psycopg2.Error as e:
        print("Error connecting to the database")
        print(e)
        return
    finally:
        w_cur.close()
        w_conn.close()
        print("Connection closed")

    w_cur.close()
    w_conn.close()


def create_table(
    w_cur: psycopg2.extensions.cursor,
    w_conn: psycopg2.extensions.connection,
    create_table_statement: str,
) -> None:
    try:
        w_cur.execute(create_table_statement)
        print("Table created successfully")
        w_conn.commit()
    except psycopg2.Error as e:
        print("Error executing query")
        print(e)
        print(e.pgcode)
        w_conn.rollback()


def import_csv_data(
    w_cur: psycopg2.extensions.cursor,
    w_conn: psycopg2.extensions.connection,
    csv_path: Path,
) -> None:
    try:
        with csv_path.open() as f:
            next(f)  # Skip header row
            print(f.readline())
            w_cur.copy_from(f, csv_path.stem, sep=",")
            w_conn.commit()
            print(f"Data imported successfully from {csv_path} into {csv_path.stem}")
    except psycopg2.Error as e:
        print("Error executing query")
        print(e)
        print(e.pgcode)
        w_conn.rollback()


if __name__ == "__main__":
    main()
