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

TEST_ACCOUNTS_TABLE = "SELECT * FROM accounts;"
TEST_PRODUCTS_TABLE = "SELECT * FROM products;"
TEST_TRANSACTIONS_TABLE = "SELECT * FROM transactions;"

TEST_TABLES = [
    TEST_ACCOUNTS_TABLE,
    TEST_PRODUCTS_TABLE,
    TEST_TRANSACTIONS_TABLE,
]


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
        conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
        cur = conn.cursor()
        # Write Connection established
        print("Connection established")

        print("Clearing existing tables...")
        cur.execute(CLEAR_TABLES)
        conn.commit()
        print("Cleared existing tables...")
        # Definining the tables
        for table in list_of_tables:
            create_table(cur, conn, table)
        print("Defined tables:")
        # Importing the CSV data into tables
        for csv_file in csv_list:
            import_csv_data(cur, conn, csv_file)
        print("Imported CSV data into tables")
        
        # Testing the tables
        for test_query in TEST_TABLES:
            test_tables(cur, conn, test_query)

    except psycopg2.Error as e:
        print("Error connecting to the database")
        print(e)
        return
    finally:
        cur.close()
        conn.close()
        print("Connection closed")


def create_table(
    cur: psycopg2.extensions.cursor,
    conn: psycopg2.extensions.connection,
    create_table_statement: str,
) -> None:
    try:
        cur.execute(create_table_statement)
        print("Table created successfully")
        conn.commit()
    except psycopg2.Error as e:
        print("Error executing query")
        print(e)
        print(e.pgcode)
        conn.rollback()


def import_csv_data(
    cur: psycopg2.extensions.cursor,
    conn: psycopg2.extensions.connection,
    csv_path: Path,
) -> None:
    try:
        with csv_path.open() as f:
            next(f)  # Skip header row
            cur.copy_from(f, csv_path.stem, sep=",")
            conn.commit()
            print(f"Data imported successfully from {csv_path} into {csv_path.stem}")
    except psycopg2.Error as e:
        print("Error executing query")
        print(e)
        print(e.pgcode)
        conn.rollback()

def test_tables(cur: psycopg2.extensions.cursor,
    conn: psycopg2.extensions.connection,
    test_query : str) -> None:
    try:
        cur.execute(test_query)
        results = cur.fetchall()
        for row in results:
            print(row)
    except psycopg2.Error as e:
        print("Error executing query")
        print(e)
        print(e.pgcode)
        conn.rollback()


if __name__ == "__main__":
    main()
