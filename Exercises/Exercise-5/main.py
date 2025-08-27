import psycopg2


def main() -> None:
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    #Establish WRITE connection
    try:
        w_conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
        w_cur = w_conn.cursor()
    except psycopg2.Error as e:
        print("Error connecting to the database")
        print(e)
        return

    accounts_query = "CREATE TABLE accounts" \
                    "(account_id int PRIMARY KEY, " \
                    "first_name varchar(255) NOT NULL, " \
                    "last_name varchar(255) NOT NULL, " \
                    "address_1 varchar(255) NOT NULL, " \
                    "address_2 varchar(255) NOT NULL, " \
                    "city varchar(255) NOT NULL, " \
                    "state varchar(255) NOT NULL, " \
                    "zip_code varchar(10) NOT NULL, " \
                    "join_date date NOT NULL, " \
                    ")"

    try:
        w_cur.execute(accounts_query)
        w_conn.commit()
    except psycopg2.Error as e:
        print("Error executing query")
        print(e)
        return

    # Write Connection established
    print("Connection established")

    w_cur.close()
    w_conn.close()


if __name__ == "__main__":
    main()
