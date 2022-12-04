def load_data(customer_database):
    import pandas as pd

    data = pd.read_csv("customer_segmentation.csv")
    import mysql.connector as mysql
    from mysql.connector import Error

    global username
    username = input("Please enter your mysql username: ")
    global password
    password = input("Please now enter your mysql password: ")

    db_name = "CustomersDatabase1"
    try:
        mydb = mysql.connect(
            host="localhost",
            user=username,
            password=password,
            auth_plugin="mysql_native_password",
        )  # you can add the auth_plugin here too (ref line 26)
        if mydb.is_connected():
            mycursor = mydb.cursor()
            mycursor.execute("SHOW DATABASES")
            result = mycursor.fetchall()
            print(result)
            for x in result:
                if db_name == x[0]:
                    # mycursor.execute('DROP DATABASE ' + db_name) # delete old database
                    # mydb.commit() # make the changes official
                    print("The database already exists!")
                    return None

            mycursor.execute("CREATE DATABASE IF NOT EXISTS" + db_name)
            print("Database is created")
    except Error as e:
        print("Error while connecting to MySQL", e)

    mycursor.execute("USE CustomersDatabase1")

    mycursor.execute(
        """ 
        CREATE TABLE customer(
            customer_unique_id VARCHAR(40),
            customer_id VARCHAR(40),
            customer_city VARCHAR(40),
            customer_state VARCHAR(40),
            PRIMARY KEY (customer_unique_id)
            );
      """
    )

    mycursor.execute(
        """
       CREATE TABLE seller(
            seller_id VARCHAR(40),
            seller_city VARCHAR(40),
            seller_state VARCHAR(40),
            PRIMARY KEY (seller_id)
            );


    """
    )

    mycursor.execute(
        """
       CREATE TABLE product(
            product_id VARCHAR(40),
            price int,
            freight_value int,
            product_category_name VARCHAR(60),
            product_description_lenght int,
            PRIMARY KEY (product_id)
            );


    """
    )

    mycursor.execute(
        """
            CREATE TABLE order_(
                order_id VARCHAR(40),
                order_status VARCHAR(40),
                order_purchase_time DATETIME,
                order_approved_at DATETIME,
                order_delivered_carrier_date DATETIME,
                order_delivered_customer_date DATETIME,
                order_estimated_delivery_date DATETIME,
                payment_type VARCHAR(40),
                payment_value int,
                payment_installments int,
                order_customer VARCHAR(40),
                order_product VARCHAR(40),
                order_seller VARCHAR(40),
                PRIMARY KEY (order_id),
                FOREIGN KEY (order_customer) REFERENCES customer(customer_unique_id),
                FOREIGN KEY (order_seller) REFERENCES seller(seller_id),
                FOREIGN KEY (order_product) REFERENCES product(product_id)
                ); 

        """
    )
    mycursor.execute(
        """
        CREATE TABLE offer(
            id VARCHAR(40),
            seller_id VARCHAR(40),
            product_id VARCHAR(40),
            PRIMARY KEY (id),
            FOREIGN KEY (seller_id) REFERENCES seller(seller_id),
            FOREIGN KEY (product_id) REFERENCES product(product_id)
        ); 
    """
    )
    for i, row in data.iterrows():
        sql = "INSERT IGNORE INTO CustomersDatabase1.customer VALUES (%s,%s,%s,%s)"
        mycursor.execute(
            sql,
            tuple(
                [
                    row["customer_unique_id"],
                    row["customer_id"],
                    row["customer_city"],
                    row["customer_state"],
                ]
            ),
        )
        # print("Record inserted")
        mydb.commit()
    for i, row in data.iterrows():
        sql = "INSERT IGNORE INTO CustomersDatabase1.seller VALUES (%s,%s,%s)"
        mycursor.execute(
            sql, tuple([row["seller_id"], row["seller_city"], row["seller_state"]])
        )
        # print("Record inserted")
        mydb.commit()
    for i, row in data.iterrows():
        sql = "INSERT IGNORE INTO CustomersDatabase1.product VALUES (%s,%s,%s,%s,%s)"
        mycursor.execute(
            sql,
            tuple(
                [
                    row["product_id"],
                    row["price"],
                    row["freight_value"],
                    row["product_category_name"],
                    row["product_description_lenght"],
                ]
            ),
        )
        # print("Record inserted")
        mydb.commit()

    for i, row in data.iterrows():
        sql = "INSERT IGNORE INTO CustomersDatabase1.order_ VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        mycursor.execute(
            sql,
            tuple(
                [
                    row["order_id"],
                    row["order_status"],
                    row["order_purchase_timestamp"],
                    row["order_approved_at"],
                    row["order_delivered_carrier_date"],
                    row["order_delivered_customer_date"],
                    row["order_estimated_delivery_date"],
                    row["payment_type"],
                    row["payment_value"],
                    row["payment_installments"],
                    row["customer_unique_id"],
                    row["product_id"],
                    row["seller_id"],
                ]
            ),
        )
        # print("Record inserted")
        mydb.commit()
    df2 = data.loc[:, ["product_id", "seller_id"]]
    df2.drop_duplicates(inplace=True)
    c = 0
    for i, row in df2.iterrows():
        sql = "INSERT IGNORE INTO CustomersDatabase1.offer VALUES (%s,%s,%s)"
        mycursor.execute(sql, tuple([c, row["seller_id"], row["product_id"]]))
        c += 1
        # print("Record inserted")
        mydb.commit()

    print("I loaded the dataset and built the database!\n")


def query1():  # returns the number of orders for each product
    import mysql.connector as mysql
    from mysql.connector import Error

    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    )  # you can add the auth_plugin here too (ref line 26)
    mycursor = mydb.cursor()
    mycursor.execute("USE CustomersDatabase1")
    sql = mycursor.execute(
        """
        select product_category_name,count(product_category_name) from product
        GROUP BY
        product_category_name
        ORDER BY count(product_category_name) desc
    """
    )
    mycursor.execute(sql)
    result = mycursor.fetchall()
    for element in result:
        print(element)


def query2():  # returns the number of orders for each city
    import mysql.connector as mysql
    from mysql.connector import Error

    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    )  # you can add the auth_plugin here too (ref line 26)
    mycursor = mydb.cursor()
    mycursor.execute("USE CustomersDatabase1")
    sql = mycursor.execute(
        """
        select count(c.customer_city), c.customer_city from customer as c, order_ as o
        where c.customer_unique_id = o.order_customer
        group by c.customer_city
        order by count(c.customer_city) DESC;
    """
    )
    mycursor.execute(sql)
    result = mycursor.fetchall()
    for element in result:
        print(element)


def query3():  # returns the number of orders for each customer
    import mysql.connector as mysql
    from mysql.connector import Error

    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    )  # you can add the auth_plugin here too (ref line 26)
    mycursor = mydb.cursor()
    mycursor.execute("USE CustomersDatabase1")
    sql = mycursor.execute(
        """
    select count(c.customer_id),C.customer_id from customer as c, order_ as o
    where c.customer_unique_id = o.order_customer
    group by c.customer_id
    order by count(c.customer_unique_id) DESC;
    """
    )
    mycursor.execute(sql)
    result = mycursor.fetchall()
    for element in result:
        print(element)


def query4():  # returns the avarage number of installments
    import mysql.connector as mysql
    from mysql.connector import Error

    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    )  # you can add the auth_plugin here too (ref line 26)
    mycursor = mydb.cursor()
    mycursor.execute("USE CustomersDatabase1")
    sql = mycursor.execute(
        """
    select avg(o.payment_installments) from order_ as o;

    """
    )
    mycursor.execute(sql)
    result = mycursor.fetchall()
    for element in result:
        print(element)


def query5():  # returns the payment value, price and installments for each order.
    import mysql.connector as mysql
    from mysql.connector import Error

    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    )  # you can add the auth_plugin here too (ref line 26)
    mycursor = mydb.cursor()
    mycursor.execute("USE CustomersDatabase1")
    sql = mycursor.execute(
        """
    select c.customer_id, o.payment_value, o.payment_installments, o.payment_type from order_ as o, customer as c
    where c.customer_unique_id = o.order_customer
    group by c.customer_id, o.payment_installments, o.payment_value,o.payment_type;
    """
    )
    mycursor.execute(sql)
    result = mycursor.fetchall()
    for element in result:
        print(element)


def query6():  # number of orders for a specific city and a specific payment_type
    import mysql.connector as mysql
    from mysql.connector import Error
    import pandas as pd

    data = pd.read_csv("customer_segmentation.csv")
    possible_cities = []
    possible_payment_types = []
    for city in data["customer_city"]:
        if city not in possible_cities:
            possible_cities.append(city)
    for payments in data["payment_type"]:
        if payments not in possible_payment_types:
            possible_payment_types.append(payments)
    print("Possible cities to select: ", possible_cities)
    print("")
    print("Possible payment types to select: ", possible_payment_types)
    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    )  # you can add the auth_plugin here too (ref line 26)
    mycursor = mydb.cursor()
    mycursor.execute("USE CustomersDatabase1")

    city = input("Please insert the desired city: ")
    payment_type = input("Please insert the desired payment_type: ")
    sql = mycursor.execute(
        """
    select  count(c.customer_city), c.customer_city, o.payment_type from customer as c, order_ as o
    where c.customer_unique_id = o.order_customer and c.customer_city = %s and o.payment_type = %s;

    """,
        (city, payment_type),
    )
    mycursor.execute(sql)
    result = mycursor.fetchall()
    for element in result:
        print(element)


def query7():  # return the avarage payment value for a specific payment type
    import mysql.connector as mysql
    from mysql.connector import Error
    import pandas as pd

    data = pd.read_csv("customer_segmentation.csv")
    possible_payment_types = []
    for payments in data["payment_type"]:
        if payments not in possible_payment_types:
            possible_payment_types.append(payments)
    payment_type = list(input("please enter the desired payment type "))
    print("Possible payment types to select: ", possible_payment_types)

    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    )  # you can add the auth_plugin here too (ref line 26)
    mycursor = mydb.cursor()
    mycursor.execute("USE CustomersDatabase1")
    sql = mycursor.execute(
        """
    select round(avg(o.payment_value)), o.payment_type from order_ as o
    where o.payment_type = %s;
    """,
        (payment_type),
    )
    mycursor.execute(sql)
    result = mycursor.fetchall()
    for element in result:
        print(element)


def query8():
    import mysql.connector as mysql
    from mysql.connector import Error
    import pandas as pd

    data = pd.read_csv("customer_segmentation.csv")
    possible_cities = []
    for city in data["customer_city"]:
        if city not in possible_cities:
            possible_cities.append(city)
    print("Possible cities to select: ", possible_cities)

    amount = int(input("please enter minimum amount \n"))
    city = input("please enter the desired city \n")
    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    )  # you can add the auth_plugin here too (ref line 26)
    mycursor = mydb.cursor()
    mycursor.execute("USE CustomersDatabase1")
    sql = mycursor.execute(
        """
        select c.customer_id, sum(o.payment_value) as x, c.customer_city from customer as c, order_ as o
        where c.customer_unique_id = o.order_customer and c.customer_city = %s
        group by c.customer_id,o.payment_value, c.customer_city
        having x > %s
        order by o.payment_value DESC;
    """,
        (city, amount),
    )
    mycursor.execute(sql)
    result = mycursor.fetchall()
    for element in result:
        print(element)


if __name__ == "__main__":
    print("Welcome to our project!\n")
    load_data("customer_segmentation.csv")

    valid_choices = ["1", "2", "3", "4", "5", "6", "7", "8", "quit"]

    while True:
        print("possible choices: \n")
        for choices in valid_choices:
            print(choices)
        choice = input("""\n\nPlease choose a query to run!\n""").lower()

        if choice not in valid_choices:
            print(f"Your choice '{choice}' is not valid. Please insert a valid option")
            continue
        if choice == "quit":
            break
        print(f"\nYou chose to execute query {choice}")
        if choice == "1":
            print("This query will show the number of orders for each product \n ")
            query1()
            continue
        elif choice == "2":
            print("This query will show the number of orders for each city \n")
            query2()
            continue
        elif choice == "3":
            print("This query will show the number of orders for each customer \n")
            query3()
            continue
        elif choice == "4":
            print("This query will show the avarage number of installments \n")
            query4()
            continue
        elif choice == "5":
            print(
                "This query will show payment value, payment type and installments for each order \n"
            )
            query5()
            continue
        elif choice == "6":
            print(
                "This query will show the number of orders for your desired city and payment type \n"
            )
            query6()
            continue
        elif choice == "7":
            print(
                "This query will show the average payment value for the desired payment type \n"
            )
            query7()
            continue
        elif choice == "8":
            print(
                "This query will show the customers that have spent at least the desired amount and living in the determined city \n"
            )
            query8()
            continue
        else:
            raise Exception("Something went really wrong")
    print("\nGoodbye!\n")
