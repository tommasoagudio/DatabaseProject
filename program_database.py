def load_data(customer_database):
    import pandas as pd
    global file_path
    file_path = input('Please insert the file path for the dataset: ')
    data = pd.read_csv(file_path+"/customer_segmentation.csv")
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
        ) 
        if mydb.is_connected():
            mycursor = mydb.cursor()
            mycursor.execute("SHOW DATABASES")
            result = mycursor.fetchall()
            print(result)
            for x in result:
                if x[0] == 'customersdatabase1':
                    print("The database already exists!")
                    return None

            mycursor.execute("CREATE DATABASE CustomersDatabase1")
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
        
        mydb.commit()
    for i, row in data.iterrows():
        sql = "INSERT IGNORE INTO CustomersDatabase1.seller VALUES (%s,%s,%s)"
        mycursor.execute(
            sql, tuple([row["seller_id"], row["seller_city"], row["seller_state"]])
        )
        
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
                    row["product_category_name_english"],
                    row["product_description_lenght"],
                ]
            ),
        )
        
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
        
        mydb.commit()
    df2 = data.loc[:, ["product_id", "seller_id"]]
    df2.drop_duplicates(inplace=True)
    c = 0
    for i, row in df2.iterrows():
        sql = "INSERT IGNORE INTO CustomersDatabase1.offer VALUES (%s,%s,%s)"
        mycursor.execute(sql, tuple([c, row["seller_id"], row["product_id"]]))
        c += 1
        
        mydb.commit()

    print("I loaded the dataset and built the database!\n")


def query1():  
    import mysql.connector as mysql
    from mysql.connector import Error
    import matplotlib.pyplot as plt
    import pandas as pd

    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    ) 
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
    df=pd.DataFrame(result)
    df.columns=["Categories","Occurencies"]
    print(df)
    df.plot(x="Categories", y="Occurencies", kind='bar')
    plt.xlabel("Categories")
    plt.ylabel('Number of occurencies')
    plt.title('Frequence of different Categories')
    for element in result:
        print(element)


def query2():  
    import mysql.connector as mysql
    from mysql.connector import Error
    import matplotlib.pyplot as plt
    import pandas as pd

    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    ) 
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
    df=pd.DataFrame(result)
    df.columns=["Occurencies","Cities"]
    df.plot(x="Cities", y="Occurencies", kind='bar')
    plt.xlabel("Cities")
    plt.ylabel('Number of occurencies')
    plt.title('Frequence of different Cities')
    for element in result:
        print(element)


def query3():  
    import mysql.connector as mysql
    from mysql.connector import Error
    import matplotlib.pyplot as plt
    import pandas as pd

    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    )  
    mycursor = mydb.cursor()
    mycursor.execute("USE CustomersDatabase1")
    sql = mycursor.execute(
        """
    select count(s.seller_id),s.seller_id from seller as s, order_ as o
    where s.seller_id = o.order_seller
    group by s.seller_id
    order by count(s.seller_id) DESC;
    """
    )
    mycursor.execute(sql)
    result = mycursor.fetchall()
    df=pd.DataFrame(result)
    df.columns=["Occurencies","Seller"]
    print(df)
    df.plot(x="Seller" , kind='Occurencies')
    plt.xlabel("Sellers")
    plt.ylabel('Number of occurencies')
    plt.title('Frequency of different Sellers')
    for element in result:
        print(element)


def query4():  
    import mysql.connector as mysql
    from mysql.connector import Error
    import matplotlib.pyplot as plt
    import pandas as pd

    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    ) 
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


def query5(): 
    import mysql.connector as mysql
    from mysql.connector import Error
    import matplotlib.pyplot as plt

    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    ) 
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


def query6(): 
    import mysql.connector as mysql
    from mysql.connector import Error
    import pandas as pd

    data = pd.read_csv(file_path+"/customer_segmentation.csv")
    possible_cities = []
    possible_payment_types = []
    yes_no = input('Do you want to see the list of all possible cities? ').lower()
    if yes_no == 'yes':
        for city in data["customer_city"]:
            if city not in possible_cities:
                possible_cities.append(city)
        print("Possible cities to select: ", possible_cities)
    for payments in data["payment_type"]:
        if payments not in possible_payment_types:
            possible_payment_types.append(payments)
    print("")
    print("Possible payment types to select: ", possible_payment_types)
    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    ) 
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

 
def query7():
    import mysql.connector as mysql
    from mysql.connector import Error
    import pandas as pd

    data = pd.read_csv(file_path+"/customer_segmentation.csv")
    possible_types = []
    for types in data["payment_type"]:
        if types not in possible_types:
            possible_types.append(types)
    print("Possible payment types to select: ", possible_types)

    payment_type = input("please enter the desired payment type: ")
    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    ) 
    mycursor = mydb.cursor()
    mycursor.execute("USE CustomersDatabase1")
    sql = mycursor.execute(
        """
        select avg(o.payment_value), o.payment_type from order_ as o
        where o.payment_type = %s;
    """,
        (payment_type,)
    )
    mycursor.execute(sql)
    result = mycursor.fetchall()
    for element in result:
        print(element)

def query8():
    import mysql.connector as mysql
    from mysql.connector import Error
    import pandas as pd

    data = pd.read_csv(file_path+"/customer_segmentation.csv")
    possible_cities = []
    yes_no = input('Do you want to see the list of all possible cities? ').lower()
    if yes_no == 'yes':
        for city in data["customer_city"]:
            if city not in possible_cities:
                possible_cities.append(city)
        print("Possible cities to select: ", possible_cities)

    amount = int(input("please enter minimum amount: "))
    city = input("please enter the desired city: ")
    possible_types = []
    for types in data["payment_type"]:
        if types not in possible_types:
            possible_types.append(types)
    print("Possible payment types to select: ", possible_types)
    payment_type = input('please enter the desired payment type: ')
    mydb = mysql.connect(
        host="localhost",
        user=username,
        password=password,
        auth_plugin="mysql_native_password",
    ) 
    mycursor = mydb.cursor()
    mycursor.execute("USE CustomersDatabase1")
    sql = mycursor.execute(
        """
        select count(c.customer_id), sum(o.payment_value) as x, c.customer_city, o.payment_type from customer as c, order_ as o
        where c.customer_unique_id = o.order_customer and c.customer_city = %s and o.payment_type = %s
        having x > %s;
    """,
        (city,payment_type, amount,),
    )
    mycursor.execute(sql)
    result = mycursor.fetchall()
    if len(result) == 0:
        print('There are no orders in this city with amount: ',amount)
        return None
    for element in result:
        print(element)


if __name__ == "__main__":
    print("Welcome to our project!\n")
    load_data("customer_segmentation.csv")

    valid_choices = [ "Query to show the number of orders for each product: ","1","Query to show the number of orders for each city: ", "2", "Query to show the number of orders sold by sellers: ","3","Query to show the avarage number of installments: ", "4","Query to show the avarage number of installments: ", "5","Query to show the number of orders for your desired city and payment type: ", "6","query will show the average payment value for the desired payment type: ", "7","Query to show the number of customers that have spent at least the desired amount and living in the determined city: ", "8", "quit"]

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
            print("This query will show the number of orders sold by each seller \n")
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
                "This query will show the number of customers that have spent at least the desired amount, are living in a determined city and have used a specific payment type \n"
            )
            query8()
            continue
        else:
            raise Exception("Something went really wrong")
    print("\nGoodbye!\n")
