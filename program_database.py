def load_data(customer_database):
    import pandas as pd
    data = pd.read_csv('customer_segmentation.csv')
    

    import mysql.connector as mysql
    from mysql.connector import Error 

    db_name='CustomersDatabase1'
    try:
        mydb = mysql.connect(host='localhost', user='root', password='Verazzano1', auth_plugin='mysql_native_password') # you can add the auth_plugin here too (ref line 26)
        if mydb.is_connected():
            mycursor = mydb.cursor()
            mycursor.execute('SHOW DATABASES')
            result = mycursor.fetchall()
            print(result)
            for x in result:
                if db_name == x[0]:
                    mycursor.execute('DROP DATABASE ' + db_name) # delete old database
                    mydb.commit() # make the changes official
                    print("The database already exists! The old database has been deleted!)")
        
            mycursor.execute("CREATE DATABASE "+ db_name)
            print("Database is created")
    except Error as e:
        print("Error while connecting to MySQL", e)

    mycursor.execute("USE CustomersDatabase1")

    mycursor.execute(
      ''' 
        CREATE TABLE customer(
            customer_unique_id VARCHAR(40),
            customer_id VARCHAR(40),
            customer_city VARCHAR(40),
            customer_state VARCHAR(40),
            PRIMARY KEY (customer_unique_id)
            );
      '''
    )

    mycursor.execute(
    '''
       CREATE TABLE seller(
            seller_id VARCHAR(40),
            seller_city VARCHAR(40),
            seller_state VARCHAR(40),
            PRIMARY KEY (seller_id)
            );


    '''
    )


    mycursor.execute(
    '''
       CREATE TABLE product(
            product_id VARCHAR(40),
            price int,
            freight_value int,
            product_category_name VARCHAR(60),
            product_description_lenght int,
            PRIMARY KEY (product_id)
            );


    '''
    )
    

    mycursor.execute(
        '''
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

        '''
        )
    mycursor.execute(
    '''
        CREATE TABLE offer(
            id VARCHAR(40),
            seller_id VARCHAR(40),
            product_id VARCHAR(40),
            PRIMARY KEY (id),
            FOREIGN KEY (seller_id) REFERENCES seller(seller_id),
            FOREIGN KEY (product_id) REFERENCES product(product_id)
        ); 
    '''
    )
    for i,row in data.iterrows():
        sql = "INSERT IGNORE INTO CustomersDatabase1.customer VALUES (%s,%s,%s,%s)"
        mycursor.execute(sql, tuple([row['customer_unique_id'], row['customer_id'], row['customer_city'], row['customer_state']]))
        #print("Record inserted")
        mydb.commit()
    for i,row in data.iterrows():
        sql = "INSERT IGNORE INTO CustomersDatabase1.seller VALUES (%s,%s,%s)"
        mycursor.execute(sql, tuple([row['seller_id'], row['seller_city'], row['seller_state']]))
        #print("Record inserted")
        mydb.commit()
    for i,row in data.iterrows():
        sql = "INSERT IGNORE INTO CustomersDatabase1.product VALUES (%s,%s,%s,%s,%s)"
        mycursor.execute(sql, tuple([row['product_id'], row['price'], row['freight_value'], row['product_category_name'], row['product_description_lenght']]))
        #print("Record inserted")
        mydb.commit()
        
    for i,row in data.iterrows():
        sql = "INSERT IGNORE INTO CustomersDatabase1.order_ VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        mycursor.execute(sql, tuple([row['order_id'], row['order_status'], row['order_purchase_timestamp'], row['order_approved_at'], row['order_delivered_carrier_date'], row['order_delivered_customer_date'], row['order_estimated_delivery_date'], row['payment_type'], row['payment_value'], row['payment_installments'], row['customer_unique_id'], row['product_id'] , row['seller_id']]))
        #print("Record inserted")
        mydb.commit()
    df2 = data.loc[:,['product_id','seller_id']]
    df2.drop_duplicates(inplace = True)
    c = 0
    for i,row in df2.iterrows():
        sql = "INSERT IGNORE INTO CustomersDatabase1.offer VALUES (%s,%s,%s)"
        mycursor.execute(sql, tuple([c, row['seller_id'], row['product_id']]))
        c+=1
        #print("Record inserted")
        mydb.commit()
    




    print("I loaded the dataset and built the database!\n")

def query1():
    import mysql.connector as mysql
    from mysql.connector import Error
    mydb = mysql.connect(host='localhost', user='root', password='Verazzano1', auth_plugin='mysql_native_password') # you can add the auth_plugin here too (ref line 26)
    mycursor = mydb.cursor()
    mycursor.execute("USE CustomersDatabase1")
    sql = (mycursor.execute(
    '''
        select product_category_name,count(product_category_name) from product
        GROUP BY
        product_category_name
        ORDER BY count(product_category_name) desc
    '''
    ))
    mycursor.execute(sql)
    result = mycursor.fetchall()
    print(result)
def query2():
    import mysql.connector as mysql
    from mysql.connector import Error
    mydb = mysql.connect(host='localhost', user='root', password='Verazzano1', auth_plugin='mysql_native_password') # you can add the auth_plugin here too (ref line 26)
    mycursor = mydb.cursor()
    mycursor.execute("USE CustomersDatabase1")
    sql = (mycursor.execute(
    '''
        select count(c.customer_city), c.customer_city from customer as c, order_ as o
        where c.customer_unique_id = o.order_customer
        group by c.customer_city
        order by count(c.customer_city) DESC;
    '''
    ))
    mycursor.execute(sql)
    result = mycursor.fetchall()
    print(result)
def query3():
    """inser sql code"""
def query4():
    """inser sql code"""
def query5():
    """inser sql code"""

if __name__=="__main__":
    print("Welcome to our project!\n")
    load_data("customer_segmentation.csv")

    valid_choices=['insert ','quit']

    while True:
        choice=input('''\n\nChoose query to execute bla bla bla''')

        if choice not in valid_choices:
            print(f"Your choice '{choice}' is not valid. Please insert a valid option")
            continue
        if choice=="quit":
            break
        print(f"\nYou chose to execute query {choice}")
        if choice == "":
            continue
        elif choice == "":
            continue
        elif choice == "":
            continue
        elif choice == "":
            continue
        elif choice == "":
            continue
        else:
            raise Exception("Something went really wrong")
    print("\nGoodbye!\n")
