#%%
"""DuckDB is a fast in-process analytical database.
DuckDB supports a feature-rich SQL dialect complemented with deep integrations into client APIs: SQL, Python, R, Java, NodeJs
"""

import pandas as pd
import glob
import time
import duckdb


"""If you want to read an existing duckdb database. No update/insert
Use this syntaxe below:
conn = duckdb.connect("mydb.db", read_only=True)
"""
"""As we don't want to persist the data. We don't put a parameter"""


# %%
conn = duckdb.connect()
"""Let's see first, how we can read all the data inside 
the datase folder with pandas. And we will see the execution time.
we got: 0.39121484756469727 seconds"""
cur_time = time.time()
df = pd.concat([pd.read_csv(f) for f in glob.glob("dataset/*.csv")])
print(f"time: {time.time() - cur_time}")
print(df.head(1))

# %%
cur_time = time.time()

""" It will process all files as a single table. Amazing. The dataframe returned is a pandas DF
First run we got:  0.02091193199157715 seconds
"""
df = conn.execute("""
    SELECT *
    FROM 'dataset/*.csv'     
""").df()
print(f"time: {time.time() - cur_time}")
df.head(1)
conn.close()
# %%
# We can also use <with context> to create a connection
with duckdb.connect() as conn:
    conn.execute("""
    CREATE TABLE T1 AS SELECT * FROM 'dataset/Sales*.csv'
""")
    #csv
    conn.execute("""
            COPY (SELECT * FROM T1)
                TO 'dataset/result.csv'
                (FORMAT 'csv')
    """)
    #parquet
    conn.execute("""
            COPY (SELECT * FROM T1)
                TO 'dataset/result-snappy.parquet'
                (FORMAT 'parquet');
    """)
    #parquet PARTITION_BY
    conn.execute("""
            COPY (SELECT * FROM T1)
                TO 'dataset/sales_partition/'
                (FORMAT 'parquet', PARTITION_BY (City, 'Product Type'), OVERWRITE_OR_IGNORE 1);
    """)
    #%%
    # Casting is so easy
    
    with duckdb.connect() as conn:
        df = conn.execute("""SELECT * FROM read_parquet('dataset/*/*/*/*.parquet')""").df()
        conn.execute("""
            CREATE OR REPLACE TABLE sales AS
                SELECT
                    TRY_CAST("Order ID" AS INTEGER) AS order_id,
                    Product as product,
                    "Quantity Ordered"::INTEGER AS quantity,
                    TRY_CAST(Price AS DECIMAL) AS price,
                    TRY_CAST("Order Date" AS DATE) as order_date,
                    "Purchase Address" as purchase_address
                FROM df
                
                ;
        """)
        conn.execute("DESCRIBE sales")
        print("Printing the types of columns")
        columns_type = [(f[0],f[1]) for f in conn.fetchall()]
        print(f"Columns Type: {columns_type}")
        # Creating a view
        conn.execute("""
                CREATE OR REPLACE VIEW aggregate_view AS
                    SELECT
                        order_id,
                        count(1) AS nbr_orders,
                        month(order_date) AS month,
                        split(purchase_address,',')[2] AS city,
                        Product as product,
                        sum(quantity * price) as revenue
                    FROM sales
                    GROUP BY ALL;
        """)
        conn.execute("select * FROM aggregate_view").df()
        # Save the result in a parquet file
        conn.execute("""
                COPY (SELECT * FROM aggregate_view)
                    TO 'dataset/aggregate_view-snappy.parquet'
                    (FORMAT 'parquet');
        """)
        # Or save it by partitioning
        conn.execute("""
            COPY (SELECT * FROM aggregate_view)
                TO 'dataset/aggregate_partition/'
                (FORMAT 'parquet', PARTITION_BY (city, product), OVERWRITE_OR_IGNORE 1);
    """)
        

        
#Order ID,Product,Quantity Ordered,Price,Order Date,Time,Purchase Address,City,Product Type
