#the database ofcourse


from sqlalchemy import create_engine
import pandas as pd

table_schemas = {
    "customers": [
        "customer_id",
        "name",
        "country",
        "state",
        "city",
        "region",
        "customer_type"
    ],

    "products": [
        "product_id",
        "product_name",
        "category",
        "sub_category"
    ],

    "orders": [
        "order_id",
        "order_date",
        "ship_date",
        "ship_mode"
    ],

    "transactions": [
        "order_id",
        "product_id",
        "customer_id",
        "sales",
        "quantity",
        "discount",
        "profit"
    ],

    "daily_revenue_kpis": [
        "order_date",
        "total_sales",
        "items_sold",
        "day_profit",
        "discount_provided",
        "average_sold_value", 
        "customer_id"
    ]
}

#create and return a database connection
def get_engine():
    engine = create_engine(
        "sqlite:///auto_bi.db",
        echo=False
    )
    return engine


#saving the df to database
def save_dataframe(df, table_name, if_exists="append"):
    if df.empty:
        raise ValueError("df is empty")
    
    engine = get_engine()

    df.to_sql(
        name = table_name,
        con = engine,
        if_exists = if_exists,
        index = False
    )

    print(f"{table_name} saved in db")
    

def load_dataframe(table_name):
    engine = get_engine()
    if table_exists(table_name):
        db = pd.read_sql_table(table_name, con=engine)
        return db
    else:
        raise ValueError(f"{table_name} does not exist")


def table_exists(table_name):
    engine = get_engine()

    from sqlalchemy import inspect

    inspector = inspect(engine)
    tables = inspector.get_table_names()

    if table_name in tables:
        return True
    else:
        return False


def create_tables():

    engine = get_engine()

    for table_name, columns in table_schemas.items():
        if table_exists(table_name):
            continue

        db = pd.DataFrame(columns=columns)

        db.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False
        )

        print(f"{table_name} created")
    

if __name__ == "__main__":

    import pandas as pd

    print("🚀 Starting DB layer test...\n")

    # 1️⃣ Create all tables from schema registry
    print("📦 Creating tables...")
    create_tables()

    # 2️⃣ Verify tables exist
    print("\n🔍 Verifying tables:")
    for table in table_schemas.keys():
        print(f" - {table}: {table_exists(table)}")

    # 3️⃣ Create a sample DataFrame to test save/load
    sample_df = pd.DataFrame({
           "order_date": [
        "2024-01-01","2024-01-02","2024-01-03","2024-01-04","2024-01-05",
        "2024-01-06","2024-01-07","2024-01-08","2024-01-09","2024-01-10",
        "2024-01-11","2024-01-12","2024-01-13","2024-01-14","2024-01-15"
    ],

    "total_sales": [
        12000,15000,9800,21000,17500,
        14200,16000,8900,13400,22000,
        19500,12500,16800,14300,15800
    ],

    "items_sold": [
        45,60,38,72,64,
        50,58,30,49,80,
        66,44,59,52,55
    ],

    "day_profit": [
        3200,4100,2400,6200,5100,
        3900,4200,1800,3500,7000,
        5800,3100,4600,3700,4300
    ],

    "discount_provided": [
        300,450,200,600,550,
        350,400,150,280,750,
        620,310,480,360,390
    ],

    "average_sold_value": [
        266.6,250.0,257.8,291.6,273.4,
        284.0,275.8,296.6,273.4,275.0,
        295.4,284.0,284.7,275.0,287.2
    ],

    # FK → customer_id from customer table
    "customer_id": [
        101,102,103,104,105,
        106,107,108,109,110,
        111,112,113,114,115
    ]
    })

    sample_customer_df = pd.DataFrame({
            "customer_id": [101,102,103,104,105,106,107,108,109,110,111,112,113,114,115],

    "name": [
        "Punya","Shree","Aarav","Diya","Rohan",
        "Meera","Kabir","Isha","Aditya","Neha",
        "Karan","Sneha","Rahul","Ananya","Vikram"
    ],

    "country": [
        "India","India","India","India","India",
        "India","India","India","India","India",
        "India","India","India","India","India"
    ],

    "state": [
        "Karnataka","Karnataka","Maharashtra","Delhi","Tamil Nadu",
        "Kerala","Gujarat","Punjab","Rajasthan","West Bengal",
        "Haryana","Bihar","Telangana","UP","MP"
    ],

    "city": [
        "Bangalore","Mysore","Mumbai","Delhi","Chennai",
        "Kochi","Ahmedabad","Amritsar","Jaipur","Kolkata",
        "Gurgaon","Patna","Hyderabad","Lucknow","Indore"
    ],

    "region": [
        "South","South","West","North","South",
        "South","West","North","West","East",
        "North","East","South","North","Central"
    ],

    "customer_type": [
        "New","Returning","Returning","New","Returning",
        "Returning","New","Returning","New","Returning",
        "Returning","New","Returning","New","Returning"
    ]
    })

    # 4️⃣ Save DataFrame
    print("\n💾 Saving sample data to daily_revenue_kpis...")
    save_dataframe(
        df=sample_df,
        table_name="daily_revenue_kpis",
        if_exists="append"
    )

    save_dataframe(
        df=sample_customer_df,
        table_name="customers",
        if_exists="append"
    )

    print("\n📤 Loading data from daily_revenue_kpis...")
    loaded_df = load_dataframe("daily_revenue_kpis")

    print("\n✅ Loaded DataFrame:")
    print(loaded_df.head())

    print("\n🎉 DB layer test completed successfully!")
