import pandas as pd
import sqlite3

conn = sqlite3.connect('shipping_data.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS shipping_data (
    product_name TEXT,
    quantity INTEGER,
    origin TEXT,
    destination TEXT
)
''')

def process_spreadsheet_0():
    df0 = pd.read_csv('shipping_data_0.csv')
    for index, row in df0.iterrows():
        cursor.execute('''
            INSERT INTO shipping_data (product_name, quantity, origin, destination)
            VALUES (?, ?, ?, ?)
        ''', (row['product_name'], row['quantity'], row['origin'], row['destination']))
    print("Inserted data from shipping_data_0.csv into the database.")

def process_spreadsheet_1_2():
    df1 = pd.read_csv('shipping_data_1.csv')
    df2 = pd.read_csv('shipping_data_2.csv')

    df1.columns = df1.columns.str.strip().str.lower()
    df2.columns = df2.columns.str.strip().str.lower()

    merged_df = pd.merge(df1, df2, on='shipping_identifier')

    for index, row in merged_df.iterrows():
        cursor.execute('''
            INSERT INTO shipping_data (product_name, quantity, origin, destination)
            VALUES (?, ?, ?, ?)
        ''', (row['product_name'], row['quantity'], row['origin'], row['destination']))
    print("Inserted data from shipping_data_1.csv and shipping_data_2.csv into the database.")

if __name__ == "__main__":
    process_spreadsheet_0()
    process_spreadsheet_1_2()
    conn.commit()
    conn.close()
    print("All data has been inserted and the connection has been closed.")
