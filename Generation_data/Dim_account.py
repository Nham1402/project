# account_key SERIAL PRIMARY KEY ,
#     account_id VARCHAR(30) UNIQUE NOT NULL,
#     account_number VARCHAR(50) NOT NULL,
#     account_type VARCHAR(30) NOT NULL, -- SAVINGS, CURRENT, FIXED_DEPOSIT, LOAN
#     customer_key varchar(50), 
#     currency_key varchar(50),
#     account_status VARCHAR(20) NOT NULL, -- ACTIVE, INACTIVE, CLOSED, SUSPENDED
#     account_condition VARCHAR(30), -- NORMAL, DORMANT, FROZEN, BLOCKED
#     account_opening_date DATE NOT NULL,
#     account_closing_date DATE,
#     last_transaction_date DATE,
#     opening_balance DECIMAL(18,4) DEFAULT 0,
#     current_balance DECIMAL(18,4),
#     available_balance DECIMAL(18,4),
#     minimum_balance DECIMAL(18,4) DEFAULT 0,
#     maximum_balance DECIMAL(18,4),
#     monthly_fee DECIMAL(10,4) DEFAULT 0,
#     annual_fee DECIMAL(10,4) DEFAULT 0,
#     channel_opened VARCHAR(20), -- BRANCH, ONLINE, MOBILE, PHONE
#     effective_date DATE NOT NULL,
#     expiry_date DATE DEFAULT '9999-12-31',
#     created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#     updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#     created_by VARCHAR(50),
#     updated_by VARCHAR(50)

import psycopg2
from faker import Faker
from datetime import datetime, timedelta
import random
import uuid
import pandas as pd


class GetCustomer:
    def __init__(self, conn_params):
        self.conn_params = conn_params

    def GetCustomerId(self):
        conn = psycopg2.connect(**self.conn_params)
        query = """
            SELECT c.customer_key
            FROM banking_dw.dim_customer c
            WHERE NOT EXISTS (
                SELECT 1 
                FROM banking_dw.dim_account a
                WHERE a.customer_key = c.customer_key
            )
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df["customer_key"].tolist()


class GeneratorAccount:
    def __init__(self, conn_params, locale="vi_VN"):
        self.fake = Faker(locale)
        self.conn_params = conn_params
        self.customers = GetCustomer(conn_params).GetCustomerId()

        # lookup values
        self.account_types = ["SAVINGS", "CURRENT", "LOAN", "FIXED_DEPOSIT"]
        self.account_statuses = ["ACTIVE", "INACTIVE", "CLOSED", "SUSPENDED"]
        self.account_conditions = ["NORMAL", "DORMANT", "FROZEN", "BLOCKED"]
        self.channels = ["BRANCH", "ONLINE", "MOBILE", "PHONE"]
        self.currencies = ["VND", "USD", "EUR"]

    def generator_record(self, customer_key, acc_type=None):
        opening_date = self.fake.date_between(start_date="-5y", end_date="today")
        last_txn_date = opening_date + timedelta(days=random.randint(30, 1000))
        if last_txn_date > datetime.today().date():
            last_txn_date = None

        return {
            "account_id": f"{uuid.uuid4().hex[:8]}",
            "account_number": f"{uuid.uuid4().hex[:13]}",
            "account_type": acc_type if acc_type else random.choice(self.account_types),
            "customer_key": customer_key,
            "currency_key": random.choice(self.currencies),
            "account_status": random.choice(self.account_statuses),
            "account_condition": random.choice(self.account_conditions),
            "account_opening_date": opening_date,
            "account_closing_date": None,
            "last_transaction_date": last_txn_date,
            "opening_balance": round(random.uniform(1_000_000, 50_000_000), 2),
            "current_balance": round(random.uniform(500_000, 70_000_000), 2),
            "available_balance": round(random.uniform(500_000, 70_000_000), 2),
            "minimum_balance": 50_000,
            "maximum_balance": 100_000_000,
            "monthly_fee": 5000,
            "annual_fee": 50_000,
            "channel_opened": random.choice(self.channels),
            "effective_date": opening_date,
            "expiry_date": datetime(9999, 12, 31).date(),
            "created_by": "system",
            "updated_by": "system",
        }

    def generate_accounts(self, allow_multi=False, max_accounts=2):
        accounts = []
        for cust in self.customers:
            if allow_multi:  
                # Mỗi customer có thể có từ 1 đến max_accounts account, không trùng account_type
                n_accounts = random.randint(1, max_accounts)
                acc_types = random.sample(self.account_types, n_accounts)
                for acc_type in acc_types:
                    accounts.append(self.generator_record(cust, acc_type))
            else:
                # Mỗi customer đúng 1 account
                accounts.append(self.generator_record(cust))
        return pd.DataFrame(accounts)

    def insert_to_db(self, df):
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()

        insert_query = """
            INSERT INTO banking_dw.dim_account (
                account_id, account_number, account_type, customer_key, currency_key,
                account_status, account_condition, account_opening_date, account_closing_date,
                last_transaction_date, opening_balance, current_balance, available_balance,
                minimum_balance, maximum_balance, monthly_fee, annual_fee, channel_opened,
                effective_date, expiry_date, created_by, updated_by
            )
            VALUES (
                %(account_id)s, %(account_number)s, %(account_type)s, %(customer_key)s, %(currency_key)s,
                %(account_status)s, %(account_condition)s, %(account_opening_date)s, %(account_closing_date)s,
                %(last_transaction_date)s, %(opening_balance)s, %(current_balance)s, %(available_balance)s,
                %(minimum_balance)s, %(maximum_balance)s, %(monthly_fee)s, %(annual_fee)s, %(channel_opened)s,
                %(effective_date)s, %(expiry_date)s, %(created_by)s, %(updated_by)s
            )
        """

        data = df.to_dict(orient="records")
        cur.executemany(insert_query, data)
        conn.commit()
        cur.close()
        conn.close()
        print(f"Inserted {len(df)} accounts into dim_account")


if __name__ == "__main__":
    conn_params = {
        "host": "192.168.235.136",
        "port": 5432,
        "dbname": "dwh",
        "user": "nhamnn",
        "password": "Nham1402"
    }

    gen = GeneratorAccount(conn_params)
    
    #cho phép mỗi customer có nhiều account, nhưng không trùng loại account_type
    df_accounts_multi = gen.generate_accounts(allow_multi=True, max_accounts=3)
    print(df_accounts_multi.head(10))

    # Insert vào DB 
    gen.insert_to_db(df_accounts_multi)
