from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import random
import uuid
import psycopg2
from sqlalchemy import create_engine
import os 
from dotenv import load_dotenv

class getInfoTransaction:
    def __init__(self, conn_params):
        self.engine = create_engine(
            f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}"
        )

    def _query(self, sql: str) -> pd.DataFrame:
        try:
            conn = self.engine.raw_connection()   
            try:
                df = pd.read_sql_query(sql, conn)
            finally:
                conn.close()  
            return df
        except Exception as e:  
            print(f"Error executing query: {e}")
            return pd.DataFrame()

    def getCustomer(self):
        df = self._query("SELECT customer_key FROM banking_dw.dim_customer")
        return df

    def getLocation(self):
        df = self._query("SELECT location_key   FROM banking_dw.dim_location")
        return df

    def getApplication(self):
        df = self._query("SELECT application_key FROM banking_dw.dim_application")
        return df

    def getAccount(self):
        df = self._query("SELECT account_key , account_number , customer_key , current_balance FROM banking_dw.dim_account")
        return df

class GenerationTranaction :
    def __init__(self , conn_params):
        self.conn_params = conn_params
        self.account = getInfoTransaction(conn_params).getAccount()
        self.location = getInfoTransaction(conn_params).getLocation()
        self.application = getInfoTransaction(conn_params).getApplication()
        self.transaction_types = ["Chuyển khoản", "Rút tiền", "Nạp tiền", "Nhận Tiền"]
        self.transaction_category = ["Nội bộ", "Liên ngân hàng", "Quốc tế"]
        self.transaction_status = ["Thành công", "Thất bại"]
        self.channel = ["ATM", "Mobile App", "Internet Banking"]
        
    def get_customer_key(self , account_key):
        df = self.account[self.account["account_key"] == account_key]
        if not df.empty:
            return df.iloc[0]["customer_key"]
        return None
    
    # tinh tien net nhan duoc khi thuc hien giao dich
    def amount(self ,transaction_types , transaction_status,  transaction_amount , fee_amount , tax_amount , ):
        if transaction_status == "Thất bại":
            return 0.0
        else:
            if transaction_types == "Rút tiền":
                return round(transaction_amount + fee_amount + tax_amount , 2)
            elif transaction_types == "Nạp tiền":
                return round(transaction_amount)
            elif transaction_types == "Chuyển khoản":  
                return  round(transaction_amount + fee_amount + tax_amount , 2)
            elif transaction_types == "Nhận Tiền":
                return round(transaction_amount)
        
        
    def transaction_account_number(self , transaction_category ):
        if transaction_category == "Nội bộ":
            return random.choice(self.account["account_number"])
        elif transaction_category == "Liên ngân hàng":
            return f"LB{random.randint(100000, 999999)}"
        elif transaction_category == "Quốc tế":
            return f"QT{random.randint(100000, 999999)}"
        
    def generator_data_transaction(self):
        
        account_key   =  int(random.choice(self.account["account_key"]))
        customer_key  =  int(self.get_customer_key(account_key))
        location_key  =  int(random.choice(self.location["location_key"]))
        event_key = uuid.uuid4().int >> 64
        application_key = int(random.choice(self.account["account_key"]))
        transaction_id = f"TXN{datetime.now().strftime('%Y%m%d%H%M%S')}{random.randint(100, 999)}"
        reference_number = f"REF{uuid.uuid4().hex[:10].upper()}"
        transaction_type = random.choice(self.transaction_types)
        transaction_category = random.choice(self.transaction_category)
        transaction_amount = round(random.uniform(10000, 10000000), 2)
        transaction_status = random.choice(self.transaction_status)
        fee_amount = round(transaction_amount * random.uniform(0.001, 0.01), 2)
        tax_amount = round(fee_amount * 0.1, 2)
        net_amount = self.amount(transaction_type,transaction_status,transaction_amount, fee_amount, tax_amount) 
        currency = "VND"
        account_number = self.transaction_account_number(transaction_category)

        channel = random.choice(self.channel)
        description = f"Giao dịch {transaction_type} qua {channel}"
        created_timestamp = datetime.now()
        processed_timestamp = created_timestamp + timedelta(seconds=random.randint(1, 300))
        updated_timestamp = processed_timestamp + timedelta(seconds=random.randint(1, 300))

        
        return {
            "account_key": account_key,
            "customer_key": customer_key,
            "location_key": location_key,
            "event_key": event_key,
            "application_key": application_key,
            "transaction_id": transaction_id,
            "reference_number": reference_number,
            "transaction_type": transaction_type,
            "transaction_category": transaction_category,
            "transaction_amount": transaction_amount,
            "fee_amount": fee_amount,
            "tax_amount": tax_amount,
            "net_amount": net_amount,
            "currency": currency,
            "account_number": account_number,
            "transaction_status": transaction_status,
            "channel": channel,
            "description": description,
            "created_timestamp": created_timestamp,
            "processed_timestamp": processed_timestamp,
            "updated_timestamp": updated_timestamp
        }
