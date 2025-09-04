### DIM_APPLICATION - Application Dimension
# **Mô tả**: Ứng dụng và hệ thống thực hiện giao dịch

# | Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
# |------------|-----------|--------------------------|----------------------|----------------|---------|
# | application_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 456 |
# | application_id | string | Mã ứng dụng | Application ID | NOT NULL, UNIQUE | APP_CORE_001 |
# | application_name | string | Tên ứng dụng | Application name | NOT NULL, Max 200 chars | Core Banking System |
# | application_type | string | Loại ứng dụng | Application type | NOT NULL | Core System, Mobile App, Web Portal |
# | version | string | Phiên bản | Version | NOT NULL | 2.1.5 |
# | vendor | string | Nhà cung cấp | Vendor | Max 100 chars | FIS, Temenos, Oracle |
# | environment | string | Môi trường | Environment | NOT NULL | Production, UAT, Development |
# | is_active | boolean | Trạng thái hoạt động | Active status | NOT NULL | true |

import random
from faker import Faker
import uuid
import datetime
import psycopg2
import os
from dotenv import load_dotenv



class GeneratorApplication :
    def __init__(self , locale ="vi_VN"):
        self.fake = Faker(locale)
        self.application_name= ["Core Banking","Mobile Banking","ATM","Credit Card","Debit Card","E-Wallet","Internet Banking"]
        
    def Application_record (self):
        application_name = random.choice(self.application_name)
        # application_id
        if application_name =="Core Banking": 
            application_id = "CORE"
        elif application_name =="Mobile Banking":
            application_id ="MOBILE"
        elif application_name == "ATM":
            application_id ="ATM"
        elif application_name in("Credit Card","Debit Card","E-Wallet") :
            application_id ="CARD"
        else:
            application_id="INTERNET"
            
        return {
            "application_id":f"APP_{application_id}_{uuid.uuid4().hex[:3].upper()}",
            "application_name": application_name,
            "application_type": application_id,
            "version": f"{random.randint(1,10)}.{random.randint(1,10)}.{random.randint(1,10)}",
            "vendor": None,
            "environment": "Production",
            "is_active": True
        }
        
class ApplicationWriter:
    
    def __init__(self , conn_params):
        self.conn_params = conn_params
    
    def insert_application(self , application):
        
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO banking_dw.dim_application (
                application_id 
                ,application_name 
                ,application_type 
                ,version 
                ,vendor 
                ,environment 
                ,is_active
            )
            VALUES(
                %(application_id)s
                ,%(application_name)s
                ,%(application_type)s
                ,%(version)s 
                ,%(vendor)s 
                ,%(environment)s 
                ,%(is_active)s
            )
            """, application
        )
        conn.commit()
        cur.close()
        conn.close()

if __name__ =="__main__":
    load_dotenv()
    
    conn_params = {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS")
    }
    
    fake = GeneratorApplication()
    write = ApplicationWriter(conn_params)
    
    for _ in range(10):
        record = fake.Application_record()
        write.insert_application(record)
    print("insert_to_db_sucess")        
