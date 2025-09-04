# generation information customer
# table banking_dw.dim_customer
#     customer_key SERIAL PRIMARY KEY,
#     customer_id VARCHAR(50) NOT NULL,
#     customer_name VARCHAR(200) NOT NULL,
#     customer_type VARCHAR(50) NOT NULL CHECK (customer_type IN ('Cá nhân', 'Doanh nghiệp', 'Tổ chức')),
#     gender VARCHAR(10) CHECK (gender IN ('Nam', 'Nữ', 'Khác')),
#     birth_date DATE CHECK (birth_date > '1900-01-01' AND birth_date < CURRENT_DATE),
#     phone VARCHAR(20),
#     email VARCHAR(100) CHECK (email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
#     address VARCHAR(500),
#     city VARCHAR(100) NOT NULL,
#     district VARCHAR(100) NOT NULL,
#     province VARCHAR(100) NOT NULL,
#     occupation VARCHAR(100),
#     income_level VARCHAR(50),
#     account_open_date DATE NOT NULL,
#     kyc_status VARCHAR(20) NOT NULL CHECK (kyc_status IN ('Đã xác thực', 'Chưa xác thực', 'Hết hạn')),
#     risk_profile VARCHAR(20) NOT NULL CHECK (risk_profile IN ('Thấp', 'Trung bình', 'Cao')),
#     scd_start_date DATE NOT NULL,
#     scd_end_date DATE,
#     is_current BOOLEAN NOT NULL DEFAULT TRUE,
#     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

import random
from faker import Faker
import psycopg2
from dotenv import load_dotenv
import os

class CustomerGenerator:
    CUSTOMER_TYPES = ["Cá nhân", "Doanh nghiệp", "Tổ chức"]
    GENDERS = ["Nam", "Nữ", "Khác"]
    INCOME_LEVELS = ["Thấp", "Trung bình", "Cao"]
    KYC_STATUS = ["Đã xác thực", "Chưa xác thực", "Hết hạn"]
    RISK_PROFILES = ["Thấp", "Trung bình", "Cao"]

    def __init__(self, locale="vi_VN"):
        self.fake = Faker(locale)

    def generate_customer(self):
        customer_type = random.choice(self.CUSTOMER_TYPES)
        gender = random.choice(self.GENDERS)
        name = self.fake.name_male() if gender == "Nam" else self.fake.name_female()
        if customer_type != "Cá nhân":
            name = f"Công ty {self.fake.company()}"

        return {
            "customer_id": self.fake.uuid4()[:8],
            "customer_name": name,
            "customer_type": customer_type,
            "gender": gender,
            "birth_date": self.fake.date_of_birth(minimum_age=18, maximum_age=70) if customer_type == "Cá nhân" else None,
            "phone": self.fake.phone_number(),
            "email": self.fake.email(),
            "address": self.fake.address().replace("\n", " "),
            "city": self.fake.city(),
            "district": self.fake.street_name(),
            "province": self.fake.state(),
            "occupation": self.fake.job() if customer_type == "Cá nhân" else "Kinh doanh",
            "income_level": random.choice(self.INCOME_LEVELS),
            "account_open_date": self.fake.date_between(start_date="-5y", end_date="today"),
            "kyc_status": random.choice(self.KYC_STATUS),
            "risk_profile": random.choice(self.RISK_PROFILES),
            "scd_start_date": self.fake.date_between(start_date="-2y", end_date="today"),
            "scd_end_date": None,
            "is_current": True
        }


class CustomerDBWriter:
    def __init__(self, conn_params):
        self.conn_params = conn_params

    def insert_customer(self, customer):
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO banking_dw.dim_customer (
                customer_id, customer_name, customer_type, gender, birth_date,
                phone, email, address, city, district, province, occupation,
                income_level, account_open_date, kyc_status, risk_profile,
                scd_start_date, scd_end_date, is_current
            )
            VALUES (
                %(customer_id)s, %(customer_name)s, %(customer_type)s, %(gender)s, %(birth_date)s,
                %(phone)s, %(email)s, %(address)s, %(city)s, %(district)s, %(province)s, %(occupation)s,
                %(income_level)s, %(account_open_date)s, %(kyc_status)s, %(risk_profile)s,
                %(scd_start_date)s, %(scd_end_date)s, %(is_current)s
            )
        """, customer)
        conn.commit()
        cur.close()
        conn.close()
        
    def reset_db (self):
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE banking_dw.dim_customer RESTART IDENTITY CASCADE;")
        conn.commit()
        cur.close()
        conn.close()


if __name__ == "__main__":
    
    load_dotenv()
    
    conn_params = {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS")
    }

    generator = CustomerGenerator()
    writer = CustomerDBWriter(conn_params)
    
    
    for _ in range(10):
        cust = generator.generate_customer()
        writer.insert_customer(cust)
        
    #writer.reset_db()
    
