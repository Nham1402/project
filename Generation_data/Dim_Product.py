#  ### DIM_PRODUCT - Product Dimension
# **Mô tả**: Danh mục sản phẩm và dịch vụ ngân hàng

# | Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
# |------------|-----------|--------------------------|----------------------|----------------|---------|
# | product_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 567 |
# | product_id | string | Mã sản phẩm từ hệ thống nguồn | Product ID from source system | NOT NULL, UNIQUE | PRD_SAVINGS_001 |
# | product_name | string | Tên sản phẩm | Product name | NOT NULL, Max 200 chars | Tài khoản tiết kiệm VIP |
# | product_type | string | Loại sản phẩm chính | Main product type | NOT NULL | Tiết kiệm, Vay, Thẻ, Đầu tư |
# | product_category | string | Danh mục sản phẩm | Product category | NOT NULL | Tài khoản thanh toán, Tín dụng |
# | product_group | string | Nhóm sản phẩm | Product group | NOT NULL | Bán lẻ, Doanh nghiệp |
# | interest_rate | decimal | Lãi suất (%/năm) | Annual interest rate | >= 0, <= 100 | 5.5 |
# | fee_structure | decimal | Cơ cấu phí | Fee structure | >= 0 | 50000 |
# | currency | string | Loại tiền tệ | Currency code | ISO 4217 format | VND, USD, EUR |
# | terms_conditions | string | Điều khoản và điều kiện | Terms and conditions | Max 1000 chars | Lãi suất có thể thay đổi theo quy định |
# | is_active | boolean | Trạng thái hoạt động | Active status | NOT NULL | true |
# | effective_date | date | Ngày hiệu lực | Effective date | NOT NULL | 2020-01-01 |
# | expiry_date | date | Ngày hết hạn | Expiry date | NULL if no expiry | 2025-12-31 |


import random
import uuid
from faker import Faker
from datetime import date
import psycopg2
from dotenv import load_dotenv
import os

class GeneratorProduct:
    def __init__(self, locale="vi_VN"):
        self.fake = Faker(locale)
        self.product_types = ["Tiết kiệm", "Vay", "Thẻ", "Đầu tư"]
        self.product_categories = [
            "Tài khoản thanh toán", "Tài khoản tiết kiệm",
            "Tín dụng", "Thẻ ghi nợ", "Thẻ tín dụng", "Chứng chỉ quỹ"
        ]
        self.product_groups = ["Bán lẻ", "Doanh nghiệp"]
        self.currencies = ["VND", "USD", "EUR"]

    def generate_record(self):
        """Sinh 1 record DIM_PRODUCT theo business rule"""
        p_type = random.choice(self.product_types)
        p_category = random.choice(self.product_categories)
        p_group = random.choice(self.product_groups)

        effective_date = self.fake.date_between(start_date="-5y", end_date="today")
        expiry_date = None if random.random() < 0.5 else self.fake.date_between(start_date="today", end_date="+5y")
        is_active = True if expiry_date is None or expiry_date >= date.today() else False

        return {
            "product_id": f"PRD_{uuid.uuid4().hex[:8].upper()}",
            "product_name": f"{p_type} {self.fake.word().capitalize()} {random.randint(100,999)}",
            "product_type": p_type,
            "product_category": p_category,
            "product_group": p_group,
            "interest_rate": round(random.uniform(0, 15), 2) if p_type in ["Tiết kiệm", "Vay"] else 0.0,
            "fee_structure": random.choice([0, 20000, 50000, 100000]),
            "currency": random.choice(self.currencies),
            "terms_conditions": self.fake.sentence(nb_words=12),
            "is_active": is_active,
            "effective_date": effective_date,
            "expiry_date": expiry_date
        }

class ProductDBWriter:
    def __init__(self, conn_params):
        self.conn_params = conn_params
    
    def insert_product(self, product):
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO banking_dw.dim_product (
                product_id,
                product_name,
                product_type,
                product_category,
                product_group,
                interest_rate,
                fee_structure,
                currency,
                terms_conditions,
                is_active,
                effective_date,
                expiry_date
            )
            VALUES (
                %(product_id)s,
                %(product_name)s,
                %(product_type)s,
                %(product_category)s,
                %(product_group)s,
                %(interest_rate)s,
                %(fee_structure)s,
                %(currency)s,
                %(terms_conditions)s,
                %(is_active)s,
                %(effective_date)s,
                %(expiry_date)s
            )
            """,
            product 
        )
        conn.commit()
        cur.close()
        conn.close()
        
    def reset_db (self):
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE banking_dw.dim_product RESTART IDENTITY CASCADE;")
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
        
    fake = GeneratorProduct()
    writer = ProductDBWriter(conn_params)
    
    for _ in range(10):
        product = fake.generate_record()
        writer.insert_product(product)
        print(product)
