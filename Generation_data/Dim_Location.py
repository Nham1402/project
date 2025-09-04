### DIM_LOCATION - Location Dimension
# **Mô tả**: Thông tin chi nhánh và địa điểm giao dịch

# | Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
# |------------|-----------|--------------------------|----------------------|----------------|---------|
# | location_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 789 |
# | location_id | string | Mã địa điểm | Location ID | NOT NULL, UNIQUE | LOC_HCM_001 |
# | branch_code | string | Mã chi nhánh | Branch code | NOT NULL | BR001 |
# | branch_name | string | Tên chi nhánh | Branch name | NOT NULL, Max 200 chars | Chi nhánh Nguyễn Huệ |
# | branch_type | string | Loại chi nhánh | Branch type | NOT NULL | Chi nhánh chính, Chi nhánh phụ, ATM |
# | address | string | Địa chỉ chi tiết | Detailed address | NOT NULL, Max 500 chars | 123 Nguyễn Huệ, Q1, TPHCM |
# | city | string | Thành phố | City | NOT NULL | Hồ Chí Minh |
# | district | string | Quận/Huyện | District | NOT NULL | Quận 1 |
# | province | string | Tỉnh/Thành phố | Province | NOT NULL | TP. Hồ Chí Minh |
# | region | string | Vùng miền | Region | NOT NULL | Miền Nam |
# | country | string | Quốc gia | Country | NOT NULL | Việt Nam |
# | timezone | string | Múi giờ | Timezone | Standard timezone format | Asia/Ho_Chi_Minh |
# | is_active | boolean | Trạng thái hoạt động | Active status | NOT NULL | true |


from faker import Faker
from datetime import datetime, timedelta
import random
import uuid
import psycopg2

class GeneratorLocation:
    def __init__(self, locale ="vi_VN"):
        self.fake = Faker(locale)
        self.location_id =["HCM","HN","DN","TB","HP"]
        self.region =["Miền Nam","Miền Bắc","Miền Trung"]
        self.branch_type = ["Chi nhánh chính","Chi nhánh phụ","ATM"]
        self.country ="Việt Nam"
        self.is_active =["True","False"]
    
    def Generator_record (self):
        location_id = random.choice(self.location_id)
        
        if location_id =="HCM":
            city = "Hồ Chí Minh"
        elif location_id =="DN":
            city = "Đà Nẵng"
        elif location_id =="HP":
            city = "Hải Phòng"
        elif location_id =="TB":
            city = "Thái Bình"
        else:
            city = "Hà Nội"
    
        return {
            "location_id": f"LOC_{location_id}_{uuid.uuid4().hex[:3].upper()}",
            "branch_code": f"BR{uuid.uuid4().hex[:3]}",
            "branch_name": f"Chi Nhánh {self.fake.city()}",
            "branch_type": random.choice(self.branch_type),
            "address": f"{self.fake.street_name()} , {city}",
            "city": f"{city} City",
            "region": random.choice(self.region),
            "district": city,
            "province": city,
            "country": self.country,
            "timezone": "Asia/Ho_Chi_Minh",
                 
        }
        
class LocationWiter:
    def __init__(self , conn_params):
        self.conn_params = conn_params
        
    def insert_location(self,location):
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()
        cur.execute(
        """
            INSERT INTO banking_dw.dim_location (
                location_id 
                ,branch_code
                ,branch_name
                ,branch_type 
                ,address 
                ,city
                ,district 
                ,province 
                ,region 
                ,country 
                ,timezone 
            )
            VALUES(
               %(location_id)s,
                %(branch_code)s,
                %(branch_name)s,
                %(branch_type)s,
                %(address)s,
                %(city)s,
                %(district)s,
                %(province)s,
                %(region)s,
                %(country)s,
                %(timezone)s
            )
        """,location
        )
        conn.commit()
        cur.close()
        conn.close()
    
    def reset_db(self):
        conn = psycopg2.connect(**self.conn_params)
        cur = conn.cursor()
        cur.execute(""" 
            TRUNCATE TABLE banking_dw.dim_location RESTART IDENTITY CASCADE;
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("reset database success!")
                   
if __name__ == "__main__":
    # config ket noi den db 
    con_params ={
        "host":"192.168.235.136",
        "port":"5432",
        "dbname":"dwh",
        "user":"nhamnn",
        "password":"Nham1402"
    }
    
    #gen du lieu 
    generator = GeneratorLocation()
    
    write = LocationWiter(con_params)
    #write.reset_db()
    for _ in range(10):
        location = generator.Generator_record()
        write.insert_location(location)
        
        print(location)


