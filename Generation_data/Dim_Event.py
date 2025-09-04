# | Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
# |------------|-----------|--------------------------|----------------------|----------------|---------|
# | event_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 101 |
# | event_id | string | Mã sự kiện | Event ID | NOT NULL, UNIQUE | EVT_TRANSFER_001 |
# | event_type | string | Loại sự kiện | Event type | NOT NULL | Chuyển khoản, Rút tiền, Nạp tiền |
# | event_category | string | Danh mục sự kiện | Event category | NOT NULL | Giao dịch tài chính, Thay đổi thông tin |
# | event_description | string | Mô tả sự kiện | Event description | Max 500 chars | Chuyển khoản nội bộ giữa các tài khoản |
# | event_source | string | Nguồn sự kiện | Event source | NOT NULL | Core Banking, ATM, Mobile App |
# | event_channel | string | Kênh giao dịch | Transaction channel | NOT NULL | Quầy, ATM, Internet Banking, Mobile |
# | requires_approval | boolean | Yêu cầu phê duyệt | Requires approval | NOT NULL | true |
# | risk_level | string | Mức độ rủi ro | Risk level | NOT NULL | Thấp, Trung bình, Cao |


from faker import Faker
import random
import uuid
import psycopg2

class GeneratorEvent:
    def __init__(self):
        self.fake = Faker("vi_VN")
        self.event_types = ["Chuyển khoản", "Rút tiền", "Nạp tiền", "Thanh toán hóa đơn", "Mở tài khoản", "Đóng tài khoản"]
        self.event_categories = ["Giao dịch tài chính", "Thay đổi thông tin",   "Yêu cầu dịch vụ"]
        self.event_sources = ["Core Banking", "ATM", "Mobile App", "Internet Banking", "Quầy giao dịch"]
        self.event_channels = ["Quầy", "ATM", "Internet Banking", "Mobile App"]
        self.risk_levels = ["Thấp", "Trung bình", "Cao"]
    
    def generate_record(self):
        event_type = random.choice(self.event_types)
        event_category = random.choice(self.event_categories)
        event_source = random.choice(self.event_sources)
        event_channel = random.choice(self.event_channels)
        requires_approval = random.choice([True, False])
        risk_level = random.choice(self.risk_levels)
        
        return {
            "event_id": f"EVT_{event_type[:3].upper()}_{uuid.uuid4().hex[:3].upper()}",
            "event_type": event_type,
            "event_category": event_category,
            "event_description": f"{event_type} qua {event_channel} từ nguồn {event_source}",
            "event_source": event_source,
            "event_channel": event_channel,
            "requires_approval": requires_approval,
            "risk_level": risk_level
        }
   
class EventWriter:
    def __init__(self, conn_params):
        self.conn_params = conn_params
        self.conn = psycopg2.connect(**conn_params)
        self.cur = self.conn.cursor()
    
    def reset_db(self):
        self.cur.execute("TRUNCATE TABLE banking_dw.dim_event RESTART IDENTITY CASCADE;")
        self.conn.commit()
    
    def insert_event(self, event):
        insert_query = """
        INSERT INTO banking_dw.dim_event (
            event_id, event_type, event_category, event_description,
            event_source, event_channel, requires_approval, risk_level
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        self.cur.execute(insert_query, (
            event["event_id"], event["event_type"], event["event_category"],
            event["event_description"], event["event_source"], event["event_channel"],
            event["requires_approval"], event["risk_level"]
        ))
        self.conn.commit()
    
    def close(self):
        self.cur.close()
        self.conn.close() 
    
if __name__ == "__main__":
    con_params ={
        "host":"192.168.235.136",
        "port":"5432",
        "dbname":"dwh",
        "user":"nhamnn",
        "password":"Nham1402"
    }
    generator = GeneratorEvent()
    writer = EventWriter(con_params)    
    writer.reset_db()
    for _ in range(10):
        event = generator.generate_record()
        writer.insert_event(event)
        print(event)