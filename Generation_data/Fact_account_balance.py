
### FACT_ACCOUNT_BALANCE - Account Balance Fact
# **Mô tả**: Số dư tài khoản theo thời gian (snapshot hàng ngày)

# | Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
# |------------|-----------|--------------------------|----------------------|----------------|---------|
# | balance_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 2001 |
# | customer_key | int | Khóa khách hàng | Customer foreign key | FK to DIM_CUSTOMER | 12345 |
# | product_key | int | Khóa sản phẩm | Product foreign key | FK to DIM_PRODUCT | 567 |
# | location_key | int | Khóa địa điểm | Location foreign key | FK to DIM_LOCATION | 789 |
# | balance_date_key | int | Khóa ngày số dư | Balance date key | FK to DIM_DATE | 20240315 |
# | balance_time_key | int | Khóa giờ số dư | Balance time key | FK to DIM_TIME | 235959 |
# | account_number | string | Số tài khoản | Account number | NOT NULL, UNIQUE per date | 1234567890 |
# | opening_balance | decimal | Số dư đầu ngày | Opening balance | Can be negative | 5000000 |
# | closing_balance | decimal | Số dư cuối ngày | Closing balance | NOT NULL | 4500000 |
# | available_balance | decimal | Số dư khả dụng | Available balance | <= closing_balance | 4300000 |
# | hold_balance | decimal | Số dư bị phong tỏa | Hold balance | >= 0 | 200000 |
# | pending_balance | decimal | Số dư chờ xử lý | Pending balance | >= 0 | 150000 |
# | overdraft_limit | decimal | Hạn mức thấu chi | Overdraft limit | >= 0 | 1000000 |
# | currency | string | Loại tiền tệ | Currency | ISO 4217, NOT NULL | VND |
# | usd_equivalent | decimal | Tương đương USD | USD equivalent | >= 0 | 184.93 |
# | transaction_count | int | Số lượng giao dịch trong ngày | Daily transaction count | >= 0 | 12 |
# | total_debits | decimal | Tổng số tiền ghi nợ | Total debit amount | >= 0 | 800000 |
# | total_credits | decimal | Tổng số tiền ghi có | Total credit amount | >= 0 | 300000 |
# | last_transaction_time | timestamp | Thời gian giao dịch cuối | Last transaction time | <= balance_date | 2024-03-15 18:45:30 |
# | account_status | string | Trạng thái tài khoản | Account status | NOT NULL | Hoạt động, Đóng, Tạm khóa |


class GeneratorAccountBalance:
    def __init__(self):
        pass
