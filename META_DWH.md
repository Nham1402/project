# Banking Data Warehouse - Metadata Dictionary

## Table of Contents
1. [Dimension Tables](#dimension-tables)
2. [Fact Tables](#fact-tables)
3. [Bridge Tables](#bridge-tables)
4. [Aggregate Tables](#aggregate-tables)
5. [Data Quality Rules](#data-quality-rules)
6. [Business Rules](#business-rules)

---

## Dimension Tables

### DIM_CUSTOMER - Customer Dimension
**Mô tả**: Chứa thông tin chi tiết về khách hàng của ngân hàng với lịch sử thay đổi (SCD Type 2)

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| customer_key | int | Khóa chính surrogate, tự động tăng | Primary surrogate key, auto-increment | PK, NOT NULL, UNIQUE | 12345 |
| customer_id | string | Mã khách hàng duy nhất từ hệ thống nguồn | Unique customer ID from source system | NOT NULL, UNIQUE per SCD | CUS001234567 |
| customer_name | string | Họ và tên đầy đủ của khách hàng | Full name of customer | NOT NULL, Max 200 chars | Nguyễn Văn An |
| customer_type | string | Loại khách hàng | Customer type classification | NOT NULL | Cá nhân, Doanh nghiệp, Tổ chức |
| gender | string | Giới tính | Gender | NULL for corporate | Nam, Nữ, Khác |
| birth_date | date | Ngày sinh | Date of birth | Must be > 1900 and < today | 1985-03-15 |
| phone | string | Số điện thoại chính | Primary phone number | Format validation | +84901234567 |
| email | string | Địa chỉ email | Email address | Email format validation | nguyen.van.a@email.com |
| address | string | Địa chỉ chi tiết | Detailed address | Max 500 chars | 123 Nguyễn Huệ, P. Bến Nghé |
| city | string | Thành phố | City | NOT NULL | Hồ Chí Minh |
| district | string | Quận/Huyện | District | NOT NULL | Quận 1 |
| province | string | Tỉnh/Thành phố | Province/State | NOT NULL | TP. Hồ Chí Minh |
| occupation | string | Nghề nghiệp | Occupation | Max 100 chars | Kỹ sư phần mềm |
| income_level | string | Mức thu nhập | Income level bracket | Predefined ranges | 10-20 triệu/tháng |
| account_open_date | date | Ngày mở tài khoản đầu tiên | First account opening date | NOT NULL | 2020-01-15 |
| kyc_status | string | Trạng thái xác thực danh tính | KYC verification status | NOT NULL | Đã xác thực, Chưa xác thực, Hết hạn |
| risk_profile | string | Hồ sơ rủi ro khách hàng | Customer risk profile | NOT NULL | Thấp, Trung bình, Cao |
| scd_start_date | date | Ngày bắt đầu hiệu lực bản ghi | Record effective start date | NOT NULL | 2020-01-15 |
| scd_end_date | date | Ngày kết thúc hiệu lực bản ghi | Record effective end date | NULL for current | 2021-06-30 |
| is_current | boolean | Bản ghi hiện tại | Current record indicator | NOT NULL, Only one TRUE per customer_id | true |

### DIM_PRODUCT - Product Dimension
**Mô tả**: Danh mục sản phẩm và dịch vụ ngân hàng

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| product_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 567 |
| product_id | string | Mã sản phẩm từ hệ thống nguồn | Product ID from source system | NOT NULL, UNIQUE | PRD_SAVINGS_001 |
| product_name | string | Tên sản phẩm | Product name | NOT NULL, Max 200 chars | Tài khoản tiết kiệm VIP |
| product_type | string | Loại sản phẩm chính | Main product type | NOT NULL | Tiết kiệm, Vay, Thẻ, Đầu tư |
| product_category | string | Danh mục sản phẩm | Product category | NOT NULL | Tài khoản thanh toán, Tín dụng |
| product_group | string | Nhóm sản phẩm | Product group | NOT NULL | Bán lẻ, Doanh nghiệp |
| interest_rate | decimal | Lãi suất (%/năm) | Annual interest rate | >= 0, <= 100 | 5.5 |
| fee_structure | decimal | Cơ cấu phí | Fee structure | >= 0 | 50000 |
| currency | string | Loại tiền tệ | Currency code | ISO 4217 format | VND, USD, EUR |
| terms_conditions | string | Điều khoản và điều kiện | Terms and conditions | Max 1000 chars | Lãi suất có thể thay đổi theo quy định |
| is_active | boolean | Trạng thái hoạt động | Active status | NOT NULL | true |
| effective_date | date | Ngày hiệu lực | Effective date | NOT NULL | 2020-01-01 |
| expiry_date | date | Ngày hết hạn | Expiry date | NULL if no expiry | 2025-12-31 |

### DIM_LOCATION - Location Dimension
**Mô tả**: Thông tin chi nhánh và địa điểm giao dịch

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| location_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 789 |
| location_id | string | Mã địa điểm | Location ID | NOT NULL, UNIQUE | LOC_HCM_001 |
| branch_code | string | Mã chi nhánh | Branch code | NOT NULL | BR001 |
| branch_name | string | Tên chi nhánh | Branch name | NOT NULL, Max 200 chars | Chi nhánh Nguyễn Huệ |
| branch_type | string | Loại chi nhánh | Branch type | NOT NULL | Chi nhánh chính, Chi nhánh phụ, ATM |
| address | string | Địa chỉ chi tiết | Detailed address | NOT NULL, Max 500 chars | 123 Nguyễn Huệ, Q1, TPHCM |
| city | string | Thành phố | City | NOT NULL | Hồ Chí Minh |
| district | string | Quận/Huyện | District | NOT NULL | Quận 1 |
| province | string | Tỉnh/Thành phố | Province | NOT NULL | TP. Hồ Chí Minh |
| region | string | Vùng miền | Region | NOT NULL | Miền Nam |
| country | string | Quốc gia | Country | NOT NULL | Việt Nam |
| timezone | string | Múi giờ | Timezone | Standard timezone format | Asia/Ho_Chi_Minh |
| is_active | boolean | Trạng thái hoạt động | Active status | NOT NULL | true |

### DIM_EVENT - Event Dimension
**Mô tả**: Loại sự kiện và giao dịch trong hệ thống

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| event_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 101 |
| event_id | string | Mã sự kiện | Event ID | NOT NULL, UNIQUE | EVT_TRANSFER_001 |
| event_type | string | Loại sự kiện | Event type | NOT NULL | Chuyển khoản, Rút tiền, Nạp tiền |
| event_category | string | Danh mục sự kiện | Event category | NOT NULL | Giao dịch tài chính, Thay đổi thông tin |
| event_description | string | Mô tả sự kiện | Event description | Max 500 chars | Chuyển khoản nội bộ giữa các tài khoản |
| event_source | string | Nguồn sự kiện | Event source | NOT NULL | Core Banking, ATM, Mobile App |
| event_channel | string | Kênh giao dịch | Transaction channel | NOT NULL | Quầy, ATM, Internet Banking, Mobile |
| requires_approval | boolean | Yêu cầu phê duyệt | Requires approval | NOT NULL | true |
| risk_level | string | Mức độ rủi ro | Risk level | NOT NULL | Thấp, Trung bình, Cao |

### DIM_INVOLVED_PARTY - Involved Party Dimension
**Mô tả**: Các bên liên quan trong giao dịch (người thụ hưởng, người chuyển, v.v.)

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| party_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 234 |
| party_id | string | Mã bên liên quan | Party ID | NOT NULL, UNIQUE | PTY_BENEF_001 |
| party_name | string | Tên bên liên quan | Party name | NOT NULL, Max 200 chars | Nguyễn Thị Bình |
| party_type | string | Loại bên liên quan | Party type | NOT NULL | Người thụ hưởng, Người chuyển, Ngân hàng |
| relationship_type | string | Loại mối quan hệ | Relationship type | NOT NULL | Vợ/Chồng, Con, Đối tác kinh doanh |
| contact_info | string | Thông tin liên hệ | Contact information | Max 300 chars | SĐT: 0901234567, Email: abc@email.com |
| verification_status | string | Trạng thái xác minh | Verification status | NOT NULL | Đã xác minh, Chưa xác minh |
| is_internal | boolean | Bên nội bộ | Internal party flag | NOT NULL | false |

### DIM_CONDITION - Condition Dimension
**Mô tả**: Điều kiện áp dụng cho giao dịch và sản phẩm

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| condition_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 345 |
| condition_id | string | Mã điều kiện | Condition ID | NOT NULL, UNIQUE | COND_LIMIT_001 |
| condition_type | string | Loại điều kiện | Condition type | NOT NULL | Hạn mức, Thời gian, Địa lý |
| condition_category | string | Danh mục điều kiện | Condition category | NOT NULL | Giới hạn giao dịch, Yêu cầu KYC |
| condition_description | string | Mô tả điều kiện | Condition description | Max 500 chars | Hạn mức chuyển khoản tối đa 500 triệu/ngày |
| condition_value | string | Giá trị điều kiện | Condition value | Max 100 chars | 500000000 |
| operator | string | Toán tử so sánh | Comparison operator | NOT NULL | >, <, =, >=, <=, IN |
| is_active | boolean | Trạng thái hoạt động | Active status | NOT NULL | true |
| effective_date | date | Ngày hiệu lực | Effective date | NOT NULL | 2020-01-01 |

### DIM_APPLICATION - Application Dimension
**Mô tả**: Ứng dụng và hệ thống thực hiện giao dịch

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| application_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 456 |
| application_id | string | Mã ứng dụng | Application ID | NOT NULL, UNIQUE | APP_CORE_001 |
| application_name | string | Tên ứng dụng | Application name | NOT NULL, Max 200 chars | Core Banking System |
| application_type | string | Loại ứng dụng | Application type | NOT NULL | Core System, Mobile App, Web Portal |
| version | string | Phiên bản | Version | NOT NULL | 2.1.5 |
| vendor | string | Nhà cung cấp | Vendor | Max 100 chars | FIS, Temenos, Oracle |
| environment | string | Môi trường | Environment | NOT NULL | Production, UAT, Development |
| is_active | boolean | Trạng thái hoạt động | Active status | NOT NULL | true |

### DIM_ASSET - Asset Dimension
**Mô tả**: Tài sản liên quan đến giao dịch (bất động sản, chứng khoán, v.v.)

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| asset_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 678 |
| asset_id | string | Mã tài sản | Asset ID | NOT NULL, UNIQUE | AST_RE_001 |
| asset_type | string | Loại tài sản | Asset type | NOT NULL | Bất động sản, Chứng khoán, Vàng |
| asset_category | string | Danh mục tài sản | Asset category | NOT NULL | Nhà ở, Đất, Cổ phiếu, Trái phiếu |
| asset_description | string | Mô tả tài sản | Asset description | Max 500 chars | Căn hộ 3PN tại Q1, TPHCM |
| asset_value | decimal | Giá trị tài sản | Asset value | >= 0 | 5000000000 |
| currency | string | Loại tiền tệ | Currency | ISO 4217 format | VND |
| ownership_type | string | Loại sở hữu | Ownership type | NOT NULL | Cá nhân, Chung, Doanh nghiệp |
| status | string | Trạng thái tài sản | Asset status | NOT NULL | Hoạt động, Thế chấp, Đã bán |
| acquisition_date | date | Ngày mua/sở hữu | Acquisition date | <= today | 2019-05-15 |
| valuation_date | date | Ngày định giá | Valuation date | <= today | 2023-12-31 |

### DIM_DATE - Date Dimension
**Mô tả**: Chiều thời gian theo ngày với các thuộc tính mở rộng

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| date_key | int | Khóa chính (YYYYMMDD) | Primary key (YYYYMMDD format) | PK, NOT NULL, UNIQUE | 20240315 |
| full_date | date | Ngày đầy đủ | Full date | NOT NULL, UNIQUE | 2024-03-15 |
| year | int | Năm | Year | 4 digits | 2024 |
| quarter | int | Quý | Quarter | 1-4 | 1 |
| month | int | Tháng | Month | 1-12 | 3 |
| day | int | Ngày | Day of month | 1-31 | 15 |
| day_of_week | int | Thứ trong tuần | Day of week | 1-7 (1=Monday) | 5 |
| day_of_year | int | Ngày trong năm | Day of year | 1-366 | 75 |
| week_of_year | int | Tuần trong năm | Week of year | 1-53 | 11 |
| month_name | string | Tên tháng | Month name | NOT NULL | Tháng 3 |
| day_name | string | Tên thứ | Day name | NOT NULL | Thứ Sáu |
| is_weekend | boolean | Cuối tuần | Weekend indicator | NOT NULL | false |
| is_holiday | boolean | Ngày lễ | Holiday indicator | NOT NULL | false |
| holiday_name | string | Tên ngày lễ | Holiday name | NULL if not holiday | Quốc khánh |
| fiscal_year | string | Năm tài chính | Fiscal year | Format: FY2024 | FY2024 |
| fiscal_quarter | string | Quý tài chính | Fiscal quarter | Format: FY2024-Q1 | FY2024-Q1 |

### DIM_TIME - Time Dimension
**Mô tả**: Chiều thời gian theo giờ/phút/giây

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| time_key | int | Khóa chính (HHMMSS) | Primary key (HHMMSS format) | PK, NOT NULL, UNIQUE | 143025 |
| full_time | time | Thời gian đầy đủ | Full time | NOT NULL, UNIQUE | 14:30:25 |
| hour | int | Giờ | Hour | 0-23 | 14 |
| minute | int | Phút | Minute | 0-59 | 30 |
| second | int | Giây | Second | 0-59 | 25 |
| time_period | string | Buổi | Time period | NOT NULL | Sáng, Chiều, Tối |
| shift | string | Ca làm việc | Work shift | Morning, Afternoon, Night | Afternoon |
| is_business_hour | boolean | Giờ làm việc | Business hour indicator | NOT NULL | true |

---

## Fact Tables

### FACT_TRANSACTION - Main Transaction Fact
**Mô tả**: Bảng sự kiện chính chứa tất cả giao dịch của ngân hàng

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| transaction_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 1001 |
| customer_key | int | Khóa khách hàng | Customer foreign key | FK to DIM_CUSTOMER | 12345 |
| product_key | int | Khóa sản phẩm | Product foreign key | FK to DIM_PRODUCT | 567 |
| location_key | int | Khóa địa điểm | Location foreign key | FK to DIM_LOCATION | 789 |
| event_key | int | Khóa sự kiện | Event foreign key | FK to DIM_EVENT | 101 |
| involved_party_key | int | Khóa bên liên quan | Involved party foreign key | FK to DIM_INVOLVED_PARTY | 234 |
| condition_key | int | Khóa điều kiện | Condition foreign key | FK to DIM_CONDITION | 345 |
| application_key | int | Khóa ứng dụng | Application foreign key | FK to DIM_APPLICATION | 456 |
| asset_key | int | Khóa tài sản | Asset foreign key | FK to DIM_ASSET | 678 |
| transaction_date_key | int | Khóa ngày giao dịch | Transaction date key | FK to DIM_DATE | 20240315 |
| transaction_time_key | int | Khóa giờ giao dịch | Transaction time key | FK to DIM_TIME | 143025 |
| processing_date_key | int | Khóa ngày xử lý | Processing date key | FK to DIM_DATE | 20240315 |
| transaction_id | string | Mã giao dịch duy nhất | Unique transaction ID | NOT NULL, UNIQUE | TXN20240315143025001 |
| reference_number | string | Số tham chiếu | Reference number | Max 50 chars | REF123456789 |
| transaction_type | string | Loại giao dịch | Transaction type | NOT NULL | Chuyển khoản, Rút tiền, Nạp tiền |
| transaction_category | string | Danh mục giao dịch | Transaction category | NOT NULL | Nội bộ, Liên ngân hàng, Quốc tế |
| transaction_subcategory | string | Danh mục phụ | Transaction subcategory | Max 100 chars | Chuyển khoản cùng ngân hàng |
| transaction_amount | decimal | Số tiền giao dịch | Transaction amount | NOT NULL, >= 0 | 1000000 |
| fee_amount | decimal | Phí giao dịch | Fee amount | >= 0 | 11000 |
| tax_amount | decimal | Thuế | Tax amount | >= 0 | 1100 |
| net_amount | decimal | Số tiền thực nhận | Net amount | NOT NULL | 987900 |
| currency | string | Loại tiền tệ | Currency code | ISO 4217, NOT NULL | VND |
| exchange_rate | decimal | Tỷ giá | Exchange rate | > 0 | 24350 |
| usd_amount | decimal | Số tiền quy đổi USD | USD equivalent | >= 0 | 41.05 |
| debit_account | string | Tài khoản ghi nợ | Debit account | Max 50 chars | 1234567890 |
| credit_account | string | Tài khoản ghi có | Credit account | Max 50 chars | 0987654321 |
| transaction_status | string | Trạng thái giao dịch | Transaction status | NOT NULL | Thành công, Thất bại, Đang xử lý |
| approval_status | string | Trạng thái phê duyệt | Approval status | NOT NULL | Đã duyệt, Chờ duyệt, Từ chối |
| channel | string | Kênh giao dịch | Transaction channel | NOT NULL | ATM, Mobile App, Internet Banking |
| description | string | Mô tả giao dịch | Transaction description | Max 500 chars | Chuyển khoản cho thuê nhà |
| memo | string | Ghi chú | Memo field | Max 200 chars | Tiền thuê tháng 3/2024 |
| is_reversal | boolean | Giao dịch đảo chiều | Reversal transaction flag | NOT NULL | false |
| reversal_reason | string | Lý do đảo chiều | Reversal reason | Max 200 chars | Khách hàng yêu cầu |
| batch_id | string | Mã lô xử lý | Batch processing ID | Max 50 chars | BATCH20240315001 |
| created_timestamp | timestamp | Thời gian tạo | Creation timestamp | NOT NULL | 2024-03-15 14:30:25.123 |
| processed_timestamp | timestamp | Thời gian xử lý | Processing timestamp | >= created_timestamp | 2024-03-15 14:30:27.456 |
| updated_timestamp | timestamp | Thời gian cập nhật cuối | Last update timestamp | >= created_timestamp | 2024-03-15 14:31:00.789 |
| created_by | string | Người tạo | Created by user | NOT NULL | system_user |
| processed_by | string | Người xử lý | Processed by user | Max 50 chars | teller_001 |
| ip_address | string | Địa chỉ IP | IP address | IP format validation | 192.168.1.100 |
| device_id | string | Mã thiết bị | Device identifier | Max 100 chars | MOBILE_ABC123XYZ |
| session_id | string | Mã phiên làm việc | Session identifier | Max 100 chars | SESS_20240315_143025 |

### FACT_ACCOUNT_BALANCE - Account Balance Fact
**Mô tả**: Số dư tài khoản theo thời gian (snapshot hàng ngày)

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| balance_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 2001 |
| customer_key | int | Khóa khách hàng | Customer foreign key | FK to DIM_CUSTOMER | 12345 |
| product_key | int | Khóa sản phẩm | Product foreign key | FK to DIM_PRODUCT | 567 |
| location_key | int | Khóa địa điểm | Location foreign key | FK to DIM_LOCATION | 789 |
| balance_date_key | int | Khóa ngày số dư | Balance date key | FK to DIM_DATE | 20240315 |
| balance_time_key | int | Khóa giờ số dư | Balance time key | FK to DIM_TIME | 235959 |
| account_number | string | Số tài khoản | Account number | NOT NULL, UNIQUE per date | 1234567890 |
| opening_balance | decimal | Số dư đầu ngày | Opening balance | Can be negative | 5000000 |
| closing_balance | decimal | Số dư cuối ngày | Closing balance | NOT NULL | 4500000 |
| available_balance | decimal | Số dư khả dụng | Available balance | <= closing_balance | 4300000 |
| hold_balance | decimal | Số dư bị phong tỏa | Hold balance | >= 0 | 200000 |
| pending_balance | decimal | Số dư chờ xử lý | Pending balance | >= 0 | 150000 |
| overdraft_limit | decimal | Hạn mức thấu chi | Overdraft limit | >= 0 | 1000000 |
| currency | string | Loại tiền tệ | Currency | ISO 4217, NOT NULL | VND |
| usd_equivalent | decimal | Tương đương USD | USD equivalent | >= 0 | 184.93 |
| transaction_count | int | Số lượng giao dịch trong ngày | Daily transaction count | >= 0 | 12 |
| total_debits | decimal | Tổng số tiền ghi nợ | Total debit amount | >= 0 | 800000 |
| total_credits | decimal | Tổng số tiền ghi có | Total credit amount | >= 0 | 300000 |
| last_transaction_time | timestamp | Thời gian giao dịch cuối | Last transaction time | <= balance_date | 2024-03-15 18:45:30 |
| account_status | string | Trạng thái tài khoản | Account status | NOT NULL | Hoạt động, Đóng, Tạm khóa |

### FACT_LOAN_TRANSACTION - Loan Transaction Fact
**Mô tả**: Giao dịch liên quan đến khoản vay (giải ngân, trả nợ, lãi)

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| loan_transaction_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 3001 |
| customer_key | int | Khóa khách hàng | Customer foreign key | FK to DIM_CUSTOMER | 12345 |
| product_key | int | Khóa sản phẩm vay | Loan product foreign key | FK to DIM_PRODUCT | 567 |
| location_key | int | Khóa địa điểm | Location foreign key | FK to DIM_LOCATION | 789 |
| transaction_date_key | int | Khóa ngày giao dịch | Transaction date key | FK to DIM_DATE | 20240315 |
| loan_account_number | string | Số tài khoản vay | Loan account number | NOT NULL | LOAN1234567890 |
| transaction_type | string | Loại giao dịch vay | Loan transaction type | NOT NULL | Giải ngân, Trả gốc, Trả lãi, Phạt |
| principal_amount | decimal | Số tiền gốc | Principal amount | >= 0 | 500000 |
| interest_amount | decimal | Số tiền lãi | Interest amount | >= 0 | 25000 |
| penalty_amount | decimal | Số tiền phạt | Penalty amount | >= 0 | 5000 |
| total_amount | decimal | Tổng số tiền | Total amount | NOT NULL, >= 0 | 530000 |
| outstanding_principal | decimal | Dư nợ gốc | Outstanding principal | >= 0 | 49500000 |
| outstanding_interest | decimal | Lãi phải trả | Outstanding interest | >= 0 | 125000 |
| days_overdue | int | Số ngày quá hạn | Days overdue | >= 0 | 5 |
| payment_status | string | Trạng thái thanh toán | Payment status | NOT NULL | Đúng hạn, Quá hạn, Xử lý nợ |
| currency | string | Loại tiền tệ | Currency | ISO 4217, NOT NULL | VND |
| usd_equivalent | decimal | Tương đương USD | USD equivalent | >= 0 | 2175.31 |

### FACT_CARD_TRANSACTION - Card Transaction Fact
**Mô tả**: Giao dịch thẻ tín dụng/ghi nợ

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| card_transaction_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 4001 |
| customer_key | int | Khóa khách hàng | Customer foreign key | FK to DIM_CUSTOMER | 12345 |
| location_key | int | Khóa địa điểm | Location foreign key | FK to DIM_LOCATION | 789 |
| transaction_date_key | int | Khóa ngày giao dịch | Transaction date key | FK to DIM_DATE | 20240315 |
| transaction_time_key | int | Khóa giờ giao dịch | Transaction time key | FK to DIM_TIME | 143025 |
| card_number_masked | string | Số thẻ đã che | Masked card number | NOT NULL | ****-****-****-1234 |
| merchant_name | string | Tên thương gia | Merchant name | NOT NULL, Max 200 chars | Siêu thị BigC |
| merchant_category | string | Danh mục thương gia | Merchant category | NOT NULL | Siêu thị, Nhà hàng, Xăng dầu |
| merchant_location | string | Địa điểm thương gia | Merchant location | Max 300 chars | Q1, TP.HCM |
| transaction_amount | decimal | Số tiền giao dịch | Transaction amount | NOT NULL, > 0 | 250000 |
| fee_amount | decimal | Phí giao dịch | Fee amount | >= 0 | 0 |
| currency | string | Loại tiền tệ | Currency | ISO 4217, NOT NULL | VND |
| transaction_type | string | Loại giao dịch thẻ | Card transaction type | NOT NULL | Mua hàng, Rút tiền, Trả góp |
| authorization_code | string | Mã ủy quyền | Authorization code | Max 20 chars | 123456 |
| response_code | string | Mã phản hồi | Response code | NOT NULL | 00, 05, 14 |
| is_international | boolean | Giao dịch quốc tế | International transaction | NOT NULL | false |
| is_contactless | boolean | Thanh toán không tiếp xúc | Contactless payment | NOT NULL | true |
| channel_type | string | Loại kênh | Channel type | NOT NULL | POS, ATM, Online |

### FACT_INVESTMENT_TRANSACTION - Investment Transaction Fact
**Mô tả**: Giao dịch đầu tư (mua/bán chứng khoán, quỹ đầu tư)

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| investment_transaction_key | int | Khóa chính surrogate | Primary surrogate key | PK, NOT NULL, UNIQUE | 5001 |
| customer_key | int | Khóa khách hàng | Customer foreign key | FK to DIM_CUSTOMER | 12345 |
| product_key | int | Khóa sản phẩm đầu tư | Investment product key | FK to DIM_PRODUCT | 567 |
| asset_key | int | Khóa tài sản | Asset foreign key | FK to DIM_ASSET | 678 |
| transaction_date_key | int | Khóa ngày giao dích | Transaction date key | FK to DIM_DATE | 20240315 |
| portfolio_id | string | Mã danh mục đầu tư | Portfolio ID | NOT NULL | PORT123456 |
| security_id | string | Mã chứng khoán | Security ID | NOT NULL | VCB, VIC, VNM |
| transaction_type | string | Loại giao dịch đầu tư | Investment transaction type | NOT NULL | Mua, Bán, Cổ tức, Quyền |
| quantity | decimal | Số lượng | Quantity | NOT NULL, > 0 | 100 |
| unit_price | decimal | Giá đơn vị | Unit price | NOT NULL, > 0 | 85000 |
| total_amount | decimal | Tổng giá trị | Total amount | NOT NULL, > 0 | 8500000 |
| commission | decimal | Hoa hồng | Commission | >= 0 | 17000 |
| tax | decimal | Thuế | Tax amount | >= 0 | 850 |
| net_amount | decimal | Số tiền thực | Net amount | NOT NULL | 8482150 |
| currency | string | Loại tiền tệ | Currency | ISO 4217, NOT NULL | VND |
| market | string | Thị trường | Market | NOT NULL | HOSE, HNX, UPCOM |
| settlement_date | string | Ngày thanh toán | Settlement date | Format: YYYY-MM-DD | 2024-03-18 |

---

## Bridge Tables

### BRIDGE_TRANSACTION_CATEGORIES - Transaction Categories Bridge
**Mô tả**: Bảng cầu nối cho mối quan hệ nhiều-nhiều giữa giao dịch và danh mục

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| transaction_key | int | Khóa giao dịch | Transaction foreign key | FK to FACT_TRANSACTION | 1001 |
| category_type | string | Loại danh mục | Category type | NOT NULL | Ngành nghề, Khu vực, Độ rủi ro |
| category_value | string | Giá trị danh mục | Category value | NOT NULL | Bán lẻ, Miền Nam, Thấp |
| weight | decimal | Trọng số | Weight factor | 0 <= weight <= 1 | 0.8 |

---

## Aggregate Tables

### AGG_DAILY_CUSTOMER_SUMMARY - Daily Customer Summary
**Mô tả**: Tổng hợp giao dịch khách hàng theo ngày

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| customer_key | int | Khóa khách hàng | Customer foreign key | FK to DIM_CUSTOMER | 12345 |
| date_key | int | Khóa ngày | Date foreign key | FK to DIM_DATE | 20240315 |
| product_key | int | Khóa sản phẩm | Product foreign key | FK to DIM_PRODUCT | 567 |
| transaction_count | int | Số lượng giao dịch | Transaction count | >= 0 | 15 |
| total_amount | decimal | Tổng số tiền | Total amount | >= 0 | 25000000 |
| total_fees | decimal | Tổng phí | Total fees | >= 0 | 125000 |
| avg_transaction_amount | decimal | Số tiền giao dịch trung bình | Average transaction amount | >= 0 | 1666667 |
| max_transaction_amount | decimal | Giao dịch lớn nhất | Maximum transaction amount | >= 0 | 10000000 |
| min_transaction_amount | decimal | Giao dịch nhỏ nhất | Minimum transaction amount | >= 0 | 50000 |

### AGG_MONTHLY_PRODUCT_SUMMARY - Monthly Product Summary
**Mô tả**: Tổng hợp sản phẩm theo tháng

| Field Name | Data Type | Description (Vietnamese) | Description (English) | Business Rules | Example |
|------------|-----------|--------------------------|----------------------|----------------|---------|
| product_key | int | Khóa sản phẩm | Product foreign key | FK to DIM_PRODUCT | 567 |
| year_month | int | Tháng năm | Year-month | Format: YYYYMM | 202403 |
| customer_count | int | Số lượng khách hàng | Customer count | >= 0 | 1250 |
| transaction_count | int | Số lượng giao dịch | Transaction count | >= 0 | 15000 |
| total_volume | decimal | Tổng khối lượng | Total volume | >= 0 | 750000000000 |
| total_fees | decimal | Tổng phí | Total fees | >= 0 | 3750000 |
| avg_balance | decimal | Số dư trung bình | Average balance | >= 0 | 5000000 |

---

## Data Quality Rules

### Field-Level Rules
| Rule Type | Description | Example |
|-----------|-------------|---------|
| **NOT NULL** | Trường bắt buộc phải có giá trị | customer_name, transaction_amount |
| **UNIQUE** | Giá trị không được trùng lặp | customer_id, transaction_id |
| **RANGE CHECK** | Kiểm tra giá trị trong khoảng cho phép | interest_rate (0-100), month (1-12) |
| **FORMAT CHECK** | Kiểm tra định dạng chuẩn | email, phone, currency (ISO 4217) |
| **REFERENCE CHECK** | Kiểm tra tính toàn vẹn tham chiếu | Foreign keys phải tồn tại trong bảng cha |
| **DATE LOGIC** | Kiểm tra logic ngày tháng | end_date >= start_date, birth_date < today |

### Record-Level Rules
| Rule | Description | Example |
|------|-------------|---------|
| **SCD Type 2** | Chỉ có một bản ghi current cho mỗi customer_id | is_current = true (unique per customer_id) |
| **Balance Equation** | available_balance + hold_balance = closing_balance | Trong FACT_ACCOUNT_BALANCE |
| **Amount Consistency** | net_amount = transaction_amount - fee_amount - tax_amount | Trong FACT_TRANSACTION |
| **Status Logic** | Trạng thái phải hợp lệ theo workflow | approved transaction_status ≠ 'Thất bại' |

---

## Business Rules

### Transaction Processing Rules
1. **Giao dịch trên 500 triệu VND**: Bắt buộc phải có approval
2. **Giao dịch quốc tế**: Yêu cầu xác thực bổ sung
3. **Giao dịch ngoài giờ**: Phí giao dịch tăng 50%
4. **Khách hàng VIP**: Miễn phí giao dịch dưới 1 tỷ VND
5. **Giao dịch nghi ngờ**: Tự động đánh dấu để review

### Data Retention Rules
1. **Transaction Data**: Lưu trữ vĩnh viễn
2. **Session Data**: Lưu trữ 2 năm
3. **Log Data**: Lưu trữ 7 năm
4. **Customer Changes**: Lưu trữ lịch sử đầy đủ (SCD Type 2)

### Security & Privacy Rules
1. **PII Masking**: Card numbers, account numbers trong reports
2. **Access Control**: Phân quyền theo vai trò và chi nhánh
3. **Audit Trail**: Ghi log đầy đủ mọi thay đổi dữ liệu
4. **Data Encryption**: Encrypt sensitive fields at rest và in transit

### Performance Rules
1. **Partitioning**: Partition theo tháng cho fact tables
2. **Indexing**: Index trên date_key, customer_key, transaction_id
3. **Aggregation**: Pre-calculate daily/monthly summaries
4. **Archiving**: Archive dữ liệu cũ hơn 5 năm sang cold storage

### Regulatory Compliance
1. **Anti-Money Laundering (AML)**: Flag giao dịch bất thường
2. **Know Your Customer (KYC)**: Cập nhật định kỳ thông tin KYC
3. **Basel III**: Tính toán risk exposure theo quy định
4. **Circular 14**: Báo cáo theo quy định của NHNN Việt Nam

---

## Data Lineage & ETL Information

### Source Systems
- **Core Banking System**: Temenos T24, FIS Profile
- **Card Management System**: First Data, Way4
- **Investment System**: Misys Sophis, Bloomberg AIMS
- **Mobile Banking**: Proprietary mobile application
- **Internet Banking**: Web-based banking portal
- **ATM Network**: NCR, Diebold ATM systems

### ETL Schedule
- **Real-time**: Critical transactions (< 5 minutes delay)
- **Hourly**: Account balances, card transactions
- **Daily**: Customer information updates, loan data
- **Monthly**: Product information, organizational changes

### Data Quality Monitoring
- **Completeness**: 99.5% target for required fields
- **Accuracy**: Cross-validation with source systems
- **Timeliness**: 95% of data within SLA windows
- **Consistency**: Referential integrity checks daily