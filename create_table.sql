-- =============================================
-- Banking Data Warehouse - PostgreSQL DDL
-- Created: 2024
-- Description: Complete DDL for Banking DW
-- =============================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS banking_dw;
SET search_path = banking_dw;

-- =============================================
-- DIMENSION TABLES
-- =============================================

-- DIM_DATE - Date Dimension
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL CHECK (year >= 1900 AND year <= 2100),
    quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    day INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
    day_of_week INTEGER NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    day_of_year INTEGER NOT NULL CHECK (day_of_year BETWEEN 1 AND 366),
    week_of_year INTEGER NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    month_name VARCHAR(20) NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL DEFAULT FALSE,
    is_holiday BOOLEAN NOT NULL DEFAULT FALSE,
    holiday_name VARCHAR(100),
    fiscal_year VARCHAR(10) NOT NULL,
    fiscal_quarter VARCHAR(15) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM_TIME - Time Dimension
CREATE TABLE dim_time (
    time_key INTEGER PRIMARY KEY,
    full_time TIME NOT NULL UNIQUE,
    hour INTEGER NOT NULL CHECK (hour BETWEEN 0 AND 23),
    minute INTEGER NOT NULL CHECK (minute BETWEEN 0 AND 59),
    second INTEGER NOT NULL CHECK (second BETWEEN 0 AND 59),
    time_period VARCHAR(10) NOT NULL CHECK (time_period IN ('Sáng', 'Chiều', 'Tối')),
    shift VARCHAR(20) NOT NULL CHECK (shift IN ('Morning', 'Afternoon', 'Night')),
    is_business_hour BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM_CUSTOMER - Customer Dimension (SCD Type 2)
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    customer_type VARCHAR(50) NOT NULL CHECK (customer_type IN ('Cá nhân', 'Doanh nghiệp', 'Tổ chức')),
    gender VARCHAR(10) CHECK (gender IN ('Nam', 'Nữ', 'Khác')),
    birth_date DATE CHECK (birth_date > '1900-01-01' AND birth_date < CURRENT_DATE),
    phone VARCHAR(20),
    email VARCHAR(100) CHECK (email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
    address VARCHAR(500),
    city VARCHAR(100) NOT NULL,
    district VARCHAR(100) NOT NULL,
    province VARCHAR(100) NOT NULL,
    occupation VARCHAR(100),
    income_level VARCHAR(50),
    account_open_date DATE NOT NULL,
    kyc_status VARCHAR(20) NOT NULL CHECK (kyc_status IN ('Đã xác thực', 'Chưa xác thực', 'Hết hạn')),
    risk_profile VARCHAR(20) NOT NULL CHECK (risk_profile IN ('Thấp', 'Trung bình', 'Cao')),
    scd_start_date DATE NOT NULL,
    scd_end_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_scd_dates CHECK (scd_end_date IS NULL OR scd_end_date >= scd_start_date),
    CONSTRAINT uk_customer_current UNIQUE (customer_id, is_current) 
        DEFERRABLE INITIALLY DEFERRED
);

-- DIM_PRODUCT - Product Dimension
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(200) NOT NULL,
    product_type VARCHAR(50) NOT NULL CHECK (product_type IN ('Tiết kiệm', 'Vay', 'Thẻ', 'Đầu tư')),
    product_category VARCHAR(100) NOT NULL,
    product_group VARCHAR(50) NOT NULL CHECK (product_group IN ('Bán lẻ', 'Doanh nghiệp')),
    interest_rate DECIMAL(5,2) CHECK (interest_rate >= 0 AND interest_rate <= 100),
    fee_structure DECIMAL(15,2) CHECK (fee_structure >= 0),
    currency CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    terms_conditions TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    effective_date DATE NOT NULL,
    expiry_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_product_dates CHECK (expiry_date IS NULL OR expiry_date >= effective_date)
);

-- DIM_LOCATION - Location Dimension
CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    location_id VARCHAR(50) NOT NULL UNIQUE,
    branch_code VARCHAR(20) NOT NULL,
    branch_name VARCHAR(200) NOT NULL,
    branch_type VARCHAR(50) NOT NULL CHECK (branch_type IN ('Chi nhánh chính', 'Chi nhánh phụ', 'ATM', 'Kiosk')),
    address VARCHAR(500) NOT NULL,
    city VARCHAR(100) NOT NULL,
    district VARCHAR(100) NOT NULL,
    province VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL,
    country VARCHAR(100) NOT NULL DEFAULT 'Việt Nam',
    timezone VARCHAR(50) NOT NULL DEFAULT 'Asia/Ho_Chi_Minh',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM_EVENT - Event Dimension
CREATE TABLE dim_event (
    event_key SERIAL PRIMARY KEY,
    event_id VARCHAR(50) NOT NULL UNIQUE,
    event_type VARCHAR(100) NOT NULL,
    event_category VARCHAR(100) NOT NULL,
    event_description VARCHAR(500),
    event_source VARCHAR(100) NOT NULL,
    event_channel VARCHAR(50) NOT NULL CHECK (event_channel IN ('Quầy', 'ATM', 'Internet Banking', 'Mobile', 'Call Center')),
    requires_approval BOOLEAN NOT NULL DEFAULT FALSE,
    risk_level VARCHAR(20) NOT NULL CHECK (risk_level IN ('Thấp', 'Trung bình', 'Cao')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM_INVOLVED_PARTY - Involved Party Dimension
CREATE TABLE dim_involved_party (
    party_key SERIAL PRIMARY KEY,
    party_id VARCHAR(50) NOT NULL UNIQUE,
    party_name VARCHAR(200) NOT NULL,
    party_type VARCHAR(50) NOT NULL,
    relationship_type VARCHAR(100),
    contact_info VARCHAR(300),
    verification_status VARCHAR(20) NOT NULL CHECK (verification_status IN ('Đã xác minh', 'Chưa xác minh')),
    is_internal BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM_CONDITION - Condition Dimension
CREATE TABLE dim_condition (
    condition_key SERIAL PRIMARY KEY,
    condition_id VARCHAR(50) NOT NULL UNIQUE,
    condition_type VARCHAR(100) NOT NULL,
    condition_category VARCHAR(100) NOT NULL,
    condition_description VARCHAR(500),
    condition_value VARCHAR(100),
    operator VARCHAR(10) CHECK (operator IN ('>', '<', '=', '>=', '<=', 'IN', 'NOT IN')),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    effective_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM_APPLICATION - Application Dimension
CREATE TABLE dim_application (
    application_key SERIAL PRIMARY KEY,
    application_id VARCHAR(50) NOT NULL UNIQUE,
    application_name VARCHAR(200) NOT NULL,
    application_type VARCHAR(100) NOT NULL,
    version VARCHAR(20) NOT NULL,
    vendor VARCHAR(100),
    environment VARCHAR(20) NOT NULL CHECK (environment IN ('Production', 'UAT', 'Development')),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM_ASSET - Asset Dimension
CREATE TABLE dim_asset (
    asset_key SERIAL PRIMARY KEY,
    asset_id VARCHAR(50) NOT NULL UNIQUE,
    asset_type VARCHAR(100) NOT NULL,
    asset_category VARCHAR(100) NOT NULL,
    asset_description VARCHAR(500),
    asset_value DECIMAL(20,2) CHECK (asset_value >= 0),
    currency CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    ownership_type VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    acquisition_date DATE,
    valuation_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_asset_dates CHECK (valuation_date >= acquisition_date)
);

-- =============================================
-- FACT TABLES
-- =============================================

-- FACT_TRANSACTION - Main Transaction Fact
CREATE TABLE fact_transaction (
    transaction_key BIGSERIAL PRIMARY KEY,
    customer_key INTEGER NOT NULL,
    product_key INTEGER,
    location_key INTEGER,
    event_key INTEGER NOT NULL,
    involved_party_key INTEGER,
    condition_key INTEGER,
    application_key INTEGER,
    asset_key INTEGER,
    transaction_date_key INTEGER NOT NULL,
    transaction_time_key INTEGER NOT NULL,
    processing_date_key INTEGER,
    
    -- Business Keys
    transaction_id VARCHAR(100) NOT NULL UNIQUE,
    reference_number VARCHAR(50),
    
    -- Transaction Classification
    transaction_type VARCHAR(100) NOT NULL,
    transaction_category VARCHAR(100) NOT NULL,
    transaction_subcategory VARCHAR(100),
    
    -- Amounts
    transaction_amount DECIMAL(20,2) NOT NULL CHECK (transaction_amount >= 0),
    fee_amount DECIMAL(20,2) DEFAULT 0 CHECK (fee_amount >= 0),
    tax_amount DECIMAL(20,2) DEFAULT 0 CHECK (tax_amount >= 0),
    net_amount DECIMAL(20,2) NOT NULL,
    
    -- Currency & Exchange
    currency CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    exchange_rate DECIMAL(10,6) DEFAULT 1 CHECK (exchange_rate > 0),
    usd_amount DECIMAL(20,2) CHECK (usd_amount >= 0),
    
    -- Account Information
    debit_account VARCHAR(50),
    credit_account VARCHAR(50),
    
    -- Status
    transaction_status VARCHAR(20) NOT NULL CHECK (transaction_status IN ('Thành công', 'Thất bại', 'Đang xử lý', 'Đã hủy')),
    approval_status VARCHAR(20) NOT NULL CHECK (approval_status IN ('Đã duyệt', 'Chờ duyệt', 'Từ chối')),
    
    -- Channel & Description
    channel VARCHAR(50) NOT NULL,
    description VARCHAR(500),
    memo VARCHAR(200),
    
    -- Reversal Information
    is_reversal BOOLEAN NOT NULL DEFAULT FALSE,
    reversal_reason VARCHAR(200),
    
    -- Processing Information
    batch_id VARCHAR(50),
    created_timestamp TIMESTAMP NOT NULL,
    processed_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) NOT NULL,
    processed_by VARCHAR(50),
    
    -- Security & Audit
    ip_address INET,
    device_id VARCHAR(100),
    session_id VARCHAR(100),
    
    -- Constraints
    CONSTRAINT fk_transaction_customer FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_transaction_product FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    CONSTRAINT fk_transaction_location FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
    CONSTRAINT fk_transaction_event FOREIGN KEY (event_key) REFERENCES dim_event(event_key),
    CONSTRAINT fk_transaction_party FOREIGN KEY (involved_party_key) REFERENCES dim_involved_party(party_key),
    CONSTRAINT fk_transaction_condition FOREIGN KEY (condition_key) REFERENCES dim_condition(condition_key),
    CONSTRAINT fk_transaction_application FOREIGN KEY (application_key) REFERENCES dim_application(application_key),
    CONSTRAINT fk_transaction_asset FOREIGN KEY (asset_key) REFERENCES dim_asset(asset_key),
    CONSTRAINT fk_transaction_date FOREIGN KEY (transaction_date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_transaction_time FOREIGN KEY (transaction_time_key) REFERENCES dim_time(time_key),
    CONSTRAINT fk_processing_date FOREIGN KEY (processing_date_key) REFERENCES dim_date(date_key),
    
    CONSTRAINT chk_net_amount CHECK (net_amount = transaction_amount - fee_amount - tax_amount),
    CONSTRAINT chk_timestamps CHECK (processed_timestamp >= created_timestamp)
)
PARTITION BY RANGE (transaction_date_key);

-- FACT_ACCOUNT_BALANCE - Account Balance Fact
CREATE TABLE fact_account_balance (
    balance_key BIGSERIAL PRIMARY KEY,
    customer_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    location_key INTEGER,
    balance_date_key INTEGER NOT NULL,
    balance_time_key INTEGER NOT NULL,
    
    -- Account Information
    account_number VARCHAR(50) NOT NULL,
    
    -- Balance Amounts
    opening_balance DECIMAL(20,2),
    closing_balance DECIMAL(20,2) NOT NULL,
    available_balance DECIMAL(20,2) NOT NULL,
    hold_balance DECIMAL(20,2) DEFAULT 0 CHECK (hold_balance >= 0),
    pending_balance DECIMAL(20,2) DEFAULT 0 CHECK (pending_balance >= 0),
    overdraft_limit DECIMAL(20,2) DEFAULT 0 CHECK (overdraft_limit >= 0),
    
    -- Currency
    currency CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    usd_equivalent DECIMAL(20,2) CHECK (usd_equivalent >= 0),
    
    -- Transaction Summary
    transaction_count INTEGER DEFAULT 0 CHECK (transaction_count >= 0),
    total_debits DECIMAL(20,2) DEFAULT 0 CHECK (total_debits >= 0),
    total_credits DECIMAL(20,2) DEFAULT 0 CHECK (total_credits >= 0),
    last_transaction_time TIMESTAMP,
    
    -- Status
    account_status VARCHAR(20) NOT NULL CHECK (account_status IN ('Hoạt động', 'Đóng', 'Tạm khóa')),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT fk_balance_customer FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_balance_product FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    CONSTRAINT fk_balance_location FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
    CONSTRAINT fk_balance_date FOREIGN KEY (balance_date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_balance_time FOREIGN KEY (balance_time_key) REFERENCES dim_time(time_key),
    
    CONSTRAINT uk_account_balance_date UNIQUE (account_number, balance_date_key),
    CONSTRAINT chk_balance_logic CHECK (available_balance + hold_balance <= closing_balance + overdraft_limit)
)
PARTITION BY RANGE (balance_date_key);

-- FACT_LOAN_TRANSACTION - Loan Transaction Fact
CREATE TABLE fact_loan_transaction (
    loan_transaction_key BIGSERIAL PRIMARY KEY,
    customer_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    location_key INTEGER,
    transaction_date_key INTEGER NOT NULL,
    
    -- Loan Information
    loan_account_number VARCHAR(50) NOT NULL,
    transaction_type VARCHAR(50) NOT NULL CHECK (transaction_type IN ('Giải ngân', 'Trả gốc', 'Trả lãi', 'Phạt', 'Cơ cấu lại')),
    
    -- Amounts
    principal_amount DECIMAL(20,2) DEFAULT 0 CHECK (principal_amount >= 0),
    interest_amount DECIMAL(20,2) DEFAULT 0 CHECK (interest_amount >= 0),
    penalty_amount DECIMAL(20,2) DEFAULT 0 CHECK (penalty_amount >= 0),
    total_amount DECIMAL(20,2) NOT NULL CHECK (total_amount >= 0),
    
    -- Outstanding Balances
    outstanding_principal DECIMAL(20,2) DEFAULT 0 CHECK (outstanding_principal >= 0),
    outstanding_interest DECIMAL(20,2) DEFAULT 0 CHECK (outstanding_interest >= 0),
    
    -- Payment Status
    days_overdue INTEGER DEFAULT 0 CHECK (days_overdue >= 0),
    payment_status VARCHAR(20) NOT NULL CHECK (payment_status IN ('Đúng hạn', 'Quá hạn', 'Xử lý nợ')),
    
    -- Currency
    currency CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    usd_equivalent DECIMAL(20,2) CHECK (usd_equivalent >= 0),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT fk_loan_customer FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_loan_product FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    CONSTRAINT fk_loan_location FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
    CONSTRAINT fk_loan_date FOREIGN KEY (transaction_date_key) REFERENCES dim_date(date_key),
    
    CONSTRAINT chk_loan_total CHECK (total_amount = principal_amount + interest_amount + penalty_amount)
)
PARTITION BY RANGE (transaction_date_key);

-- FACT_CARD_TRANSACTION - Card Transaction Fact
CREATE TABLE fact_card_transaction (
    card_transaction_key BIGSERIAL PRIMARY KEY,
    customer_key INTEGER NOT NULL,
    location_key INTEGER,
    transaction_date_key INTEGER NOT NULL,
    transaction_time_key INTEGER NOT NULL,
    
    -- Card Information
    card_number_masked VARCHAR(20) NOT NULL,
    
    -- Merchant Information
    merchant_name VARCHAR(200) NOT NULL,
    merchant_category VARCHAR(100) NOT NULL,
    merchant_location VARCHAR(300),
    
    -- Transaction Details
    transaction_amount DECIMAL(20,2) NOT NULL CHECK (transaction_amount > 0),
    fee_amount DECIMAL(20,2) DEFAULT 0 CHECK (fee_amount >= 0),
    currency CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    transaction_type VARCHAR(50) NOT NULL CHECK (transaction_type IN ('Mua hàng', 'Rút tiền', 'Trả góp', 'Chuyển khoản')),
    
    -- Processing Information
    authorization_code VARCHAR(20),
    response_code VARCHAR(5) NOT NULL,
    
    -- Transaction Characteristics
    is_international BOOLEAN NOT NULL DEFAULT FALSE,
    is_contactless BOOLEAN NOT NULL DEFAULT FALSE,
    channel_type VARCHAR(20) NOT NULL CHECK (channel_type IN ('POS', 'ATM', 'Online', 'Mobile')),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT fk_card_customer FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_card_location FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
    CONSTRAINT fk_card_date FOREIGN KEY (transaction_date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_card_time FOREIGN KEY (transaction_time_key) REFERENCES dim_time(time_key)
)
PARTITION BY RANGE (transaction_date_key);


-- =============================================
-- AGGREGATE TABLES
-- =============================================

-- AGG_DAILY_CUSTOMER_SUMMARY - Daily Customer Summary
CREATE TABLE agg_daily_customer_summary (
    customer_key INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    product_key INTEGER,
    
    -- Metrics
    transaction_count INTEGER DEFAULT 0 CHECK (transaction_count >= 0),
    total_amount DECIMAL(20,2) DEFAULT 0 CHECK (total_amount >= 0),
    total_fees DECIMAL(20,2) DEFAULT 0 CHECK (total_fees >= 0),
    avg_transaction_amount DECIMAL(20,2) CHECK (avg_transaction_amount >= 0),
    max_transaction_amount DECIMAL(20,2) CHECK (max_transaction_amount >= 0),
    min_transaction_amount DECIMAL(20,2) CHECK (min_transaction_amount >= 0),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (customer_key, date_key, COALESCE(product_key, 0)),
    CONSTRAINT fk_agg_daily_customer FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_agg_daily_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_agg_daily_product FOREIGN KEY (product_key) REFERENCES dim_product(product_key)
)
PARTITION BY RANGE (date_key);

-- =============================================
-- PARTITIONING
-- =============================================

-- Create partitions for fact_transaction (monthly partitions for current and next year)
DO $$
DECLARE
    start_date DATE := DATE_TRUNC('year', CURRENT_DATE);
    end_date DATE := start_date + INTERVAL '2 years';
    partition_start DATE;
    partition_end DATE;
    partition_name TEXT;
    start_key INTEGER;
    end_key INTEGER;
BEGIN
    partition_start := start_date;
    
    WHILE partition_start < end_date LOOP
        partition_end := partition_start + INTERVAL '1 month';
        partition_name := 'fact_transaction_' || TO_CHAR(partition_start, 'YYYY_MM');
        start_key := TO_CHAR(partition_start, 'YYYYMMDD')::INTEGER;
        end_key := TO_CHAR(partition_end, 'YYYYMMDD')::INTEGER;
        
        EXECUTE format('CREATE TABLE %I PARTITION OF fact_transaction 
                       FOR VALUES FROM (%L) TO (%L)', 
                       partition_name, start_key, end_key);
        
        partition_start := partition_end;
    END LOOP;
END $$;

-- Create partitions for other fact tables
DO $$
DECLARE
    tables TEXT[] := ARRAY['fact_account_balance', 'fact_loan_transaction', 'fact_card_transaction', 'fact_investment_transaction', 'agg_daily_customer_summary'];
    table_name TEXT;
    start_date DATE := DATE_TRUNC('year', CURRENT_DATE);
    end_date DATE := start_date + INTERVAL '2 years';
    partition_start DATE;
    partition_end DATE;
    partition_name TEXT;
    start_key INTEGER;
    end_key INTEGER;
BEGIN
    FOREACH table_name IN ARRAY tables LOOP
        partition_start := start_date;
        
        WHILE partition_start < end_date LOOP
            partition_end := partition_start + INTERVAL '1 month';
            partition_name := table_name || '_' || TO_CHAR(partition_start, 'YYYY_MM');
            start_key := TO_CHAR(partition_start, 'YYYYMMDD')::INTEGER;
            end_key := TO_CHAR(partition_end, 'YYYYMMDD')::INTEGER;
            
            EXECUTE format('CREATE TABLE %I PARTITION OF %I 
                           FOR VALUES FROM (%L) TO (%L)', 
                           partition_name, table_name, start_key, end_key);
            
            partition_start := partition_end;
        END LOOP;
    END LOOP;
END $$;

-- =============================================
-- INDEXES
-- =============================================

-- Dimension table indexes
CREATE INDEX idx_dim_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_dim_customer_current ON dim_customer(customer_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_customer_scd_dates ON dim_customer(scd_start_date, scd_end_date);

CREATE INDEX idx_dim_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_product_type ON dim_product(product_type);
CREATE INDEX idx_dim_product_active ON dim_product(is_active) WHERE is_active = TRUE;

CREATE INDEX idx_dim_location_branch ON dim_location(branch_code);
CREATE INDEX idx_dim_location_active ON dim_location(is_active) WHERE is_active = TRUE;

-- Fact table indexes
CREATE INDEX idx_fact_transaction_customer ON fact_transaction(customer_key);
CREATE INDEX idx_fact_transaction_date ON fact_transaction(transaction_date_key);
CREATE INDEX idx_fact_transaction_id ON fact_transaction(transaction_id);
CREATE INDEX idx_fact_transaction_status ON fact_transaction(transaction_status);
CREATE INDEX idx_fact_transaction_channel ON fact_transaction(channel);
CREATE INDEX idx_fact_transaction_amount ON fact_transaction(transaction_amount) WHERE transaction_amount > 500000000; -- Large transactions

CREATE INDEX idx_fact_balance_account ON fact_account_balance(account_number);
CREATE INDEX idx_fact_balance_customer_date ON fact_account_balance(customer_key, balance_date_key);

CREATE INDEX idx_fact_loan_account ON fact_loan_transaction(loan_account_number);
CREATE INDEX idx_fact_loan_overdue ON fact_loan_transaction(days_overdue) WHERE days_overdue > 0;

CREATE INDEX idx_fact_card_merchant ON fact_card_transaction(merchant_category);
CREATE INDEX idx_fact_card_international ON fact_card_transaction(is_international) WHERE is_international = TRUE;
CREATE INDEX idx_fact_card_contactless ON fact_card_transaction(is_contactless) WHERE is_contactless = TRUE;

CREATE INDEX idx_fact_invest_portfolio ON fact_investment_transaction(portfolio_id);
CREATE INDEX idx_fact_invest_security ON fact_investment_transaction(security_id);
CREATE INDEX idx_fact_invest_market ON fact_investment_transaction(market);

-- Bridge table indexes
CREATE INDEX idx_bridge_category_type ON bridge_transaction_categories(category_type, category_value);

-- Aggregate table indexes
CREATE INDEX idx_agg_daily_date ON agg_daily_customer_summary(date_key);
CREATE INDEX idx_agg_monthly_yearmonth ON agg_monthly_product_summary(year_month);

-- =============================================
-- CONSTRAINTS AND TRIGGERS
-- =============================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$ LANGUAGE plpgsql;

-- Create triggers for updated_at
CREATE TRIGGER tr_dim_customer_updated_at 
    BEFORE UPDATE ON dim_customer 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_dim_product_updated_at 
    BEFORE UPDATE ON dim_product 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_dim_location_updated_at 
    BEFORE UPDATE ON dim_location 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_dim_event_updated_at 
    BEFORE UPDATE ON dim_event 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_dim_involved_party_updated_at 
    BEFORE UPDATE ON dim_involved_party 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_dim_condition_updated_at 
    BEFORE UPDATE ON dim_condition 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_dim_application_updated_at 
    BEFORE UPDATE ON dim_application 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_dim_asset_updated_at 
    BEFORE UPDATE ON dim_asset 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_fact_transaction_updated_at 
    BEFORE UPDATE ON fact_transaction 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_agg_daily_updated_at 
    BEFORE UPDATE ON agg_daily_customer_summary 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER tr_agg_monthly_updated_at 
    BEFORE UPDATE ON agg_monthly_product_summary 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Function to validate SCD Type 2 for customers
CREATE OR REPLACE FUNCTION validate_customer_scd()
RETURNS TRIGGER AS $
BEGIN
    -- Ensure only one current record per customer_id
    IF NEW.is_current = TRUE THEN
        UPDATE dim_customer 
        SET is_current = FALSE, 
            scd_end_date = CURRENT_DATE,
            updated_at = CURRENT_TIMESTAMP
        WHERE customer_id = NEW.customer_id 
          AND is_current = TRUE 
          AND customer_key != COALESCE(NEW.customer_key, -1);
    END IF;
    
    RETURN NEW;
END;
$ LANGUAGE plpgsql;

CREATE TRIGGER tr_customer_scd_validation 
    BEFORE INSERT OR UPDATE ON dim_customer 
    FOR EACH ROW EXECUTE FUNCTION validate_customer_scd();

-- Function to calculate USD equivalent
CREATE OR REPLACE FUNCTION calculate_usd_equivalent()
RETURNS TRIGGER AS $
BEGIN
    IF NEW.currency != 'USD' AND NEW.exchange_rate IS NOT NULL AND NEW.exchange_rate > 0 THEN
        NEW.usd_amount = NEW.transaction_amount / NEW.exchange_rate;
    ELSIF NEW.currency = 'USD' THEN
        NEW.usd_amount = NEW.transaction_amount;
        NEW.exchange_rate = 1;
    END IF;
    
    RETURN NEW;
END;
$ LANGUAGE plpgsql;

CREATE TRIGGER tr_transaction_usd_calculation 
    BEFORE INSERT OR UPDATE ON fact_transaction 
    FOR EACH ROW EXECUTE FUNCTION calculate_usd_equivalent();

-- Function to validate transaction business rules
CREATE OR REPLACE FUNCTION validate_transaction_business_rules()
RETURNS TRIGGER AS $
BEGIN
    -- Large transaction approval requirement
    IF NEW.transaction_amount > 500000000 AND NEW.approval_status != 'Đã duyệt' THEN
        RAISE EXCEPTION 'Giao dịch trên 500 triệu VND phải được phê duyệt';
    END IF;
    
    -- International transaction validation
    IF EXISTS (
        SELECT 1 FROM fact_card_transaction fct 
        JOIN dim_location dl ON fct.location_key = dl.location_key 
        WHERE fct.card_transaction_key = NEW.transaction_key 
          AND fct.is_international = TRUE
          AND dl.country != 'Việt Nam'
    ) AND NEW.approval_status = 'Chờ duyệt' THEN
        -- Additional validation for international transactions can be added here
        NULL;
    END IF;
    
    RETURN NEW;
END;
$ LANGUAGE plpgsql;

-- Apply only to transactions (not all fact tables to avoid complexity)
-- CREATE TRIGGER tr_transaction_business_rules 
--     BEFORE INSERT OR UPDATE ON fact_transaction 
--     FOR EACH ROW EXECUTE FUNCTION validate_transaction_business_rules();

-- =============================================
-- VIEWS FOR REPORTING
-- =============================================

-- Customer 360 View
CREATE OR REPLACE VIEW v_customer_360 AS
SELECT 
    dc.customer_key,
    dc.customer_id,
    dc.customer_name,
    dc.customer_type,
    dc.city,
    dc.province,
    dc.kyc_status,
    dc.risk_profile,
    dc.account_open_date,
    
    -- Account Summary
    COUNT(DISTINCT fab.account_number) as total_accounts,
    SUM(CASE WHEN fab.account_status = 'Hoạt động' THEN fab.closing_balance ELSE 0 END) as total_balance,
    SUM(CASE WHEN fab.account_status = 'Hoạt động' THEN fab.available_balance ELSE 0 END) as available_balance,
    
    -- Transaction Summary (Last 30 days)
    COUNT(ft.transaction_key) as transactions_last_30d,
    COALESCE(SUM(ft.transaction_amount), 0) as total_amount_last_30d,
    COALESCE(AVG(ft.transaction_amount), 0) as avg_transaction_amount,
    
    -- Last Transaction
    MAX(ft.created_timestamp) as last_transaction_date,
    
    dc.updated_at
FROM dim_customer dc
LEFT JOIN fact_account_balance fab ON dc.customer_key = fab.customer_key 
    AND fab.balance_date_key = (SELECT MAX(date_key) FROM dim_date WHERE full_date <= CURRENT_DATE)
LEFT JOIN fact_transaction ft ON dc.customer_key = ft.customer_key 
    AND ft.transaction_date_key >= (SELECT date_key FROM dim_date WHERE full_date = CURRENT_DATE - INTERVAL '30 days')
WHERE dc.is_current = TRUE
GROUP BY dc.customer_key, dc.customer_id, dc.customer_name, dc.customer_type, 
         dc.city, dc.province, dc.kyc_status, dc.risk_profile, dc.account_open_date, dc.updated_at;

-- Transaction Summary View
CREATE OR REPLACE VIEW v_transaction_summary AS
SELECT 
    ft.transaction_date_key,
    dd.full_date,
    dd.month_name,
    dd.year,
    dc.customer_type,
    dp.product_type,
    dl.region,
    de.event_channel,
    
    COUNT(*) as transaction_count,
    SUM(ft.transaction_amount) as total_amount,
    SUM(ft.fee_amount) as total_fees,
    AVG(ft.transaction_amount) as avg_amount,
    MIN(ft.transaction_amount) as min_amount,
    MAX(ft.transaction_amount) as max_amount,
    
    COUNT(CASE WHEN ft.transaction_status = 'Thành công' THEN 1 END) as successful_count,
    COUNT(CASE WHEN ft.transaction_status = 'Thất bại' THEN 1 END) as failed_count,
    
    ROUND(
        COUNT(CASE WHEN ft.transaction_status = 'Thành công' THEN 1 END) * 100.0 / COUNT(*), 
        2
    ) as success_rate
FROM fact_transaction ft
JOIN dim_date dd ON ft.transaction_date_key = dd.date_key
JOIN dim_customer dc ON ft.customer_key = dc.customer_key
JOIN dim_product dp ON ft.product_key = dp.product_key
JOIN dim_location dl ON ft.location_key = dl.location_key
JOIN dim_event de ON ft.event_key = de.event_key
WHERE dc.is_current = TRUE
GROUP BY ft.transaction_date_key, dd.full_date, dd.month_name, dd.year,
         dc.customer_type, dp.product_type, dl.region, de.event_channel;

-- Risk Monitoring View
CREATE OR REPLACE VIEW v_risk_monitoring AS
SELECT 
    dc.customer_key,
    dc.customer_id,
    dc.customer_name,
    dc.risk_profile,
    
    -- Large transactions (last 7 days)
    COUNT(CASE WHEN ft.transaction_amount > 1000000000 
               AND ft.transaction_date_key >= (SELECT date_key FROM dim_date WHERE full_date = CURRENT_DATE - INTERVAL '7 days')
          THEN 1 END) as large_transactions_7d,
    
    -- International transactions (last 30 days)
    COUNT(CASE WHEN fct.is_international = TRUE 
               AND fct.transaction_date_key >= (SELECT date_key FROM dim_date WHERE full_date = CURRENT_DATE - INTERVAL '30 days')
          THEN 1 END) as international_transactions_30d,
    
    -- Failed transactions ratio (last 30 days)
    ROUND(
        COUNT(CASE WHEN ft.transaction_status = 'Thất bại' 
                   AND ft.transaction_date_key >= (SELECT date_key FROM dim_date WHERE full_date = CURRENT_DATE - INTERVAL '30 days')
              THEN 1 END) * 100.0 / 
        NULLIF(COUNT(CASE WHEN ft.transaction_date_key >= (SELECT date_key FROM dim_date WHERE full_date = CURRENT_DATE - INTERVAL '30 days')
                     THEN 1 END), 0),
        2
    ) as failed_transaction_rate_30d,
    
    -- Overdue loans
    COUNT(CASE WHEN flt.days_overdue > 0 THEN 1 END) as overdue_loans,
    MAX(flt.days_overdue) as max_overdue_days,
    
    CURRENT_TIMESTAMP as analysis_timestamp
FROM dim_customer dc
LEFT JOIN fact_transaction ft ON dc.customer_key = ft.customer_key
LEFT JOIN fact_card_transaction fct ON dc.customer_key = fct.customer_key
LEFT JOIN fact_loan_transaction flt ON dc.customer_key = flt.customer_key
WHERE dc.is_current = TRUE
GROUP BY dc.customer_key, dc.customer_id, dc.customer_name, dc.risk_profile
HAVING COUNT(CASE WHEN ft.transaction_amount > 1000000000 
                  AND ft.transaction_date_key >= (SELECT date_key FROM dim_date WHERE full_date = CURRENT_DATE - INTERVAL '7 days')
             THEN 1 END) > 0
    OR COUNT(CASE WHEN fct.is_international = TRUE 
                  AND fct.transaction_date_key >= (SELECT date_key FROM dim_date WHERE full_date = CURRENT_DATE - INTERVAL '30 days')
             THEN 1 END) > 5
    OR ROUND(
        COUNT(CASE WHEN ft.transaction_status = 'Thất bại' 
                   AND ft.transaction_date_key >= (SELECT date_key FROM dim_date WHERE full_date = CURRENT_DATE - INTERVAL '30 days')
              THEN 1 END) * 100.0 / 
        NULLIF(COUNT(CASE WHEN ft.transaction_date_key >= (SELECT date_key FROM dim_date WHERE full_date = CURRENT_DATE - INTERVAL '30 days')
                     THEN 1 END), 0),
        2
    ) > 10
    OR COUNT(CASE WHEN flt.days_overdue > 0 THEN 1 END) > 0;

-- =============================================
-- STORED PROCEDURES
-- =============================================

-- Procedure to populate date dimension
CREATE OR REPLACE FUNCTION populate_dim_date(start_date DATE, end_date DATE)
RETURNS VOID AS $
DECLARE
    current_date_val DATE;
    date_key_val INTEGER;
    fiscal_year_start DATE;
BEGIN
    current_date_val := start_date;
    
    WHILE current_date_val <= end_date LOOP
        date_key_val := TO_CHAR(current_date_val, 'YYYYMMDD')::INTEGER;
        fiscal_year_start := CASE 
            WHEN EXTRACT(MONTH FROM current_date_val) >= 4 
            THEN DATE_TRUNC('year', current_date_val) + INTERVAL '3 months'
            ELSE DATE_TRUNC('year', current_date_val) - INTERVAL '9 months'
        END;
        
        INSERT INTO dim_date (
            date_key, full_date, year, quarter, month, day,
            day_of_week, day_of_year, week_of_year,
            month_name, day_name, is_weekend, is_holiday,
            fiscal_year, fiscal_quarter
        ) VALUES (
            date_key_val,
            current_date_val,
            EXTRACT(YEAR FROM current_date_val),
            EXTRACT(QUARTER FROM current_date_val),
            EXTRACT(MONTH FROM current_date_val),
            EXTRACT(DAY FROM current_date_val),
            EXTRACT(DOW FROM current_date_val) + 1, -- Convert to 1-7 (Mon-Sun)
            EXTRACT(DOY FROM current_date_val),
            EXTRACT(WEEK FROM current_date_val),
            CASE EXTRACT(MONTH FROM current_date_val)
                WHEN 1 THEN 'Tháng 1' WHEN 2 THEN 'Tháng 2' WHEN 3 THEN 'Tháng 3'
                WHEN 4 THEN 'Tháng 4' WHEN 5 THEN 'Tháng 5' WHEN 6 THEN 'Tháng 6'
                WHEN 7 THEN 'Tháng 7' WHEN 8 THEN 'Tháng 8' WHEN 9 THEN 'Tháng 9'
                WHEN 10 THEN 'Tháng 10' WHEN 11 THEN 'Tháng 11' WHEN 12 THEN 'Tháng 12'
            END,
            CASE EXTRACT(DOW FROM current_date_val)
                WHEN 0 THEN 'Chủ Nhật' WHEN 1 THEN 'Thứ Hai' WHEN 2 THEN 'Thứ Ba'
                WHEN 3 THEN 'Thứ Tư' WHEN 4 THEN 'Thứ Năm' WHEN 5 THEN 'Thứ Sáu'
                WHEN 6 THEN 'Thứ Bảy'
            END,
            EXTRACT(DOW FROM current_date_val) IN (0, 6), -- Weekend
            FALSE, -- Holiday - to be updated separately
            'FY' || EXTRACT(YEAR FROM fiscal_year_start),
            'FY' || EXTRACT(YEAR FROM fiscal_year_start) || '-Q' || EXTRACT(QUARTER FROM current_date_val)
        )
        ON CONFLICT (date_key) DO NOTHING;
        
        current_date_val := current_date_val + INTERVAL '1 day';
    END LOOP;
END;
$ LANGUAGE plpgsql;

-- Procedure to populate time dimension
CREATE OR REPLACE FUNCTION populate_dim_time()
RETURNS VOID AS $
DECLARE
    current_time_val TIME;
    time_key_val INTEGER;
    hour_val INTEGER;
    minute_val INTEGER;
    second_val INTEGER;
BEGIN
    current_time_val := '00:00:00';
    
    WHILE current_time_val <= '23:59:59' LOOP
        hour_val := EXTRACT(HOUR FROM current_time_val);
        minute_val := EXTRACT(MINUTE FROM current_time_val);
        second_val := EXTRACT(SECOND FROM current_time_val);
        time_key_val := hour_val * 10000 + minute_val * 100 + second_val;
        
        INSERT INTO dim_time (
            time_key, full_time, hour, minute, second,
            time_period, shift, is_business_hour
        ) VALUES (
            time_key_val,
            current_time_val,
            hour_val,
            minute_val,
            second_val,
            CASE 
                WHEN hour_val < 12 THEN 'Sáng'
                WHEN hour_val < 18 THEN 'Chiều'
                ELSE 'Tối'
            END,
            CASE 
                WHEN hour_val BETWEEN 6 AND 13 THEN 'Morning'
                WHEN hour_val BETWEEN 14 AND 21 THEN 'Afternoon'
                ELSE 'Night'
            END,
            hour_val BETWEEN 8 AND 17 -- Business hours 8 AM to 5 PM
        )
        ON CONFLICT (time_key) DO NOTHING;
        
        current_time_val := current_time_val + INTERVAL '1 minute';
    END LOOP;
END;
$ LANGUAGE plpgsql;

-- Procedure to refresh daily aggregates
CREATE OR REPLACE FUNCTION refresh_daily_aggregates(target_date DATE DEFAULT CURRENT_DATE)
RETURNS VOID AS $
DECLARE
    target_date_key INTEGER;
BEGIN
    target_date_key := TO_CHAR(target_date, 'YYYYMMDD')::INTEGER;
    
    -- Delete existing data for the target date
    DELETE FROM agg_daily_customer_summary WHERE date_key = target_date_key;
    
    -- Insert fresh aggregated data
    INSERT INTO agg_daily_customer_summary (
        customer_key, date_key, product_key,
        transaction_count, total_amount, total_fees,
        avg_transaction_amount, max_transaction_amount, min_transaction_amount
    )
    SELECT 
        ft.customer_key,
        ft.transaction_date_key,
        ft.product_key,
        COUNT(*),
        SUM(ft.transaction_amount),
        SUM(ft.fee_amount),
        AVG(ft.transaction_amount),
        MAX(ft.transaction_amount),
        MIN(ft.transaction_amount)
    FROM fact_transaction ft
    WHERE ft.transaction_date_key = target_date_key
      AND ft.transaction_status = 'Thành công'
    GROUP BY ft.customer_key, ft.transaction_date_key, ft.product_key;
    
    RAISE NOTICE 'Daily aggregates refreshed for date: %', target_date;
END;
$ LANGUAGE plpgsql;

-- =============================================
-- INITIAL DATA POPULATION
-- =============================================

-- Populate date dimension for current and next 2 years
SELECT populate_dim_date(
    DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1 year'),
    DATE_TRUNC('year', CURRENT_DATE + INTERVAL '2 years') + INTERVAL '1 year' - INTERVAL '1 day'
);

-- Populate time dimension
SELECT populate_dim_time();

-- =============================================
-- SECURITY
-- =============================================

-- Create roles
CREATE ROLE banking_dw_read;
CREATE ROLE banking_dw_write;
CREATE ROLE banking_dw_admin;

-- Grant permissions
GRANT USAGE ON SCHEMA banking_dw TO banking_dw_read, banking_dw_write, banking_dw_admin;
GRANT SELECT ON ALL TABLES IN SCHEMA banking_dw TO banking_dw_read;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA banking_dw TO banking_dw_write;
GRANT ALL ON ALL TABLES IN SCHEMA banking_dw TO banking_dw_admin;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA banking_dw TO banking_dw_write, banking_dw_admin;

-- Row Level Security (RLS) example for customer data
ALTER TABLE dim_customer ENABLE ROW LEVEL SECURITY;

-- Create policy for regional access (example)
-- CREATE POLICY customer_regional_policy ON dim_customer
--     FOR ALL TO banking_dw_read
--     USING (province = current_setting('app.user_province', TRUE));

-- =============================================
-- MAINTENANCE PROCEDURES
-- =============================================

-- Procedure for partition maintenance
CREATE OR REPLACE FUNCTION maintain_partitions()
RETURNS VOID AS $
DECLARE
    tables TEXT[] := ARRAY['fact_transaction', 'fact_account_balance', 'fact_loan_transaction', 
                          'fact_card_transaction', 'fact_investment_transaction', 'agg_daily_customer_summary'];
    table_name TEXT;
    future_date DATE := CURRENT_DATE + INTERVAL '3 months';
    partition_start DATE;
    partition_end DATE;
    partition_name TEXT;
    start_key INTEGER;
    end_key INTEGER;
BEGIN
    -- Create future partitions
    FOREACH table_name IN ARRAY tables LOOP
        partition_start := DATE_TRUNC('month', future_date);
        partition_end := partition_start + INTERVAL '1 month';
        partition_name := table_name || '_' || TO_CHAR(partition_start, 'YYYY_MM');
        start_key := TO_CHAR(partition_start, 'YYYYMMDD')::INTEGER;
        end_key := TO_CHAR(partition_end, 'YYYYMMDD')::INTEGER;
        
        -- Check if partition already exists
        IF NOT EXISTS (
            SELECT 1 FROM pg_class WHERE relname = partition_name
        ) THEN
            EXECUTE format('CREATE TABLE %I PARTITION OF %I 
                           FOR VALUES FROM (%L) TO (%L)', 
                           partition_name, table_name, start_key, end_key);
            RAISE NOTICE 'Created partition: %', partition_name;
        END IF;
    END LOOP;
    
    RAISE NOTICE 'Partition maintenance completed';
END;
$ LANGUAGE plpgsql;

-- =============================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================

-- Add table comments
COMMENT ON SCHEMA banking_dw IS 'Banking Data Warehouse Schema';

COMMENT ON TABLE dim_customer IS 'Customer dimension with SCD Type 2 implementation';
COMMENT ON TABLE dim_product IS 'Product and service catalog dimension';
COMMENT ON TABLE dim_location IS 'Branch and location dimension';
COMMENT ON TABLE dim_event IS 'Transaction event types dimension';
COMMENT ON TABLE dim_involved_party IS 'Third parties involved in transactions';
COMMENT ON TABLE dim_condition IS 'Business conditions and rules dimension';
COMMENT ON TABLE dim_application IS 'Source systems and applications dimension';
COMMENT ON TABLE dim_asset IS 'Assets related to transactions (collateral, securities)';
COMMENT ON TABLE dim_date IS 'Date dimension with fiscal calendar support';
COMMENT ON TABLE dim_time IS 'Time dimension for intraday analysis';

COMMENT ON TABLE fact_transaction IS 'Main transaction fact table - all banking transactions';
COMMENT ON TABLE fact_account_balance IS 'Daily account balance snapshots';
COMMENT ON TABLE fact_loan_transaction IS 'Loan-specific transactions (disbursements, payments)';
COMMENT ON TABLE fact_card_transaction IS 'Credit/Debit card transactions';
COMMENT ON TABLE fact_investment_transaction IS 'Investment and securities transactions';

COMMENT ON TABLE bridge_transaction_categories IS 'Many-to-many relationship between transactions and categories';
COMMENT ON TABLE agg_daily_customer_summary IS 'Pre-aggregated daily customer transaction summary';
COMMENT ON TABLE agg_monthly_product_summary IS 'Pre-aggregated monthly product performance summary';

COMMENT ON VIEW v_customer_360 IS 'Comprehensive customer view with account and transaction summary';
COMMENT ON VIEW v_transaction_summary IS 'Transaction analytics summary by various dimensions';
COMMENT ON VIEW v_risk_monitoring IS 'Risk monitoring view for suspicious activities';

-- Add column comments for key fields
COMMENT ON COLUMN dim_customer.customer_key IS 'Surrogate key for customer dimension';
COMMENT ON COLUMN dim_customer.is_current IS 'Flag indicating current version of customer record (SCD Type 2)';
COMMENT ON COLUMN fact_transaction.transaction_key IS 'Surrogate key for transaction fact';
COMMENT ON COLUMN fact_transaction.usd_amount IS 'Transaction amount converted to USD for reporting';

RAISE NOTICE 'Banking Data Warehouse schema created successfully!';
RAISE NOTICE 'Next steps:';
RAISE NOTICE '1. Configure ETL processes to load dimension data';
RAISE NOTICE '2. Set up data quality monitoring';
RAISE NOTICE '3. Schedule partition maintenance job';
RAISE NOTICE '4. Create additional indexes based on query patterns';
RAISE NOTICE '5. Set up backup and archival procedures';