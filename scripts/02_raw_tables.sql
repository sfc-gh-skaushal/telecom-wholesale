-- ============================================================================
-- 02: RAW DATA TABLES
-- ============================================================================
-- Schema: telco_demo.raw_whl
-- Tables: 8 raw tables with sample data
-- ============================================================================

-- Partners Table
CREATE OR REPLACE TABLE telco_demo.raw_whl.raw_partners (
    partner_id NUMBER,
    partner_code STRING,
    partner_name STRING,
    partner_type STRING,
    country_iso2 STRING,
    credit_limit_amt NUMBER(18,4),
    payment_terms_days NUMBER,
    risk_rating STRING,
    historical_dso_days NUMBER,
    active_flag BOOLEAN,
    netting_agreement_flag BOOLEAN,
    created_at TIMESTAMP_NTZ
);

INSERT INTO telco_demo.raw_whl.raw_partners VALUES
(1, 'MVNO001', 'TelcoMobile Virtual', 'MVNO', 'US', 5000000.00, 30, 'A', 28, TRUE, FALSE, '2020-01-15 10:00:00'),
(2, 'CARR001', 'GlobalTel Carrier', 'CARRIER', 'GB', 15000000.00, 45, 'AA', 42, TRUE, TRUE, '2019-06-20 14:30:00'),
(3, 'ENT001', 'Enterprise Connect Ltd', 'ENTERPRISE', 'DE', 2000000.00, 30, 'B', 55, TRUE, FALSE, '2021-03-10 09:15:00'),
(4, 'CARR002', 'PacificNet Telecom', 'CARRIER', 'JP', 25000000.00, 60, 'AA', 35, TRUE, TRUE, '2018-09-05 11:45:00'),
(5, 'MVNO002', 'BudgetMobile Inc', 'MVNO', 'US', 1500000.00, 30, 'C', 78, TRUE, FALSE, '2022-01-20 16:00:00'),
(6, 'CARR003', 'EuroConnect SA', 'CARRIER', 'FR', 12000000.00, 45, 'A', 38, TRUE, TRUE, '2019-11-12 08:30:00'),
(7, 'ENT002', 'TechCorp Communications', 'ENTERPRISE', 'US', 3000000.00, 30, 'B', 48, TRUE, FALSE, '2020-07-25 13:20:00'),
(8, 'MVNO003', 'ValueConnect Mobile', 'MVNO', 'GB', 800000.00, 30, 'C', 92, TRUE, FALSE, '2023-02-14 10:45:00'),
(9, 'CARR004', 'AfricaTel International', 'CARRIER', 'ZA', 8000000.00, 60, 'B', 65, TRUE, FALSE, '2020-04-30 15:10:00'),
(10, 'ENT003', 'FinServ Global', 'ENTERPRISE', 'SG', 4500000.00, 30, 'A', 32, TRUE, FALSE, '2021-08-18 09:00:00'),
(11, 'CARR005', 'LatAmTel Network', 'CARRIER', 'BR', 10000000.00, 45, 'B', 58, TRUE, TRUE, '2019-12-01 12:30:00'),
(12, 'MVNO004', 'SmartSIM Wireless', 'MVNO', 'CA', 2200000.00, 30, 'B', 45, TRUE, FALSE, '2022-05-10 14:15:00');

-- CDR Aggregates Table (with data gaps)
CREATE OR REPLACE TABLE telco_demo.raw_whl.raw_cdr_aggregates (
    record_id NUMBER AUTOINCREMENT,
    cdr_date DATE,
    partner_id NUMBER,
    service_type STRING,
    traffic_direction STRING,
    destination_country STRING,
    destination_prefix STRING,
    total_events NUMBER,
    total_units NUMBER(20,6),
    rated_amount NUMBER(18,6),
    currency STRING,
    rating_version STRING,
    cdr_source STRING,
    cdr_age_hours NUMBER,
    reconciliation_status STRING,
    created_at TIMESTAMP_NTZ
);

INSERT INTO telco_demo.raw_whl.raw_cdr_aggregates 
(cdr_date, partner_id, service_type, traffic_direction, destination_country, destination_prefix, 
 total_events, total_units, rated_amount, currency, rating_version, cdr_source, cdr_age_hours, 
 reconciliation_status, created_at)
SELECT 
    DATEADD(day, -seq4() % 180, CURRENT_DATE()) as cdr_date,
    (seq4() % 12) + 1 as partner_id,
    CASE seq4() % 4 WHEN 0 THEN 'VOICE' WHEN 1 THEN 'SMS' WHEN 2 THEN 'DATA' ELSE 'ROAMING' END as service_type,
    CASE seq4() % 2 WHEN 0 THEN 'OUTBOUND' ELSE 'INBOUND' END as traffic_direction,
    CASE seq4() % 10 WHEN 0 THEN 'US' WHEN 1 THEN 'GB' WHEN 2 THEN 'DE' WHEN 3 THEN 'FR' WHEN 4 THEN 'JP' 
        WHEN 5 THEN 'AU' WHEN 6 THEN 'BR' WHEN 7 THEN 'IN' WHEN 8 THEN 'ZA' ELSE 'CA' END as destination_country,
    '+' || CAST(UNIFORM(1, 99, RANDOM()) as STRING) as destination_prefix,
    UNIFORM(100, 50000, RANDOM()) as total_events,
    UNIFORM(1000, 500000, RANDOM())::NUMBER(20,6) as total_units,
    UNIFORM(100, 50000, RANDOM())::NUMBER(18,6) as rated_amount,
    CASE seq4() % 3 WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' ELSE 'GBP' END as currency,
    'V' || CAST(2024 + (seq4() % 3) as STRING) || '.' || CAST((seq4() % 12) + 1 as STRING) as rating_version,
    CASE seq4() % 5 WHEN 0 THEN 'SWITCH_A' WHEN 1 THEN 'SWITCH_B' WHEN 2 THEN 'MEDIATION' 
        WHEN 3 THEN 'PARTNER_FEED' ELSE 'RECONCILED' END as cdr_source,
    CASE WHEN seq4() % 20 = 0 THEN UNIFORM(200, 720, RANDOM()) ELSE UNIFORM(1, 48, RANDOM()) END as cdr_age_hours,
    CASE seq4() % 15 WHEN 0 THEN 'MISSING_FROM_SWITCH' WHEN 1 THEN 'MISMATCH' WHEN 2 THEN 'LATE_DATA' 
        ELSE 'MATCHED' END as reconciliation_status,
    DATEADD(hour, -UNIFORM(1, 168, RANDOM()), CURRENT_TIMESTAMP()) as created_at
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

-- Invoices Table
CREATE OR REPLACE TABLE telco_demo.raw_whl.raw_invoices (
    invoice_id NUMBER AUTOINCREMENT,
    invoice_number STRING,
    partner_id NUMBER,
    billing_period_start DATE,
    billing_period_end DATE,
    issue_date DATE,
    due_date DATE,
    invoice_currency STRING,
    invoice_amount NUMBER(18,6),
    tax_amount NUMBER(18,6),
    status STRING,
    payment_terms_days NUMBER,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

INSERT INTO telco_demo.raw_whl.raw_invoices
(invoice_number, partner_id, billing_period_start, billing_period_end, issue_date, due_date,
 invoice_currency, invoice_amount, tax_amount, status, payment_terms_days, created_at, updated_at)
SELECT 
    'INV-' || CAST(2024 as STRING) || '-' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY seq4()) as STRING), 6, '0') as invoice_number,
    (seq4() % 12) + 1 as partner_id,
    DATE_TRUNC('month', DATEADD(month, -(seq4() % 12), CURRENT_DATE())) as billing_period_start,
    LAST_DAY(DATE_TRUNC('month', DATEADD(month, -(seq4() % 12), CURRENT_DATE()))) as billing_period_end,
    DATEADD(day, 5, LAST_DAY(DATE_TRUNC('month', DATEADD(month, -(seq4() % 12), CURRENT_DATE())))) as issue_date,
    DATEADD(day, 5 + CASE (seq4() % 12) + 1 
        WHEN 2 THEN 45 WHEN 4 THEN 60 WHEN 6 THEN 45 WHEN 9 THEN 60 WHEN 11 THEN 45 ELSE 30 END,
        LAST_DAY(DATE_TRUNC('month', DATEADD(month, -(seq4() % 12), CURRENT_DATE())))) as due_date,
    CASE seq4() % 3 WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' ELSE 'GBP' END as invoice_currency,
    UNIFORM(10000, 500000, RANDOM())::NUMBER(18,6) as invoice_amount,
    UNIFORM(500, 25000, RANDOM())::NUMBER(18,6) as tax_amount,
    CASE seq4() % 10 
        WHEN 0 THEN 'DISPUTED' 
        WHEN 1 THEN 'PARTIALLY_PAID' 
        WHEN 2 THEN 'OPEN'
        WHEN 3 THEN 'OPEN'
        WHEN 4 THEN 'OPEN'
        WHEN 5 THEN 'PAID'
        WHEN 6 THEN 'PAID'
        WHEN 7 THEN 'PAID'
        WHEN 8 THEN 'PAID'
        ELSE 'CANCELLED' 
    END as status,
    CASE (seq4() % 12) + 1 WHEN 2 THEN 45 WHEN 4 THEN 60 WHEN 6 THEN 45 WHEN 9 THEN 60 WHEN 11 THEN 45 ELSE 30 END as payment_terms_days,
    DATEADD(day, 5, LAST_DAY(DATE_TRUNC('month', DATEADD(month, -(seq4() % 12), CURRENT_DATE())))) as created_at,
    CURRENT_TIMESTAMP() as updated_at
FROM TABLE(GENERATOR(ROWCOUNT => 150));

-- Invoice Lines Table (with billing discrepancies)
CREATE OR REPLACE TABLE telco_demo.raw_whl.raw_invoice_lines (
    invoice_line_id NUMBER AUTOINCREMENT,
    invoice_id NUMBER,
    service_type STRING,
    charge_category STRING,
    rating_basis STRING,
    usage_start_date DATE,
    usage_end_date DATE,
    billed_units NUMBER(20,6),
    billed_amount NUMBER(18,6),
    billed_currency STRING,
    rate_applied NUMBER(18,8),
    rate_plan_version STRING,
    created_at TIMESTAMP_NTZ
);

INSERT INTO telco_demo.raw_whl.raw_invoice_lines
(invoice_id, service_type, charge_category, rating_basis, usage_start_date, usage_end_date,
 billed_units, billed_amount, billed_currency, rate_applied, rate_plan_version, created_at)
SELECT 
    (seq4() % 150) + 1 as invoice_id,
    CASE seq4() % 5 WHEN 0 THEN 'VOICE' WHEN 1 THEN 'SMS' WHEN 2 THEN 'DATA' WHEN 3 THEN 'ROAMING' ELSE 'FEE' END as service_type,
    CASE seq4() % 4 WHEN 0 THEN 'USAGE' WHEN 1 THEN 'RECURRING' WHEN 2 THEN 'ONE_OFF' ELSE 'DISCOUNT' END as charge_category,
    CASE seq4() % 4 WHEN 0 THEN 'PEAK' WHEN 1 THEN 'OFFPEAK' WHEN 2 THEN 'DESTINATION_TIER' ELSE 'BUNDLE' END as rating_basis,
    DATE_TRUNC('month', DATEADD(month, -((seq4() % 150) / 12), CURRENT_DATE())) as usage_start_date,
    LAST_DAY(DATE_TRUNC('month', DATEADD(month, -((seq4() % 150) / 12), CURRENT_DATE()))) as usage_end_date,
    UNIFORM(1000, 100000, RANDOM())::NUMBER(20,6) as billed_units,
    CASE 
        WHEN seq4() % 20 = 0 THEN UNIFORM(100, 10000, RANDOM())::NUMBER(18,6) * 0.85  -- Under-billing
        WHEN seq4() % 25 = 0 THEN UNIFORM(100, 10000, RANDOM())::NUMBER(18,6) * 1.15  -- Over-billing
        ELSE UNIFORM(100, 10000, RANDOM())::NUMBER(18,6)
    END as billed_amount,
    CASE seq4() % 3 WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' ELSE 'GBP' END as billed_currency,
    CASE 
        WHEN seq4() % 20 = 0 THEN 0.0085
        WHEN seq4() % 25 = 0 THEN 0.0115
        ELSE 0.01
    END::NUMBER(18,8) as rate_applied,
    'V2024.' || CAST((seq4() % 12) + 1 as STRING) as rate_plan_version,
    CURRENT_TIMESTAMP() as created_at
FROM TABLE(GENERATOR(ROWCOUNT => 600));

-- Payments Table (with partial payments and delays)
CREATE OR REPLACE TABLE telco_demo.raw_whl.raw_payments (
    payment_id NUMBER AUTOINCREMENT,
    partner_id NUMBER,
    invoice_id NUMBER,
    payment_date DATE,
    payment_amount NUMBER(18,6),
    payment_currency STRING,
    fx_rate_to_usd NUMBER(18,8),
    payment_method STRING,
    reference STRING,
    is_partial BOOLEAN,
    netting_offset_amount NUMBER(18,6),
    created_at TIMESTAMP_NTZ
);

INSERT INTO telco_demo.raw_whl.raw_payments
(partner_id, invoice_id, payment_date, payment_amount, payment_currency, fx_rate_to_usd,
 payment_method, reference, is_partial, netting_offset_amount, created_at)
SELECT 
    (seq4() % 12) + 1 as partner_id,
    CASE WHEN seq4() % 5 = 0 THEN NULL ELSE (seq4() % 120) + 1 END as invoice_id,
    DATEADD(day, UNIFORM(0, 90, RANDOM()), DATEADD(month, -(seq4() % 10), CURRENT_DATE())) as payment_date,
    CASE 
        WHEN seq4() % 8 = 0 THEN UNIFORM(5000, 50000, RANDOM())::NUMBER(18,6)  -- Partial payment
        ELSE UNIFORM(10000, 300000, RANDOM())::NUMBER(18,6)
    END as payment_amount,
    CASE seq4() % 3 WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' ELSE 'GBP' END as payment_currency,
    CASE CASE seq4() % 3 WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' ELSE 'GBP' END
        WHEN 'USD' THEN 1.0
        WHEN 'EUR' THEN UNIFORM(105, 115, RANDOM()) / 100.0
        ELSE UNIFORM(125, 135, RANDOM()) / 100.0
    END::NUMBER(18,8) as fx_rate_to_usd,
    CASE seq4() % 4 WHEN 0 THEN 'WIRE' WHEN 1 THEN 'ACH' WHEN 2 THEN 'DIRECT_DEBIT' ELSE 'NETTING' END as payment_method,
    'PMT-' || CAST(2024 as STRING) || '-' || LPAD(CAST(seq4() as STRING), 8, '0') as reference,
    seq4() % 8 = 0 as is_partial,
    CASE WHEN seq4() % 4 = 3 THEN UNIFORM(1000, 50000, RANDOM())::NUMBER(18,6) ELSE 0 END as netting_offset_amount,
    CURRENT_TIMESTAMP() as created_at
FROM TABLE(GENERATOR(ROWCOUNT => 200));

-- Disputes Table
CREATE OR REPLACE TABLE telco_demo.raw_whl.raw_disputes (
    dispute_id NUMBER AUTOINCREMENT,
    partner_id NUMBER,
    invoice_id NUMBER,
    dispute_date DATE,
    disputed_amount NUMBER(18,6),
    dispute_reason STRING,
    dispute_status STRING,
    resolution_date DATE,
    resolution_amount NUMBER(18,6),
    created_at TIMESTAMP_NTZ
);

INSERT INTO telco_demo.raw_whl.raw_disputes
(partner_id, invoice_id, dispute_date, disputed_amount, dispute_reason, dispute_status, 
 resolution_date, resolution_amount, created_at)
SELECT 
    (seq4() % 12) + 1 as partner_id,
    (seq4() % 100) + 1 as invoice_id,
    DATEADD(day, UNIFORM(5, 30, RANDOM()), DATEADD(month, -(seq4() % 8), CURRENT_DATE())) as dispute_date,
    UNIFORM(500, 25000, RANDOM())::NUMBER(18,6) as disputed_amount,
    CASE seq4() % 6 
        WHEN 0 THEN 'MISSING_CDRS'
        WHEN 1 THEN 'WRONG_RATE_APPLIED'
        WHEN 2 THEN 'DUPLICATE_CHARGES'
        WHEN 3 THEN 'PERIOD_MISMATCH'
        WHEN 4 THEN 'SURCHARGE_DISPUTE'
        ELSE 'VOLUME_DISCREPANCY'
    END as dispute_reason,
    CASE seq4() % 4 
        WHEN 0 THEN 'OPEN'
        WHEN 1 THEN 'UNDER_REVIEW'
        WHEN 2 THEN 'RESOLVED'
        ELSE 'REJECTED'
    END as dispute_status,
    CASE seq4() % 4 
        WHEN 2 THEN DATEADD(day, UNIFORM(10, 60, RANDOM()), DATEADD(month, -(seq4() % 8), CURRENT_DATE()))
        WHEN 3 THEN DATEADD(day, UNIFORM(10, 45, RANDOM()), DATEADD(month, -(seq4() % 8), CURRENT_DATE()))
        ELSE NULL
    END as resolution_date,
    CASE seq4() % 4 
        WHEN 2 THEN UNIFORM(200, 20000, RANDOM())::NUMBER(18,6)
        WHEN 3 THEN 0
        ELSE NULL
    END as resolution_amount,
    CURRENT_TIMESTAMP() as created_at
FROM TABLE(GENERATOR(ROWCOUNT => 50));

-- Rate Cards Table
CREATE OR REPLACE TABLE telco_demo.raw_whl.raw_rate_cards (
    rate_card_id NUMBER AUTOINCREMENT,
    partner_id NUMBER,
    service_type STRING,
    destination_country STRING,
    destination_prefix STRING,
    rate_per_unit NUMBER(18,8),
    currency STRING,
    effective_from DATE,
    effective_to DATE,
    rate_version STRING,
    commitment_tier STRING,
    peak_rate NUMBER(18,8),
    offpeak_rate NUMBER(18,8),
    created_at TIMESTAMP_NTZ
);

INSERT INTO telco_demo.raw_whl.raw_rate_cards
(partner_id, service_type, destination_country, destination_prefix, rate_per_unit, currency,
 effective_from, effective_to, rate_version, commitment_tier, peak_rate, offpeak_rate, created_at)
SELECT 
    (seq4() % 12) + 1 as partner_id,
    CASE seq4() % 4 WHEN 0 THEN 'VOICE' WHEN 1 THEN 'SMS' WHEN 2 THEN 'DATA' ELSE 'ROAMING' END as service_type,
    CASE seq4() % 10 WHEN 0 THEN 'US' WHEN 1 THEN 'GB' WHEN 2 THEN 'DE' WHEN 3 THEN 'FR' WHEN 4 THEN 'JP' 
        WHEN 5 THEN 'AU' WHEN 6 THEN 'BR' WHEN 7 THEN 'IN' WHEN 8 THEN 'ZA' ELSE 'CA' END as destination_country,
    '+' || CAST(UNIFORM(1, 99, RANDOM()) as STRING) as destination_prefix,
    UNIFORM(5, 150, RANDOM()) / 10000.0 as rate_per_unit,
    CASE seq4() % 3 WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' ELSE 'GBP' END as currency,
    DATEADD(month, -(seq4() % 24), CURRENT_DATE()) as effective_from,
    CASE WHEN seq4() % 4 = 0 THEN DATEADD(month, -(seq4() % 12), CURRENT_DATE()) ELSE NULL END as effective_to,
    'V2024.' || CAST(((seq4() / 50) % 12) + 1 as STRING) as rate_version,
    CASE seq4() % 3 WHEN 0 THEN 'TIER_1_1M_PLUS' WHEN 1 THEN 'TIER_2_500K_1M' ELSE 'TIER_3_UNDER_500K' END as commitment_tier,
    (UNIFORM(5, 150, RANDOM()) / 10000.0) * 1.2 as peak_rate,
    (UNIFORM(5, 150, RANDOM()) / 10000.0) * 0.8 as offpeak_rate,
    CURRENT_TIMESTAMP() as created_at
FROM TABLE(GENERATOR(ROWCOUNT => 500));

-- FX Rates Table
CREATE OR REPLACE TABLE telco_demo.raw_whl.raw_fx_rates (
    rate_date DATE,
    from_currency STRING,
    to_currency STRING,
    exchange_rate NUMBER(18,8),
    created_at TIMESTAMP_NTZ
);

INSERT INTO telco_demo.raw_whl.raw_fx_rates
SELECT 
    DATEADD(day, -seq4(), CURRENT_DATE()) as rate_date,
    src.currency as from_currency,
    'USD' as to_currency,
    CASE src.currency
        WHEN 'USD' THEN 1.0
        WHEN 'EUR' THEN 1.08 + (UNIFORM(-5, 5, RANDOM()) / 100.0)
        WHEN 'GBP' THEN 1.27 + (UNIFORM(-5, 5, RANDOM()) / 100.0)
    END as exchange_rate,
    CURRENT_TIMESTAMP() as created_at
FROM TABLE(GENERATOR(ROWCOUNT => 365)) g
CROSS JOIN (SELECT 'USD' as currency UNION SELECT 'EUR' UNION SELECT 'GBP') src;
