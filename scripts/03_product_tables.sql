-- ============================================================================
-- 03: PRODUCT TABLES (Data Transformation)
-- ============================================================================
-- Schema: telco_demo.product_whl
-- Tables: 7 transformed dimension and fact tables
-- ============================================================================

-- Dimension: Partner
CREATE OR REPLACE TABLE telco_demo.product_whl.dim_partner AS
SELECT
    partner_id,
    partner_code,
    partner_name,
    partner_type,
    country_iso2,
    credit_limit_amt,
    payment_terms_days,
    active_flag
FROM telco_demo.raw_whl.raw_partners;

-- Dimension: Time
CREATE OR REPLACE TABLE telco_demo.product_whl.dim_time AS
WITH date_spine AS (
    SELECT 
        DATEADD(day, seq4(), '2023-01-01'::DATE) as date_id
    FROM TABLE(GENERATOR(ROWCOUNT => 1100))
)
SELECT
    date_id,
    YEAR(date_id) as year,
    QUARTER(date_id) as quarter,
    MONTH(date_id) as month,
    WEEKOFYEAR(date_id) as week,
    DAY(date_id) as day_of_month
FROM date_spine
WHERE date_id <= CURRENT_DATE();

-- Fact: Invoice AR
CREATE OR REPLACE TABLE telco_demo.product_whl.fact_invoice_ar AS
SELECT
    invoice_id,
    invoice_number,
    partner_id,
    billing_period_start,
    billing_period_end,
    issue_date,
    due_date,
    invoice_currency,
    invoice_amount,
    tax_amount,
    status,
    payment_terms_days,
    created_at,
    updated_at
FROM telco_demo.raw_whl.raw_invoices;

-- Fact: Usage Daily
CREATE OR REPLACE TABLE telco_demo.product_whl.fact_usage_daily AS
SELECT
    cdr_date as usage_date,
    partner_id,
    service_type,
    traffic_direction as traffic_dir,
    destination_country as destination,
    SUM(total_events) as total_events,
    SUM(total_units) as total_units,
    SUM(rated_amount) as rated_revenue,
    currency
FROM telco_demo.raw_whl.raw_cdr_aggregates
GROUP BY 
    cdr_date,
    partner_id,
    service_type,
    traffic_direction,
    destination_country,
    currency;

-- Fact: Invoice AR Line (with validation)
CREATE OR REPLACE TABLE telco_demo.product_whl.fact_invoice_ar_line AS
WITH invoice_usage AS (
    SELECT
        l.invoice_line_id,
        l.invoice_id,
        i.partner_id,
        l.service_type,
        l.usage_start_date,
        l.usage_end_date,
        SUM(c.rated_amount) as usage_rated_amount
    FROM telco_demo.raw_whl.raw_invoice_lines l
    JOIN telco_demo.raw_whl.raw_invoices i ON l.invoice_id = i.invoice_id
    LEFT JOIN telco_demo.raw_whl.raw_cdr_aggregates c 
        ON c.partner_id = i.partner_id
        AND c.service_type = l.service_type
        AND c.cdr_date BETWEEN l.usage_start_date AND l.usage_end_date
    GROUP BY l.invoice_line_id, l.invoice_id, i.partner_id, l.service_type, 
             l.usage_start_date, l.usage_end_date
)
SELECT
    l.invoice_line_id,
    l.invoice_id,
    l.service_type,
    l.charge_category,
    l.rating_basis,
    l.usage_start_date,
    l.usage_end_date,
    l.billed_units,
    l.billed_amount,
    l.billed_currency,
    l.rate_applied,
    iu.usage_rated_amount as recomputed_amount,
    CASE
        WHEN iu.usage_rated_amount IS NULL THEN 'NO_USAGE_MATCH'
        WHEN ABS(l.billed_amount - iu.usage_rated_amount) < 0.01 THEN 'OK'
        WHEN l.billed_amount < iu.usage_rated_amount THEN 'UNDER_BILLED'
        WHEN l.billed_amount > iu.usage_rated_amount THEN 'OVER_BILLED'
        ELSE 'NO_USAGE_MATCH'
    END as validation_status,
    l.billed_amount - COALESCE(iu.usage_rated_amount, 0) as validation_delta
FROM telco_demo.raw_whl.raw_invoice_lines l
LEFT JOIN invoice_usage iu ON l.invoice_line_id = iu.invoice_line_id;

-- Fact: Payment AR
CREATE OR REPLACE TABLE telco_demo.product_whl.fact_payment_ar AS
SELECT
    payment_id,
    partner_id,
    invoice_id,
    payment_date,
    payment_amount,
    payment_currency,
    fx_rate_to_usd as fx_rate_to_reporting,
    payment_amount * COALESCE(fx_rate_to_usd, 1) as amount_reporting_ccy,
    payment_method,
    reference
FROM telco_demo.raw_whl.raw_payments;

-- Fact: AR Position Daily (Aging Buckets)
CREATE OR REPLACE TABLE telco_demo.product_whl.fact_ar_position_daily AS
WITH date_range AS (
    SELECT date_id FROM telco_demo.product_whl.dim_time
    WHERE date_id >= DATEADD(month, -6, CURRENT_DATE())
      AND date_id <= CURRENT_DATE()
),
ar_by_date AS (
    SELECT
        d.date_id,
        i.partner_id,
        i.invoice_id,
        i.due_date,
        i.invoice_amount,
        COALESCE(SUM(p.payment_amount), 0) as total_paid
    FROM date_range d
    CROSS JOIN telco_demo.product_whl.fact_invoice_ar i
    LEFT JOIN telco_demo.product_whl.fact_payment_ar p 
        ON p.invoice_id = i.invoice_id
        AND p.payment_date <= d.date_id
    WHERE i.issue_date <= d.date_id
      AND i.status NOT IN ('CANCELLED')
    GROUP BY d.date_id, i.partner_id, i.invoice_id, i.due_date, i.invoice_amount
),
ar_open AS (
    SELECT
        date_id,
        partner_id,
        invoice_id,
        due_date,
        invoice_amount - total_paid as open_amount
    FROM ar_by_date
    WHERE invoice_amount - total_paid > 0
)
SELECT
    a.date_id,
    a.partner_id,
    SUM(a.open_amount) as total_open_ar,
    SUM(CASE WHEN DATEDIFF(day, a.date_id, a.due_date) >= 0 THEN a.open_amount ELSE 0 END) as current_bucket,
    SUM(CASE WHEN DATEDIFF(day, a.due_date, a.date_id) BETWEEN 1 AND 30 THEN a.open_amount ELSE 0 END) as bucket_1_30_days,
    SUM(CASE WHEN DATEDIFF(day, a.due_date, a.date_id) BETWEEN 31 AND 60 THEN a.open_amount ELSE 0 END) as bucket_31_60_days,
    SUM(CASE WHEN DATEDIFF(day, a.due_date, a.date_id) BETWEEN 61 AND 90 THEN a.open_amount ELSE 0 END) as bucket_61_90_days,
    SUM(CASE WHEN DATEDIFF(day, a.due_date, a.date_id) > 90 THEN a.open_amount ELSE 0 END) as bucket_90_plus_days,
    p.credit_limit_amt
FROM ar_open a
JOIN telco_demo.product_whl.dim_partner p ON p.partner_id = a.partner_id
GROUP BY a.date_id, a.partner_id, p.credit_limit_amt;
