-- ============================================================================
-- 04: SEMANTIC VIEW - Cash-Flow & Working Capital Forecasting
-- ============================================================================
-- Use Case: AR aging, payment behavior, cash flow forecasting, late payers,
--           credit limits, and working capital analysis
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW telco_demo.product_whl.sv_cashflow_working_capital
TABLES (
    partners AS telco_demo.product_whl.dim_partner
        PRIMARY KEY (partner_id)
        COMMENT = 'Wholesale partner dimension with credit profiles',
    invoices AS telco_demo.product_whl.fact_invoice_ar
        PRIMARY KEY (invoice_id)
        COMMENT = 'Accounts receivable invoices',
    payments AS telco_demo.product_whl.fact_payment_ar
        PRIMARY KEY (payment_id)
        COMMENT = 'Payment transactions',
    ar_aging AS telco_demo.product_whl.fact_ar_position_daily
        COMMENT = 'Daily AR position and aging'
)
RELATIONSHIPS (
    invoices_to_partners AS invoices(partner_id) REFERENCES partners(partner_id),
    payments_to_partners AS payments(partner_id) REFERENCES partners(partner_id),
    payments_to_invoices AS payments(invoice_id) REFERENCES invoices(invoice_id),
    ar_aging_to_partners AS ar_aging(partner_id) REFERENCES partners(partner_id)
)
FACTS (
    invoices.invoice_amount AS invoices.invoice_amount
        COMMENT = 'Total invoice amount',
    invoices.tax_amount AS invoices.tax_amount
        COMMENT = 'Tax amount on the invoice',
    payments.payment_amount AS payments.payment_amount
        COMMENT = 'Amount of the payment',
    payments.amount_reporting_ccy AS payments.amount_reporting_ccy
        COMMENT = 'Payment amount in reporting currency',
    ar_aging.total_open_ar AS ar_aging.total_open_ar
        COMMENT = 'Total open accounts receivable',
    ar_aging.current_bucket AS ar_aging.current_bucket
        COMMENT = 'AR amount not yet due',
    ar_aging.bucket_1_30_days AS ar_aging.bucket_1_30_days
        COMMENT = 'AR amount 1-30 days past due',
    ar_aging.bucket_31_60_days AS ar_aging.bucket_31_60_days
        COMMENT = 'AR amount 31-60 days past due',
    ar_aging.bucket_61_90_days AS ar_aging.bucket_61_90_days
        COMMENT = 'AR amount 61-90 days past due',
    ar_aging.bucket_90_plus_days AS ar_aging.bucket_90_plus_days
        COMMENT = 'AR amount more than 90 days past due',
    ar_aging.credit_limit_amt AS ar_aging.credit_limit_amt
        COMMENT = 'Credit limit snapshot'
)
DIMENSIONS (
    partners.partner_id AS partners.partner_id
        COMMENT = 'Unique partner identifier',
    partners.partner_code AS partners.partner_code
        COMMENT = 'Partner code',
    partners.partner_name AS partners.partner_name
        COMMENT = 'Full name of partner',
    partners.partner_type AS partners.partner_type
        COMMENT = 'Type of partner: MVNO, CARRIER, ENTERPRISE',
    partners.country_iso2 AS partners.country_iso2
        COMMENT = 'ISO country code',
    partners.credit_limit_amt AS partners.credit_limit_amt
        COMMENT = 'Credit limit',
    partners.payment_terms_days AS partners.payment_terms_days
        COMMENT = 'Payment terms in days',
    invoices.invoice_id AS invoices.invoice_id
        COMMENT = 'Invoice identifier',
    invoices.invoice_number AS invoices.invoice_number
        COMMENT = 'Invoice number',
    invoices.status AS invoices.status
        COMMENT = 'Invoice status',
    invoices.invoice_currency AS invoices.invoice_currency
        COMMENT = 'Invoice currency',
    invoices.billing_period_start AS invoices.billing_period_start
        COMMENT = 'Billing period start',
    invoices.billing_period_end AS invoices.billing_period_end
        COMMENT = 'Billing period end',
    invoices.issue_date AS invoices.issue_date
        COMMENT = 'Invoice issue date',
    invoices.due_date AS invoices.due_date
        COMMENT = 'Payment due date',
    payments.payment_id AS payments.payment_id
        COMMENT = 'Payment identifier',
    payments.payment_method AS payments.payment_method
        COMMENT = 'Payment method',
    payments.payment_date AS payments.payment_date
        COMMENT = 'Payment date',
    ar_aging.date_id AS ar_aging.date_id
        COMMENT = 'AR snapshot date'
)
METRICS (
    invoices.total_invoiced AS SUM(invoices.invoice_amount)
        COMMENT = 'Sum of all invoice amounts',
    invoices.invoice_count AS COUNT(DISTINCT invoices.invoice_id)
        COMMENT = 'Count of invoices',
    payments.total_payments_received AS SUM(payments.payment_amount)
        COMMENT = 'Sum of all payments received',
    payments.payment_count AS COUNT(DISTINCT payments.payment_id)
        COMMENT = 'Count of payment transactions',
    ar_aging.total_ar AS SUM(ar_aging.total_open_ar)
        COMMENT = 'Total open AR',
    ar_aging.total_current AS SUM(ar_aging.current_bucket)
        COMMENT = 'Total AR not yet due',
    ar_aging.total_overdue AS SUM(ar_aging.bucket_1_30_days + ar_aging.bucket_31_60_days + ar_aging.bucket_61_90_days + ar_aging.bucket_90_plus_days)
        COMMENT = 'Total AR that is past due',
    ar_aging.total_severely_overdue AS SUM(ar_aging.bucket_61_90_days + ar_aging.bucket_90_plus_days)
        COMMENT = 'AR more than 60 days past due'
)
COMMENT = 'Semantic view for Cash-Flow & Working Capital Forecasting in wholesale telecom';
