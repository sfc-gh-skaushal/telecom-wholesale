-- ============================================================================
-- 05: SEMANTIC VIEW - Revenue Integrity
-- ============================================================================
-- Use Case: Invoice accuracy, billing discrepancies, under-billing,
--           over-billing, and revenue leakage analysis
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW telco_demo.product_whl.sv_revenue_integrity
TABLES (
    partners AS telco_demo.product_whl.dim_partner
        PRIMARY KEY (partner_id)
        COMMENT = 'Wholesale partner dimension',
    invoices AS telco_demo.product_whl.fact_invoice_ar
        PRIMARY KEY (invoice_id)
        COMMENT = 'Invoice headers',
    invoice_lines AS telco_demo.product_whl.fact_invoice_ar_line
        PRIMARY KEY (invoice_line_id)
        COMMENT = 'Invoice line items with validation status',
    usage AS telco_demo.product_whl.fact_usage_daily
        COMMENT = 'Daily usage - source of truth'
)
RELATIONSHIPS (
    invoices_to_partners AS invoices(partner_id) REFERENCES partners(partner_id),
    invoice_lines_to_invoices AS invoice_lines(invoice_id) REFERENCES invoices(invoice_id),
    usage_to_partners AS usage(partner_id) REFERENCES partners(partner_id)
)
FACTS (
    invoices.invoice_amount AS invoices.invoice_amount
        COMMENT = 'Total invoice amount',
    invoice_lines.billed_units AS invoice_lines.billed_units
        COMMENT = 'Number of units billed',
    invoice_lines.billed_amount AS invoice_lines.billed_amount
        COMMENT = 'Amount charged on the invoice',
    invoice_lines.rate_applied AS invoice_lines.rate_applied
        COMMENT = 'Rate applied per unit',
    invoice_lines.recomputed_amount AS invoice_lines.recomputed_amount
        COMMENT = 'Amount that should have been billed',
    invoice_lines.validation_delta AS invoice_lines.validation_delta
        COMMENT = 'Difference between billed and expected',
    usage.total_events AS usage.total_events
        COMMENT = 'Number of usage events',
    usage.total_units AS usage.total_units
        COMMENT = 'Total usage units',
    usage.rated_revenue AS usage.rated_revenue
        COMMENT = 'Rated revenue from usage'
)
DIMENSIONS (
    partners.partner_id AS partners.partner_id
        COMMENT = 'Partner identifier',
    partners.partner_code AS partners.partner_code
        COMMENT = 'Partner code',
    partners.partner_name AS partners.partner_name
        COMMENT = 'Full name of partner',
    partners.partner_type AS partners.partner_type
        COMMENT = 'Type of partner',
    partners.country_iso2 AS partners.country_iso2
        COMMENT = 'ISO country code',
    invoices.invoice_id AS invoices.invoice_id
        COMMENT = 'Invoice identifier',
    invoices.invoice_number AS invoices.invoice_number
        COMMENT = 'Invoice number',
    invoices.status AS invoices.status
        COMMENT = 'Invoice status',
    invoices.billing_period_start AS invoices.billing_period_start
        COMMENT = 'Billing period start',
    invoices.billing_period_end AS invoices.billing_period_end
        COMMENT = 'Billing period end',
    invoices.issue_date AS invoices.issue_date
        COMMENT = 'Invoice issue date',
    invoice_lines.invoice_line_id AS invoice_lines.invoice_line_id
        COMMENT = 'Invoice line identifier',
    invoice_lines.service_type AS invoice_lines.service_type
        COMMENT = 'Type of service: VOICE, SMS, DATA, ROAMING, FEE',
    invoice_lines.charge_category AS invoice_lines.charge_category
        COMMENT = 'Charge category: USAGE, RECURRING, ONE_OFF, DISCOUNT',
    invoice_lines.rating_basis AS invoice_lines.rating_basis
        COMMENT = 'Rating basis: PEAK, OFFPEAK, DESTINATION_TIER, BUNDLE',
    invoice_lines.validation_status AS invoice_lines.validation_status
        COMMENT = 'Validation result: OK, UNDER_BILLED, OVER_BILLED, NO_USAGE_MATCH',
    invoice_lines.usage_start_date AS invoice_lines.usage_start_date
        COMMENT = 'Usage period start',
    invoice_lines.usage_end_date AS invoice_lines.usage_end_date
        COMMENT = 'Usage period end',
    usage.usage_date AS usage.usage_date
        COMMENT = 'Date of usage',
    usage.service_type AS usage.service_type
        COMMENT = 'Service type',
    usage.traffic_dir AS usage.traffic_dir
        COMMENT = 'Traffic direction',
    usage.destination AS usage.destination
        COMMENT = 'Destination country'
)
METRICS (
    invoices.total_invoiced AS SUM(invoices.invoice_amount)
        COMMENT = 'Sum of invoice amounts',
    invoices.invoice_count AS COUNT(DISTINCT invoices.invoice_id)
        COMMENT = 'Number of invoices',
    invoice_lines.total_billed AS SUM(invoice_lines.billed_amount)
        COMMENT = 'Sum of all billed amounts',
    invoice_lines.total_expected AS SUM(invoice_lines.recomputed_amount)
        COMMENT = 'Sum of expected amounts',
    invoice_lines.total_discrepancy AS SUM(invoice_lines.validation_delta)
        COMMENT = 'Total billing discrepancy',
    invoice_lines.under_billed_amount AS SUM(CASE WHEN invoice_lines.validation_status = 'UNDER_BILLED' THEN ABS(invoice_lines.validation_delta) ELSE 0 END)
        COMMENT = 'Total amount under-billed',
    invoice_lines.over_billed_amount AS SUM(CASE WHEN invoice_lines.validation_status = 'OVER_BILLED' THEN invoice_lines.validation_delta ELSE 0 END)
        COMMENT = 'Total amount over-billed',
    invoice_lines.line_count AS COUNT(DISTINCT invoice_lines.invoice_line_id)
        COMMENT = 'Number of invoice lines',
    invoice_lines.error_line_count AS COUNT(CASE WHEN invoice_lines.validation_status <> 'OK' THEN 1 END)
        COMMENT = 'Number of lines with errors',
    usage.total_usage_events AS SUM(usage.total_events)
        COMMENT = 'Sum of usage events',
    usage.total_usage_units AS SUM(usage.total_units)
        COMMENT = 'Sum of usage units',
    usage.total_rated_revenue AS SUM(usage.rated_revenue)
        COMMENT = 'Sum of rated revenue'
)
COMMENT = 'Semantic view for Outgoing Invoice Accuracy (Revenue Integrity) in wholesale telecom';
