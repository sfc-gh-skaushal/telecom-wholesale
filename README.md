I want to build these Telecom wholesale specific use cases

Cash-flow & working capital forecasting
Use historical wholesale usage and payment behavior to forecast monthly cash inflows by partner and identify late-payers or partners likely to hit credit limits. Feed treasury with more accurate short-term liquidity views.
Outgoing invoice accuracy (revenue integrity)
Validate your outbound wholesale invoices against rated usage, contract terms, and previous periods. Detect under-billing (wrong rate plans, missed surcharges) before invoices are sent.

Use database telco_demo for this.
Create a new schema called raw_whl for all raw data
Create a new schema called product_whl for all processed data to store schema model described below, semantic view and create agent
Here is a breakdown of the data elements you’ll need and the hurdles you’ll likely face.

1. Cash-Flow & Working Capital Forecasting
This use case relies on the intersection of usage trends and payment velocity. You want to predict when the money actually hits the bank, not just when the invoice is generated.
Essential Data Elements
CDR (Call Detail Records) Aggregates: Total volume/minutes/data per partner, segmented by service type (Voice, SMS, Roaming).
Partner Credit Profiles: Credit limits, risk ratings, and historical "Days Sales Outstanding" (DSO).
A/R Aging Reports: Detailed history of when past invoices were issued vs. when they were paid in full.
Dispute History: Frequency and value of historical disputes (as disputes significantly delay cash inflows).
Settlement Terms: Net-30, Net-60, or bi-lateral netting agreements where you only pay the "difference" to a partner.
Potential Data Challenges
Payment Volatility: Wholesale partners often make partial payments or batch payments for multiple periods, making it hard to map cash back to specific invoices.
Currency Fluctuations: Since wholesale is global, a sudden shift in exchange rates can create a gap between forecasted and actual cash in your base currency.
Manual Adjustments: Treasury often makes "off-book" adjustments or offsets that aren't captured in the standard billing feed.

2. Outgoing Invoice Accuracy (Revenue Integrity)
This is essentially a "Pre-flight Check." You are comparing what the switch says happened against what the contract says you should charge.
Essential Data Elements
Rated CDRs: The output from your billing engine before it’s finalized.
Reference Data (The "Golden Record"):
Rate Cards: Destination-based pricing (A-Z rates), including peak/off-peak logic.
Commitment Tiers: Stepped pricing (e.g., price drops after 1M minutes).
Historical Baselines: Invoiced amounts from the same partner for the previous 3–6 months to detect anomalies.
Surcharge Logic: Data on specialized surcharges (e.g., Origin-Based Rating (OBR) for calls into the EU).
Potential Data Challenges
Rating Complexity: Wholesale contracts are notoriously messy. Trying to model "Origin-Based Rating" or "Least Cost Routing" logic in a demo can get very technical, very fast.
Data Volume: Telecoms generate billions of CDRs. For a demo, you'll need to use sampled or synthesized data that still maintains the "statistical fingerprints" of real traffic.
Mismatched Cycles: Your billing cycle might be monthly, but partner data feeds might come in weekly, leading to "hanging" usage that hasn't been invoiced yet.

The "Wholesale Data Gap"
In both cases, you will run into the Interconnect Paradox: your data says one thing, and the partner’s switch says another.
Challenge
Impact
How to address in a demo
Missing CDRs
Under-billing and lost revenue.
Include a "reconciliation" view showing gaps between Switch vs. Billing.
Late Data
"Back-billing" issues that partners often dispute.
Show an "Age of CDR" metric in your dashboard.
Contract Fluidity
Rates change weekly/daily in wholesale voice.
Show a "Rate Version" history to ensure the right price was applied.


Based on above information, please generate sample raw data for both these use cases with data having the described gaps.

And then use the above raw data to convert this data into below schema definitions, use dbt for this transformation.
Here’s a focused schema for the two use cases.

1) Shared dimensions
CREATE TABLE dim_partner (
  partner_id          NUMBER        PRIMARY KEY,
  partner_code        STRING        NOT NULL,
  partner_name        STRING        NOT NULL,
  partner_type        STRING,            -- 'MVNO','CARRIER','ENTERPRISE'
  country_iso2        STRING,
  credit_limit_amt    NUMBER(18,4),      -- for working capital / risk
  payment_terms_days  NUMBER,           -- base terms, can be overridden on invoice
  active_flag         BOOLEAN
);

CREATE TABLE dim_time (
  date_id      DATE PRIMARY KEY,
  year         INTEGER,
  quarter      INTEGER,
  month        INTEGER,
  week         INTEGER,
  day_of_month INTEGER
);

2) Outgoing invoices (AR) + revenue integrity
-- Header level: one row per outgoing invoice
CREATE TABLE fact_invoice_ar (
  invoice_id          NUMBER        PRIMARY KEY,
  invoice_number      STRING        NOT NULL,
  partner_id          NUMBER        NOT NULL REFERENCES dim_partner(partner_id),
  billing_period_start DATE         NOT NULL,
  billing_period_end   DATE         NOT NULL,
  issue_date          DATE          NOT NULL,
  due_date            DATE          NOT NULL,
  invoice_currency    STRING        NOT NULL,
  invoice_amount      NUMBER(18,6)  NOT NULL,   -- total incl. usage, fees, discounts
  tax_amount          NUMBER(18,6),
  status              STRING,                   -- 'OPEN','PARTIALLY_PAID','PAID','DISPUTED','CANCELLED'
  payment_terms_days  NUMBER,                   -- actual terms used
  created_at          TIMESTAMP_NTZ,
  updated_at          TIMESTAMP_NTZ
);

-- Line level: used for revenue integrity (compare to internal rating)
CREATE TABLE fact_invoice_ar_line (
  invoice_line_id    NUMBER        PRIMARY KEY,
  invoice_id         NUMBER        NOT NULL REFERENCES fact_invoice_ar(invoice_id),
  service_type       STRING,                   -- 'VOICE','SMS','DATA','ROAMING','FEE'
  charge_category    STRING,                   -- 'USAGE','RECURRING','ONE_OFF','DISCOUNT'
  rating_basis       STRING,                   -- 'PEAK','OFFPEAK','DESTINATION_TIER','BUNDLE'
  usage_start_date   DATE,
  usage_end_date     DATE,
  billed_units       NUMBER(20,6),             -- minutes / MB / messages
  billed_amount      NUMBER(18,6),
  billed_currency    STRING,
  rate_applied       NUMBER(18,8),
  -- revenue integrity fields (filled by validation process)
  recomputed_amount  NUMBER(18,6),
  validation_status  STRING,                   -- 'OK','UNDER_BILLED','OVER_BILLED','NO_USAGE_MATCH'
  validation_delta   NUMBER(18,6)              -- billed_amount - recomputed_amount
);

-- Internal usage & rating (your “source of truth”)
CREATE TABLE fact_usage_daily (
  usage_date    DATE        NOT NULL REFERENCES dim_time(date_id),
  partner_id    NUMBER      NOT NULL REFERENCES dim_partner(partner_id),
  service_type  STRING      NOT NULL,          -- 'VOICE','SMS','DATA','ROAMING'
  traffic_dir   STRING      NOT NULL,          -- 'OUTBOUND','INBOUND'
  destination   STRING,                        -- country / prefix / MNO
  total_events  NUMBER,
  total_units   NUMBER(20,6),
  rated_revenue NUMBER(18,6),                  -- what *you* say should be billed
  currency      STRING,
  PRIMARY KEY (usage_date, partner_id, service_type, traffic_dir, destination)
);

3) Cash-flow & working capital (AR aging, collections, forecasting)
-- Payments applied to AR invoices
CREATE TABLE fact_payment_ar (
  payment_id            NUMBER        PRIMARY KEY,
  partner_id            NUMBER        NOT NULL REFERENCES dim_partner(partner_id),
  invoice_id            NUMBER        REFERENCES fact_invoice_ar(invoice_id),
  payment_date          DATE          NOT NULL,
  payment_amount        NUMBER(18,6)  NOT NULL,
  payment_currency      STRING        NOT NULL,
  fx_rate_to_reporting  NUMBER(18,8),         -- to convert to group currency
  amount_reporting_ccy  NUMBER(18,6),         -- payment_amount * fx_rate_to_reporting
  payment_method        STRING,               -- 'WIRE','ACH','DIRECT_DEBIT','NETTING'
  reference             STRING
);

-- Optional: daily AR snapshot for fast aging & forecasting
CREATE TABLE fact_ar_position_daily (
  date_id             DATE          NOT NULL REFERENCES dim_time(date_id),
  partner_id          NUMBER        NOT NULL REFERENCES dim_partner(partner_id),
  total_open_ar       NUMBER(18,6)  NOT NULL,   -- total open receivables
  current_bucket      NUMBER(18,6),             -- not yet due
  bucket_1_30_days    NUMBER(18,6),
  bucket_31_60_days   NUMBER(18,6),
  bucket_61_90_days   NUMBER(18,6),
  bucket_90_plus_days NUMBER(18,6),
  credit_limit_amt    NUMBER(18,4),             -- snapshot from dim_partner
  PRIMARY KEY (date_id, partner_id)
);
This is the minimal backbone you need; you can add more dimensions (e.g. dim_product, dim_route) if you later want richer reporting, but the above covers:
cash-flow & working capital (invoices, payments, AR position)
outgoing invoice accuracy (usage vs billed, with validation fields).


Based on above schema model, build me two semantic views catering to the use cases described above and use these semantic views to create cortex analyst, which can use below verified queries.
Below are compact example queries for both use cases on the schema we defined.

1) Outgoing invoice accuracy (revenue integrity)
1.1 Recompute “should bill” per invoice line
Assumption:
fact_usage_daily is the source of truth.
Invoice line grain = partner × service_type × usage period.
-- Recompute expected (should-be) amount per invoice line
WITH usage_recomputed AS (
  SELECT
      i.invoice_id,
      l.invoice_line_id,
      SUM(u.rated_revenue) AS recomputed_amount
  FROM fact_invoice_ar            i
  JOIN fact_invoice_ar_line       l  ON l.invoice_id = i.invoice_id
  JOIN fact_usage_daily           u  ON u.partner_id   = i.partner_id
                                     AND u.service_type = l.service_type
                                     AND u.usage_date BETWEEN l.usage_start_date
                                                          AND l.usage_end_date
  GROUP BY i.invoice_id, l.invoice_line_id
)
UPDATE fact_invoice_ar_line l
SET
  recomputed_amount = u.recomputed_amount,
  validation_delta  = l.billed_amount - u.recomputed_amount,
  validation_status = CASE
                        WHEN ABS(l.billed_amount - u.recomputed_amount) < 0.01
                          THEN 'OK'
                        WHEN l.billed_amount < u.recomputed_amount
                          THEN 'UNDER_BILLED'
                        WHEN l.billed_amount > u.recomputed_amount
                          THEN 'OVER_BILLED'
                        ELSE 'NO_USAGE_MATCH'
                      END
FROM usage_recomputed u
WHERE l.invoice_line_id = u.invoice_line_id;
1.2 Discrepancy report by partner
SELECT
    p.partner_name,
    i.invoice_number,
    SUM(l.billed_amount)        AS total_billed,
    SUM(l.recomputed_amount)    AS total_should_be,
    SUM(l.validation_delta)     AS total_delta,
    SUM(CASE WHEN l.validation_status <> 'OK' THEN 1 ELSE 0 END) AS bad_lines
FROM fact_invoice_ar         i
JOIN fact_invoice_ar_line    l ON l.invoice_id  = i.invoice_id
JOIN dim_partner             p ON p.partner_id  = i.partner_id
WHERE i.billing_period_start >= DATEADD(month, -1, CURRENT_DATE)  -- last month
GROUP BY p.partner_name, i.invoice_number
HAVING SUM(l.validation_delta) <> 0
ORDER BY ABS(SUM(l.validation_delta)) DESC;

2) Cash‑flow & working capital forecasting
2.1 Current open AR & aging by partner (on the fly)
WITH ar_open AS (
  SELECT
      i.invoice_id,
      i.partner_id,
      i.due_date,
      (i.invoice_amount - COALESCE(SUM(p.payment_amount),0)) AS open_amount
  FROM fact_invoice_ar i
  LEFT JOIN fact_payment_ar p
         ON p.invoice_id = i.invoice_id
  WHERE i.status NOT IN ('CANCELLED')
  GROUP BY i.invoice_id, i.partner_id, i.due_date, i.invoice_amount
  HAVING (i.invoice_amount - COALESCE(SUM(p.payment_amount),0)) > 0
)
SELECT
    d.partner_name,
    SUM(open_amount)                                           AS total_open_ar,
    SUM(CASE WHEN DATEDIFF(day, CURRENT_DATE, due_date) >= 0
             THEN open_amount ELSE 0 END)                      AS current_not_due,
    SUM(CASE WHEN DATEDIFF(day, due_date, CURRENT_DATE) BETWEEN 1 AND 30
             THEN open_amount ELSE 0 END)                      AS bucket_1_30,
    SUM(CASE WHEN DATEDIFF(day, due_date, CURRENT_DATE) BETWEEN 31 AND 60
             THEN open_amount ELSE 0 END)                      AS bucket_31_60,
    SUM(CASE WHEN DATEDIFF(day, due_date, CURRENT_DATE) BETWEEN 61 AND 90
             THEN open_amount ELSE 0 END)                      AS bucket_61_90,
    SUM(CASE WHEN DATEDIFF(day, due_date, CURRENT_DATE) > 90
             THEN open_amount ELSE 0 END)                      AS bucket_90_plus
FROM ar_open o
JOIN dim_partner d ON d.partner_id = o.partner_id
GROUP BY d.partner_name
ORDER BY total_open_ar DESC;
2.2 Simple 4‑week cash‑in forecast (based on due dates)
WITH ar_open AS (
  SELECT
      i.invoice_id,
      i.partner_id,
      i.due_date,
      (i.invoice_amount - COALESCE(SUM(p.payment_amount),0)) AS open_amount
  FROM fact_invoice_ar i
  LEFT JOIN fact_payment_ar p
         ON p.invoice_id = i.invoice_id
  WHERE i.status NOT IN ('CANCELLED')
  GROUP BY i.invoice_id, i.partner_id, i.due_date, i.invoice_amount
  HAVING (i.invoice_amount - COALESCE(SUM(p.payment_amount),0)) > 0
),
calendar AS (
  SELECT
      week_start::DATE AS week_start,
      DATEADD(day, 6, week_start)::DATE AS week_end
  FROM TABLE(GENERATOR(ROWCOUNT => 4)) g,
       LATERAL (SELECT DATE_TRUNC('week', CURRENT_DATE) + (g.rowcount * 7) AS week_start)
)
SELECT
    c.week_start,
    c.week_end,
    SUM(o.open_amount) AS expected_cash_in
FROM calendar c
LEFT JOIN ar_open o
       ON o.due_date BETWEEN c.week_start AND c.week_end
GROUP BY c.week_start, c.week_end
ORDER BY c.week_start;
These three patterns cover:
updating line‑level validation status and deltas
spotting bad invoices/partners
seeing current AR aging and a basic near‑term cash‑in forecast.
Use above cortex analysts as tool and build a snowflake agent to call both these under.
Use name wholesale_fin_agent


