-- ============================================================================
-- 06: CORTEX AGENT - Wholesale Finance Agent
-- ============================================================================
-- Agent: wholesale_fin_agent
-- Tools: cashflow_analyst, revenue_integrity_analyst
-- ============================================================================

CREATE OR REPLACE AGENT telco_demo.product_whl.wholesale_fin_agent
  COMMENT = 'Wholesale Finance Agent for Cash-Flow Forecasting and Revenue Integrity analysis'
  PROFILE = '{"display_name": "Wholesale Finance Agent"}'
  FROM SPECIFICATION $$
  {
    "models": {
      "orchestration": "claude-4-sonnet"
    },
    "instructions": {
      "orchestration": "You are a wholesale telecom finance assistant. Use the cashflow_analyst tool for questions about AR aging, payment behavior, cash flow forecasting, late payers, credit limits, and working capital. Use the revenue_integrity_analyst tool for questions about invoice accuracy, billing discrepancies, under-billing, over-billing, and revenue leakage.",
      "response": "Provide clear, concise financial insights. Format numbers appropriately for finance users. Highlight any concerning trends or anomalies in the data."
    },
    "tools": [
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "cashflow_analyst",
          "description": "Analyze AR aging, payment behavior, cash flow forecasting, late payers, credit limits, and working capital metrics for wholesale partners."
        }
      },
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "revenue_integrity_analyst",
          "description": "Analyze invoice accuracy, billing discrepancies, under-billing, over-billing, and revenue integrity metrics for wholesale invoices."
        }
      }
    ],
    "tool_resources": {
      "cashflow_analyst": {
        "semantic_view": "telco_demo.product_whl.sv_cashflow_working_capital",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "SID_WH"
        },
        "query_timeout": 120
      },
      "revenue_integrity_analyst": {
        "semantic_view": "telco_demo.product_whl.sv_revenue_integrity",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "SID_WH"
        },
        "query_timeout": 120
      }
    }
  }
  $$;
