#!/usr/bin/env python3
"""
Fundamental Analysis Engine
==========================

Comprehensive fundamental analysis system for deep-dive company evaluation.
Integrates financial statement analysis, valuation modeling, peer comparison,
and risk assessment to provide holistic investment analysis.

Core Analysis Components:
------------------------
1. **Financial Statement Analysis**:
   - Income Statement: Revenue, profit margins, earnings quality
   - Balance Sheet: Assets, liabilities, equity structure
   - Cash Flow Statement: Operating, investing, financing flows
   - Statement Trends: Multi-period growth and ratio analysis

2. **Valuation Models**:
   - DCF (Discounted Cash Flow): Intrinsic value calculation
   - P/E Ratio Analysis: Earnings-based valuation
   - P/B Ratio Analysis: Book value-based valuation
   - PEG Ratio: Growth-adjusted P/E analysis
   - EV/EBITDA: Enterprise value multiples

3. **Financial Ratios**:
   - **Profitability**: ROE, ROA, gross/net margins
   - **Liquidity**: Current ratio, quick ratio, cash ratio
   - **Leverage**: Debt-to-equity, interest coverage
   - **Efficiency**: Asset turnover, inventory turnover
   - **Growth**: Revenue growth, earnings growth, dividend growth

4. **Quality Metrics**:
   - Earnings Quality: Accruals, cash conversion
   - Management Effectiveness: ROE trends, capital allocation
   - Competitive Position: Market share, moats analysis
   - Corporate Governance: Board structure, compensation

Peer Comparison Analysis:
------------------------
- **Industry Benchmarking**: Compare against sector averages
- **Competitor Analysis**: Direct competitor evaluation
- **Relative Valuation**: Peer multiple analysis
- **Market Position**: Competitive advantage assessment
- **Performance Ranking**: Percentile-based peer ranking

Risk Assessment Integration:
---------------------------
- **Business Risk**: Industry and company-specific risks
- **Financial Risk**: Leverage and liquidity analysis
- **Market Risk**: Beta, correlation analysis
- **Operational Risk**: Management and execution risks
- **ESG Risk**: Environmental, social, governance factors

Data Sources Integration:
------------------------
- **yfinance**: Basic financials and market data
- **SEC Filings**: 10-K, 10-Q, 8-K regulatory filings
- **Earnings Data**: Quarterly and annual earnings reports
- **Analyst Estimates**: Consensus estimates and revisions
- **Economic Data**: Macro indicators affecting valuation

Valuation Methodologies:
-----------------------
```python
# DCF Valuation Model
def calculate_dcf_valuation(cash_flows, discount_rate, terminal_growth):
    # Multi-stage DCF calculation
    # Present value of cash flows
    # Terminal value calculation
    # Risk-adjusted discount rates
    
# Comparable Company Analysis
def peer_valuation_analysis(target_ticker, peer_tickers):
    # Multiple-based valuation
    # Peer group selection
    # Relative valuation metrics
```

Financial Health Scoring:
------------------------
- **Altman Z-Score**: Bankruptcy prediction model
- **Piotroski F-Score**: Financial strength assessment
- **Custom Health Score**: Proprietary scoring algorithm
- **Credit Rating Proxy**: Credit worthiness estimation

Growth Analysis:
---------------
- **Historical Growth**: Revenue, earnings, cash flow trends
- **Future Projections**: Analyst consensus and model forecasts
- **Growth Quality**: Sustainable vs unsustainable growth
- **Investment Returns**: ROIC, ROE sustainability analysis

Dividend Analysis:
-----------------
- **Dividend Yield**: Current and historical yields
- **Dividend Growth**: Historical dividend growth rates
- **Payout Ratios**: Sustainability analysis
- **Dividend Coverage**: Earnings and cash flow coverage

Management Analysis:
-------------------
- **Capital Allocation**: ROIC, reinvestment rates
- **Management Guidance**: Track record of meeting guidance
- **Share Buybacks**: Value creation vs destruction
- **Strategic Initiatives**: Growth strategy effectiveness

Industry Analysis:
-----------------
- **Industry Trends**: Sector growth and cyclicality
- **Competitive Landscape**: Market structure analysis
- **Regulatory Environment**: Impact of regulations
- **Technology Disruption**: Digital transformation risks/opportunities

Output Reports:
--------------
- **Executive Summary**: Key findings and investment thesis
- **Detailed Analysis**: Comprehensive financial analysis
- **Valuation Summary**: Multiple valuation approaches
- **Risk Assessment**: Key risks and mitigation strategies
- **Investment Recommendation**: Buy/Hold/Sell with rationale

Integration Points:
------------------
- Called by automation_scripts/pipeline.py for complete analysis
- Uses earnings_reports/data_collector.py for financial data
- Integrates with risk_analysis.py for comprehensive risk assessment
- Provides data for report_generator.py fundamental sections

Usage Examples:
--------------
```python
# Comprehensive fundamental analysis
analysis = FundamentalAnalyzer()
result = analysis.analyze_company(
    ticker='AAPL',
    peer_tickers=['GOOGL', 'MSFT', 'AMZN'],
    analysis_depth='comprehensive'
)

# Valuation analysis
valuation = analysis.calculate_intrinsic_value(
    ticker='AAPL',
    methods=['DCF', 'P/E', 'EV/EBITDA']
)

# Peer comparison
peer_analysis = analysis.compare_to_peers(
    target='AAPL',
    peers=['GOOGL', 'MSFT'],
    metrics=['P/E', 'ROE', 'Growth']
)
```

Quality Assurance:
-----------------
- **Data Validation**: Financial statement consistency checks
- **Calculation Verification**: Cross-validation of ratios and metrics
- **Outlier Detection**: Identification of unusual financial metrics
- **Historical Consistency**: Trend validation and anomaly detection

Performance Metrics:
-------------------
- **Analysis Accuracy**: Historical prediction accuracy tracking
- **Valuation Precision**: Intrinsic value vs market price analysis
- **Risk Prediction**: Risk assessment validation
- **Peer Ranking Stability**: Consistency of peer comparisons

Author: TickZen Development Team
Version: 2.8
Last Updated: January 2026
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime # Import datetime
import re
# Import the new analysis modules with proper fallback handling
try:
    # Try relative import first (when imported as part of analysis_scripts package)
    from .risk_analysis import RiskAnalyzer
    from .sentiment_analysis import SentimentAnalyzer
except ImportError:
    try:
        # Try absolute import from analysis_scripts package
        from analysis_scripts.risk_analysis import RiskAnalyzer
        from analysis_scripts.sentiment_analysis import SentimentAnalyzer
    except ImportError:
        # Final fallback for direct execution
        try:
            from risk_analysis import RiskAnalyzer
            from sentiment_analysis import SentimentAnalyzer
        except ImportError:
            # If all imports fail, create dummy classes
            logging.warning("Could not import RiskAnalyzer and SentimentAnalyzer. Using dummy classes.")
            class RiskAnalyzer:
                def __init__(self):
                    pass
                def analyze_risk(self, *args, **kwargs):
                    return {'risk_score': 'N/A', 'risk_factors': []}
            
            class SentimentAnalyzer:
                def __init__(self):
                    pass
                def analyze_sentiment(self, *args, **kwargs):
                    return {'sentiment': 'Neutral', 'score': 0}

# --- Helpers ---

def safe_get(data_dict, key, default="N/A"):
    """Safely get a value from a dictionary, checking for None and NaN."""
    if data_dict is None:
        return default
    value = data_dict.get(key, default)
    # Handle cases where yfinance might return None or NaN-like values
    # Check for pandas NaN specifically
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return default
    # Sometimes yfinance returns strings like 'Infinity' or empty strings
    if isinstance(value, str) and (value.lower() == 'infinity' or value.strip() == ''):
        return default
    return value


def _matches_any(text: str, keywords: tuple[str, ...]) -> bool:
    text = text or ''
    return any(keyword in text for keyword in keywords)


def _safe_float_or_none(value):
    """Convert value to float safely; return None if unavailable/invalid."""
    if value is None or value == "N/A":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _get_currency_precision(num_value, default_precision=2):
    """Choose a precision that preserves small quoted prices."""
    try:
        if num_value is None:
            return default_precision
        abs_val = abs(float(num_value))
        if abs_val == 0:
            return default_precision
        if abs_val < 0.01:
            return max(default_precision, 6)
        if abs_val < 1:
            return max(default_precision, 4)
        return default_precision
    except (ValueError, TypeError):
        return default_precision


def get_consistent_market_cap(info: dict, current_price=None):
    """
    Compute market cap consistently using shares * price when possible.
    Falls back to reported marketCap when required inputs are unavailable.
    """
    shares = _safe_float_or_none(safe_get(info, 'sharesOutstanding'))
    price = _safe_float_or_none(current_price)
    if price is None:
        price = _safe_float_or_none(safe_get(info, 'currentPrice', safe_get(info, 'regularMarketPrice')))

    market_cap = _safe_float_or_none(safe_get(info, 'marketCap'))
    
    # Adjust for multi-class share structure underreporting (e.g. DELL)
    if market_cap and market_cap > 0 and price and price > 0:
        implied_shares = market_cap / price
        if shares is None or shares <= 0 or shares < implied_shares * 0.85 or shares > implied_shares * 1.15:
            shares = implied_shares

    if shares is not None and price is not None and shares > 0 and price > 0:
        return shares * price

    return market_cap

def format_value(value, value_type="number", precision=2, ticker=None):
    """Formats values for display, handling 'N/A' and potential errors."""
    import pandas as pd
    
    # Convert Series/array-like to scalar first
    if isinstance(value, (pd.Series, pd.Index)):
        if len(value) == 0:
            return "N/A"
        value = value.iloc[0] if hasattr(value, 'iloc') else value[0]
    
    # Handle numpy arrays
    if hasattr(value, 'item'):
        try:
            value = value.item()
        except (ValueError, AttributeError):
            pass
    
    # Now check for N/A conditions
    if value == "N/A" or value is None or (isinstance(value, float) and np.isnan(value)):
        return "N/A"

    try:
        # Attempt to convert to float, except for date/string types
        if value_type not in ["date", "string", "factor"]:
            num = float(value)
        else:
            num = value # Keep as original type for date/string

        if value_type == "currency":
            # Import currency function locally to avoid circular imports
            from app.html_components import get_currency_symbol
            currency_symbol = get_currency_symbol(ticker) if ticker else "$"
            currency_precision = _get_currency_precision(num, precision)
            return f"{currency_symbol}{num:,.{currency_precision}f}"
        elif value_type == "percent":
            # Assumes input is a fraction (e.g., 0.25 for 25%)
            return f"{num * 100:.{precision}f}%"
        elif value_type == "percent_direct":
            # Assumes input is already a percentage number (e.g., 25 for 25%)
            return f"{num:.{precision}f}%"
        elif value_type == "ratio":
             # Check if number is extremely large or small before formatting
             if abs(num) > 1e6 or (abs(num) < 1e-3 and num != 0):
                 return f"{num:.{precision}e}x" # Use scientific notation for extremes
             return f"{num:.{precision}f}x"
        elif value_type == "currency_large":
            # Formats large numbers with B, M, K, T suffixes and prepends the currency symbol
            num = float(value)
            from app.html_components import get_currency_symbol
            currency_symbol = get_currency_symbol(ticker) if ticker else "$"
            if abs(num) >= 1e12: return f"{currency_symbol}{num / 1e12:.{precision}f}T"
            elif abs(num) >= 1e9: return f"{currency_symbol}{num / 1e9:.{precision}f}B"
            elif abs(num) >= 1e6: return f"{currency_symbol}{num / 1e6:.{precision}f}M"
            elif abs(num) >= 1e3: return f"{currency_symbol}{num / 1e3:.{precision}f}K"
            else: return f"{currency_symbol}{num:,.0f}"
        elif value_type == "large_number":
            # Handles formatting large numbers with B, M, K suffixes
            # Remove currency symbols from large number formatting
            num = float(value) # Ensure it's float for comparison
            if abs(num) >= 1e12: return f"{num / 1e12:.{precision}f}T"
            elif abs(num) >= 1e9: return f"{num / 1e9:.{precision}f}B"
            elif abs(num) >= 1e6: return f"{num / 1e6:.{precision}f}M"
            elif abs(num) >= 1e3: return f"{num / 1e3:.{precision}f}K"
            else: return f"{num:,.0f}" # No decimals for small large numbers
        elif value_type == "integer":
             return f"{int(float(num)):,}" # Convert via float first for safety
        elif value_type == "date":
             # Assuming value might be epoch seconds (common in yfinance)
             try:
                 # BUG FIX: yfinance returns Unix timestamps representing dates at midnight UTC.
                 # We should NOT convert to any timezone - just extract the UTC date directly.
                 # Otherwise Feb 19 00:00 UTC becomes Feb 18 19:00 ET (previous day!)
                 if isinstance(num, datetime):
                     # If it's already a datetime, just format it
                     return num.strftime('%B %d, %Y')
                 # Convert Unix timestamp to UTC datetime and extract the date
                 dt_utc = datetime.utcfromtimestamp(num)
                 return dt_utc.strftime('%B %d, %Y')
             except (ValueError, TypeError, OverflowError):
                 return str(value) # Fallback if conversion fails
        elif value_type == "factor": # For split factors like '2:1'
             return str(value)
        elif value_type == "string":
            return str(value)
        else: # Default number format
            return f"{num:,.{precision}f}"
    except (ValueError, TypeError):
        # Fallback for values that cannot be converted to float (like split factors)
        return str(value)

# --- Existing Extraction Functions (Refined) ---

def extract_company_profile(fundamentals: dict, current_price=None):
    """Extracts key company profile info."""
    info = fundamentals.get('info', {})
    market_cap_final = get_consistent_market_cap(info, current_price=current_price)

    raw_summary = safe_get(info, 'longBusinessSummary', 'No summary available.')
    if isinstance(raw_summary, str):
        # Remove founded-year sentence from free-text summary because provider text can be stale/inaccurate.
        summary_cleaned = re.sub(r'\b[^.]*\b(?:was\s+)?founded\s+in\s+\d{4}[^.]*\.?\s*', '', raw_summary, flags=re.IGNORECASE)
        summary_cleaned = re.sub(r'\s+', ' ', summary_cleaned).strip()
    else:
        summary_cleaned = 'No summary available.'

    profile = {
        "Company Name": safe_get(info, 'longName', safe_get(info, 'shortName', 'N/A')), # Fallback to shortName
        "Sector": safe_get(info, 'sector', 'N/A'),
        "Industry": safe_get(info, 'industry', 'N/A'),
        "Website": safe_get(info, 'website', 'N/A'),
        "Market Cap": format_value(market_cap_final, 'currency_large', 3),
        "Employees": format_value(safe_get(info, 'fullTimeEmployees'), 'integer'),
        "Summary": summary_cleaned or 'No summary available.'
    }
    return profile

def extract_valuation_metrics(fundamentals: dict):
    """Extracts key valuation metrics."""
    info = fundamentals.get('info', {})

    # Try to get Levered FCF for P/FCF calculation
    total_cash = safe_get(info, 'totalCash')
    market_cap = get_consistent_market_cap(info)
    levered_fcf = safe_get(info, 'freeCashflow') # Often represents levered FCF in yfinance
    p_fcf = "N/A"

    if market_cap != "N/A" and levered_fcf != "N/A":
        try:
            mcap_f = float(market_cap)
            fcf_f = float(levered_fcf)
            if fcf_f != 0:
                p_fcf_val = mcap_f / fcf_f
                p_fcf = format_value(p_fcf_val, 'ratio')
            else:
                p_fcf = "N/A (Zero FCF)"
        except (ValueError, TypeError):
            p_fcf = "N/A (Calc Error)"

    metrics = {
        "Trailing P/E": format_value(safe_get(info, 'trailingPE'), 'ratio'),
        "Forward P/E": format_value(safe_get(info, 'forwardPE'), 'ratio'),
        "Price/Sales (TTM)": format_value(safe_get(info, 'priceToSalesTrailing12Months'), 'ratio'),
        "Price/Book (MRQ)": format_value(safe_get(info, 'priceToBook'), 'ratio'),
        "PEG Ratio": format_value(safe_get(info, 'pegRatio'), 'ratio'),
        "EV/Revenue (TTM)": format_value(safe_get(info, 'enterpriseToRevenue'), 'ratio'),
        "EV/EBITDA (TTM)": format_value(safe_get(info, 'enterpriseToEbitda'), 'ratio'),
        "Price/FCF (TTM)": format_value(safe_get(info, 'priceToFreeCashFlow'), 'ratio')
    }
    return metrics

def extract_financial_health(fundamentals: dict):
    """Extracts key financial health metrics."""
    info = fundamentals.get('info', {})

    # FIX: Correct for potential percentage-based Debt/Equity from yfinance
    debt_equity_raw = safe_get(info, 'debtToEquity')
    debt_equity_corrected = debt_equity_raw
    try:
        # Heuristic: If D/E ratio is unusually high, assume it was given as a percentage
        if debt_equity_raw != "N/A" and float(debt_equity_raw) > 10.0:
            debt_equity_corrected = float(debt_equity_raw) / 100.0
    except (ValueError, TypeError):
        # If conversion fails, keep the original value
        debt_equity_corrected = debt_equity_raw

    metrics = {
        "Return on Equity (ROE TTM)": format_value(safe_get(info, 'returnOnEquity'), 'percent'),
        "Return on Assets (ROA TTM)": format_value(safe_get(info, 'returnOnAssets'), 'percent'),
        "Debt/Equity (MRQ)": format_value(debt_equity_corrected, 'ratio'),
        "Total Cash (MRQ)": format_value(safe_get(info, 'totalCash'), 'currency_large'),
        "Total Debt (MRQ)": format_value(safe_get(info, 'totalDebt'), 'currency_large'),
        "Current Ratio (MRQ)": format_value(safe_get(info, 'currentRatio'), 'ratio'),
        "Quick Ratio (MRQ)": format_value(safe_get(info, 'quickRatio'), 'ratio'),
        "Operating Cash Flow (TTM)": format_value(safe_get(info, 'operatingCashflow'), 'currency_large'),
        "Levered Free Cash Flow (TTM)": format_value(safe_get(info, 'freeCashflow'), 'currency_large'),
    }
    return metrics

def extract_profitability(fundamentals: dict):
    """Extracts key profitability and growth metrics."""
    info = fundamentals.get('info', {})

    revenue_raw = _safe_float_or_none(safe_get(info, 'totalRevenue'))
    net_income_raw = _safe_float_or_none(safe_get(info, 'netIncomeToCommon'))
    profit_margin_raw = _safe_float_or_none(safe_get(info, 'profitMargins'))

    metrics = {
        "Profit Margin (TTM)": format_value(profit_margin_raw, 'percent'),
        "Operating Margin (TTM)": format_value(safe_get(info, 'operatingMargins'), 'percent'),
        "Gross Margin (TTM)": format_value(safe_get(info, 'grossMargins'), 'percent'),
        "EBITDA Margin (TTM)": format_value(safe_get(info, 'ebitdaMargins'), 'percent'),
        "Revenue (TTM)": format_value(revenue_raw, 'currency_large'),
        "Quarterly Revenue Growth (YoY)": format_value(safe_get(info, 'revenueGrowth'), 'percent'), # Quarterly YoY growth
        "Gross Profit (TTM)": format_value(safe_get(info, 'grossProfits'), 'currency_large'),
        "EBITDA (TTM)": format_value(safe_get(info, 'ebitda'), 'currency_large'),
        "Net Income (TTM)": format_value(net_income_raw, 'currency_large'),
        "Earnings Growth (YoY)": format_value(safe_get(info, 'earningsGrowth'), 'percent'), # Quarterly YoY usually
    }
    return metrics

def _format_dividend_yield(yield_value):
    """
    Helper function to properly return dividend yield values from yfinance.
    CRITICAL FIX: Returns the numeric value as-is (not formatted with % symbol).
    The display functions will handle the percentage formatting.
    
    yfinance returns dividend yields as decimal values (e.g., 0.02 for 2%).
    This function just validates and returns the numeric value for later formatting.
    """
    if yield_value == "N/A" or yield_value is None:
        return "N/A"
    
    try:
        yield_float = float(yield_value)
        # Return the raw numeric value (not multiplied by 100, not formatted with %)
        # The display functions will apply percent_direct formatting
        return yield_float
    except (ValueError, TypeError):
        return "N/A"

def extract_dividends_splits(fundamentals: dict):
    """
    FIXED: Extracts dividend and stock split information with proper yield formatting.
    This version correctly handles yfinance's inconsistent dividend yield formats.
    """
    info = fundamentals.get('info', {})
    
    # Get trailing dividend yield with fallback calculation.
    # IMPORTANT: yfinance returns trailingAnnualDividendYield in true decimal form
    # (e.g. 0.000225 = 0.02%) while dividendYield / fiveYearAvgDividendYield are
    # already in percentage-point form (e.g. 0.02 = 0.02%).  We normalise the
    # trailing value to percentage-point form by multiplying by 100 so that
    # percent_direct formatting shows the correct figure.
    _trailing_raw = safe_get(info, 'trailingAnnualDividendYield')
    trailing_div_yield = None
    if _trailing_raw is not None and _trailing_raw != "N/A":
        try:
            trailing_div_yield = float(_trailing_raw) * 100  # decimal → percentage-point
        except (ValueError, TypeError):
            trailing_div_yield = None

    # If still missing or effectively zero, calculate from rate / price
    if not trailing_div_yield:
        trailing_div_rate = safe_get(info, 'trailingAnnualDividendRate')
        current_price = safe_get(info, 'currentPrice')
        if trailing_div_rate and current_price and trailing_div_rate != "N/A" and current_price != "N/A":
            try:
                # rate/price is also decimal form → multiply by 100
                trailing_div_yield = (float(trailing_div_rate) / float(current_price)) * 100
            except (ValueError, TypeError, ZeroDivisionError):
                trailing_div_yield = safe_get(info, 'dividendYield')  # already in pct-point form
        else:
            trailing_div_yield = safe_get(info, 'dividendYield')  # already in pct-point form
    
    # Use the helper function to properly format dividend yields
    # instead of relying on format_value's 'percent_direct' which assumes 
    # the values are already percentages
    
    metrics = {
        "Dividend Rate": format_value(safe_get(info, 'dividendRate'), 'currency'),
        "Dividend Yield": _format_dividend_yield(safe_get(info, 'dividendYield')),
        "Payout Ratio": format_value(safe_get(info, 'payoutRatio'), 'percent'),
        "5 Year Average Dividend Yield": _format_dividend_yield(safe_get(info, 'fiveYearAvgDividendYield')),
        "Forward Annual Dividend Rate": format_value(safe_get(info, 'forwardDividendRate'), 'currency'),
        "Forward Annual Dividend Yield": _format_dividend_yield(safe_get(info, 'forwardDividendYield')),
        "Trailing Dividend Rate": format_value(safe_get(info, 'trailingAnnualDividendRate'), 'currency'),
        "Trailing Dividend Yield": _format_dividend_yield(trailing_div_yield),
        "Ex-Dividend Date": format_value(safe_get(info, 'exDividendDate'), 'date'),
        "Last Split Date": format_value(safe_get(info, 'lastSplitDate'), 'date'),
        "Last Split Factor": safe_get(info, 'lastSplitFactor', 'N/A'),
    }
    return metrics


def extract_analyst_info(fundamentals: dict):
    """Extracts analyst recommendation and target price info."""
    
    info = fundamentals.get('info', {})
    recommendations_data = fundamentals.get('recommendations')
    recommendation_summary = "N/A"
    strong_buy, buy, hold, sell, strong_sell = 0, 0, 0, 0, 0
    total_ratings = 0

    # Process List format (less common now?)
    if isinstance(recommendations_data, list) and recommendations_data:
        grades = [rec.get('toGrade', '').lower() for rec in recommendations_data if isinstance(rec, dict) and rec.get('toGrade')]
        for grade in grades:
             if any(term in grade for term in ['strong buy', 'buy', 'outperform', 'overweight', 'accumulate']): strong_buy += 1
             elif any(term in grade for term in ['hold', 'neutral', 'peer perform', 'equal-weight', 'market perform']): hold += 1
             elif any(term in grade for term in ['sell', 'strong sell', 'underperform', 'underweight', 'reduce']): strong_sell += 1
        total_ratings = strong_buy + hold + strong_sell

    # Process DataFrame format
    elif isinstance(recommendations_data, pd.DataFrame) and not recommendations_data.empty:
        if 'To Grade' in recommendations_data.columns:
            grades = recommendations_data['To Grade'].astype(str).str.lower().tolist()
            for grade in grades:
                 if any(term in grade for term in ['strong buy', 'buy', 'outperform', 'overweight', 'accumulate']): strong_buy += 1
                 elif any(term in grade for term in ['hold', 'neutral', 'peer perform', 'equal-weight', 'market perform']): hold += 1
                 elif any(term in grade for term in ['sell', 'strong sell', 'underperform', 'underweight', 'reduce']): strong_sell += 1
            total_ratings = strong_buy + hold + strong_sell

    # Determine consensus string
    if total_ratings > 0:
        if strong_buy / total_ratings >= 0.6: recommendation_summary = f"Strong Buy ({total_ratings} Ratings)"
        elif (strong_buy + hold) / total_ratings >= 0.7 and strong_buy > strong_sell: recommendation_summary = f"Buy ({total_ratings} Ratings)"
        elif strong_sell / total_ratings >= 0.6: recommendation_summary = f"Sell ({total_ratings} Ratings)"
        elif (strong_sell + hold) / total_ratings >= 0.7 and strong_sell > strong_buy: recommendation_summary = f"Underperform ({total_ratings} Ratings)"
        else: recommendation_summary = f"Hold ({total_ratings} Ratings)"
    elif 'recommendationKey' in info: # Fallback to simple key if no detailed ratings
         recommendation_summary = safe_get(info, 'recommendationKey', 'N/A').replace('_', ' ').title()
    else: # If still N/A
        recommendation_summary = "N/A"


    metrics = {
        "Recommendation": recommendation_summary,
        "Mean Target Price": format_value(safe_get(info, 'targetMeanPrice'), 'currency'),
        "Median Target Price": format_value(safe_get(info, 'targetMedianPrice'), 'currency'),  # Added for transparency
        "High Target Price": format_value(safe_get(info, 'targetHighPrice'), 'currency'),
        "Low Target Price": format_value(safe_get(info, 'targetLowPrice'), 'currency'),
        "Number of Analyst Opinions": format_value(safe_get(info, 'numberOfAnalystOpinions'), 'integer'),
    }
    return metrics

def extract_news(fundamentals: dict, ticker=None):
    """
    Fetches ticker-specific news directly from yfinance and filters to articles
    that are genuinely relevant to the requested ticker.

    yfinance's .news feed is a *mixed* feed — it includes both ticker-specific
    articles and general market/sector articles.  We apply relevance filtering
    on title + summary to keep only articles that actually mention the ticker
    symbol or the company name.

    Returns a list of up to 6 article dicts with keys:
        title, publisher, link, published, summary, thumbnail
    """

    # ── Build relevance search terms ──────────────────────────────────────────
    _stop_words = {'corporation', 'company', 'incorporated', 'limited', 'inc',
                   'corp', 'ltd', 'the', 'and', 'group', 'holdings', 'holding'}

    def _build_search_terms(ticker_sym, fundamentals_data):
        terms = set()
        if ticker_sym:
            sym = ticker_sym.upper()
            base = sym.split('.')[0]   # strip .NS / .L suffix for matching
            terms.add(sym.lower())
            terms.add(base.lower())

        info = fundamentals_data.get('info', {})
        long_name  = safe_get(info, 'longName',  '')
        short_name = safe_get(info, 'shortName', '')

        for name in (long_name, short_name):
            if not name or name == 'N/A':
                continue
            terms.add(name.lower())
            # individual significant words from the company name
            for word in name.split():
                clean = word.strip('.,®™').lower()
                if len(clean) > 3 and clean not in _stop_words:
                    terms.add(clean)
        return terms

    def _is_relevant(title: str, summary: str, search_terms: set) -> bool:
        """Return True if any search term appears in the title or summary."""
        haystack = (title + ' ' + summary).lower()
        return any(term in haystack for term in search_terms)

    # ── Helper: extract clean URL ─────────────────────────────────────────────
    def _extract_url(val):
        if isinstance(val, dict):
            return val.get('url', '#')
        if isinstance(val, str) and val.startswith('http'):
            return val
        return '#'

    # ── Helper: best thumbnail URL ────────────────────────────────────────────
    def _thumbnail_url(content: dict) -> str:
        thumb = content.get('thumbnail')
        if not isinstance(thumb, dict):
            return ''
        for res in thumb.get('resolutions', []):
            if isinstance(res, dict) and res.get('tag') == '170x128':
                return res.get('url', '')
        return thumb.get('originalUrl', '')

    # ── Helper: parse one raw yfinance news item ──────────────────────────────
    def _parse_item(item: dict):
        if not isinstance(item, dict):
            return None

        # New yfinance structure: data nested inside 'content'
        if 'content' in item:
            content = item['content']
            provider = content.get('provider', {})
            provider_name = (provider.get('displayName', 'Yahoo Finance')
                             if isinstance(provider, dict) else str(provider))

            raw_link = content.get('canonicalUrl') or content.get('clickThroughUrl') or '#'
            title    = content.get('title', '').strip()
            if not title:
                return None

            pub_date_raw = content.get('pubDate') or content.get('displayTime')
            published    = format_value(pub_date_raw, 'date')
            summary      = (content.get('summary') or content.get('description') or '').strip()

            return {
                'title':     title,
                'publisher': provider_name,
                'link':      _extract_url(raw_link),
                'published': published,
                'summary':   summary,
                'thumbnail': _thumbnail_url(content),
            }

        # Legacy flat-dict structure
        title = item.get('title', '').strip()
        if not title:
            return None
        return {
            'title':     title,
            'publisher': item.get('publisher', 'N/A'),
            'link':      _extract_url(item.get('link', '#')),
            'published': format_value(item.get('providerPublishTime'), 'date'),
            'summary':   '',
            'thumbnail': '',
        }

    # ── Main logic ────────────────────────────────────────────────────────────
    MAX_ARTICLES  = 6
    search_terms  = _build_search_terms(ticker, fundamentals)
    raw_items     = []

    # Primary: live fetch keyed to this ticker
    if ticker:
        try:
            from data_processing_scripts.data_collection import _yf_ticker
            raw_items = _yf_ticker(ticker).news or []
        except Exception as e:
            logging.warning(f"extract_news: yfinance fetch failed for {ticker}: {e}")

    # Fallback: pre-fetched data already in fundamentals dict
    if not raw_items:
        raw_items = fundamentals.get('news') or []

    # Parse all items first
    parsed_all = [p for item in raw_items if (p := _parse_item(item)) is not None]

    # Apply relevance filter if we have search terms
    if search_terms:
        relevant = [
            p for p in parsed_all
            if _is_relevant(p['title'], p['summary'], search_terms)
        ]
    else:
        relevant = parsed_all

    # If the filter was too strict and removed everything, fall back to
    # the top parsed items (at least show something)
    if not relevant and parsed_all:
        logging.warning(
            f"extract_news: no relevant articles found for {ticker} after filtering "
            f"({len(parsed_all)} raw items). Returning top unfiltered items."
        )
        relevant = parsed_all

    return relevant[:MAX_ARTICLES]


# --- NEW Extraction Functions ---

def extract_total_valuation_data(fundamentals: dict, current_price=None):
    """Extracts/Calculates data for the Total Valuation section."""
    info = fundamentals.get('info', {})
    market_cap = get_consistent_market_cap(info, current_price=current_price)
    total_debt = safe_get(info, 'totalDebt')
    total_cash = safe_get(info, 'totalCash')
    enterprise_value = safe_get(info, 'enterpriseValue')
    ev_revenue = safe_get(info, 'enterpriseToRevenue', "N/A") # Check if direct value exists
    ev_ebitda = safe_get(info, 'enterpriseToEbitda', "N/A") # Check if direct value exists

    # Calculate Enterprise Value if possible
    if market_cap != "N/A" and total_debt != "N/A" and total_cash != "N/A":
        try:
            ev = float(market_cap) + float(total_debt) - float(total_cash)
            enterprise_value = ev
        except (ValueError, TypeError):
            enterprise_value = "N/A (Calc Error)"

    # Use formatted values where available, otherwise calculate
    if enterprise_value != "N/A" and ev_revenue == "N/A":
        total_revenue = safe_get(info, 'totalRevenue')
        if total_revenue != "N/A":
            try:
                ev_f = float(market_cap) + float(total_debt) - float(total_cash)
                rev_f = float(total_revenue)
                if rev_f != 0: ev_revenue = format_value(ev_f / rev_f, 'ratio')
            except: pass # Keep N/A if calc fails

    if enterprise_value != "N/A" and ev_ebitda == "N/A":
        ebitda = safe_get(info, 'ebitda')
        if ebitda != "N/A":
             try:
                 ev_f = float(market_cap) + float(total_debt) - float(total_cash)
                 ebitda_f = float(ebitda)
                 if ebitda_f != 0: ev_ebitda = format_value(ev_f / ebitda_f, 'ratio')
             except: pass # Keep N/A if calc fails


    # Filter earnings timestamp to only future dates.
    # Use earningsTimestampEnd → earningsTimestampStart → earningsTimestamp in priority order.
    # BUG #3 FIX: Compare dates in US/Eastern timezone so "after close" timestamps
    # (9 PM EDT = next-day IST) are not incorrectly skipped as "past" events.
    from datetime import datetime
    import pytz as _pytz_earnings
    _et_tz = _pytz_earnings.timezone('America/New_York')
    today_et = datetime.now(_et_tz).date()
    earnings_ts = "N/A"
    for _ts_field in ('earningsTimestampEnd', 'earningsTimestampStart', 'earningsTimestamp'):
        _candidate = safe_get(info, _ts_field)
        if _candidate != "N/A":
            try:
                _dt = datetime.fromtimestamp(_candidate, tz=_et_tz)
                if _dt.date() > today_et:
                    earnings_ts = _candidate
                    print(f"Using earnings timestamp from '{_ts_field}': {_dt.strftime('%Y-%m-%d')} ET")
                    break
            except (ValueError, TypeError, OSError):
                pass
    
    metrics = {
        "Market Cap": format_value(market_cap, 'currency_large'),
        "Enterprise Value": format_value(enterprise_value, 'currency_large'),
        "EV/Revenue (TTM)": format_value(ev_revenue, 'ratio') if ev_revenue != "N/A" else "N/A",
        "EV/EBITDA (TTM)": format_value(ev_ebitda, 'ratio') if ev_ebitda != "N/A" else "N/A",
        "Next Earnings Date": format_value(earnings_ts, 'date'),
        "Ex-Dividend Date": format_value(safe_get(info, 'exDividendDate'), 'date'),
    }
    return metrics

def extract_share_statistics_data(fundamentals: dict, current_price=None):
    """Extracts/Calculates data for the Share Statistics section."""
    info = fundamentals.get('info', {})
    shares_outstanding = safe_get(info, 'sharesOutstanding')
    market_cap = safe_get(info, 'marketCap')

    # Calculate Shares Outstanding if not directly available
    if shares_outstanding == "N/A" and market_cap != "N/A" and current_price is not None:
        try:
            shares_outstanding = float(market_cap) / float(current_price)
        except (ValueError, TypeError, ZeroDivisionError):
            shares_outstanding = "N/A (Calc Error)"

    # Fix float_shares validation - float MUST be <= outstanding
    raw_float = safe_get(info, 'floatShares')
    validated_float = raw_float
    
    if raw_float != "N/A" and shares_outstanding != "N/A":
        try:
            float_val = float(raw_float) if isinstance(raw_float, str) else raw_float
            shares_out_val = float(shares_outstanding) if isinstance(shares_outstanding, str) else shares_outstanding
            
            if float_val > shares_out_val:
                # Float can't exceed outstanding - correct it to 98% of outstanding
                validated_float = shares_out_val * 0.98
        except (ValueError, TypeError):
            pass  # Keep raw value if conversion fails
    
    metrics = {
        "Shares Outstanding": format_value(shares_outstanding, 'large_number', 3),
        "Implied Shares Outstanding": format_value(safe_get(info, 'impliedSharesOutstanding'), 'large_number', 3),
        "Shares Float": format_value(validated_float, 'large_number', 3),
        "Insider Ownership": format_value(safe_get(info, 'heldPercentInsiders'), 'percent'),
        "Institutional Ownership": format_value(safe_get(info, 'heldPercentInstitutions'), 'percent'),  
        "Shares Short": format_value(safe_get(info, 'sharesShort'), 'large_number', 3),
        "Shares Change (YoY)": "N/A",
    }
    return metrics


def extract_financial_efficiency_data(fundamentals: dict):
    """Extracts/Calculates data for the Financial Efficiency section."""
    info = fundamentals.get('info', {})
    
    # Get basic financial data
    total_revenue = safe_get(info, 'totalRevenue')
    total_assets = "N/A"
    inventory = "N/A"
    receivables = "N/A"
    current_assets = "N/A"
    working_capital = "N/A"
    
    # Try to get data from balance sheet if available
    try:
        balance_sheet = fundamentals.get('balance_sheet')
        if balance_sheet is not None and not balance_sheet.empty:
            # Get most recent period (first column)
            recent_period = balance_sheet.columns[0] if len(balance_sheet.columns) > 0 else None
            
            if recent_period is not None:
                if 'Total Assets' in balance_sheet.index:
                    total_assets = balance_sheet.loc['Total Assets', recent_period]
                if 'Inventory' in balance_sheet.index:
                    inventory = balance_sheet.loc['Inventory', recent_period]
                if 'Net Receivables' in balance_sheet.index:
                    receivables = balance_sheet.loc['Net Receivables', recent_period]
                elif 'Accounts Receivable' in balance_sheet.index:
                    receivables = balance_sheet.loc['Accounts Receivable', recent_period]
                if 'Current Assets' in balance_sheet.index:
                    current_assets = balance_sheet.loc['Current Assets', recent_period]
                if 'Working Capital' in balance_sheet.index:
                    working_capital = balance_sheet.loc['Working Capital', recent_period]
    except Exception as e:
        logging.warning(f"Error accessing balance sheet data: {e}")
    
    # Try to get Cost of Revenue from income statement
    cost_of_revenue = "N/A"
    try:
        financials = fundamentals.get('financials')
        if financials is not None and not financials.empty:
            recent_period = financials.columns[0] if len(financials.columns) > 0 else None
            if recent_period is not None:
                if 'Cost Of Revenue' in financials.index:
                    cost_of_revenue = financials.loc['Cost Of Revenue', recent_period]
                elif 'Total Revenue' in financials.index and total_revenue == "N/A":
                    total_revenue = financials.loc['Total Revenue', recent_period]
    except Exception as e:
        logging.warning(f"Error accessing income statement data: {e}")
    
    # Calculate efficiency ratios
    asset_turnover = "N/A"
    inventory_turnover = "N/A"
    receivables_turnover = "N/A"
    current_ratio = safe_get(info, 'currentRatio')  # Available directly
    working_capital_turnover = "N/A"
    
    # Asset Turnover = Revenue / Total Assets
    if total_revenue != "N/A" and total_assets != "N/A":
        try:
            rev = float(total_revenue)
            assets = float(total_assets)
            if assets != 0:
                asset_turnover = format_value(rev / assets, 'ratio')
            else:
                asset_turnover = "N/A (Zero Assets)"
        except (ValueError, TypeError):
            asset_turnover = "N/A (Calc Error)"
    
    # Inventory Turnover = Cost of Revenue / Inventory
    if cost_of_revenue != "N/A" and inventory != "N/A":
        try:
            cogs = float(cost_of_revenue)
            inv = float(inventory)
            if inv != 0:
                inventory_turnover = format_value(cogs / inv, 'ratio')
            else:
                inventory_turnover = "N/A (Zero Inventory)"
        except (ValueError, TypeError):
            inventory_turnover = "N/A (Calc Error)"
    
    # Receivables Turnover = Revenue / Net Receivables
    if total_revenue != "N/A" and receivables != "N/A":
        try:
            rev = float(total_revenue)
            rec = float(receivables)
            if rec != 0:
                receivables_turnover = format_value(rev / rec, 'ratio')
            else:
                receivables_turnover = "N/A (Zero Receivables)"
        except (ValueError, TypeError):
            receivables_turnover = "N/A (Calc Error)"
    
    # Working Capital Turnover = Revenue / Working Capital
    if total_revenue != "N/A" and working_capital != "N/A":
        try:
            rev = float(total_revenue)
            wc = float(working_capital)
            if wc != 0:
                working_capital_turnover = format_value(rev / wc, 'ratio')
            else:
                working_capital_turnover = "N/A (Zero WC)"
        except (ValueError, TypeError):
            working_capital_turnover = "N/A (Calc Error)"
    
    # Calculate Days Sales Outstanding (DSO) = 365 / Receivables Turnover
    days_sales_outstanding = "N/A"
    if receivables_turnover != "N/A" and "N/A" not in str(receivables_turnover):
        try:
            rt_val = float(str(receivables_turnover).replace('x', ''))
            if rt_val != 0:
                days_sales_outstanding = format_value(365 / rt_val, 'number', 1)
        except (ValueError, TypeError):
            pass
    
    # Calculate Days Inventory Outstanding (DIO) = 365 / Inventory Turnover
    days_inventory_outstanding = "N/A"
    if inventory_turnover != "N/A" and "N/A" not in str(inventory_turnover):
        try:
            it_val = float(str(inventory_turnover).replace('x', ''))
            if it_val != 0:
                days_inventory_outstanding = format_value(365 / it_val, 'number', 1)
        except (ValueError, TypeError):
            pass
    
    # Return on Invested Capital (ROIC) - use available data
    roic = "N/A"
    try:
        # Try to get it from info first
        roic_raw = safe_get(info, 'returnOnInvestedCapital')
        if roic_raw != "N/A":
            roic = format_value(roic_raw, 'percent')
        else:
            # Calculate approximate ROIC using available data
            net_income = safe_get(info, 'netIncomeToCommon')
            invested_capital = "N/A"
            
            # Try to get invested capital from balance sheet
            try:
                balance_sheet = fundamentals.get('balance_sheet')
                if balance_sheet is not None and not balance_sheet.empty:
                    recent_period = balance_sheet.columns[0]
                    if 'Invested Capital' in balance_sheet.index:
                        invested_capital = balance_sheet.loc['Invested Capital', recent_period]
            except:
                pass
            
            if net_income != "N/A" and invested_capital != "N/A":
                ni = float(net_income)
                ic = float(invested_capital)
                if ic != 0:
                    roic = format_value(ni / ic, 'percent')
    except (ValueError, TypeError):
        pass
    
    # Cash Conversion Cycle (CCC) = DIO + DSO - DPO (Days Payable Outstanding)
    cash_conversion_cycle = "N/A"
    if (days_inventory_outstanding != "N/A" and days_sales_outstanding != "N/A" and 
        "N/A" not in str(days_inventory_outstanding) and "N/A" not in str(days_sales_outstanding)):
        try:
            dio_val = float(str(days_inventory_outstanding))
            dso_val = float(str(days_sales_outstanding))
            
            # Try to calculate DPO = (Accounts Payable / COGS) * 365
            dpo_val = None
            try:
                balance_sheet = fundamentals.get('balance_sheet')
                if balance_sheet is not None and not balance_sheet.empty:
                    recent_period = balance_sheet.columns[0]
                    accounts_payable = None
                    
                    if 'Accounts Payable' in balance_sheet.index:
                        accounts_payable = float(balance_sheet.loc['Accounts Payable', recent_period])
                    elif 'Payables' in balance_sheet.index:
                        accounts_payable = float(balance_sheet.loc['Payables', recent_period])
                    
                    if accounts_payable and cost_of_revenue != "N/A":
                        cogs = float(str(cost_of_revenue))
                        if cogs != 0:
                            dpo_val = (accounts_payable / cogs) * 365
            except:
                pass
            
            # Calculate CCC with or without DPO
            if dpo_val is not None:
                # Complete CCC formula: DSO + DIO - DPO
                ccc_val = dio_val + dso_val - dpo_val
                cash_conversion_cycle = format_value(ccc_val, 'number', 1)
            else:
                # Partial CCC without DPO (incomplete but still useful)
                partial_ccc = dio_val + dso_val
                cash_conversion_cycle = f"~{partial_ccc:.1f} days (est.)"
        except (ValueError, TypeError):
            pass

    metrics = {
        "Asset Turnover (TTM)": asset_turnover,
        "Inventory Turnover (TTM)": inventory_turnover,
        "Receivables Turnover (TTM)": receivables_turnover,
        "Working Capital Turnover (TTM)": working_capital_turnover,
        "Current Ratio (MRQ)": format_value(current_ratio, 'ratio'),
        "Days Sales Outstanding": days_sales_outstanding,
        "Days Inventory Outstanding": days_inventory_outstanding,
        "Cash Conversion Cycle": cash_conversion_cycle,
        "Return on Invested Capital (ROIC TTM)": roic,
    }
    return metrics


def extract_stock_price_stats_data(fundamentals: dict):
    """Extracts data for the Stock Price Statistics section."""
    info = fundamentals.get('info', {})
    metrics = {
        "52 Week High": format_value(safe_get(info, 'fiftyTwoWeekHigh'), 'currency'),
        "52 Week Low": format_value(safe_get(info, 'fiftyTwoWeekLow'), 'currency'),
        "50 Day MA": format_value(safe_get(info, 'fiftyDayAverage'), 'currency'),
        "200 Day MA": format_value(safe_get(info, 'twoHundredDayAverage'), 'currency'),
        "52 Week Change": format_value(safe_get(info, 'fiftyTwoWeekChange'), 'percent_direct'),
        "Beta": format_value(safe_get(info, 'beta'), 'ratio'),
        "Average Volume (3 month)": format_value(safe_get(info, 'averageVolume3Month'), 'integer')
    }
    return metrics

def extract_short_selling_data(fundamentals: dict):
    """Extracts data for the Short Selling Information section."""
    info = fundamentals.get('info', {})
    metrics = {
        "Shares Short": format_value(safe_get(info, 'sharesShort'), 'large_number', 0),
        "Short Ratio (Days To Cover)": format_value(safe_get(info, 'shortRatio'), 'ratio', 1),
        "Short % of Float": format_value(safe_get(info, 'shortPercentOfFloat'), 'percent'),
        "Shares Short (Prior Month)": format_value(safe_get(info, 'sharesShortPriorMonth'), 'large_number', 0),
        "Short Date": format_value(safe_get(info, 'dateShortInterest'), 'date'), # Date of last short interest report
    }
    return metrics

def extract_peer_comparison_data(ticker, db_client=None):
    """Extracts peer comparison data using the peer_comparison module."""
    try:
        from analysis_scripts.peer_comparison import get_peer_comparison_data
        return get_peer_comparison_data(ticker, db_client=db_client)
    except Exception as e:
        logging.warning(f"Error extracting peer comparison data: {e}")
        return {}

def extract_risk_analysis_data(historical_data, market_data=None, ticker=None):
    """Extract risk analysis metrics using the RiskAnalyzer module."""
    try:
        logging.info(f"Starting risk analysis for {ticker}")
        
        # Check if RiskAnalyzer is available (might be dummy class)
        if not hasattr(RiskAnalyzer, 'comprehensive_risk_profile'):
            logging.warning(f"RiskAnalyzer not fully available for {ticker}, using dummy class")
            return {}
        
        risk_analyzer = RiskAnalyzer()
        
        if historical_data is None or historical_data.empty:
            logging.warning(f"No historical data available for risk analysis: {ticker}")
            return {}
        
        # Try to find Close column - might be named differently
        close_col = None
        for col in ['Close', 'close', 'Close Price', 'Adj Close']:
            if col in historical_data.columns:
                close_col = col
                break
        
        if close_col is None:
            # If no Close column, check if there's a 'y' column from Prophet data
            if 'y' in historical_data.columns:
                close_col = 'y'
                logging.info(f"Using 'y' column as price data for {ticker}")
            else:
                logging.warning(f"No price column found in historical data for {ticker}. Columns: {historical_data.columns.tolist()}")
                return {}
        
        price_data = historical_data[close_col].dropna()
        # Ensure data is sorted by date (index) for correct rolling calculations
        if isinstance(price_data.index, pd.DatetimeIndex):
            price_data = price_data.sort_index()

        if len(price_data) < 30:  # Need sufficient data for meaningful risk analysis
            logging.warning(f"Insufficient price data for risk analysis: {ticker} (only {len(price_data)} data points)")
            return {}
        
        logging.info(f"Calculating risk metrics for {ticker} with {len(price_data)} data points using column '{close_col}'")
        
        # Calculate comprehensive risk profile (with ticker for liquidity score - P2.2 Day 8)
        risk_metrics = risk_analyzer.comprehensive_risk_profile(price_data, market_data, ticker=ticker)
        
        # Format the metrics for display
        formatted_metrics = {
            "HV C2C (30d, Ann.)": format_value(risk_metrics.get('volatility_30d_annualized') * 100, 'percent_direct', 1),  # Close-to-Close HV, ~20 trading sessions × sqrt(252)
            "HV C2C (Full History, Ann.)": format_value(risk_metrics.get('volatility_annualized') * 100, 'percent_direct', 1),  # Close-to-Close HV over full price history
            "Value at Risk (5%)": format_value(risk_metrics.get('var_5') * 100, 'percent_direct', 2),  # Convert decimal to percentage
            "Value at Risk (1%)": format_value(risk_metrics.get('var_1') * 100, 'percent_direct', 2),  # Convert decimal to percentage
            "CVaR (5%)": format_value(risk_metrics.get('cvar_5') * 100, 'percent_direct', 2),  # Expected loss in worst 5% scenarios
            "CVaR (1%)": format_value(risk_metrics.get('cvar_1') * 100, 'percent_direct', 2),  # Expected loss in worst 1% scenarios
            "Sharpe Ratio": format_value(risk_metrics.get('sharpe_ratio'), 'ratio', 2),
            "Sortino Ratio": format_value(risk_metrics.get('sortino_ratio'), 'ratio', 2),
            "Maximum Drawdown": format_value(risk_metrics.get('max_drawdown') * 100, 'percent_direct', 2),  # Convert decimal to percentage
            "Skewness": format_value(risk_metrics.get('skewness'), 'ratio', 2),
            "Kurtosis": format_value(risk_metrics.get('kurtosis'), 'ratio', 2),
            "Liquidity Score": format_value(risk_metrics.get('liquidity_score'), 'ratio', 1) if risk_metrics.get('liquidity_score') is not None else "N/A",  # P2.2 - Hasbrouck Model
            "Liquidity Risk": risk_metrics.get('liquidity_risk_level', 'Unknown'),  # Provides interpretation: Very Low, Low, Medium, High
            "Altman Z-Score": format_value(risk_metrics.get('altman_z_score'), 'ratio', 2) if risk_metrics.get('altman_z_score') is not None else "N/A",  # P2.3 - Altman Z-Score Model
            "Bankruptcy Risk": risk_metrics.get('altman_bankruptcy_risk', 'Unknown'),  # Interpretation: Low, Medium, High
            "Financial Health Zone": risk_metrics.get('altman_risk_zone', 'Unknown'),  # Safe Zone, Grey Zone, Distress Zone
        }
        
        # Add regime risk metrics if available (P2.4 - MSCI/NBER Regime Detection)
        regime_risk = risk_metrics.get('regime_risk', {})
        if regime_risk and isinstance(regime_risk, dict):
            formatted_metrics["Bull Market Volatility"] = format_value(regime_risk.get('bull_market_volatility') * 100 if regime_risk.get('bull_market_volatility') is not None else None, 'percent_direct', 1)
            formatted_metrics["Bear Market Volatility"] = format_value(regime_risk.get('bear_market_volatility') * 100 if regime_risk.get('bear_market_volatility') is not None else None, 'percent_direct', 1)
            formatted_metrics["Volatility Ratio (Bear/Bull)"] = format_value(regime_risk.get('volatility_ratio'), 'ratio', 2) if regime_risk.get('volatility_ratio') is not None else "N/A"
            formatted_metrics["Defensive Score"] = format_value(regime_risk.get('defensive_score'), 'number', 1) if regime_risk.get('defensive_score') is not None else "N/A"  # 0-100 scale (not a ratio)
            formatted_metrics["Regime Profile"] = regime_risk.get('profile', 'Unknown')  # Defensive, Balanced, Aggressive
        
        # Add beta and correlation if market data is available
        if market_data is not None:
            formatted_metrics["Beta"] = format_value(risk_metrics.get('beta'), 'ratio', 2)
            formatted_metrics["Market Correlation"] = format_value(risk_metrics.get('correlation_market'), 'ratio', 2)
        
        logging.info(f"Successfully calculated {len(formatted_metrics)} risk metrics for {ticker}")
        return formatted_metrics
        
    except Exception as e:
        logging.error(f"Error in risk analysis for {ticker}: {e}", exc_info=True)
        import traceback
        traceback.print_exc()
        return {}

def extract_sentiment_analysis_data(fundamentals: dict, ticker=None):
    """Extract sentiment analysis metrics using the SentimentAnalyzer module."""
    try:
        logging.info(f"Starting sentiment analysis for {ticker}")
        
        # Check if SentimentAnalyzer is available (might be dummy class)
        if not hasattr(SentimentAnalyzer, 'analyze_news_sentiment'):
            logging.warning(f"SentimentAnalyzer not fully available for {ticker}, using dummy class")
            return {}
        
        sentiment_analyzer = SentimentAnalyzer()
        
        # Extract news data
        news_data = fundamentals.get('news', [])
        logging.info(f"Found {len(news_data) if news_data else 0} news items for {ticker}")
        
        # Extract analyst data
        analyst_data = extract_analyst_info(fundamentals)
        logging.info(f"Extracted analyst data for {ticker}")
        
        # Analyze different sentiment components
        news_sentiment = sentiment_analyzer.analyze_news_sentiment(news_data)
        logging.info(f"News sentiment for {ticker}: {news_sentiment}")
        
        analyst_sentiment = sentiment_analyzer.analyze_analyst_sentiment(analyst_data)
        logging.info(f"Analyst sentiment for {ticker}: {analyst_sentiment}")
        
        options_sentiment = sentiment_analyzer.analyze_options_sentiment(ticker) if ticker else {'score': 0, 'classification': 'neutral', 'confidence': 0}
        logging.info(f"Options sentiment for {ticker}: {options_sentiment}")
        
        # Calculate composite sentiment
        composite_sentiment = sentiment_analyzer.calculate_composite_sentiment(
            news_sentiment, analyst_sentiment, options_sentiment
        )
        
        logging.info(f"Composite sentiment for {ticker}: {composite_sentiment}")
        
        # Format the metrics for display
        formatted_metrics = {
            "Composite Sentiment Score": format_value(composite_sentiment.get('score'), 'ratio', 2),
            "Sentiment Classification": composite_sentiment.get('classification', 'neutral').title(),
            "Sentiment Confidence": format_value(composite_sentiment.get('confidence') * 100, 'percent_direct', 1),  # Convert decimal to percentage
            "News Sentiment": f"{news_sentiment.get('classification', 'neutral').title()} ({format_value(news_sentiment.get('score'), 'ratio', 2)})",
            "Analyst Sentiment": f"{analyst_sentiment.get('classification', 'neutral').title()} ({format_value(analyst_sentiment.get('score'), 'ratio', 2)})",
            "Options Sentiment": f"{options_sentiment.get('classification', 'neutral').title()} ({format_value(options_sentiment.get('score'), 'ratio', 2)})",
        }
        
        # Add put/call ratio if available
        if 'put_call_ratio' in options_sentiment:
            formatted_metrics["Put/Call Ratio"] = format_value(options_sentiment.get('put_call_ratio'), 'ratio', 2)
        
        logging.info(f"Successfully calculated sentiment metrics for {ticker}")
        return formatted_metrics
        
    except Exception as e:
        logging.error(f"Error in sentiment analysis for {ticker}: {e}", exc_info=True)
        import traceback
        traceback.print_exc()
        return {}

def extract_quarterly_earnings_data(fundamentals: dict, ticker=None):
    """Extract quarterly earnings performance data."""
    try:
        from data_processing_scripts.data_collection import _yf_ticker
        
        # Get ticker object to access quarterly data
        ticker_obj = _yf_ticker(ticker) if ticker else None
        if not ticker_obj:
            return {}
        
        # Get quarterly financials
        quarterly_financials = ticker_obj.quarterly_financials
        if quarterly_financials.empty:
            return {}
        
        # Extract key quarterly metrics for up to 8 quarters
        quarterly_data = {}
        quarters = quarterly_financials.columns[:8]  # Up to 8 most recent quarters
        
        key_metrics = ['Total Revenue', 'Net Income', 'Gross Profit', 'Operating Income', 'Diluted EPS', 'Basic EPS']
        
        for i, quarter in enumerate(quarters):
            quarter_key = f"Q{i+1}"
            # Fix quarter name formatting
            if hasattr(quarter, 'strftime'):
                quarter_num = (quarter.month - 1) // 3 + 1  # Calculate quarter number
                quarter_name = f"{quarter.year}-Q{quarter_num}"
            else:
                quarter_name = str(quarter)[:10]
            
            quarterly_data[quarter_key] = {
                'date': quarter_name,
                'quarter_end': quarter
            }
            
            # Extract financial metrics
            for metric in key_metrics:
                if metric in quarterly_financials.index:
                    value = quarterly_financials.loc[metric, quarter]
                    if pd.notna(value):
                        if metric in ['Total Revenue', 'Net Income', 'Gross Profit', 'Operating Income']:
                            # Format large numbers
                            formatted_value = format_value(value, 'currency_large')
                        else:
                            # EPS metrics
                            formatted_value = format_value(value, 'ratio', 2)
                        quarterly_data[quarter_key][metric] = formatted_value
                        quarterly_data[quarter_key][f"{metric}_raw"] = value
            
            # Calculate gross margin only when both inputs are meaningful.
            # Avoid emitting 0.0% for quarters where gross profit is effectively unavailable.
            if 'Gross Profit' in quarterly_financials.index and 'Total Revenue' in quarterly_financials.index:
                gross_profit = quarterly_financials.loc['Gross Profit', quarter]
                total_revenue = quarterly_financials.loc['Total Revenue', quarter]
                if (
                    pd.notna(gross_profit) and pd.notna(total_revenue) and
                    total_revenue > 0 and gross_profit > 0 and gross_profit <= total_revenue * 1.2
                ):
                    gross_margin = (gross_profit / total_revenue) * 100
                    quarterly_data[quarter_key]['Gross Margin'] = f"{gross_margin:.1f}%"
                    quarterly_data[quarter_key]['Gross Margin_raw'] = gross_margin

        # Add quarterly cash flow data (OCF, CapEx, FCF)
        try:
            quarterly_cashflow = ticker_obj.quarterly_cashflow
            if not quarterly_cashflow.empty:
                cf_metrics = ['Operating Cash Flow', 'Capital Expenditure', 'Free Cash Flow']
                for i, cf_quarter in enumerate(quarterly_cashflow.columns[:len(quarters)]):
                    qk = f"Q{i+1}"
                    if qk in quarterly_data:
                        for metric in cf_metrics:
                            if metric in quarterly_cashflow.index:
                                value = quarterly_cashflow.loc[metric, cf_quarter]
                                if pd.notna(value):
                                    quarterly_data[qk][metric] = format_value(value, 'currency_large')
                                    quarterly_data[qk][f"{metric}_raw"] = value
        except Exception as cf_err:
            logging.debug(f"Cash flow extraction failed for {ticker}: {cf_err}")

        # Match EPS estimate vs actual from earnings_dates (beat/miss)
        try:
            earnings_dates_df = ticker_obj.earnings_dates
            if earnings_dates_df is not None and not earnings_dates_df.empty:
                from datetime import timedelta as _td
                for i, quarter in enumerate(quarters):
                    qk = f"Q{i+1}"
                    q_end = pd.Timestamp(quarter).date()
                    q_window_end = q_end + _td(days=90)
                    for ed_ts, ed_row in earnings_dates_df.iterrows():
                        ed_date = pd.Timestamp(ed_ts)
                        if ed_date.tzinfo is not None:
                            ed_date = ed_date.tz_convert(None)
                        ed_date = ed_date.date()
                        if q_end < ed_date <= q_window_end:
                            eps_est  = ed_row.get('EPS Estimate')
                            eps_rep  = ed_row.get('Reported EPS')
                            eps_surp = ed_row.get('Surprise(%)')
                            if pd.notna(eps_est):
                                quarterly_data[qk]['EPS Estimate'] = round(float(eps_est), 2)
                            if pd.notna(eps_rep):
                                quarterly_data[qk]['EPS Reported'] = round(float(eps_rep), 2)
                            if pd.notna(eps_surp):
                                quarterly_data[qk]['EPS Surprise'] = round(float(eps_surp), 2)
                            break
        except Exception as ed_err:
            logging.debug(f"Earnings dates extraction failed for {ticker}: {ed_err}")

        # Calculate growth metrics
        growth_metrics = {}
        if len(quarters) >= 2:
            # Quarter-over-quarter growth (Q1 vs Q2)
            q1_revenue = quarterly_data.get('Q1', {}).get('Total Revenue_raw')
            q2_revenue = quarterly_data.get('Q2', {}).get('Total Revenue_raw')
            if q1_revenue and q2_revenue:
                qoq_revenue_growth = ((q1_revenue - q2_revenue) / q2_revenue) * 100
                growth_metrics['QoQ Revenue Growth'] = f"{qoq_revenue_growth:+.1f}%"
            
            q1_income = quarterly_data.get('Q1', {}).get('Net Income_raw')
            q2_income = quarterly_data.get('Q2', {}).get('Net Income_raw')
            if q1_income and q2_income and q2_income != 0:
                qoq_income_growth = ((q1_income - q2_income) / q2_income) * 100
                growth_metrics['QoQ Net Income Growth'] = f"{qoq_income_growth:+.1f}%"
        
        if len(quarters) >= 4:
            # Use the same yfinance revenueGrowth data for consistency instead of manual calculation
            info = fundamentals.get('info', {})
            yf_revenue_growth = info.get('revenueGrowth')
            if yf_revenue_growth is not None:
                growth_metrics['YoY Revenue Growth'] = f"{yf_revenue_growth * 100:+.1f}%"
            else:
                # Fallback to manual calculation if yfinance data unavailable
                q1_revenue = quarterly_data.get('Q1', {}).get('Total Revenue_raw')
                q4_revenue = quarterly_data.get('Q4', {}).get('Total Revenue_raw')
                if q1_revenue and q4_revenue:
                    yoy_revenue_growth = ((q1_revenue - q4_revenue) / q4_revenue) * 100
                    growth_metrics['YoY Revenue Growth'] = f"{yoy_revenue_growth:+.1f}%"
        
        # Get next earnings date from fundamentals (ONLY strictly future dates)
        info = fundamentals.get('info', {})
        next_earnings = info.get('earningsTimestamp')
        if next_earnings:
            from datetime import datetime
            try:
                # Use consistent local (naive) time for both sides of the comparison
                earnings_date = datetime.fromtimestamp(next_earnings)
                today_local = datetime.now()

                # Strict future check: earnings must be AFTER today, not same-day
                if earnings_date.date() > today_local.date():
                    growth_metrics['Next Earnings Date'] = earnings_date.strftime('%B %d, %Y')

                    # earningsCallTimestampStart can point to the LAST call, not the next one.
                    # Guard with an independent strict-future check to avoid showing past calls.
                    earnings_call_start = info.get('earningsCallTimestampStart')
                    if earnings_call_start:
                        call_time = datetime.fromtimestamp(earnings_call_start)
                        if call_time.date() > today_local.date():
                            growth_metrics['Earnings Call Time'] = call_time.strftime('%B %d, %Y at %I:%M %p ET')
            except (OSError, OverflowError, ValueError):
                pass  # Bad timestamp – skip silently
        
        return {
            'quarterly_data': quarterly_data,
            'growth_metrics': growth_metrics,
            'ticker': ticker
        }
        
    except Exception as e:
        logging.error(f"Error extracting quarterly earnings data for {ticker}: {e}")
        return {}


# Sector benchmark multiples for relative valuation (conservative medians).
_FV_SECTOR_PE = {
    'technology': 24.0,
    'healthcare': 18.0,
    'financial': 12.0,
    'financial services': 12.0,
    'consumer cyclical': 15.0,
    'consumer defensive': 20.0,
    'industrials': 16.0,
    'energy': 10.0,
    'basic materials': 11.0,
    'real estate': 16.0,
    'utilities': 15.0,
    'communication services': 17.0,
}
_FV_SECTOR_EV_EBITDA = {
    'technology': 18.0,
    'healthcare': 14.0,
    'financial': 10.0,
    'financial services': 10.0,
    'consumer cyclical': 11.0,
    'consumer defensive': 13.0,
    'industrials': 12.0,
    'energy': 6.0,
    'basic materials': 8.0,
    'real estate': 14.0,
    'utilities': 10.0,
    'communication services': 12.0,
}
_FV_SECTOR_EV_REV = {
    'technology': 5.0,
    'healthcare': 4.0,
    'financial': 3.0,
    'financial services': 3.0,
    'consumer cyclical': 2.0,
    'consumer defensive': 2.5,
    'industrials': 2.0,
    'energy': 1.5,
    'basic materials': 1.5,
    'real estate': 6.0,
    'utilities': 3.0,
    'communication services': 3.5,
}
_FV_SECTOR_PB = {
    'financial': 1.2,
    'financial services': 1.2,
    'real estate': 1.5,
    'utilities': 1.4,
}


def _fv_sector_lookup(sector: str, table: dict, default: float) -> float:
    sector_lower = (sector or '').lower()
    for key, value in table.items():
        if key in sector_lower:
            return value
    return default


def _fv_add_source(
    components,
    weighted_sum,
    total_weight,
    *,
    source: str,
    value,
    weight: float,
    note: str,
    current_price: float,
    min_ratio: float = 0.15,
    max_ratio: float = 4.0,
):
    """Append a valuation source if it passes sanity bounds."""
    if value is None or value <= 0 or weight <= 0:
        return weighted_sum, total_weight

    ratio = value / current_price if current_price > 0 else 0.0
    if not (min_ratio < ratio < max_ratio):
        components.append({
            'source': source,
            'value': round(value, 2),
            'weight': 0.0,
            'note': f"Excluded: {ratio:.1f}x current price (outside sanity band)",
        })
        return weighted_sum, total_weight

    components.append({
        'source': source,
        'value': round(value, 2),
        'weight': weight,
        'note': note,
    })
    return weighted_sum + value * weight, total_weight + weight


def extract_fair_value_estimate(fundamentals: dict, ticker: str = None, **kwargs) -> dict:
    """
    Blended Fair Value Estimate — one headline number for the report gauge.

    Uses relative valuation and consensus inputs only (no DCF):
    1. Analyst consensus target (median preferred)
    2. P/E implied value (positive earnings)
    3. EV/EBITDA implied value (positive EBITDA)
    4. EV/Revenue implied value (revenue-backed companies, growth profiles)
    5. P/B implied value (financials or negative-earnings fallback)

    Weights adapt by sector, cash-flow profile, and data availability.
  """
    if kwargs.get('dcf_data') is not None:
        logging.debug("dcf_data passed to extract_fair_value_estimate is ignored (DCF removed from reports)")

    try:
        info = fundamentals.get('info', {})

        def _f(v, default=None):
            try:
                return float(v) if v not in (None, 'N/A') else default
            except (TypeError, ValueError):
                return default

        current_price = _f(safe_get(info, 'currentPrice')) or _f(safe_get(info, 'regularMarketPrice'))
        if not current_price or current_price <= 0:
            return {'error': 'Current price unavailable'}

        market_cap = _f(safe_get(info, 'marketCap'))
        # Determine total shares outstanding, correcting for multi-class underreporting
        shares = None
        if market_cap and market_cap > 0 and current_price > 0:
            implied_shares = market_cap / current_price
            reported_shares = _f(safe_get(info, 'sharesOutstanding'))
            if not reported_shares or reported_shares < implied_shares * 0.85 or reported_shares > implied_shares * 1.15:
                shares = implied_shares
            else:
                shares = reported_shares
        else:
            shares = _f(safe_get(info, 'sharesOutstanding'))

        if shares is None or shares <= 0:
            return {'error': 'Shares outstanding unavailable'}

        sector = str(safe_get(info, 'sector', '')).lower()
        is_financial = 'financial' in sector

        fcf = _f(safe_get(info, 'freeCashflow'))
        ocf = _f(safe_get(info, 'operatingCashflow'))
        is_negative_cashflow = (fcf is not None and fcf < 0) or (ocf is not None and ocf < 0)

        trailing_eps = _f(safe_get(info, 'trailingEps'))
        forward_eps = _f(safe_get(info, 'forwardEps'))
        eps = forward_eps if forward_eps and forward_eps > 0 else trailing_eps
        has_positive_earnings = eps is not None and eps > 0

        components = []
        total_weight = 0.0
        weighted_sum = 0.0

        # ───────────────────────────────────────────────────────────
        # Source 1: Analyst Consensus Target Price
        # ───────────────────────────────────────────────────────────
        analyst_target = None
        analyst_weight = 0.0
        target_mean = _f(safe_get(info, 'targetMeanPrice'))
        target_median = _f(safe_get(info, 'targetMedianPrice'))
        target_high = _f(safe_get(info, 'targetHighPrice'))
        target_low = _f(safe_get(info, 'targetLowPrice'))
        num_analysts = _f(safe_get(info, 'numberOfAnalystOpinions'), 0)

        # Enforce Median strictly to avoid massive outliers skewing the mean
        if target_median and target_median > 0:
            analyst_target = target_median
            target_note = f"Median of {int(num_analysts)} analyst{'s' if num_analysts != 1 else ''}"
        elif target_mean and target_mean > 0:
            analyst_target = target_mean
            target_note = f"Mean of {int(num_analysts)} analyst{'s' if num_analysts != 1 else ''}"

        if analyst_target and analyst_target > 0:
            # Sanity check: cap analyst influence if it's absurdly high (>2.5x)
            ratio = analyst_target / current_price
            if 0.2 < ratio < 2.5:
                if num_analysts >= 15:
                    analyst_weight = 0.40
                elif num_analysts >= 5:
                    analyst_weight = 0.35
                elif num_analysts >= 2:
                    analyst_weight = 0.25
                else:
                    analyst_weight = 0.15
            else:
                # Target is too extreme (likely stale or speculative hype)
                analyst_weight = 0.10
                target_note += " (Weight capped due to extreme variance)"

            components.append({
                'source': 'Analyst Consensus',
                'value': round(analyst_target, 2),
                'weight': analyst_weight,
                'note': target_note
            })
            weighted_sum += analyst_target * analyst_weight
            total_weight += analyst_weight

        # ───────────────────────────────────────────────────────────
        # Source 2: P/E Implied Value (positive earnings)
        # ───────────────────────────────────────────────────────────
        if has_positive_earnings and not is_financial:
            sector_pe = _fv_sector_lookup(sector, _FV_SECTOR_PE, 16.0)
            trailing_pe = _f(safe_get(info, 'trailingPE'))
            forward_pe = _f(safe_get(info, 'forwardPE'))
            own_pe = forward_pe if forward_pe and forward_pe > 0 else trailing_pe
            if own_pe and 5 < own_pe < 80:
                target_pe = (sector_pe + own_pe) / 2.0
            else:
                target_pe = sector_pe
            target_pe = min(max(target_pe, 8.0), 45.0)
            pe_value = eps * target_pe
            pe_weight = 0.15 if is_negative_cashflow else 0.25
            eps_label = 'forward' if forward_eps and forward_eps > 0 else 'trailing'
            weighted_sum, total_weight = _fv_add_source(
                components, weighted_sum, total_weight,
                source='P/E Implied',
                value=pe_value,
                weight=pe_weight,
                note=f"{target_pe:.1f}x on {eps_label} EPS",
                current_price=current_price,
            )

        # ───────────────────────────────────────────────────────────
        # Source 3: EV/EBITDA Implied Value
        # ───────────────────────────────────────────────────────────
        total_debt = _f(safe_get(info, 'totalDebt'), 0.0) or 0.0
        total_cash = _f(safe_get(info, 'totalCash'), 0.0) or 0.0
        ebitda = _f(safe_get(info, 'ebitda'))
        enterprise_value = _f(safe_get(info, 'enterpriseValue'))
        ev_ebitda_ratio = _f(safe_get(info, 'enterpriseToEbitda'))
        if ebitda is None and enterprise_value and ev_ebitda_ratio and ev_ebitda_ratio > 0:
            ebitda = enterprise_value / ev_ebitda_ratio

        if ebitda and ebitda > 0 and shares > 0:
            sector_ev_ebitda = _fv_sector_lookup(sector, _FV_SECTOR_EV_EBITDA, 12.0)
            if ev_ebitda_ratio and 2 < ev_ebitda_ratio < 60:
                target_ev_ebitda = (sector_ev_ebitda + ev_ebitda_ratio) / 2.0
            else:
                target_ev_ebitda = sector_ev_ebitda
            target_ev_ebitda = min(max(target_ev_ebitda, 4.0), 35.0)
            implied_ev = ebitda * target_ev_ebitda
            implied_equity = implied_ev - total_debt + total_cash
            if implied_equity > 0:
                ev_ebitda_value = implied_equity / shares
                ev_ebitda_weight = 0.30 if is_negative_cashflow else 0.20
                weighted_sum, total_weight = _fv_add_source(
                    components, weighted_sum, total_weight,
                    source='EV/EBITDA Implied',
                    value=ev_ebitda_value,
                    weight=ev_ebitda_weight,
                    note=f"{target_ev_ebitda:.1f}x EV/EBITDA applied",
                    current_price=current_price,
                )

        # ───────────────────────────────────────────────────────────
        # Source 4: EV/Revenue Implied Value
        # ───────────────────────────────────────────────────────────
        total_revenue = _f(safe_get(info, 'totalRevenue'))
        ev_revenue_ratio = _f(safe_get(info, 'enterpriseToRevenue'))

        if total_revenue and total_revenue > 0 and shares > 0:
            target_multiple = _fv_sector_lookup(sector, _FV_SECTOR_EV_REV, 3.0)
            rev_growth = _f(safe_get(info, 'revenueGrowth'), 0)
            if rev_growth and rev_growth > 0.25:
                target_multiple *= 1.3
            elif rev_growth and rev_growth < 0:
                target_multiple *= 0.85

            if ev_revenue_ratio and ev_revenue_ratio > 0:
                blended_multiple = (target_multiple + ev_revenue_ratio) / 2.0
            else:
                blended_multiple = target_multiple
            blended_multiple = min(blended_multiple, 15.0)

            implied_ev = total_revenue * blended_multiple
            implied_equity = implied_ev - total_debt + total_cash
            if implied_equity > 0:
                ev_rev_value = implied_equity / shares
                if is_negative_cashflow or not has_positive_earnings:
                    ev_rev_weight = 0.40
                elif is_financial:
                    ev_rev_weight = 0.10
                else:
                    ev_rev_weight = 0.20
                weighted_sum, total_weight = _fv_add_source(
                    components, weighted_sum, total_weight,
                    source='EV/Revenue Multiple',
                    value=ev_rev_value,
                    weight=ev_rev_weight,
                    note=f"{blended_multiple:.1f}x EV/Rev applied",
                    current_price=current_price,
                    min_ratio=0.1,
                    max_ratio=5.0,
                )

        # ───────────────────────────────────────────────────────────
        # Source 5: P/B Implied (financials or negative earnings)
        # ───────────────────────────────────────────────────────────
        book_per_share = _f(safe_get(info, 'bookValue'))
        price_to_book = _f(safe_get(info, 'priceToBook'))
        if book_per_share is None and price_to_book and price_to_book > 0:
            book_per_share = current_price / price_to_book

        use_pb = is_financial or (not has_positive_earnings and book_per_share and book_per_share > 0)
        if use_pb and book_per_share and book_per_share > 0:
            target_pb = _fv_sector_lookup(sector, _FV_SECTOR_PB, 2.0)
            if price_to_book and 0.3 < price_to_book < 8:
                target_pb = (target_pb + price_to_book) / 2.0
            target_pb = min(max(target_pb, 0.5), 4.0)
            pb_value = book_per_share * target_pb
            pb_weight = 0.30 if is_financial else 0.25
            weighted_sum, total_weight = _fv_add_source(
                components, weighted_sum, total_weight,
                source='P/B Implied',
                value=pb_value,
                weight=pb_weight,
                note=f"{target_pb:.1f}x price-to-book",
                current_price=current_price,
            )

        # ───────────────────────────────────────────────────────────
        # Compute Blended Fair Value
        # ───────────────────────────────────────────────────────────
        if total_weight <= 0:
            return {'error': 'Insufficient data to compute fair value'}

        fair_value = weighted_sum / total_weight
        upside_pct = ((fair_value / current_price) - 1) * 100

        active_sources = [c for c in components if c['weight'] > 0]
        num_sources = len(active_sources)

        source_spreads = []
        bullish_weight = 0.0
        bearish_weight = 0.0
        neutral_weight = 0.0
        for comp in active_sources:
            comp_upside_pct = ((comp['value'] / current_price) - 1) * 100 if current_price > 0 else 0.0
            source_spreads.append((abs(comp_upside_pct - upside_pct), comp['weight']))

            # Treat small deviations around fair value as neutral so one noisy source
            # does not force the whole blend into a mixed-signal bucket.
            if comp_upside_pct >= 8:
                bullish_weight += comp['weight']
            elif comp_upside_pct <= -8:
                bearish_weight += comp['weight']
            else:
                neutral_weight += comp['weight']

        active_weight = sum(comp['weight'] for comp in active_sources)
        weighted_dispersion_pct = (
            sum(spread * weight for spread, weight in source_spreads) / active_weight
            if active_weight > 0 else 0.0
        )

        dominant_weight = max(bullish_weight, bearish_weight, neutral_weight)
        dominant_share = dominant_weight / active_weight if active_weight > 0 else 0.0
        has_both_sides = bullish_weight > 0 and bearish_weight > 0

        # ───────────────────────────────────────────────────────────
        # Verdict logic with Speculative safeguards
        # ───────────────────────────────────────────────────────────
        earnings_based = any(
            c['source'] in ('P/E Implied', 'EV/EBITDA Implied') for c in active_sources
        )
        is_speculative = is_negative_cashflow or (
            not has_positive_earnings and active_sources and not earnings_based
        )

        # Mixed signals should mean genuine disagreement, not simply a wide range.
        # Require meaningful weight on both sides and a weak consensus share.
        has_mixed_signals = (
            num_sources >= 3
            and has_both_sides
            and dominant_share < 0.72
            and weighted_dispersion_pct > 20.0
        )

        if upside_pct >= 35:
            if is_speculative:
                verdict = 'Speculative Upside'
                verdict_color = '#6366f1'  # indigo
            else:
                verdict = 'Significantly Undervalued'
                verdict_color = '#15803d'  # dark green
        elif upside_pct >= 12:
            if is_speculative:
                verdict = 'Speculative Upside'
                verdict_color = '#818cf8'  # lighter indigo
            else:
                verdict = 'Undervalued'
                verdict_color = '#16a34a'  # green
        elif upside_pct >= -12:
            verdict = 'Fairly Valued'
            verdict_color = '#ca8a04'  # amber
        elif upside_pct >= -28:
            verdict = 'Overvalued'
            verdict_color = '#ea580c'  # orange
        else:
            verdict = 'Significantly Overvalued'
            verdict_color = '#dc2626'  # red

        # ───────────────────────────────────────────────────────────
        # Confidence based on weighted agreement and dispersion
        # ───────────────────────────────────────────────────────────
        if num_sources >= 4 and not has_mixed_signals and dominant_share >= 0.75 and weighted_dispersion_pct <= 18.0:
            confidence = 'High'
        elif num_sources >= 3 and not has_mixed_signals and dominant_share >= 0.62 and weighted_dispersion_pct <= 30.0:
            confidence = 'Medium'
        elif num_sources >= 2 and not has_mixed_signals and dominant_share >= 0.55 and weighted_dispersion_pct <= 40.0:
            confidence = 'Medium'
        elif num_sources >= 4 and weighted_dispersion_pct <= 45.0:
            confidence = 'Medium'
        else:
            confidence = 'Low'

        return {
            'fair_value': round(fair_value, 2),
            'current_price': round(current_price, 2),
            'upside_pct': round(upside_pct, 1),
            'verdict': verdict,
            'verdict_color': verdict_color,
            'confidence': confidence,
            'num_sources': num_sources,
            'components': components,
            'target_high': round(target_high, 2) if target_high else None,
            'target_low': round(target_low, 2) if target_low else None,
        }

    except Exception as e:
        logging.error(f"Fair value estimation error for {ticker}: {e}", exc_info=True)
        return {'error': str(e)}