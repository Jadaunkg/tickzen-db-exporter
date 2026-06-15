import logging
import re

import numpy as np
import yfinance as yf

# Import peer-based margin inference system
try:
    from analysis_scripts.peer_margin_builder import (
        initialize_peer_system,
        get_dynamic_margin_profile as get_peer_margin_profile
    )
    PEER_MARGIN_SYSTEM_AVAILABLE = True
except ImportError as e:
    logging.warning(f"Peer margin builder not available: {e}. Using hardcoded profiles.")
    PEER_MARGIN_SYSTEM_AVAILABLE = False
    get_peer_margin_profile = None

# =====================================================================
# DCF VALUATION MODULE - CRITICAL FIXES AND ENHANCEMENTS
# =====================================================================
# FIX: Aerospace/Space/Defense Company Classification (May 2026)
#
# ISSUE: Aerospace/defense companies (e.g., RKLB) were being misclassified
# as "automotive_industrial" due to keyword matching on "industrial",
# resulting in overly conservative margin assumptions (1%-10%) and massive
# undervaluation. This caused the model to show RKLB at $105 as being worth
# only $3-7/share, when reasonable aerospace margin assumptions (2%-25%)
# produce $4-15/share - still reflecting the stock's premium valuation but
# with more appropriate assumptions for the industry.
#
# SOLUTION: Added explicit aerospace/space/defense keyword detection that:
# 1. Checks BEFORE automotive matching to prevent misclassification
# 2. Uses 2%-25% margin assumptions (similar to software platforms)
# 3. Anchors to "high-growth aerospace/space technology platforms"
# 4. Does NOT affect other industries (automotive stays 1%-10%)
#
# IMPACT: Only affects companies with aerospace/defense keywords in their
# sector, industry, or business description. Other stocks unaffected.
# =====================================================================

# Explicit, configurable DCF policy thresholds.
DCF_CYCLICAL_RUN_RATE_THRESHOLD = 1.5
DCF_CYCLICAL_RUN_RATE_CONFIRMATION_QUARTERS = 2
DCF_CYCLICAL_RUN_RATE_REVENUE_RATIO_MIN = 0.75
DCF_CYCLICAL_RUN_RATE_REVENUE_RATIO_MAX = 1.50
DCF_CYCLICAL_RUN_RATE_MARGIN_RATIO_MIN = 0.80
DCF_CYCLICAL_RUN_RATE_MARGIN_RATIO_MAX = 1.50

# Controlled industry mapping for cycle-aware DCF classification.
DCF_CYCLICAL_INDUSTRY_KEYWORDS = {
    'cyclical': (
        'semiconductor', 'chip', 'memory',
        'automotive', 'auto manufacturer', 'vehicle manufacturing',
        'industrial machinery', 'heavy equipment', 'industrial manufacturing',
        'energy', 'oil', 'gas', 'exploration', 'mining', 'metals', 'chemicals',
        'airline', 'air transport', 'shipping', 'transport', 'railroad',
    ),
}

def safe_get(data_dict, key, default="N/A"):
    """Safely get a value from a dictionary, checking for None and NaN."""
    if data_dict is None:
        return default
    value = data_dict.get(key, default)
    # Handle cases where yfinance might return None or NaN-like values
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return default
    # Sometimes yfinance returns strings like 'Infinity' or empty strings
    if isinstance(value, str) and (value.lower() == 'infinity' or value.strip() == ''):
        return default
    return value

def _matches_any(text: str, keywords: tuple[str, ...]) -> bool:
    text = text or ''
    return any(keyword in text for keyword in keywords)

def _round_to_half(value: float) -> float:
    return round(value * 2.0) / 2.0

# Sectors/industries that should NEVER be classified as cyclical based on beta alone.
# These companies' beta reflects growth/sentiment volatility, not GDP-cycle sensitivity.
_DIGITAL_TECH_SECTORS = (
    'technology', 'communication services',
)
_DIGITAL_TECH_INDUSTRIES = (
    'internet', 'software', 'cloud', 'digital', 'saas', 'social media',
    'online', 'streaming', 'e-commerce', 'search engine', 'advertising',
    'information technology', 'data processing',
)

def _is_digital_tech_sector(sector_lower: str, industry_lower: str) -> bool:
    """Check if a company is in a digital/tech sector that shouldn't be cyclical."""
    if 'semiconductor' in industry_lower or 'hardware' in industry_lower:
        return False
    if any(s in sector_lower for s in _DIGITAL_TECH_SECTORS):
        return True
    if any(i in industry_lower for i in _DIGITAL_TECH_INDUSTRIES):
        return True
    return False

def _infer_margin_profile(sector_txt: str, industry_txt: str, summary_txt: str):
    txt = f"{sector_txt} {industry_txt} {summary_txt}".lower()

    # High-margin aerospace/space/defense companies (launch services, satellite, space tech)
    # Treat as technology/platform companies due to high leverage and recurring revenue potential.
    # Must be checked BEFORE automotive_industrial to avoid misclassification.
    if any(k in txt for k in ('aerospace', 'space', 'launch services', 'satellite', 'rocket', 'defense systems', 'space systems', 'orbital')):
        return {
            'profile': 'aerospace_space',
            'margin_start': 0.02,
            'margin_target': 0.25,
            'anchor_text': 'high-growth aerospace/space technology platforms with recurring revenue',
        }
    if any(k in txt for k in ('ai infrastructure', 'gpu cloud', 'data center', 'cloud platform', 'model training', 'inference')):
        return {
            'profile': 'ai_cloud_infrastructure',
            'margin_start': 0.01,
            'margin_target': 0.16,
            'anchor_text': 'AI cloud infrastructure peers at scale',
        }
    # Exclude aerospace from automotive_industrial matching.
    has_automotive_signal = (
        any(k in txt for k in ('automotive', 'auto manufacturers', 'electric vehicle', 'vehicle manufacturing')) or
        re.search(r'\bev\b', txt) is not None
    )
    has_industrial_signal = any(k in txt for k in ('industrial machinery', 'heavy machinery'))
    has_aerospace_signal = any(k in txt for k in ('aerospace', 'space', 'defense'))

    if (has_automotive_signal or has_industrial_signal) and not has_aerospace_signal:
        return {
            'profile': 'automotive_industrial',
            'margin_start': 0.01,
            'margin_target': 0.10,
            'anchor_text': 'capital-intensive automotive/industrial peers through cycle',
        }
    if any(k in txt for k in ('software', 'saas', 'application software', 'enterprise software', 'platform')):
        return {
            'profile': 'software_platform',
            'margin_start': 0.02,
            'margin_target': 0.20,
            'anchor_text': 'software platform peers at scale',
        }
    if any(k in txt for k in ('biotechnology', 'biotech', 'drug', 'pharma', 'clinical')):
        return {
            'profile': 'biotech',
            'margin_start': 0.00,
            'margin_target': 0.14,
            'anchor_text': 'commercial-stage biotech peers',
        }
    if any(k in txt for k in ('semiconductor', 'chip', 'fabless', 'electronics')):
        return {
            'profile': 'semiconductors',
            'margin_start': 0.03,
            'margin_target': 0.22,
            'anchor_text': 'semiconductor peers through cycle',
        }
    if any(k in txt for k in ('e-commerce', 'internet retail', 'retail', 'marketplace', 'consumer internet')):
        return {
            'profile': 'commerce_platform',
            'margin_start': 0.01,
            'margin_target': 0.12,
            'anchor_text': 'digital commerce platform peers',
        }

    return {
        'profile': 'general_growth',
        'margin_start': 0.01,
        'margin_target': 0.15,
        'anchor_text': 'application-specific peer margins at scale',
    }

def _get_margin_profile_with_peers(ticker: str, sector: str, industry: str, summary: str) -> dict:
    """
    Get margin profile using peer-based system first, fallback to hardcoded.
    
    This function bridges the peer margin builder (Tier 1/2) with hardcoded 
    fallback (Tier 3) to ensure we always have reasonable margin assumptions.
    """
    # Try peer-based system if available
    if PEER_MARGIN_SYSTEM_AVAILABLE and get_peer_margin_profile and ticker:
        try:
            peer_profile = get_peer_margin_profile(ticker)
            if peer_profile:
                logging.info(f"✓ Using peer-based margin profile for {ticker} (Tier {peer_profile.get('tier_level')})")
                return peer_profile
        except Exception as e:
            logging.warning(f"Peer margin lookup failed for {ticker}: {e}. Falling back to hardcoded.")
    
    # Fall back to hardcoded profiles
    return _infer_margin_profile(sector, industry, summary)

def extract_dcf_data(fundamentals: dict, ticker: str = None, overrides: dict = None) -> dict:
    """
    Universal DCF Framework — one engine, inputs determined by company type.

    Company classification (Stable / Cyclical / High Growth / Turnaround) drives:
      - FCF base selection (TTM vs normalized 5-yr avg vs OCF proxy)
      - Growth rate anchoring (from actual observed revenue growth)
      - 3-tier growth deceleration (Years 1-3 / 4-7 / 8-10 / Terminal)
      - WACC size/quality premium
    Additional outputs:
      - Market-implied FCF growth rate (best feature, keep forever)
      - Implied annual return at base case (new)
    """
    try:
        # Initialize peer margin system on first use
        if PEER_MARGIN_SYSTEM_AVAILABLE and get_peer_margin_profile:
            try:
                initialize_peer_system()
            except Exception as e:
                logging.warning(f"Could not initialize peer system: {e}")
        
        import yfinance as yf
        info = fundamentals.get('info', {})
        calculator_overrides = overrides or fundamentals.get('overrides') or {}
        if not isinstance(calculator_overrides, dict):
            calculator_overrides = {}

        # --- Raw inputs ---
        fcf_ttm         = safe_get(info, 'freeCashflow')
        ocf_ttm         = safe_get(info, 'operatingCashflow')
        total_debt      = safe_get(info, 'totalDebt')
        total_cash      = safe_get(info, 'totalCash')
        shares          = safe_get(info, 'sharesOutstanding')
        market_cap      = safe_get(info, 'marketCap')
        beta            = safe_get(info, 'beta')
        current_price   = safe_get(info, 'currentPrice') or safe_get(info, 'regularMarketPrice')
        rev_growth      = safe_get(info, 'revenueGrowth')       # decimal e.g. 0.12
        earnings_growth = safe_get(info, 'earningsGrowth')      # decimal e.g. -0.60
        ebitda          = safe_get(info, 'ebitda')
        total_revenue   = safe_get(info, 'totalRevenue')
        sector_raw      = safe_get(info, 'sector', '')
        industry_raw    = safe_get(info, 'industry', '')
        summary_raw     = safe_get(info, 'longBusinessSummary', '')

        def _override_float(name, current_value):
            if name not in calculator_overrides:
                return current_value
            try:
                override_value = calculator_overrides.get(name)
                if override_value in (None, ''):
                    return current_value
                return float(override_value)
            except (TypeError, ValueError):
                return current_value

        def _override_text(name, current_value):
            override_value = calculator_overrides.get(name)
            if override_value in (None, ''):
                return current_value
            return str(override_value)

        fcf_ttm         = _override_float('freeCashflow', fcf_ttm)
        ocf_ttm         = _override_float('operatingCashflow', ocf_ttm)
        total_debt      = _override_float('totalDebt', total_debt)
        total_cash      = _override_float('totalCash', total_cash)
        shares          = _override_float('sharesOutstanding', shares)
        market_cap      = _override_float('marketCap', market_cap)
        beta            = _override_float('beta', beta)
        current_price   = _override_float('currentPrice', current_price)
        rev_growth      = _override_float('revenueGrowth', rev_growth)
        earnings_growth = _override_float('earningsGrowth', earnings_growth)
        ebitda          = _override_float('ebitda', ebitda)
        total_revenue   = _override_float('totalRevenue', total_revenue)
        sector_raw      = _override_text('sector', sector_raw)
        industry_raw    = _override_text('industry', industry_raw)
        summary_raw     = _override_text('longBusinessSummary', summary_raw)

        def _f(v, default=None):
            try:
                return float(v) if v not in (None, 'N/A') else default
            except (TypeError, ValueError):
                return default

        fcf_ttm_f         = _f(fcf_ttm)
        ocf_ttm_f         = _f(ocf_ttm)
        total_debt_f      = _f(total_debt,      0.0)
        total_cash_f      = _f(total_cash,      0.0)
        market_cap_f      = _f(market_cap,      0.0)
        beta_f            = _f(beta,            1.0)
        price_f           = _f(current_price,   0.0)
        
        # --- CRITICAL FIX: Properly calculate shares_f to avoid scaling bugs ---
        shares_raw_f = _f(shares, None)
        if market_cap_f > 0 and price_f > 0:
            implied_shares = market_cap_f / price_f
            if shares_raw_f is None or shares_raw_f <= 0 or shares_raw_f < implied_shares * 0.85 or shares_raw_f > implied_shares * 1.15:
                shares_f = implied_shares
            else:
                shares_f = shares_raw_f
        elif shares_raw_f is not None and shares_raw_f > 0:
            shares_f = shares_raw_f
        else:
            shares_f = 0.0
        ebitda_f          = _f(ebitda)
        total_revenue_f   = _f(total_revenue)
        rev_growth_f      = _f(rev_growth,      0.0)
        earnings_growth_f = _f(earnings_growth, 0.0)
        # Normalize growth rates: Yahoo sometimes returns percentages (e.g. 5.4)
        # instead of decimals (0.054). If values > 1, treat them as percent and divide.
        # FIX A: Threshold raised - values 1.0-5.0 are valid decimal growth rates (100-500%)
        if rev_growth_f is not None and rev_growth_f > 5:
            rev_growth_f = rev_growth_f / 100.0
        if earnings_growth_f is not None and earnings_growth_f > 5:
            earnings_growth_f = earnings_growth_f / 100.0
        sector_l          = str(sector_raw or '').lower()
        industry_l        = str(industry_raw or '').lower()
        summary_l         = str(summary_raw or '').lower()
        cyclical_text      = f"{sector_l} {industry_l} {summary_l}"
        is_cyclical_industry = _matches_any(cyclical_text, DCF_CYCLICAL_INDUSTRY_KEYWORDS['cyclical'])

        def _fmt_compact_currency(v):
            if v is None:
                return 'N/A'
            a = abs(float(v))
            sign = '-' if v < 0 else ''
            if a >= 1e12:
                return f"{sign}${a/1e12:.2f}T"
            if a >= 1e9:
                return f"{sign}${a/1e9:.2f}B"
            if a >= 1e6:
                return f"{sign}${a/1e6:.1f}M"
            return f"{sign}${a:,.0f}"

        # --- Annual income statement: interest expense + effective tax rate ---
        interest_expense_f    = 0.0
        eff_tax_rate          = 0.21   # US statutory fallback
        historical_fcf        = []     # annual FCF values, most recent first (up to 5 years)
        stock_based_comp      = 0.0    # annual SBC, used to penalize inflated margin projections
        historical_fcf_years  = []     # corresponding fiscal years (int), parallel to historical_fcf
        latest_quarter_fcf    = None   # most recent quarterly FCF, used for cyclical run-rate checks
        quarterly_run_rate_samples = []
        ticker_obj = None

        try:
            ticker_obj = yf.Ticker(ticker) if ticker else None
            if ticker_obj:
                ann = ticker_obj.financials
                if ann is not None and not ann.empty:
                    if 'Tax Provision' in ann.index and 'Pretax Income' in ann.index:
                        taxes  = _f(ann.loc['Tax Provision'].iloc[0])
                        pretax = _f(ann.loc['Pretax Income'].iloc[0])
                        if pretax and pretax > 0 and taxes is not None and taxes >= 0:
                            eff_tax_rate = min(taxes / pretax, 0.40)
                    for ie_row in ('Interest Expense Non Operating', 'Interest Expense'):
                        if ie_row in ann.index:
                            ie_val = _f(ann.loc[ie_row].iloc[0])
                            if ie_val is not None:
                                interest_expense_f = abs(ie_val)
                                break

        # Historical annual FCF for cyclical normalization (up to 5 years).
                # We also track the fiscal years so the base_metric_label can be transparent.
                historical_fcf_years = []   # parallel list of year ints, most recent first
                cf = ticker_obj.cash_flow
                if cf is not None and not cf.empty:
                    ocf_row   = None
                    capex_row = None
                    for rname in ('Operating Cash Flow', 'Cash Flow From Continuing Operating Activities'):
                        if rname in cf.index:
                            ocf_row = cf.loc[rname]
                            break
                    for rname in ('Capital Expenditure', 'Purchase Of Property Plant And Equipment'):
                        if rname in cf.index:
                            capex_row = cf.loc[rname]
                            break
                    for rname in ('Stock Based Compensation', 'Share Based Compensation'):
                        if rname in cf.index:
                            sbc_val = _f(cf.loc[rname].iloc[0])
                            if sbc_val is not None:
                                stock_based_comp = abs(sbc_val)
                                break
                    if ocf_row is not None:
                        for i in range(min(5, len(ocf_row))):
                            ocf_v   = _f(ocf_row.iloc[i])
                            capex_v = _f(capex_row.iloc[i]) if capex_row is not None else 0.0
                            if ocf_v is not None:
                                historical_fcf.append(ocf_v - abs(capex_v or 0))
                                try:
                                    historical_fcf_years.append(int(str(ocf_row.index[i])[:4]))
                                except Exception:
                                    pass

                # Latest quarterly FCF can be a better cyclical signal than blended TTM
                # when the business is in a sharp trough-to-peak transition.
                try:
                    qcf = ticker_obj.quarterly_cashflow
                    if qcf is not None and not qcf.empty and 'Free Cash Flow' in qcf.index:
                        latest_quarter_fcf = _f(qcf.loc['Free Cash Flow'].iloc[0])

                    qf = ticker_obj.quarterly_financials
                    if (
                        qcf is not None and not qcf.empty and 'Free Cash Flow' in qcf.index and
                        qf is not None and not qf.empty and 'Total Revenue' in qf.index
                    ):
                        shared_quarters = [col for col in qcf.columns if col in qf.columns][:4]
                        for col in shared_quarters:
                            q_fcf = _f(qcf.loc['Free Cash Flow', col])
                            q_rev = _f(qf.loc['Total Revenue', col])
                            if q_fcf is not None or q_rev is not None:
                                quarterly_run_rate_samples.append({
                                    'quarter': col,
                                    'fcf': q_fcf,
                                    'revenue': q_rev,
                                })
                except Exception:
                    pass
        except Exception:
            pass

        sbc_ratio = (stock_based_comp / total_revenue_f) if (total_revenue_f is not None and total_revenue_f > 0) else 0.0
        fcf_margin_ttm = (fcf_ttm_f / total_revenue_f) if (fcf_ttm_f is not None and total_revenue_f is not None and total_revenue_f > 0) else None

        # -----------------------------------------------------------------------
        # STEP 0.5 — Annual Revenue Growth Fallback
        # Yahoo's revenueGrowth is quarterly YoY which can be negative even for
        # companies growing 50%+ annually (one bad quarter is enough). We compute
        # annual revenue growth from the income statement as a more reliable signal.
        # -----------------------------------------------------------------------
        annual_rev_growth_f = None
        try:
            if ticker_obj is not None:
                ann_financials = ticker_obj.financials
                if ann_financials is not None and not ann_financials.empty and 'Total Revenue' in ann_financials.index:
                    rev_row = ann_financials.loc['Total Revenue']
                    # Get the two most recent annual revenue figures
                    rev_values = [_f(rev_row.iloc[i]) for i in range(min(2, len(rev_row)))]
                    if len(rev_values) >= 2 and rev_values[0] is not None and rev_values[1] is not None and rev_values[1] > 0:
                        annual_rev_growth_f = (rev_values[0] - rev_values[1]) / abs(rev_values[1])
        except Exception:
            pass

        # -----------------------------------------------------------------------
        # STEP 1 — Company Classification
        # Determines FCF base, growth anchoring, and WACC premium automatically.
        # 6 types: stable / cyclical / mature_growth / high_growth /
        #          speculative_growth / turnaround
        # -----------------------------------------------------------------------
        rev_g  = rev_growth_f      or 0.0
        eps_g  = earnings_growth_f or 0.0
        beta_c = beta_f            or 1.0
        mc     = market_cap_f      or 0.0

        # Use the best available revenue growth signal: whichever is higher between
        # quarterly YoY and annual YoY.  A single bad quarter should not override
        # strong annual expansion (common for Bitcoin miners, AI infra, etc.).
        # FIX B: Use annual as primary signal, but do not let a negative annual comparison
        # suppress a large-cap transition story with strong current quarterly growth.
        if annual_rev_growth_f is not None:
            if annual_rev_growth_f < 0 and rev_g > 0:
                effective_rev_g = rev_g
            else:
                effective_rev_g = annual_rev_growth_f
                # Allow quarterly to act as a floor only if it materially exceeds annual
                # AND both are positive - prevents one good quarter from inflating anchors.
                if rev_g > 0 and annual_rev_growth_f > 0 and rev_g > annual_rev_growth_f * 1.5:
                    effective_rev_g = (annual_rev_growth_f + rev_g) / 2.0
        else:
            effective_rev_g = rev_g

        fcf_is_negative = (fcf_ttm_f is not None and fcf_ttm_f < 0)

        # Guard: A company with strong EBITDA and positive gross margins is NOT
        # a turnaround — it's investing heavily (capex-negative FCF).  Only
        # classify as turnaround when both FCF AND EBITDA are weak.
        has_strong_ebitda = (ebitda_f is not None and ebitda_f > 0 and
                            total_revenue_f is not None and total_revenue_f > 0 and
                            ebitda_f / total_revenue_f > 0.10)

        if fcf_is_negative and effective_rev_g > 0.10:
            company_type = 'high_growth'    # burning cash but expanding fast
        elif fcf_is_negative and has_strong_ebitda:
            company_type = 'high_growth'    # capex-heavy but operationally profitable
        elif fcf_is_negative and effective_rev_g <= 0.10:
            company_type = 'turnaround'     # struggling — negative FCF, low growth
        elif not is_cyclical_industry and mc >= 200e9 and 0.08 <= effective_rev_g <= 0.30 and beta_c < 1.5:
            # Mega-cap with mixed maturity: large established core + high-growth segments.
            # Examples: Alphabet (ads+cloud), Amazon (retail+AWS), Microsoft (Office+Azure).
            company_type = 'mature_growth'
        elif is_cyclical_industry and not _is_digital_tech_sector(sector_l, industry_l) and not fcf_is_negative and not (
            annual_rev_growth_f is not None and annual_rev_growth_f < 0 and rev_g > 0 and mc >= 100e9 and beta_c >= 1.4
        ) and not (effective_rev_g >= 0.30 and mc >= 1e12):
            # Cyclical industries can show temporary peak/trough revenue distortions.
            # Keep them in the cyclical bucket so the DCF uses cycle-aware FCF inputs.
            # BUT: exclude digital/internet/tech companies — their beta reflects market
            # sentiment, not GDP cyclicality. Also exclude hyper-growth mega-caps
            # (e.g. NVDA with 85% rev growth and market cap >= 1T) from cyclical treatment.
            company_type = 'cyclical'
        elif effective_rev_g >= 0.15 or (effective_rev_g >= 0.10 and beta_c >= 1.4):
            company_type = 'high_growth'    # strong revenue momentum
        elif beta_c >= 1.75 and not is_cyclical_industry:
            # Extreme beta signals the market treats this as a speculative / transitional
            # story, NOT an economic-cycle business. True cyclicals (Ford, Caterpillar,
            # chemicals, semiconductors) have high/moderate beta; their FCF volatility is driven by
            # GDP or industry cycles. A high-beta company in a non-cyclical sector with near-term revenue weakness is experiencing a
            # company-specific structural shift — not a macroeconomic cycle. Forcing it into 'cyclical'
            # produces incorrectly low terminal growth and growth anchors.
            company_type = 'speculative_growth'
        elif beta_c >= 1.35 and not _is_digital_tech_sector(sector_l, industry_l):
            company_type = 'cyclical'       # economically-sensitive, moderate-high beta
        else:
            company_type = 'stable'         # mature, predictable business

        manual_company_type = calculator_overrides.get('company_type')
        if manual_company_type in {'stable', 'cyclical', 'mature_growth', 'high_growth', 'speculative_growth', 'turnaround'}:
            company_type = manual_company_type

        company_type_labels = {
            'stable':             'Stable / Mature',
            'cyclical':           'Cyclical',
            'mature_growth':      'Mature + Growth Segments',
            'high_growth':        'High Growth',
            'speculative_growth': 'Speculative Growth',
            'turnaround':         'Turnaround / Recovery',
        }

        # -----------------------------------------------------------------------
        # STEP 2 — FCF Base Selection (one rule per type, never blindly use TTM)
        # -----------------------------------------------------------------------
        base_fcf          = None
        base_metric_label = 'N/A'
        dcf_method        = 'standard_fcf_3tier'
        sensitivity_mode  = 'growth_rate'
        margin_profile    = None
        margin_start      = None
        margin_target     = None

        if company_type == 'stable':
            # TTM FCF is reliable for stable businesses
            if fcf_ttm_f is not None and fcf_ttm_f > 0:
                base_fcf          = fcf_ttm_f
                base_metric_label = 'Free Cash Flow (TTM)'
            elif ebitda_f is not None and ebitda_f > 0:
                base_fcf          = ebitda_f * 0.55
                base_metric_label = 'EBITDA × 0.55 (FCF proxy)'

        elif company_type == 'cyclical':
            # Normalize across the cycle — single-year TTM can be peak or trough.
            # Prefer a cycle-aware base over raw TTM:
            # - annual history if it is available
            # - latest-quarter annualized run-rate if it clearly exceeds the cycle base
            # - otherwise a blended estimate to reduce quarter-specific noise
            recent_q_run_rate = None
            if latest_quarter_fcf is not None and latest_quarter_fcf > 0:
                recent_q_run_rate = latest_quarter_fcf * 4.0

            cycle_history_base = None
            if historical_fcf:
                avg_all = sum(historical_fcf) / len(historical_fcf)
                pos_fcf = [v for v in historical_fcf if v > 0]
                if avg_all > 0:
                    cycle_history_base = avg_all
                elif pos_fcf:
                    cycle_history_base = sum(pos_fcf) / len(pos_fcf)

            if cycle_history_base is not None and quarterly_run_rate_samples:
                quarter_strength_threshold = cycle_history_base * DCF_CYCLICAL_RUN_RATE_THRESHOLD
                latest_sample = quarterly_run_rate_samples[0]
                prior_sample = quarterly_run_rate_samples[1] if len(quarterly_run_rate_samples) > 1 else None

                latest_q_fcf = latest_sample.get('fcf')
                latest_q_rev = latest_sample.get('revenue')
                latest_q_run_rate = latest_q_fcf * 4.0 if latest_q_fcf is not None and latest_q_fcf > 0 else None
                latest_q_margin = (
                    latest_q_fcf / latest_q_rev if latest_q_fcf is not None and latest_q_rev is not None and latest_q_rev > 0 else None
                )

                prior_q_run_rate = None
                prior_q_rev = None
                prior_q_margin = None
                if prior_sample is not None:
                    prior_q_fcf = prior_sample.get('fcf')
                    prior_q_rev = prior_sample.get('revenue')
                    prior_q_run_rate = prior_q_fcf * 4.0 if prior_q_fcf is not None and prior_q_fcf > 0 else None
                    prior_q_margin = (
                        prior_q_fcf / prior_q_rev if prior_q_fcf is not None and prior_q_rev is not None and prior_q_rev > 0 else None
                    )

                same_quarter_prior_year_rev = None
                if len(quarterly_run_rate_samples) >= 5:
                    same_quarter_prior_year_rev = quarterly_run_rate_samples[4].get('revenue')

                two_strong_quarters = (
                    latest_q_run_rate is not None and latest_q_run_rate >= quarter_strength_threshold and
                    prior_q_run_rate is not None and prior_q_run_rate >= quarter_strength_threshold
                )

                revenue_confirm = False
                margin_confirm = False
                if latest_q_rev is not None and latest_q_rev > 0:
                    revenue_ratio = None
                    if same_quarter_prior_year_rev is not None and same_quarter_prior_year_rev > 0:
                        revenue_ratio = latest_q_rev / same_quarter_prior_year_rev
                    elif prior_q_rev is not None and prior_q_rev > 0:
                        revenue_ratio = latest_q_rev / prior_q_rev
                    if revenue_ratio is not None:
                        revenue_confirm = DCF_CYCLICAL_RUN_RATE_REVENUE_RATIO_MIN <= revenue_ratio <= DCF_CYCLICAL_RUN_RATE_REVENUE_RATIO_MAX
                if latest_q_margin is not None and prior_q_margin is not None and prior_q_margin > 0:
                    margin_ratio = latest_q_margin / prior_q_margin
                    margin_confirm = DCF_CYCLICAL_RUN_RATE_MARGIN_RATIO_MIN <= margin_ratio <= DCF_CYCLICAL_RUN_RATE_MARGIN_RATIO_MAX

                if two_strong_quarters:
                    base_fcf = latest_q_run_rate
                    base_metric_label = f'Latest Quarter FCF × 4 (confirmed by {DCF_CYCLICAL_RUN_RATE_CONFIRMATION_QUARTERS} strong quarters)'
                elif latest_q_run_rate is not None and latest_q_run_rate >= quarter_strength_threshold and revenue_confirm and margin_confirm:
                    base_fcf = latest_q_run_rate
                    base_metric_label = 'Latest Quarter FCF × 4 (revenue/margin confirmed)'
                elif (
                    recent_q_run_rate is not None and
                    cycle_history_base is not None and
                    cycle_history_base >= recent_q_run_rate * DCF_CYCLICAL_RUN_RATE_THRESHOLD
                ):
                    base_fcf = cycle_history_base
                    base_metric_label = f'Normalized FCF ({len(historical_fcf)}-yr avg, cycle-adjusted)'
                else:
                    base_fcf = (cycle_history_base + recent_q_run_rate) / 2.0
                    base_metric_label = 'Blended cyclical FCF (normalized + run-rate)'
            elif recent_q_run_rate is not None:
                base_fcf = recent_q_run_rate
                base_metric_label = 'Latest Quarter FCF × 4 (cyclical run-rate)'
            elif cycle_history_base is not None:
                base_fcf = cycle_history_base
                base_metric_label = f'Normalized FCF ({len(historical_fcf)}-yr avg, cycle-adjusted)'

            if base_fcf is None:
                if fcf_ttm_f is not None and fcf_ttm_f > 0:
                    base_fcf          = fcf_ttm_f
                    base_metric_label = 'Free Cash Flow (TTM) — cycle history unavailable'
                elif ebitda_f is not None and ebitda_f > 0:
                    base_fcf          = ebitda_f * 0.50
                    base_metric_label = 'EBITDA × 0.50 (cyclical FCF proxy)'

        elif company_type == 'high_growth':
            # Switch to revenue × margin expansion when FCF is negative or when
            # a large-cap growth company has weak current FCF conversion despite
            # strong revenue growth. This avoids valuing capital-intensive growth
            # names as if today's TTM FCF were the durable long-run base.
            use_revenue_margin = (
                total_revenue_f is not None and total_revenue_f > 0 and
                rev_g >= 0.10 and
                mc >= 50e9 and
                fcf_ttm_f is not None and fcf_ttm_f > 0 and
                fcf_margin_ttm is not None and fcf_margin_ttm < 0.08
            )
            if (fcf_is_negative or use_revenue_margin) and total_revenue_f is not None and total_revenue_f > 0:
                dcf_method       = 'revenue_margin'
                sensitivity_mode = 'terminal_margin'
                # Use peer-based margin profile with fallback to hardcoded
                margin_profile   = _get_margin_profile_with_peers(ticker, sector_l, industry_l, summary_l)
                margin_start     = max(0.04, float(margin_profile['margin_start']))
                margin_target    = max(0.20 if mc >= 100e9 else 0.18, float(margin_profile['margin_target']))
                base_fcf         = total_revenue_f * margin_start

                # FIX C: Removed ticker-specific labeling; use anchor_text only
                base_metric_label = (
                    f"FCF base: Revenue × target FCF margin approach. Base revenue {_fmt_compact_currency(total_revenue_f)}, "
                    f"FCF margin expanding from ~{margin_start*100:.0f}% (Year 1) to ~{margin_target*100:.0f}% (Year 10), "
                    f"anchored to {margin_profile['anchor_text']}"
                )
            elif fcf_ttm_f is not None and fcf_ttm_f > 0:
                base_fcf          = fcf_ttm_f
                base_metric_label = 'Free Cash Flow (TTM) — high-growth phase'
            elif ocf_ttm_f is not None and ocf_ttm_f > 0:
                base_fcf          = ocf_ttm_f * 0.80   # adjust for ongoing capex
                base_metric_label = 'Operating Cash Flow × 0.80 (capex-adjusted proxy)'
            elif ebitda_f is not None and ebitda_f > 0:
                base_fcf          = ebitda_f * 0.40    # low margin for pre-maturity stage
                base_metric_label = 'EBITDA × 0.40 (pre-maturity FCF proxy)'

        elif company_type == 'speculative_growth':
            # High-beta company in structural transition — near-term revenue may be
            # depressed by company-specific factors, not the economic cycle.
            # Use TTM FCF directly as it reflects current cash generation ability.
            if fcf_ttm_f is not None and fcf_ttm_f > 0:
                base_fcf          = fcf_ttm_f
                base_metric_label = 'Free Cash Flow (TTM) — speculative growth'
            elif ocf_ttm_f is not None and ocf_ttm_f > 0:
                base_fcf          = ocf_ttm_f * 0.75
                base_metric_label = 'Operating Cash Flow × 0.75 (capex-adjusted proxy)'
            elif ebitda_f is not None and ebitda_f > 0:
                base_fcf          = ebitda_f * 0.40
                base_metric_label = 'EBITDA × 0.40 (speculative proxy)'

        elif company_type == 'mature_growth':
            # Use the HIGHER of TTM FCF or normalized 3-year average FCF.
            # For companies with growing capex (MSFT/Azure, META/AI infra), the 3-year
            # average can be dragged down by recent capex-heavy years where FCF is
            # temporarily low. Using max() ensures we don't punish companies for
            # investing in growth, while the 3-yr avg still provides a floor when
            # TTM FCF is temporarily inflated by working capital timing.
            hist_3yr = historical_fcf[:3] if historical_fcf else []
            avg_3yr = None
            if hist_3yr:
                avg_3yr = sum(hist_3yr) / len(hist_3yr)
            if avg_3yr is not None and avg_3yr > 0:
                if fcf_ttm_f is not None and fcf_ttm_f > avg_3yr:
                    base_fcf          = fcf_ttm_f
                    base_metric_label = f'Normalized FCF (TTM, above {len(hist_3yr)}-yr avg)'
                else:
                    base_fcf          = avg_3yr
                    base_metric_label = f'Normalized FCF ({len(hist_3yr)}-yr avg, mature-growth)'
            elif fcf_ttm_f is not None and fcf_ttm_f > 0:
                base_fcf          = fcf_ttm_f
                base_metric_label = 'Free Cash Flow (TTM)'
            if base_fcf is None:
                # Fallback to TTM if history is unavailable
                if fcf_ttm_f is not None and fcf_ttm_f > 0:
                    base_fcf          = fcf_ttm_f
                    base_metric_label = 'Free Cash Flow (TTM) — history unavailable'
                elif ebitda_f is not None and ebitda_f > 0:
                    base_fcf          = ebitda_f * 0.55
                    base_metric_label = 'EBITDA × 0.55 (FCF proxy)'

        elif company_type == 'turnaround':
            if fcf_is_negative and total_revenue_f is not None and total_revenue_f > 0:
                dcf_method       = 'revenue_margin'
                sensitivity_mode = 'terminal_margin'
                # Use peer-based margin profile with fallback to hardcoded
                margin_profile   = _get_margin_profile_with_peers(ticker, sector_l, industry_l, summary_l)
                margin_start     = max(0.0, margin_profile['margin_start'] * 0.5)
                margin_target    = max(margin_start + 0.04, margin_profile['margin_target'] * 0.75)
                base_fcf         = total_revenue_f * margin_start
                base_metric_label = (
                    f"FCF base: Revenue × target FCF margin approach. Base revenue {_fmt_compact_currency(total_revenue_f)}, "
                    f"FCF margin expanding from ~{margin_start*100:.0f}% (Year 1) to ~{margin_target*100:.0f}% (Year 10), "
                    f"anchored to {margin_profile['anchor_text']}"
                )
            # Use last positive FCF year; extremely conservative otherwise
            else:
                pos_hist = [v for v in historical_fcf if v > 0]
                if pos_hist:
                    base_fcf          = pos_hist[0]         # most recent positive year
                    base_metric_label = 'Last Positive Annual FCF (turnaround estimate)'
                elif ebitda_f is not None and ebitda_f > 0:
                    base_fcf          = ebitda_f * 0.30     # deeply discounted
                    base_metric_label = 'EBITDA × 0.30 (conservative turnaround estimate)'

        # Universal fallback
        if base_fcf is None and ebitda_f is not None and ebitda_f > 0:
            base_fcf          = ebitda_f * 0.55
            base_metric_label = 'EBITDA × 0.55 (FCF proxy)'

        base_metric_label_override = calculator_overrides.get('base_metric_label')

        dcf_method_override = calculator_overrides.get('dcf_method')
        if dcf_method_override in ('standard_fcf_3tier', 'revenue_margin'):
            dcf_method = dcf_method_override

        if dcf_method == 'revenue_margin' and total_revenue_f is not None and total_revenue_f > 0:
            sensitivity_mode = 'terminal_margin'
            manual_margin_start = calculator_overrides.get('margin_start')
            manual_margin_target = calculator_overrides.get('margin_target')
            if manual_margin_start not in (None, ''):
                try:
                    margin_start = max(0.0, float(manual_margin_start))
                except (TypeError, ValueError):
                    pass
            if manual_margin_target not in (None, ''):
                try:
                    margin_target = max(0.0, float(manual_margin_target))
                except (TypeError, ValueError):
                    pass

            if margin_start is None:
                margin_start = 0.02 if fcf_ttm_f is None or fcf_ttm_f < 0 else 0.04
            if margin_target is None:
                margin_target = max(margin_start + 0.08, 0.15)
            margin_target = min(max(margin_start + 0.05, margin_target), 0.30)
            base_fcf = total_revenue_f * margin_start
            if base_metric_label_override in (None, ''):
                base_metric_label = f'Revenue × FCF margin approach (start {margin_start*100:.1f}% / target {margin_target*100:.1f}%)'

        if base_metric_label_override not in (None, ''):
            base_metric_label = str(base_metric_label_override)

        base_fcf_override = calculator_overrides.get('base_fcf')
        if base_fcf_override not in (None, ''):
            try:
                base_fcf = float(base_fcf_override)
            except (TypeError, ValueError):
                pass

        # -----------------------------------------------------------------------
        # STEP 3 — WACC + Size / Quality Premium
        # -----------------------------------------------------------------------
        total_capital = (market_cap_f + total_debt_f) if (market_cap_f + total_debt_f) > 0 else 1.0
        weight_e = market_cap_f / total_capital
        # FIX E: Debt weight uses book value from balance sheet (yfinance 'totalDebt').
        # Market value of debt would require bond price data not available in yfinance.
        # This approximation is reasonable for investment-grade companies. For distressed
        # companies or those with significant convertible notes, interpret WACC with caution.
        weight_d = total_debt_f / total_capital

        # FIX K: Use yfinance ^TNX (10Y Treasury yield index) - already imported,
        # no new dependencies. FRED fredgraph.json returns [[timestamp, float], ...]
        # arrays, not dicts, so the previous .get("value") parsing always failed silently.
        risk_free_rate = 0.043  # fallback: 10Y US Treasury
        try:
            _tnx = yf.download("^TNX", period="5d", progress=False, auto_adjust=True)
            if not _tnx.empty:
                _tnx_close = _tnx["Close"].dropna()
                # FIX K: Handle MultiIndex columns (yfinance 1.x returns DataFrame for ["Close"])
                if hasattr(_tnx_close, "ndim") and _tnx_close.ndim == 2:
                    if "^TNX" in _tnx_close.columns:
                        _tnx_close = _tnx_close["^TNX"]
                    elif len(_tnx_close.columns) == 1:
                        _tnx_close = _tnx_close.iloc[:, 0]
                if not _tnx_close.empty:
                    risk_free_rate = float(_tnx_close.iloc[-1]) / 100.0
        except Exception:
            pass  # silently use hardcoded fallback

        # ERP: Damodaran implied ERP, updated annually. Last confirmed: Jan 2025 = 4.60%.
        # Kept as a configurable constant rather than fetched - Damodaran publishes monthly
        # but the data is not in a stable machine-readable endpoint.
        # FIX: Was 5.5% which inflated WACC by ~1% across all stocks, causing systematic
        # undervaluation (50-85% below market for every mega-cap). 4.6% is Damodaran's
        # actual 2025 estimate.
        equity_risk_premium = 0.046

        # Cap beta at 2.5 for WACC purposes.  Extremely high betas (4x+) are
        # common for young, volatile stocks (crypto miners, early-stage AI) but
        # reflect stock-price volatility rather than genuine business risk.  An
        # uncapped beta of 4.18 produces a 27%+ cost of equity which makes every
        # DCF output negative — rendering the model useless.  A 2.5 cap plus an
        # explicit size premium keeps market volatility separate from failure risk.
        beta_for_wacc = min(beta_f, 2.5)
        extra_beta_risk = max(0.0, beta_f - 2.5)
        cost_of_equity = risk_free_rate + beta_for_wacc * equity_risk_premium

        if interest_expense_f > 0 and total_debt_f > 0:
            cost_of_debt = max(0.02, min(interest_expense_f / total_debt_f, 0.12))
        else:
            cost_of_debt = 0.05

        base_wacc = weight_e * cost_of_equity + weight_d * cost_of_debt * (1 - eff_tax_rate)

        # Size / quality premium — larger, stable firms need no extra padding
        if mc >= 200e9:
            size_premium = 0.000   # mega-cap
        elif mc >= 10e9:
            size_premium = 0.005   # mid/large-cap
        elif mc >= 2e9:
            size_premium = 0.010   # small-cap
        else:
            size_premium = 0.020   # micro-cap / speculative

        # Extra premium for elevated structural risk
        if company_type == 'turnaround' or (fcf_is_negative and eps_g < 0):
            size_premium += 0.010
        elif company_type == 'high_growth' and fcf_is_negative:
            size_premium += 0.015

        if extra_beta_risk > 0:
            size_premium += min(0.020, 0.005 + extra_beta_risk * 0.010)

        # WACC floor at 6%, ceiling at 20%.  The ceiling prevents extreme betas
        # and size premiums from producing discount rates so high that the model
        # outputs negative intrinsic values for every scenario.  20% still
        # represents a very aggressive discount and penalises risky companies
        # appropriately while keeping the model output meaningful.
        base_wacc_override = calculator_overrides.get('base_wacc')
        if base_wacc_override not in (None, ''):
            try:
                base_wacc = float(base_wacc_override)
            except (TypeError, ValueError):
                pass

        size_premium_override = calculator_overrides.get('size_premium')
        if size_premium_override not in (None, ''):
            try:
                size_premium = float(size_premium_override)
            except (TypeError, ValueError):
                pass

        wacc_override = calculator_overrides.get('wacc')
        if wacc_override not in (None, ''):
            try:
                wacc = max(min(float(wacc_override), 0.20), 0.06)
            except (TypeError, ValueError):
                wacc = max(min(base_wacc + size_premium, 0.20), 0.06)
        else:
            wacc = max(min(base_wacc + size_premium, 0.20), 0.06)

        # -----------------------------------------------------------------------
        # STEP 4 — Growth Rate Anchoring (type-specific caps + 3-tier deceleration)
        # -----------------------------------------------------------------------
        # Terminal growth by type.
        # Hard cap at 3.0% for any company with market cap > $500B:
        # at that scale, sustained growth above GDP is extremely unlikely.
        if company_type == 'stable':
            terminal_g = 0.025; max_base_g = 0.15
        elif company_type == 'cyclical':
            terminal_g = 0.022; max_base_g = 0.20
        elif company_type == 'mature_growth':
            # Blended view: established core (ads/Office) + growing cloud/AI segments.
            terminal_g = 0.030; max_base_g = 0.25
        elif company_type == 'high_growth':
            terminal_g = 0.030; max_base_g = 0.45
        elif company_type == 'speculative_growth':
            # More conservative than pure high_growth but not as pessimistic as cyclical.
            # Terminal stays near GDP pace, not above it.
            terminal_g = 0.025; max_base_g = 0.35
        else:   # turnaround
            terminal_g = 0.018; max_base_g = 0.15

        # Hard cap: no company above $500B market cap should use terminal_g > 3%.
        # At that scale sustained above-GDP-nominal growth is structurally very unlikely.
        terminal_growth_override = calculator_overrides.get('terminal_growth')
        if terminal_growth_override not in (None, ''):
            try:
                terminal_g = float(terminal_growth_override)
            except (TypeError, ValueError):
                pass

        if mc >= 500e9 and terminal_g > 0.030:
            terminal_g = 0.030

        share_dilution_rate = 0.0
        share_dilution_override = calculator_overrides.get('share_dilution_rate')
        if share_dilution_override in (None, ''):
            share_dilution_override = calculator_overrides.get('dilution_rate')
        if share_dilution_override not in (None, ''):
            try:
                share_dilution_rate = max(0.0, min(float(share_dilution_override), 0.10))
            except (TypeError, ValueError):
                pass
        elif company_type == 'speculative_growth':
            share_dilution_rate = 0.05
        elif company_type == 'high_growth' and fcf_is_negative:
            share_dilution_rate = 0.04
        elif company_type == 'turnaround':
            share_dilution_rate = 0.03
        elif company_type == 'mature_growth':
            share_dilution_rate = 0.01

        # Growth anchor: for speculative_growth, near-term negative/flat revenue growth
        # is company-specific (price cuts, capex cycle, product transition) rather than
        # representative of long-term potential. Use a floor of 8% to reflect the market's
        # assessment that the company has a plausible recovery/growth path.
        if dcf_method == 'revenue_margin':
            observed_g = effective_rev_g if effective_rev_g > 0 else 0.08
            if mc >= 200e9:
                growth_cap = 0.25
            elif mc >= 20e9:
                growth_cap = 0.40
            elif mc >= 5e9:
                growth_cap = 0.55
            else:
                growth_cap = 0.50
            base_g     = max(0.08, min(observed_g, growth_cap))
            bear_g     = max(0.02, base_g * 0.50)
            bull_g     = min(base_g * 1.30, growth_cap)
        elif company_type == 'speculative_growth':
            observed_g = max(abs(effective_rev_g) if effective_rev_g > 0 else 0.0, 0.08)
            base_g     = max(0.03, min(observed_g, max_base_g))
            bear_g     = max(0.00, base_g * 0.40)
            bull_g     = min(max_base_g * 1.5, base_g * 1.80)
        else:
            observed_g = abs(effective_rev_g) if effective_rev_g > 0 else (abs(eps_g) * 0.6 if eps_g > 0 else 0.05)
            base_g     = max(0.03, min(observed_g, max_base_g))
            bear_g     = max(0.00, base_g * 0.40)
            bull_g     = min(max_base_g * 1.5, base_g * 1.80)

        # -----------------------------------------------------------------------
        # STEP 5 — 3-Tier 10-Year DCF Projection
        # Years 1–3: tier-1 growth  (aggressive, anchored to recent actuals)
        # Years 4–7: tier-2 growth  (50% of tier-1, deceleration)
        # Years 8–10: tier-3 growth (50% of tier-2, normalization)
        # Terminal:   type-specific GDP-level rate
        # -----------------------------------------------------------------------
        def _dcf_3tier(fcf_base, g_t1, g_term, wacc_r):
            """10-year 3-tier DCF. Returns (pv_years_1_10, pv_terminal)."""
            # FIX: Deceleration was 50%/25% per tier which was far too aggressive.
            # A company growing 20% would drop to 5% by year 8. Changed to 65%/40%
            # for more realistic fade matching Wall Street analyst conventions.
            g_t2  = g_t1 * 0.65
            # Clamp g_t3 to max (terminal_g + 0.05) - prevents cliff-jump into Gordon Growth Model
            g_t3  = min(g_t1 * 0.40, g_term + 0.05)
            fcf   = fcf_base
            pv_sum = 0.0
            for yr in range(1, 11):
                g   = g_t1 if yr <= 3 else (g_t2 if yr <= 7 else g_t3)
                fcf *= (1 + g)
                pv_sum += fcf / (1 + wacc_r) ** yr
            fade_growth = np.linspace(g_t3, g_term, 6)[1:]
            for fade_year, fade_g in enumerate(fade_growth, start=11):
                fcf *= (1 + fade_g)
                pv_sum += fcf / (1 + wacc_r) ** fade_year
            spread = max(wacc_r - g_term, 0.03)
            pv_tv = (
                (fcf * (1 + g_term) / spread) / (1 + wacc_r) ** 15
                if wacc_r > g_term else 0.0
            )
            return pv_sum, pv_tv

        def _dcf_revenue_margin_3tier(revenue_base, g_t1, g_term, wacc_r, m_start, m_end):
            """10-year 3-tier DCF where FCF = Revenue x expanding FCF margin."""
            g_t2 = g_t1 * 0.65
            # Clamp g_t3 to max (terminal_g + 0.05) - bridge toward terminal
            g_t3 = min(g_t1 * 0.40, g_term + 0.05)
            rev = revenue_base
            pv_sum = 0.0
            fcf = 0.0
            diluted_shares = shares_f
            for yr in range(1, 11):
                g = g_t1 if yr <= 3 else (g_t2 if yr <= 7 else g_t3)
                rev *= (1 + g)
                if yr <= 3:
                    margin_progress = [0.08, 0.16, 0.24][yr - 1]
                elif yr <= 7:
                    margin_progress = [0.38, 0.52, 0.66, 0.78][yr - 4]
                else:
                    margin_progress = [0.86, 0.93, 1.00][yr - 8]
                margin_yr = m_start + (m_end - m_start) * margin_progress
                sbc_penalty = max(0.0, sbc_ratio - 0.03)
                effective_margin = max(margin_yr - min(sbc_penalty, 0.05), 0.0)
                fcf = rev * effective_margin
                pv_sum += fcf / (1 + wacc_r) ** yr
                diluted_shares *= (1 + share_dilution_rate)
            terminal_margin_start = m_start + (m_end - m_start)
            fade_growth = np.linspace(g_t3, g_term, 6)[1:]
            fade_margin_path = np.linspace(terminal_margin_start, m_end * 0.92, 5)
            for fade_year, fade_g in enumerate(fade_growth, start=11):
                rev *= (1 + fade_g)
                fade_margin = fade_margin_path[fade_year - 11]
                sbc_penalty = max(0.0, sbc_ratio - 0.03)
                effective_margin = max(fade_margin - min(sbc_penalty, 0.05), 0.0)
                fcf = rev * effective_margin
                pv_sum += fcf / (1 + wacc_r) ** fade_year
            spread = max(wacc_r - g_term, 0.03)
            pv_tv = (
                (fcf * (1 + g_term) / spread) / (1 + wacc_r) ** 15
                if wacc_r > g_term else 0.0
            )
            return pv_sum, pv_tv, diluted_shares

        # FIX F: Bull terminal capped at 4.0% - long-run nominal GDP ceiling (real ~2% + inflation ~2%)
        _BULL_TERMINAL_CAP = 0.040
        scenario_defs = [
            ('Bear', bear_g, terminal_g * 0.80),
            ('Base', base_g, terminal_g),
            ('Bull', bull_g, min(terminal_g * 1.20, _BULL_TERMINAL_CAP)),
        ]

        scenarios = {}
        has_projection_base = (
            (base_fcf is not None and base_fcf > 0) or
            (dcf_method == 'revenue_margin' and total_revenue_f is not None and total_revenue_f > 0)
        )
        if has_projection_base:
            for label, g_t1, g_term in scenario_defs:
                if dcf_method == 'revenue_margin':
                    m_end = margin_target or 0.15
                    if label == 'Bear':
                        m_end = max((margin_start or 0.01) + 0.01, m_end * 0.80)
                    elif label == 'Bull':
                        m_end = min(0.30, m_end * 1.10)
                    pv_fcf, pv_tv, projected_shares = _dcf_revenue_margin_3tier(total_revenue_f, g_t1, g_term, wacc, margin_start or 0.01, m_end)
                else:
                    pv_fcf, pv_tv = _dcf_3tier(base_fcf, g_t1, g_term, wacc)
                    projected_shares = shares_f
                equity_val = pv_fcf + pv_tv + total_cash_f - total_debt_f
                # FIX H: Equity value floors at 0 - negative equity is not a valid per-share price
                _equity_negative = equity_val < 0
                equity_val = max(equity_val, 0.0)
                intrinsic  = equity_val / projected_shares if projected_shares else 0.0
                updown     = (intrinsic / price_f - 1) * 100 if price_f else 0.0
                scenarios[label] = {
                    'growth_tier1':        round(g_t1,        4),   # years 1–3
                    'growth_tier2':        round(g_t1 * 0.65, 4),   # years 4–7
                    # Report the actual clamped g_t3 used inside _dcf_3tier
                    'growth_tier3':        round(min(g_t1 * 0.40, g_term + 0.05), 4),   # actual rate used (clamped)
                    'terminal_growth':     round(g_term,      4),
                    'pv_fcf_10yr':         round(pv_fcf / 1e9, 2),  # $B
                    'pv_terminal':         round(pv_tv  / 1e9, 2),  # $B
                    'intrinsic_value':     round(intrinsic,   2),
                    'upside_downside_pct': round(updown,      1),
                    # FIX H: Flag negative equity value for downstream messaging
                    'equity_negative':     _equity_negative,
                }
                if dcf_method == 'revenue_margin':
                    scenarios[label]['year1_fcf_margin'] = round(margin_start or 0.0, 4)
                    scenarios[label]['terminal_fcf_margin'] = round(m_end, 4)
                    scenarios[label]['projected_shares_10yr'] = round(projected_shares, 2)

        # -----------------------------------------------------------------------
        # STEP 6 — Market-Implied Growth Rate (binary search on tier-1 growth)
        # The single best output in the model — what is the market already pricing in?
        # -----------------------------------------------------------------------
        market_implied_growth = None
        if dcf_method != 'revenue_margin' and base_fcf and price_f and price_f > 0:
            target_equity = price_f * shares_f
            target_ev     = target_equity + total_debt_f - total_cash_f
            lo, hi = -0.20, 0.80
            pv_s = 0.0
            pv_tv_s = 0.0
            for _ in range(70):
                mid              = (lo + hi) / 2
                pv_s, pv_tv_s    = _dcf_3tier(base_fcf, mid, terminal_g, wacc)
                if (pv_s + pv_tv_s) > target_ev:
                    hi = mid
                else:
                    lo = mid
            market_implied_growth = round((lo + hi) / 2, 4)
            if abs((pv_s + pv_tv_s) - target_ev) / max(target_ev, 1.0) > 0.25:
                market_implied_growth = None

        # FIX I: Market-implied terminal margin for revenue_margin companies
        market_implied_margin = None
        if (
            dcf_method == 'revenue_margin'
            and total_revenue_f and total_revenue_f > 0
            and price_f and price_f > 0
            and shares_f and shares_f > 0
            and margin_start is not None
        ):
            target_equity_rm = price_f * shares_f
            lo_m, hi_m = 0.00, 0.30
            for _ in range(70):
                mid_m = (lo_m + hi_m) / 2
                pv_s_m, pv_tv_m, projected_shares_m = _dcf_revenue_margin_3tier(
                    total_revenue_f, base_g, terminal_g, wacc,
                    margin_start, mid_m
                )
                implied_eq = pv_s_m + pv_tv_m + total_cash_f - total_debt_f
                if implied_eq > target_equity_rm:
                    hi_m = mid_m
                else:
                    lo_m = mid_m
            implied_margin_candidate = round((lo_m + hi_m) / 2, 4)
            if abs(implied_eq - target_equity_rm) / max(target_equity_rm, 1.0) > 0.25:
                market_implied_margin = None
            else:
                market_implied_margin = implied_margin_candidate

        # -----------------------------------------------------------------------
        # STEP 7 — Implied Annual Return at Base Case
        # "If I buy today and the base case plays out over 10 years, what's my CAGR?"
        # Below 8%: priced for perfection. Above 15%: margin of safety exists.
        # -----------------------------------------------------------------------
        implied_annual_return = None
        if has_projection_base and price_f and price_f > 0 and 'Base' in scenarios:
            base_iv = scenarios['Base']['intrinsic_value']
            if base_iv > 0:
                implied_annual_return = round((base_iv / price_f) ** (1 / 10) - 1, 4)

        # -----------------------------------------------------------------------
        # STEP 8 — Sensitivity Table: intrinsic value grid (WACC × tier-1 growth)
        # -----------------------------------------------------------------------
        sensitivity = {}
        # Only generate sensitivity table if we have reliable shares data
        # (shares_f = 0 means missing data; don't proceed to avoid massive scaling errors)
        if has_projection_base and shares_f and shares_f > 0:
            wacc_offsets = [-0.02, -0.01, 0.00, +0.01, +0.02]
            if dcf_method == 'revenue_margin':
                margin_keys = [
                    max((margin_start or 0.01) + 0.01, (margin_target or 0.15) - 0.04),
                    max((margin_start or 0.01) + 0.02, (margin_target or 0.15) - 0.02),
                    (margin_target or 0.15),
                    min(0.30, (margin_target or 0.15) + 0.02),
                    min(0.30, (margin_target or 0.15) + 0.04),
                ]
                margin_keys = sorted(set(round(m, 4) for m in margin_keys if m > 0))
                for dw in wacc_offsets:
                    w = max(wacc + dw, 0.04)
                    row = {}
                    for m in margin_keys:
                        pv_s, pv_tv_s, projected_shares = _dcf_revenue_margin_3tier(total_revenue_f, base_g, terminal_g, w, margin_start or 0.01, m)
                        eq_val = pv_s + pv_tv_s + total_cash_f - total_debt_f
                        # FIX M: Floor equity value at 0 - consistent with Fix H in scenarios
                        eq_val = max(eq_val, 0.0)
                        row[m] = _round_to_half(eq_val / projected_shares) if projected_shares else 0.0
                    sensitivity[round(w, 4)] = row
            else:
                # FIX J: Sensitivity growth rates anchored symmetrically around base_g
                _multipliers = [0.40, 0.70, 1.00, 1.30, 1.60]
                growth_rates = sorted(set(
                    round(max(0.02, min(base_g * m, max_base_g)), 4)
                    for m in _multipliers
                ))
                for dw in wacc_offsets:
                    w   = max(wacc + dw, 0.04)
                    row = {}
                    for g in growth_rates:
                        pv_s, pv_tv_s = _dcf_3tier(base_fcf, g, terminal_g, w)
                        eq_val        = pv_s + pv_tv_s + total_cash_f - total_debt_f
                        # FIX M: Floor equity value at 0 - consistent with Fix H in scenarios
                        eq_val        = max(eq_val, 0.0)
                        row[g]        = _round_to_half(eq_val / shares_f) if shares_f else 0.0
                    sensitivity[round(w, 4)] = row

        return {
            'company_type':          company_type,
            'company_type_label':    company_type_labels[company_type],
            'base_metric_label':     base_metric_label,
            'dcf_method':            dcf_method,
            'sensitivity_mode':      sensitivity_mode,
            'base_fcf':              base_fcf,
            'base_revenue':          total_revenue_f,
            'margin_start':          margin_start,
            'margin_target':         margin_target,
            'margin_profile':        margin_profile,
            'projection_horizon_years': 15,
            'sbc_ratio':             round(sbc_ratio, 4),
            'stock_based_comp':      round(stock_based_comp, 2),
            'dilution_rate':         round(share_dilution_rate, 4),
            'fcf_ttm':               fcf_ttm_f,
            'ocf_ttm':               ocf_ttm_f,
            'historical_fcf':        historical_fcf,
            'total_debt':            total_debt_f,
            'total_cash':            total_cash_f,
            'net_debt':              total_debt_f - total_cash_f,
            'shares_outstanding':    shares_f,
            'current_price':         price_f,
            'beta':                  beta_f,
            'cost_of_equity':        round(cost_of_equity,  4),
            'cost_of_debt':          round(cost_of_debt,    4),
            'eff_tax_rate':          round(eff_tax_rate,    4),
            'wacc':                  round(wacc,            4),
            'base_wacc':             round(base_wacc,       4),
            'size_premium':          round(size_premium,    4),
            'weight_equity':         round(weight_e,        4),
            'weight_debt':           round(weight_d,        4),
            'risk_free_rate':        risk_free_rate,
            'equity_risk_premium':   equity_risk_premium,
            'terminal_growth':       terminal_g,
            'scenarios':             scenarios,
            'market_implied_growth': market_implied_growth,
            # FIX I: Market-implied terminal margin for revenue_margin companies
            'market_implied_margin': market_implied_margin,
            'implied_annual_return': implied_annual_return,
            'sensitivity':           sensitivity,
        }

    except Exception as e:
        logging.error(f"DCF extraction error for {ticker}: {e}", exc_info=True)
        return {'error': str(e)}

# --- Example Usage ---
if __name__ == "__main__":
    # Test with a ticker
    ticker_symbol = "AAPL"
    print(f"Running DCF Valuation for {ticker_symbol}...")
    try:
        t = yf.Ticker(ticker_symbol)
        info = t.info
        result = extract_dcf_data({'info': info}, ticker=ticker_symbol)
        print("\nDCF Result for", ticker_symbol)
        if 'error' in result:
            print("Error:", result['error'])
        else:
            print("Company Type:", result.get('company_type_label'))
            print("WACC:", result.get('wacc'))
            if 'Base' in result.get('scenarios', {}):
                base_case = result['scenarios']['Base']
                print("Intrinsic Value (Base Case):", base_case.get('intrinsic_value'))
                print("Upside/Downside:", base_case.get('upside_downside_pct'), "%")
    except Exception as e:
        print("Failed to run example:", e)