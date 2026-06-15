-- ============================================================================
-- TICKZEN SUPABASE DATABASE SCHEMA
-- ============================================================================
-- Generated: Complete DDL statements for Tickzen stock analysis platform
-- Database: PostgreSQL (Supabase)
-- ============================================================================

-- ============================================================================
-- EXTENSIONS
-- ============================================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- STOCKS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS stocks (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL UNIQUE,
    ticker VARCHAR(10) NOT NULL,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry TEXT,
    country VARCHAR(50),
    exchange VARCHAR(50),
    website_url TEXT,
    employee_count INTEGER,
    business_summary TEXT,
    long_business_summary TEXT,
    headquarters VARCHAR(255),
    ceo_name VARCHAR(255),
    founded_year INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_sync_date TIMESTAMP WITH TIME ZONE,
    last_sync_status VARCHAR(50),
    data_quality_score NUMERIC(5, 2),
    data_start_date DATE,
    data_end_date DATE,
    total_records INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    sync_enabled BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_stocks_symbol ON stocks (symbol);
CREATE INDEX IF NOT EXISTS idx_stocks_ticker ON stocks (ticker);
CREATE INDEX IF NOT EXISTS idx_stocks_sector ON stocks (sector);

-- ============================================================================
-- DAILY_PRICE_DATA TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS daily_price_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    date DATE NOT NULL,
    open_price NUMERIC(16, 4),
    high_price NUMERIC(16, 4),
    low_price NUMERIC(16, 4),
    close_price NUMERIC(16, 4),
    adjusted_close NUMERIC(16, 4),
    volume BIGINT,
    daily_return_pct NUMERIC(10, 4),
    price_change NUMERIC(10, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_daily_price_stock_id ON daily_price_data (stock_id);
CREATE INDEX IF NOT EXISTS idx_daily_price_date ON daily_price_data (date DESC);

-- ============================================================================
-- MARKET_PRICE_SNAPSHOT TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS market_price_snapshot (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    date DATE NOT NULL,
    current_price NUMERIC(16, 4),
    price_change NUMERIC(10, 4),
    change_pct NUMERIC(10, 4),
    change_15d_pct NUMERIC(10, 4),
    change_52w_pct NUMERIC(10, 4),
    performance_1y_pct NUMERIC(10, 4),
    overall_pct_change NUMERIC(10, 4),
    high_52w NUMERIC(16, 4),
    low_52w NUMERIC(16, 4),
    market_cap NUMERIC(20, 2),
    enterprise_value NUMERIC(20, 2),
    shares_outstanding BIGINT,
    float_shares BIGINT,
    sp500_index NUMERIC(16, 4),
    interest_rate NUMERIC(8, 4),
    from_52wk_high_pct NUMERIC(10, 4),
    from_52wk_low_pct NUMERIC(10, 4),
    pe_ratio NUMERIC(10, 4),
    pb_ratio NUMERIC(10, 4),
    day_trend VARCHAR(20),
    momentum_score NUMERIC(8, 4),
    price_alert_flags JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_market_snapshot_stock_id ON market_price_snapshot (stock_id);

-- ============================================================================
-- TECHNICAL_INDICATORS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS technical_indicators (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    date DATE NOT NULL,
    sma_7 NUMERIC(16, 4),
    sma_20 NUMERIC(16, 4),
    sma_50 NUMERIC(16, 4),
    sma_100 NUMERIC(16, 4),
    sma_200 NUMERIC(16, 4),
    ema_12 NUMERIC(16, 4),
    ema_26 NUMERIC(16, 4),
    rsi_14 NUMERIC(10, 4),
    macd_line NUMERIC(16, 4),
    macd_signal NUMERIC(16, 4),
    macd_histogram NUMERIC(16, 4),
    stochastic_osc NUMERIC(10, 4),
    bb_upper NUMERIC(16, 4),
    bb_middle NUMERIC(16, 4),
    bb_lower NUMERIC(16, 4),
    atr_14 NUMERIC(16, 4),
    volatility_7d NUMERIC(10, 4),
    volatility_30d_annual NUMERIC(10, 4),
    volume_sma_20 BIGINT,
    obv NUMERIC(20, 4),
    green_days_count INTEGER,
    support_30d NUMERIC(16, 4),
    resistance_30d NUMERIC(16, 4),
    adx NUMERIC(10, 4),
    keltner_upper NUMERIC(16, 4),
    keltner_lower NUMERIC(16, 4),
    volume_trend_5d NUMERIC(10, 4),
    chaikin_money_flow NUMERIC(16, 4),
    avg_volume_3m BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_technical_stock_id ON technical_indicators (stock_id);
CREATE INDEX IF NOT EXISTS idx_technical_date ON technical_indicators (date DESC);

-- ============================================================================
-- FUNDAMENTAL_DATA TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS fundamental_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    period_date DATE,
    period_type VARCHAR(20),
    pe_ratio NUMERIC(10, 4),
    pe_forward NUMERIC(10, 4),
    price_to_sales NUMERIC(10, 4),
    price_to_book NUMERIC(10, 4),
    ev_to_revenue NUMERIC(10, 4),
    ev_to_ebitda NUMERIC(10, 4),
    price_to_fcf NUMERIC(10, 4),
    net_margin NUMERIC(10, 4),
    operating_margin NUMERIC(10, 4),
    gross_margin NUMERIC(10, 4),
    ebitda_margin NUMERIC(10, 4),
    roe NUMERIC(10, 4),
    roa NUMERIC(10, 4),
    debt_to_equity NUMERIC(10, 4),
    total_cash NUMERIC(20, 2),
    total_debt NUMERIC(20, 2),
    free_cash_flow NUMERIC(20, 2),
    operating_cash_flow NUMERIC(20, 2),
    current_ratio NUMERIC(10, 4),
    quick_ratio NUMERIC(10, 4),
    revenue_ttm NUMERIC(20, 2),
    revenue_growth_yoy NUMERIC(10, 4),
    net_income_ttm NUMERIC(20, 2),
    earnings_growth_yoy NUMERIC(10, 4),
    ebitda_ttm NUMERIC(20, 2),
    gross_profit_ttm NUMERIC(20, 2),
    peg_ratio NUMERIC(10, 4),
    roic NUMERIC(10, 4),
    asset_turnover NUMERIC(10, 4),
    inventory_turnover NUMERIC(10, 4),
    receivables_turnover NUMERIC(10, 4),
    working_capital_turnover NUMERIC(10, 4),
    dso NUMERIC(10, 4),
    dio NUMERIC(10, 4),
    ccc NUMERIC(10, 4),
    operating_income NUMERIC(20, 2),
    eps_basic NUMERIC(10, 4),
    eps_diluted NUMERIC(10, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_fundamental_stock_id ON fundamental_data (stock_id);

-- ============================================================================
-- FORECAST_DATA TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS forecast_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    forecast_date DATE,
    forecast_price_1y NUMERIC(16, 4),
    forecast_avg_price NUMERIC(16, 4),
    forecast_range_width NUMERIC(10, 4),
    forecast_period VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_forecast_stock_id ON forecast_data (stock_id);

-- ============================================================================
-- ANALYST_DATA TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS analyst_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    target_price_mean NUMERIC(16, 4),
    target_price_median NUMERIC(16, 4),
    target_price_high NUMERIC(16, 4),
    target_price_low NUMERIC(16, 4),
    analyst_rating VARCHAR(50),
    analyst_count INTEGER,
    next_earnings_date DATE,
    earnings_call_time_utc VARCHAR(50),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_analyst_stock_id ON analyst_data (stock_id);

-- ============================================================================
-- DIVIDEND_DATA TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS dividend_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    dividend_rate NUMERIC(10, 4),
    dividend_yield_pct NUMERIC(10, 4),
    payout_ratio NUMERIC(10, 4),
    avg_dividend_yield_5y NUMERIC(10, 4),
    dividend_forward_rate NUMERIC(10, 4),
    dividend_forward_yield NUMERIC(10, 4),
    dividend_trailing_rate NUMERIC(10, 4),
    dividend_trailing_yield NUMERIC(10, 4),
    ex_dividend_date DATE,
    payment_date DATE,
    last_split_date DATE,
    last_split_factor VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dividend_stock_id ON dividend_data (stock_id);

-- ============================================================================
-- OWNERSHIP_DATA TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS ownership_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    report_date DATE,
    shares_outstanding BIGINT,
    shares_outstanding_diluted BIGINT,
    insider_ownership_pct NUMERIC(10, 4),
    institutional_ownership_pct NUMERIC(10, 4),
    shares_short BIGINT,
    short_ratio_days NUMERIC(10, 4),
    short_pct_float NUMERIC(10, 4),
    shares_short_prev BIGINT,
    shares_change_yoy BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ownership_stock_id ON ownership_data (stock_id);

-- ============================================================================
-- INSIDER_TRANSACTIONS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS insider_transactions (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    insider_name VARCHAR(255),
    relation_to_company VARCHAR(100),
    transaction_date DATE,
    shares_change BIGINT,
    transaction_price NUMERIC(16, 4),
    estimated_value NUMERIC(20, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_insider_stock_id ON insider_transactions (stock_id);
CREATE INDEX IF NOT EXISTS idx_insider_transaction_date ON insider_transactions (transaction_date DESC);

-- ============================================================================
-- RISK_DATA TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS risk_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    date DATE NOT NULL,
    var_95 NUMERIC(16, 4),
    var_99 NUMERIC(16, 4),
    sharpe_ratio NUMERIC(10, 4),
    sortino_ratio NUMERIC(10, 4),
    calmar_ratio NUMERIC(10, 4),
    max_drawdown NUMERIC(10, 4),
    beta NUMERIC(10, 4),
    market_correlation NUMERIC(10, 4),
    skewness NUMERIC(10, 4),
    kurtosis NUMERIC(10, 4),
    cvar_95 NUMERIC(16, 4),
    cvar_99 NUMERIC(16, 4),
    volatility_30d_annual NUMERIC(10, 4),
    volatility_historical_annual NUMERIC(10, 4),
    var_95_data_period_days INTEGER,
    var_95_sample_size INTEGER,
    var_95_calculation_method VARCHAR(100),
    var_95_confidence_level NUMERIC(5, 2),
    var_95_return_frequency VARCHAR(20),
    var_99_data_period_days INTEGER,
    var_99_sample_size INTEGER,
    var_99_calculation_method VARCHAR(100),
    var_99_confidence_level NUMERIC(5, 2),
    var_99_return_frequency VARCHAR(20),
    cvar_95_data_period_days INTEGER,
    cvar_95_tail_size INTEGER,
    cvar_95_calculation_method VARCHAR(100),
    cvar_95_confidence_level NUMERIC(5, 2),
    cvar_99_data_period_days INTEGER,
    cvar_99_tail_size INTEGER,
    cvar_99_calculation_method VARCHAR(100),
    cvar_99_confidence_level NUMERIC(5, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_risk_stock_id ON risk_data (stock_id);

-- ============================================================================
-- SENTIMENT_DATA TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS sentiment_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    date DATE,
    sentiment_score NUMERIC(8, 4),
    sentiment_label VARCHAR(50),
    sentiment_confidence NUMERIC(8, 4),
    analyst_sentiment NUMERIC(8, 4),
    news_sentiment NUMERIC(8, 4),
    options_sentiment NUMERIC(8, 4),
    put_call_ratio NUMERIC(8, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_sentiment_stock_id ON sentiment_data (stock_id);

-- ============================================================================
-- ALTMAN_ZSCORE_DATA TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS altman_zscore_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    date DATE NOT NULL,
    z_score NUMERIC(10, 4),
    risk_zone VARCHAR(50),
    bankruptcy_risk NUMERIC(10, 4),
    data_quality VARCHAR(50),
    working_capital_ratio NUMERIC(10, 4),
    retained_earnings_ratio NUMERIC(10, 4),
    ebit_ratio NUMERIC(10, 4),
    market_value_ratio NUMERIC(10, 4),
    sales_ratio NUMERIC(10, 4),
    components JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_altman_stock_id ON altman_zscore_data (stock_id);

-- ============================================================================
-- LIQUIDITY_RISK_DATA TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS liquidity_risk_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    date DATE NOT NULL,
    liquidity_score NUMERIC(10, 4),
    risk_level VARCHAR(50),
    trading_volume_consistency NUMERIC(10, 4),
    market_depth_score NUMERIC(10, 4),
    components JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_liquidity_stock_id ON liquidity_risk_data (stock_id);

-- ============================================================================
-- REGIME_RISK_DATA TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS regime_risk_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    date DATE NOT NULL,
    current_regime VARCHAR(50),
    regime_confidence NUMERIC(8, 4),
    bull_volatility NUMERIC(10, 4),
    bear_downside_capture NUMERIC(10, 4),
    correction_volatility NUMERIC(10, 4),
    regime_analysis JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_regime_stock_id ON regime_risk_data (stock_id);

-- ============================================================================
-- PEER_COMPARISON_DATA TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS peer_comparison_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL,
    peer_ticker VARCHAR(10) NOT NULL,
    peer_name VARCHAR(255),
    is_target BOOLEAN DEFAULT FALSE,
    market_cap NUMERIC(20, 2),
    pe_ratio NUMERIC(10, 4),
    revenue_growth NUMERIC(10, 4),
    net_margin NUMERIC(10, 4),
    eps NUMERIC(10, 4),
    roe NUMERIC(10, 4),
    debt_to_equity NUMERIC(10, 4),
    dividend_yield NUMERIC(10, 4),
    week_52_high NUMERIC(16, 4),
    week_52_low NUMERIC(16, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_peer_stock_id ON peer_comparison_data (stock_id);

-- ============================================================================
-- DATA_SYNC_LOG TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS data_sync_log (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT,
    sync_type VARCHAR(50),
    sync_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_deleted INTEGER,
    records_failed INTEGER,
    sync_status VARCHAR(50),
    error_message TEXT,
    sync_duration_seconds INTEGER,
    data_quality_score NUMERIC(5, 2),
    source_api VARCHAR(100),
    api_version VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sync_log_stock_id ON data_sync_log (stock_id);

-- ============================================================================
-- TRIGGERS FOR AUTO-UPDATING updated_at
-- ============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

-- Apply trigger to all tables with updated_at column
DO $$
DECLARE
    tbl RECORD;
BEGIN
    FOR tbl IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE tablename IN (
            'stocks', 'daily_price_data', 'technical_indicators', 'fundamental_data',
            'forecast_data', 'analyst_data', 'dividend_data', 'ownership_data',
            'insider_transactions', 'risk_data', 'sentiment_data', 'altman_zscore_data',
            'liquidity_risk_data', 'regime_risk_data', 'peer_comparison_data'
        )
    LOOP
        EXECUTE format('
            CREATE TRIGGER update_%I_updated_at
            BEFORE UPDATE ON %I
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()',
            tbl.tablename, tbl.tablename
        );
    END LOOP;
END $$;

-- ============================================================================
-- ROW LEVEL SECURITY (Optional - enable as needed)
-- ============================================================================
-- ALTER TABLE stocks ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE daily_price_data ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE technical_indicators ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE fundamental_data ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE forecast_data ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE analyst_data ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE dividend_data ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE ownership_data ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE insider_transactions ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE risk_data ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE sentiment_data ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE altman_zscore_data ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE liquidity_risk_data ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE regime_risk_data ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE peer_comparison_data ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE data_sync_log ENABLE ROW LEVEL SECURITY;

-- ============================================================================
-- POLICIES (Optional - uncomment to enable public access)
-- ============================================================================
-- CREATE POLICY "Allow public read access" ON stocks FOR SELECT USING (true);
-- CREATE POLICY "Allow public read access" ON daily_price_data FOR SELECT USING (true);
-- CREATE POLICY "Allow public read access" ON technical_indicators FOR SELECT USING (true);
-- CREATE POLICY "Allow public read access" ON fundamental_data FOR SELECT USING (true);
-- CREATE POLICY "Allow public read access" ON forecast_data FOR SELECT USING (true);
-- CREATE POLICY "Allow public read access" ON analyst_data FOR SELECT USING (true);
-- CREATE POLICY "Allow public read access" ON dividend_data FOR SELECT USING (true);
-- CREATE POLICY "Allow public read access" ON ownership_data FOR SELECT USING (true);
-- CREATE POLICY "Allow public read access" ON insider_transactions FOR SELECT USING (true);
-- CREATE POLICY "Allow public read access" ON risk_data FOR SELECT USING (true);
-- CREATE POLICY "Allow public read access" ON sentiment_data FOR SELECT USING (true);

-- ============================================================================
-- NOTES
-- ============================================================================
-- This schema is designed for the Tickzen stock analysis platform
-- Run this entire script in Supabase SQL Editor to initialize your database
-- The schema includes 15 tables with appropriate indexes and foreign key constraints