#!/usr/bin/env python3
"""
Azure Database Schema Setup
============================
Generates and executes the complete database schema on Azure PostgreSQL
based on the Supabase schema documentation.
"""

import os
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from database.azure_postgres_client import AzurePostgresClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Complete database schema with data types based on the documentation
SCHEMA_SQL = """
-- ============================================================================
-- TICKZEN COMPLETE DATABASE SCHEMA FOR AZURE POSTGRESQL
-- ============================================================================
-- Generated from Supabase schema documentation
-- ============================================================================

-- Drop existing tables if they exist (clean setup)
DROP TABLE IF EXISTS earnings_calendar CASCADE;
DROP TABLE IF EXISTS altman_zscore_data CASCADE;
DROP TABLE IF EXISTS analyst_data CASCADE;
DROP TABLE IF EXISTS daily_price_data CASCADE;
DROP TABLE IF EXISTS data_sync_log CASCADE;
DROP TABLE IF EXISTS dividend_data CASCADE;
DROP TABLE IF EXISTS forecast_data CASCADE;
DROP TABLE IF EXISTS fundamental_data CASCADE;
DROP TABLE IF EXISTS insider_transactions CASCADE;
DROP TABLE IF EXISTS liquidity_risk_data CASCADE;
DROP TABLE IF EXISTS market_price_snapshot CASCADE;
DROP TABLE IF EXISTS ownership_data CASCADE;
DROP TABLE IF EXISTS peer_comparison_data CASCADE;
DROP TABLE IF EXISTS regime_risk_data CASCADE;
DROP TABLE IF EXISTS risk_data CASCADE;
DROP TABLE IF EXISTS sentiment_data CASCADE;
DROP TABLE IF EXISTS technical_indicators CASCADE;
DROP TABLE IF EXISTS stock_news_data CASCADE;
DROP TABLE IF EXISTS stocks CASCADE;

-- ============================================================================
-- STOCKS TABLE (Main table - must be created first)
-- ============================================================================
CREATE TABLE stocks (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    country VARCHAR(50),
    exchange VARCHAR(50),
    website_url VARCHAR(255),
    employee_count INTEGER,
    business_summary TEXT,
    long_business_summary TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_sync_date TIMESTAMP WITH TIME ZONE,
    last_sync_status VARCHAR(20),
    data_quality_score NUMERIC(3,2),
    data_start_date DATE,
    data_end_date DATE,
    total_records INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    sync_enabled BOOLEAN DEFAULT true,
    headquarters VARCHAR(255),
    ceo_name VARCHAR(255),
    founded_year INTEGER
);

CREATE INDEX idx_stocks_ticker ON stocks(ticker);
CREATE INDEX idx_stocks_sector ON stocks(sector);
CREATE INDEX idx_stocks_exchange ON stocks(exchange);

-- ============================================================================
-- STOCK_NEWS_DATA
-- ============================================================================
CREATE TABLE stock_news_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    summary TEXT,
    url TEXT,
    publisher TEXT,
    published_date TIMESTAMP WITH TIME ZONE,
    sentiment_score NUMERIC(8, 4),
    sentiment_label TEXT,
    relevance_score NUMERIC(8, 4) DEFAULT 1.0,
    category TEXT,
    source_api TEXT DEFAULT 'yfinance',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, url, published_date)
);

CREATE INDEX idx_stock_news_stock_id ON stock_news_data(stock_id);
CREATE INDEX idx_stock_news_published ON stock_news_data(published_date DESC);
CREATE INDEX idx_stock_news_stock_published ON stock_news_data(stock_id, published_date DESC);

-- ============================================================================
-- ALTMAN_ZSCORE_DATA
-- ============================================================================
CREATE TABLE altman_zscore_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    z_score NUMERIC(10, 4),
    risk_zone VARCHAR(50),
    bankruptcy_risk VARCHAR(50),
    data_quality VARCHAR(50),
    working_capital_ratio NUMERIC(10, 4),
    retained_earnings_ratio NUMERIC(10, 4),
    ebit_ratio NUMERIC(10, 4),
    market_value_ratio NUMERIC(10, 4),
    sales_ratio NUMERIC(10, 4),
    components JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, date)
);

CREATE INDEX idx_altman_stock_date ON altman_zscore_data(stock_id, date);

-- ============================================================================
-- ANALYST_DATA
-- ============================================================================
CREATE TABLE analyst_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    target_price_mean NUMERIC(15, 4),
    target_price_median NUMERIC(15, 4),
    target_price_high NUMERIC(15, 4),
    target_price_low NUMERIC(15, 4),
    analyst_rating VARCHAR(50),
    analyst_count INTEGER,
    next_earnings_date DATE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id)
);

CREATE INDEX idx_analyst_stock ON analyst_data(stock_id);

-- ============================================================================
-- DAILY_PRICE_DATA
-- ============================================================================
CREATE TABLE daily_price_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    open_price NUMERIC(15, 4),
    high_price NUMERIC(15, 4),
    low_price NUMERIC(15, 4),
    close_price NUMERIC(15, 4),
    adjusted_close NUMERIC(15, 4),
    volume BIGINT,
    daily_return_pct NUMERIC(10, 4),
    price_change NUMERIC(15, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, date)
);

CREATE INDEX idx_daily_price_stock_date ON daily_price_data(stock_id, date DESC);

-- ============================================================================
-- DATA_SYNC_LOG
-- ============================================================================
CREATE TABLE data_sync_log (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT REFERENCES stocks(id) ON DELETE SET NULL,
    sync_type VARCHAR(50) NOT NULL,
    sync_date TIMESTAMP WITH TIME ZONE NOT NULL,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_deleted INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    sync_status VARCHAR(50) NOT NULL,
    error_message TEXT,
    sync_duration_seconds NUMERIC(10, 2),
    data_quality_score NUMERIC(5, 2),
    source_api VARCHAR(100),
    api_version VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_sync_log_stock ON data_sync_log(stock_id);
CREATE INDEX idx_sync_log_date ON data_sync_log(sync_date DESC);
CREATE INDEX idx_sync_log_status ON data_sync_log(sync_status);

-- ============================================================================
-- DIVIDEND_DATA
-- ============================================================================
CREATE TABLE dividend_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
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
    last_split_factor VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id)
);

CREATE INDEX idx_dividend_stock ON dividend_data(stock_id);

-- ============================================================================
-- FORECAST_DATA
-- ============================================================================
CREATE TABLE forecast_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    forecast_date DATE NOT NULL,
    forecast_price_1y NUMERIC(15, 4),
    forecast_avg_price NUMERIC(15, 4),
    forecast_range_width NUMERIC(15, 4),
    forecast_period VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, forecast_date, forecast_period)
);

CREATE INDEX idx_forecast_stock_date ON forecast_data(stock_id, forecast_date DESC);

-- ============================================================================
-- FUNDAMENTAL_DATA
-- ============================================================================
CREATE TABLE fundamental_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    period_date DATE NOT NULL,
    period_type VARCHAR(20) NOT NULL,
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
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    peg_ratio NUMERIC(10, 4),
    roic NUMERIC(10, 4),
    asset_turnover NUMERIC(10, 4),
    inventory_turnover NUMERIC(10, 4),
    receivables_turnover NUMERIC(10, 4),
    working_capital_turnover NUMERIC(10, 4),
    dso NUMERIC(10, 2),
    dio NUMERIC(10, 2),
    ccc NUMERIC(10, 2),
    operating_income NUMERIC(20, 2),
    eps_basic NUMERIC(10, 4),
    eps_diluted NUMERIC(10, 4),
    UNIQUE(stock_id, period_date, period_type)
);

CREATE INDEX idx_fundamental_stock_period ON fundamental_data(stock_id, period_date DESC);

-- ============================================================================
-- INSIDER_TRANSACTIONS
-- ============================================================================
CREATE TABLE insider_transactions (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    insider_name VARCHAR(255),
    relation_to_company VARCHAR(255),
    transaction_date DATE,
    shares_change BIGINT,
    transaction_price NUMERIC(15, 4),
    estimated_value NUMERIC(20, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_insider_stock_date ON insider_transactions(stock_id, transaction_date DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_insider_unique ON insider_transactions(stock_id, insider_name, transaction_date, shares_change);

-- ============================================================================
-- LIQUIDITY_RISK_DATA
-- ============================================================================
CREATE TABLE liquidity_risk_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    liquidity_score NUMERIC(10, 4),
    risk_level VARCHAR(50),
    trading_volume_consistency NUMERIC(10, 4),
    market_depth_score NUMERIC(10, 4),
    components JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, date)
);

CREATE INDEX idx_liquidity_stock_date ON liquidity_risk_data(stock_id, date DESC);

-- ============================================================================
-- MARKET_PRICE_SNAPSHOT
-- ============================================================================
CREATE TABLE market_price_snapshot (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    current_price NUMERIC(15, 4),
    price_change NUMERIC(15, 4),
    change_pct NUMERIC(10, 4),
    change_15d_pct NUMERIC(10, 4),
    change_52w_pct NUMERIC(10, 4),
    performance_1y_pct NUMERIC(10, 4),
    overall_pct_change NUMERIC(10, 4),
    high_52w NUMERIC(15, 4),
    low_52w NUMERIC(15, 4),
    market_cap BIGINT,
    enterprise_value BIGINT,
    shares_outstanding BIGINT,
    float_shares BIGINT,
    sp500_index NUMERIC(10, 4),
    interest_rate NUMERIC(10, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    from_52wk_high_pct NUMERIC(10, 4),
    from_52wk_low_pct NUMERIC(10, 4),
    pe_ratio NUMERIC(10, 4),
    pb_ratio NUMERIC(10, 4),
    day_trend VARCHAR(20),
    momentum_score NUMERIC(10, 4),
    price_alert_flags JSONB,
    UNIQUE(stock_id, date)
);

CREATE INDEX idx_market_price_stock_date ON market_price_snapshot(stock_id, date DESC);

-- ============================================================================
-- OWNERSHIP_DATA
-- ============================================================================
CREATE TABLE ownership_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    report_date DATE NOT NULL,
    insider_ownership_pct NUMERIC(10, 4),
    institutional_ownership_pct NUMERIC(10, 4),
    shares_short BIGINT,
    short_ratio_days NUMERIC(10, 2),
    short_pct_float NUMERIC(10, 4),
    shares_short_prev BIGINT,
    shares_outstanding_diluted BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, report_date)
);

CREATE INDEX idx_ownership_stock_date ON ownership_data(stock_id, report_date DESC);

-- ============================================================================
-- PEER_COMPARISON_DATA
-- ============================================================================
CREATE TABLE peer_comparison_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    peer_ticker VARCHAR(20),
    peer_name VARCHAR(255),
    is_target BOOLEAN DEFAULT false,
    market_cap BIGINT,
    pe_ratio NUMERIC(10, 4),
    revenue_growth NUMERIC(10, 4),
    net_margin NUMERIC(10, 4),
    eps NUMERIC(10, 4),
    roe NUMERIC(10, 4),
    debt_to_equity NUMERIC(10, 4),
    dividend_yield NUMERIC(10, 4),
    week_52_high NUMERIC(15, 4),
    week_52_low NUMERIC(15, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, peer_ticker)
);

CREATE INDEX idx_peer_stock ON peer_comparison_data(stock_id);
CREATE INDEX idx_peer_ticker ON peer_comparison_data(peer_ticker);

-- ============================================================================
-- REGIME_RISK_DATA
-- ============================================================================
CREATE TABLE regime_risk_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    current_regime VARCHAR(50),
    regime_confidence NUMERIC(10, 4),
    bull_volatility NUMERIC(10, 4),
    bear_downside_capture NUMERIC(10, 4),
    correction_volatility NUMERIC(10, 4),
    regime_analysis JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, date)
);

CREATE INDEX idx_regime_stock_date ON regime_risk_data(stock_id, date DESC);

-- ============================================================================
-- RISK_DATA
-- ============================================================================
CREATE TABLE risk_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    var_95 NUMERIC(8,4),
    var_99 NUMERIC(8,4),
    sharpe_ratio NUMERIC(8,4),
    sortino_ratio NUMERIC(8,4),
    calmar_ratio NUMERIC(8,4),
    max_drawdown NUMERIC(8,4),
    beta NUMERIC(8,4),
    market_correlation NUMERIC(8,4),
    skewness NUMERIC(8,4),
    kurtosis NUMERIC(8,4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    cvar_95 NUMERIC(8,4),
    cvar_99 NUMERIC(8,4),
    volatility_30d_annual NUMERIC(8,4),
    volatility_historical_annual NUMERIC(8,4),
    var_95_data_period_days INTEGER,
    var_95_sample_size INTEGER,
    var_95_calculation_method VARCHAR(100),
    var_95_confidence_level NUMERIC(5,4),
    var_95_return_frequency VARCHAR(50),
    var_99_data_period_days INTEGER,
    var_99_sample_size INTEGER,
    var_99_calculation_method VARCHAR(100),
    var_99_confidence_level NUMERIC(5,4),
    var_99_return_frequency VARCHAR(50),
    cvar_95_data_period_days INTEGER,
    cvar_95_tail_size INTEGER,
    cvar_95_calculation_method VARCHAR(100),
    cvar_95_confidence_level NUMERIC(5,4),
    cvar_99_data_period_days INTEGER,
    cvar_99_tail_size INTEGER,
    cvar_99_calculation_method VARCHAR(100),
    cvar_99_confidence_level NUMERIC(5,4),
    volatility_30d_sample_days NUMERIC(8,2),
    volatility_30d_calculation_method VARCHAR(100),
    volatility_30d_annualization_factor NUMERIC(10,4),
    volatility_30d_return_frequency VARCHAR(50),
    volatility_30d_model_type VARCHAR(50),
    volatility_30d_fallback_logic VARCHAR(255),
    volatility_historical_sample_days NUMERIC(8,2),
    volatility_historical_calculation_method VARCHAR(100),
    volatility_historical_annualization_factor NUMERIC(10,4),
    volatility_historical_return_frequency VARCHAR(50),
    volatility_historical_model_type VARCHAR(50),
    volatility_trading_days_annual INTEGER DEFAULT 252,
    liquidity_calculation_method VARCHAR(100),
    liquidity_data_period_days NUMERIC(8,2),
    liquidity_actual_sample_days NUMERIC(8,2),
    liquidity_volume_weight NUMERIC(5,2),
    liquidity_volume_benchmark BIGINT,
    liquidity_mcap_weight NUMERIC(5,2),
    liquidity_mcap_benchmark BIGINT,
    liquidity_stability_weight NUMERIC(5,2),
    liquidity_stability_metric VARCHAR(100),
    liquidity_data_freshness VARCHAR(50),
    liquidity_minimum_required_days INTEGER DEFAULT 30,
    liquidity_sufficient_data BOOLEAN,
    altman_calculation_method VARCHAR(100),
    altman_financial_period VARCHAR(20),
    altman_financial_period_end_date DATE,
    altman_financial_data_source VARCHAR(100),
    altman_data_age_days NUMERIC(8,2),
    altman_filing_type VARCHAR(20),
    altman_required_fields_count INTEGER DEFAULT 6,
    altman_available_fields_count INTEGER,
    altman_data_completeness_percent NUMERIC(5,2),
    altman_minimum_completeness_percent NUMERIC(5,2) DEFAULT 67.0,
    altman_retained_earnings_imputed BOOLEAN DEFAULT false,
    altman_imputation_method VARCHAR(255),
    altman_next_update_expected DATE,
    altman_coefficient_a NUMERIC(8,4) DEFAULT 1.2,
    altman_coefficient_b NUMERIC(8,4) DEFAULT 1.4,
    altman_coefficient_c NUMERIC(8,4) DEFAULT 3.3,
    altman_coefficient_d NUMERIC(8,4) DEFAULT 0.6,
    altman_coefficient_e NUMERIC(8,4) DEFAULT 1.0,
    sharpe_ratio_calculation_method VARCHAR(255),
    sharpe_ratio_data_period_days NUMERIC(8,2),
    sharpe_ratio_risk_free_rate_used NUMERIC(8,6),
    sharpe_ratio_risk_free_rate_source VARCHAR(100),
    sharpe_ratio_annualization_factor NUMERIC(10,4) DEFAULT 15.8745,
    sharpe_ratio_daily_rf_rate NUMERIC(8,8),
    sortino_ratio_calculation_method VARCHAR(255),
    sortino_ratio_data_period_days NUMERIC(8,2),
    sortino_ratio_annualization_factor NUMERIC(10,4) DEFAULT 15.8745,
    sortino_ratio_downside_focus VARCHAR(100),
    max_drawdown_calculation_method VARCHAR(255),
    max_drawdown_data_period_days NUMERIC(8,2),
    max_drawdown_definition VARCHAR(255),
    beta_calculation_method VARCHAR(255),
    beta_data_period_days NUMERIC(8,2),
    beta_market_benchmark VARCHAR(50),
    beta_return_frequency VARCHAR(50),
    correlation_calculation_method VARCHAR(255),
    correlation_data_period_days NUMERIC(8,2),
    correlation_market_benchmark VARCHAR(50),
    skewness_calculation_method VARCHAR(255),
    skewness_data_period_days NUMERIC(8,2),
    skewness_interpretation VARCHAR(255),
    kurtosis_calculation_method VARCHAR(255),
    kurtosis_data_period_days NUMERIC(8,2),
    var_estimation_confidence NUMERIC(5,2),
    volatility_estimation_confidence NUMERIC(5,2),
    liquidity_estimation_confidence NUMERIC(5,2),
    altman_estimation_confidence NUMERIC(5,2),
    sharpe_estimation_confidence NUMERIC(5,2),
    overall_profile_confidence NUMERIC(5,2),
    has_data_gaps BOOLEAN DEFAULT false,
    missing_price_data BOOLEAN DEFAULT false,
    missing_financial_data BOOLEAN DEFAULT false,
    insufficient_liquidity_data BOOLEAN DEFAULT false,
    data_quality_score NUMERIC(5,2),
    metadata_calculation_timestamp TIMESTAMP WITH TIME ZONE,
    metadata_version INTEGER DEFAULT 1,
    risk_profile_calculation_method VARCHAR(100),
    UNIQUE(stock_id, date)
);

CREATE INDEX idx_risk_stock_date ON risk_data(stock_id, date DESC);

-- ============================================================================
-- SENTIMENT_DATA
-- ============================================================================
CREATE TABLE sentiment_data (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    sentiment_score NUMERIC(8, 4),
    sentiment_label VARCHAR(50),
    sentiment_confidence NUMERIC(8, 4),
    analyst_sentiment NUMERIC(8, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, date)
);

CREATE INDEX idx_sentiment_stock_date ON sentiment_data(stock_id, date DESC);

-- ============================================================================
-- TECHNICAL_INDICATORS
-- ============================================================================
CREATE TABLE technical_indicators (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    sma_7 NUMERIC(12, 4),
    sma_20 NUMERIC(12, 4),
    sma_50 NUMERIC(12, 4),
    sma_100 NUMERIC(12, 4),
    sma_200 NUMERIC(12, 4),
    ema_12 NUMERIC(12, 4),
    ema_26 NUMERIC(12, 4),
    rsi_14 NUMERIC(8, 4),
    macd_line NUMERIC(12, 4),
    macd_signal NUMERIC(12, 4),
    macd_histogram NUMERIC(12, 4),
    stochastic_osc NUMERIC(8, 4),
    bb_upper NUMERIC(12, 4),
    bb_middle NUMERIC(12, 4),
    bb_lower NUMERIC(12, 4),
    atr_14 NUMERIC(12, 4),
    volatility_7d NUMERIC(8, 4),
    volatility_30d_annual NUMERIC(8, 4),
    volume_sma_20 BIGINT,
    volume_sma_ratio NUMERIC(8, 4),
    volume_trend_5d VARCHAR(50),
    obv NUMERIC(18, 2),
    vpt NUMERIC(18, 2),
    chaikin_money_flow NUMERIC(8, 4),
    avg_volume_3m BIGINT,
    green_days_count INTEGER,
    support_30d NUMERIC(12, 4),
    resistance_30d NUMERIC(12, 4),
    adx NUMERIC(8, 4),
    parabolic_sar NUMERIC(12, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, date)
);

CREATE INDEX idx_technical_stock_date ON technical_indicators(stock_id, date DESC);

-- ============================================================================
-- EARNINGS_CALENDAR
-- ============================================================================
CREATE TABLE earnings_calendar (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    company_name VARCHAR(255),
    earnings_date DATE NOT NULL,
    eps_estimate NUMERIC(10, 4),
    revenue_estimate NUMERIC(20, 2),
    time_of_day VARCHAR(10),
    quarter INTEGER,
    year INTEGER,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_ticker_date UNIQUE (ticker, earnings_date)
);

CREATE INDEX idx_earnings_calendar_date ON earnings_calendar(earnings_date);

-- ============================================================================
-- FUNCTION TO AUTO-UPDATE updated_at COLUMN
-- ============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for all tables with updated_at
CREATE TRIGGER update_stocks_updated_at BEFORE UPDATE ON stocks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_altman_zscore_data_updated_at BEFORE UPDATE ON altman_zscore_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_analyst_data_updated_at BEFORE UPDATE ON analyst_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_daily_price_data_updated_at BEFORE UPDATE ON daily_price_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dividend_data_updated_at BEFORE UPDATE ON dividend_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_forecast_data_updated_at BEFORE UPDATE ON forecast_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_fundamental_data_updated_at BEFORE UPDATE ON fundamental_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_insider_transactions_updated_at BEFORE UPDATE ON insider_transactions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_liquidity_risk_data_updated_at BEFORE UPDATE ON liquidity_risk_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_market_price_snapshot_updated_at BEFORE UPDATE ON market_price_snapshot
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ownership_data_updated_at BEFORE UPDATE ON ownership_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_peer_comparison_data_updated_at BEFORE UPDATE ON peer_comparison_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_regime_risk_data_updated_at BEFORE UPDATE ON regime_risk_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_risk_data_updated_at BEFORE UPDATE ON risk_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_sentiment_data_updated_at BEFORE UPDATE ON sentiment_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_technical_indicators_updated_at BEFORE UPDATE ON technical_indicators
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_stock_news_data_updated_at BEFORE UPDATE ON stock_news_data
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- SCHEMA SETUP COMPLETED
-- ============================================================================
"""

def execute_schema_on_azure():
    """Execute the complete schema on Azure PostgreSQL."""
    
    logger.info("=" * 80)
    logger.info("AZURE DATABASE SCHEMA SETUP")
    logger.info("=" * 80)
    
    try:
        # Connect to Azure PostgreSQL
        logger.info("Connecting to Azure PostgreSQL...")
        db = AzurePostgresClient()
        logger.info("✅ Successfully connected to Azure PostgreSQL")
        
        # Split the SQL into individual statements
        statements = [stmt.strip() for stmt in SCHEMA_SQL.split(';') if stmt.strip()]
        
        logger.info(f"Executing {len(statements)} SQL statements...")
        
        success_count = 0
        error_count = 0
        
        for i, statement in enumerate(statements, 1):
            try:
                with db.connection.cursor() as cursor:
                    cursor.execute(statement)
                    db.connection.commit()
                success_count += 1
                if i % 10 == 0 or i == len(statements):
                    logger.info(f"Progress: {i}/{len(statements)} statements executed")
            except Exception as e:
                error_count += 1
                logger.warning(f"Statement {i} failed (might be expected): {e}")
                # Continue with next statement
                try:
                    db.connection.rollback()
                except:
                    pass
        
        logger.info("=" * 80)
        logger.info("SCHEMA SETUP SUMMARY")
        logger.info("=" * 80)
        logger.info(f"✅ Successful statements: {success_count}")
        logger.info(f"❌ Failed statements: {error_count}")
        logger.info(f"📊 Success rate: {success_count/len(statements)*100:.1f}%")
        
        # Verify tables were created
        logger.info("\nVerifying table creation...")
        with db.connection.cursor() as cursor:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
        logger.info(f"✅ {len(tables)} tables found in database:")
        for table in tables:
            logger.info(f"   • {table}")
        
        logger.info("\n✅ Azure database schema setup completed successfully!")
        logger.info("You can now start exporting data using: python database/export_to_azure_postgres.py")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Schema setup failed: {e}")
        logger.exception("Detailed error:")
        return False
    finally:
        if 'db' in locals():
            try:
                db.close()
            except:
                pass

if __name__ == "__main__":
    success = execute_schema_on_azure()
    sys.exit(0 if success else 1)