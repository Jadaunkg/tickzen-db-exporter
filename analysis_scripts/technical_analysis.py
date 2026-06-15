#!/usr/bin/env python3
"""
Technical Analysis Engine
========================

Comprehensive technical analysis module providing advanced technical indicators,
chart pattern recognition, and trading signal generation for stock market analysis.
Integrates with Plotly for interactive visualizations and Matplotlib for static charts.

Core Technical Indicators:
-------------------------
1. **Momentum Indicators**:
   - RSI (Relative Strength Index): Overbought/oversold conditions
   - MACD (Moving Average Convergence Divergence): Trend momentum
   - Stochastic Oscillator: Price momentum analysis
   - Williams %R: Momentum indicator for reversal signals

2. **Trend Indicators**:
   - Moving Averages: SMA, EMA, WMA for trend identification
   - Bollinger Bands: Volatility and mean reversion analysis
   - ADX (Average Directional Index): Trend strength measurement
   - Parabolic SAR: Stop and reverse trend signals

3. **Volume Indicators**:
   - Volume-Price Trend (VPT): Volume-based momentum
   - On-Balance Volume (OBV): Cumulative volume analysis
   - Chaikin Money Flow: Money flow indicator
   - Volume Rate of Change: Volume momentum analysis

4. **Volatility Indicators**:
   - Average True Range (ATR): Volatility measurement
   - Bollinger Band Width: Volatility expansion/contraction
   - Keltner Channels: Volatility-based trading bands

Chart Pattern Recognition:
-------------------------
- **Reversal Patterns**: Head and shoulders, double tops/bottoms
- **Continuation Patterns**: Triangles, flags, pennants
- **Candlestick Patterns**: Doji, hammer, shooting star, engulfing
- **Support/Resistance**: Dynamic level identification

Signal Generation:
-----------------
- **Buy Signals**: Bullish crossovers, breakouts, oversold recoveries
- **Sell Signals**: Bearish crossovers, breakdowns, overbought corrections
- **Neutral Signals**: Sideways trends, consolidation patterns
- **Strength Scoring**: Composite signal strength calculation

Visualization Features:
----------------------
- **Interactive Charts**: Plotly-based interactive technical charts
- **Multi-Timeframe**: Support for various timeframes (1m to 1M)
- **Overlay Indicators**: Multiple indicators on price charts
- **Subplots**: Separate panels for oscillators and volume
- **Candlestick Charts**: OHLC visualization with volume

Advanced Analysis:
-----------------
- **Multi-Timeframe Analysis**: Cross-timeframe signal confirmation
- **Divergence Detection**: Price vs indicator divergences
- **Trend Strength**: Quantitative trend strength measurement
- **Volatility Analysis**: Market volatility regime identification
- **Market Structure**: Higher highs/lows analysis

Integration Points:
------------------
- Used by automation_scripts/pipeline.py for comprehensive analysis
- Provides data for dashboard_analytics.py visualizations
- Integrated with report_generator.py for technical analysis reports
- Supports real-time analysis updates via SocketIO

Performance Optimizations:
-------------------------
- **Vectorized Calculations**: NumPy-based efficient computations
- **Caching**: Indicator calculation results caching
- **Lazy Loading**: On-demand indicator calculation
- **Memory Management**: Efficient handling of large datasets

Usage Examples:
--------------
```python
# Basic RSI calculation
rsi = calculate_rsi(price_data, window=14)

# MACD with custom parameters
macd_line, signal_line, histogram = calculate_macd(
    price_data, fast=12, slow=26, signal=9
)

# Bollinger Bands
upper, middle, lower = calculate_bollinger_bands(
    price_data, window=20, std_dev=2
)

# Comprehensive technical analysis
analysis_result = perform_technical_analysis(
    ticker='AAPL',
    timeframe='1d',
    indicators=['RSI', 'MACD', 'BB', 'SMA']
)
```

Chart Generation:
----------------
- **Plotly Charts**: Interactive web-ready visualizations
- **Matplotlib Charts**: High-quality static images for reports
- **Custom Styling**: Professional chart themes and layouts
- **Export Formats**: PNG, SVG, HTML, PDF support

Signal Interpretation:
---------------------
- **Signal Strength**: 1-10 scale for signal reliability
- **Confidence Levels**: Statistical confidence in signals
- **Risk Assessment**: Signal-based risk evaluation
- **Time Horizons**: Short, medium, long-term signal analysis

Author: TickZen Development Team
Version: 3.1
Last Updated: January 2026
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import timedelta # Import timedelta

# --- NEW: Import Matplotlib ---
import matplotlib
matplotlib.use('Agg') # Use Agg backend for non-interactive plotting
import matplotlib.pyplot as plt
import matplotlib.dates as mdates # For date formatting

# --- Calculation Functions (Keep as before) ---
def calculate_rsi(data: pd.Series, window: int = 14) -> pd.Series:
    """
    True Wilder's RSI with SMA seed — identical to TradingView, Yahoo Finance,
    Bloomberg, and every professional charting platform.

    Algorithm (Wilder 1978 / J. Welles Wilder "New Concepts in Technical
    Trading Systems"):
      1. Seed: avgGain[window] = mean(gains[1 .. window])
               avgLoss[window] = mean(losses[1 .. window])
      2. SMMA: avgGain[t] = (avgGain[t-1] × (window-1) + gain[t]) / window
               avgLoss[t] = (avgLoss[t-1] × (window-1) + loss[t]) / window
      3. RSI   = 100 - 100 / (1 + avgGain / avgLoss)

    Why NOT ewm(alpha=1/window, adjust=False):
      pandas EWM starts the exponential weighting at row 0. With fewer than
      ~3× the window of rows, all early gains/losses (including the NaN rows
      from diff()) distort the first smoothed value, giving a materially
      different — wrong — result compared to the SMA-seeded init above.
    """
    if data.isnull().all() or len(data) < window + 1:
        return pd.Series(index=data.index, dtype=float)

    delta = data.diff()
    gain  = delta.where(delta > 0, 0.0).fillna(0.0).to_numpy()
    loss  = (-delta.where(delta < 0, 0.0)).fillna(0.0).to_numpy()

    avg_gain = np.full(len(gain), np.nan)
    avg_loss = np.full(len(loss), np.nan)

    # Step 1 — SMA seed (rows 1 .. window; row 0 is NaN from diff)
    avg_gain[window] = gain[1 : window + 1].mean()
    avg_loss[window] = loss[1 : window + 1].mean()

    # Step 2 — Wilder's SMMA for all subsequent bars
    for i in range(window + 1, len(gain)):
        avg_gain[i] = (avg_gain[i - 1] * (window - 1) + gain[i]) / window
        avg_loss[i] = (avg_loss[i - 1] * (window - 1) + loss[i]) / window

    # Avoid division by zero
    avg_loss = np.where(avg_loss == 0, 1e-10, avg_loss)

    rs  = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return pd.Series(rsi, index=data.index, dtype=float)

def calculate_macd(data: pd.Series, fast_window: int = 12, slow_window: int = 26, signal_window: int = 9):
    if data.isnull().all() or len(data) < slow_window: empty_series = pd.Series(index=data.index, dtype=float); return empty_series, empty_series, empty_series
    ema_fast = data.ewm(span=fast_window, adjust=False).mean(); ema_slow = data.ewm(span=slow_window, adjust=False).mean(); macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal_window, adjust=False).mean(); histogram = macd_line - signal_line; return macd_line, signal_line, histogram

def calculate_bollinger_bands(data: pd.Series, window: int = 20, num_std: int = 2):
    if data.isnull().all() or len(data) < window: empty_series = pd.Series(index=data.index, dtype=float); return empty_series, empty_series, empty_series
    middle_band = data.rolling(window=window).mean(); std_dev = data.rolling(window=window).std(); upper_band = middle_band + (std_dev * num_std)
    lower_band = middle_band - (std_dev * num_std); return upper_band, middle_band, lower_band

def calculate_sma(data: pd.Series, window: int) -> pd.Series:
    if data.isnull().all() or len(data) < window: return pd.Series(index=data.index, dtype=float)
    return data.rolling(window=window).mean()

def calculate_ema(data: pd.Series, window: int) -> pd.Series:
    """Calculate Exponential Moving Average"""
    if data.isnull().all() or len(data) < window: return pd.Series(index=data.index, dtype=float)
    return data.ewm(span=window, adjust=False).mean()

def calculate_volume_sma(data: pd.DataFrame, window: int) -> pd.Series: # Changed input to DataFrame
    if 'Volume' not in data.columns or data['Volume'].isnull().all() or len(data) < window: return pd.Series(index=data.index, dtype=float)
    return data['Volume'].rolling(window=window).mean()

def calculate_price_cluster_levels(df: pd.DataFrame, lookback: int = 20, tolerance: float = 2.0):
    """
    Calculate support and resistance based on price clustering.
    
    Identifies price levels that have been "touched" (High touched or Low dipped)
    multiple times within a tolerance band. The most frequently touched level
    is considered the real support/resistance.
    
    Args:
        df: DataFrame with 'High' and 'Low' columns
        lookback: Number of trading sessions to analyze (default 20 = 1 month)
        tolerance: Dollar range tolerance for clustering (default $2.00)
    
    Returns:
        tuple: (support_level, support_touches, resistance_level, resistance_touches)
    
    Example:
        If price dips to 400-402 range 5 times in 20 days, returns support_level=401 with 5 touches
        If price rallies to 445 range 4 times, returns resistance_level=445 with 4 touches
    """
    if df is None or df.empty or len(df) < lookback:
        return None, 0, None, 0
    
    if 'High' not in df.columns or 'Low' not in df.columns:
        return None, 0, None, 0
    
    recent_data = df.iloc[-lookback:].copy()
    
    # Collect all price touches (lows for support, highs for resistance)
    lows = recent_data['Low'].values
    highs = recent_data['High'].values
    
    # Filter out NaN values
    lows = lows[~np.isnan(lows)]
    highs = highs[~np.isnan(highs)]
    
    if len(lows) == 0 or len(highs) == 0:
        return None, 0, None, 0
    
    # Find support level (most touched low cluster)
    support_level, support_touches = _find_cluster_level(lows, tolerance)
    
    # Find resistance level (most touched high cluster)
    resistance_level, resistance_touches = _find_cluster_level(highs, tolerance)
    
    return support_level, support_touches, resistance_level, resistance_touches

def _find_cluster_level(prices: np.ndarray, tolerance: float = 2.0):
    """
    Find the most frequently touched price level within tolerance band.
    
    Algorithm:
    1. Sort prices
    2. For each price, count how many other prices fall within tolerance band
    3. Return the price level with highest touch count
    4. Use midpoint of cluster as the level
    
    Args:
        prices: Array of prices (lows or highs)
        tolerance: Dollar range tolerance for clustering
    
    Returns:
        tuple: (cluster_level, touch_count)
    """
    if len(prices) == 0:
        return None, 0
    
    prices = np.sort(prices)
    max_touches = 0
    best_cluster_center = None
    
    for i, price in enumerate(prices):
        # Count prices within tolerance band of this price
        cluster = prices[(prices >= price - tolerance) & (prices <= price + tolerance)]
        touch_count = len(cluster)
        
        if touch_count > max_touches:
            max_touches = touch_count
            # Use the midpoint of the cluster as the level
            best_cluster_center = (cluster.min() + cluster.max()) / 2.0
    
    return best_cluster_center, max_touches

# --- Conclusion Generation Functions (Keep as before) ---
def get_rsi_conclusion(rsi_value):
    if pd.isna(rsi_value): return "RSI data not available."
    if rsi_value >= 70: return f"RSI ({rsi_value:.1f}) is at or above 70, suggesting potential overbought conditions. This could indicate a higher chance of a price pullback or consolidation."
    elif rsi_value <= 30: return f"RSI ({rsi_value:.1f}) is at or below 30, suggesting potential oversold conditions. This could indicate a higher chance of a price rebound."
    else: return f"RSI ({rsi_value:.1f}) is in the neutral zone (30-70), indicating balanced momentum."

def get_macd_conclusion(macd_line_now, signal_line_now, histogram_now, histogram_prev):
    if pd.isna(macd_line_now) or pd.isna(signal_line_now) or pd.isna(histogram_now) or pd.isna(histogram_prev): return "MACD data not available or insufficient for comparison."
    conclusion = ""
    # Check for crossovers using the sign of the histogram
    if histogram_now > 0 and histogram_prev <= 0:
        conclusion += "A bullish MACD crossover (histogram crossing above zero) may have recently occurred, suggesting potential upward momentum. "
    elif histogram_now < 0 and histogram_prev >= 0:
        conclusion += "A bearish MACD crossover (histogram crossing below zero) may have recently occurred, suggesting potential downward momentum. "

    # Describe current state
    if macd_line_now > signal_line_now:
        conclusion += f"Currently, the MACD line ({macd_line_now:.2f}) is above the signal line ({signal_line_now:.2f}), generally considered a bullish signal. "
    else:
        conclusion += f"Currently, the MACD line ({macd_line_now:.2f}) is below the signal line ({signal_line_now:.2f}), generally considered a bearish signal. "

    # Describe histogram state
    if histogram_now > 0:
        conclusion += f"The positive histogram ({histogram_now:.2f}) indicates strengthening bullish momentum (or weakening bearish momentum)."
    elif histogram_now < 0:
         conclusion += f"The negative histogram ({histogram_now:.2f}) indicates strengthening bearish momentum (or weakening bullish momentum)."
    else: # Histogram is zero
         conclusion += "The histogram is at zero, indicating the MACD and signal lines are currently equal."

    return conclusion.strip()


def get_bb_conclusion(close_price, upper_band, lower_band, middle_band):
    if pd.isna(close_price) or pd.isna(upper_band) or pd.isna(lower_band) or pd.isna(middle_band): return "Bollinger Band data not available."
    if close_price > upper_band: return f"The price (${close_price:.2f}) is currently above the upper Bollinger Band (${upper_band:.2f}), which can sometimes indicate an overbought condition or a strong breakout. Caution is advised as prices may revert towards the middle band (${middle_band:.2f})."
    elif close_price < lower_band: return f"The price (${close_price:.2f}) is currently below the lower Bollinger Band (${lower_band:.2f}), which can sometimes indicate an oversold condition or a strong breakdown. Prices may revert towards the middle band (${middle_band:.2f})."
    else: return f"The price (${close_price:.2f}) is currently trading within the Bollinger Bands (Lower: ${lower_band:.2f}, Upper: ${upper_band:.2f}), around the middle band (SMA20: ${middle_band:.2f})."

# --- Helper function to get plot data range (Keep as before) ---
def _get_plot_data(df, plot_period_years=3):
    """Slices the DataFrame to the specified number of recent years."""
    if df.empty or 'Date' not in df.columns:
        return df

    # Ensure 'Date' is datetime and sorted
    try:
        if not pd.api.types.is_datetime64_any_dtype(df['Date']):
            df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date')
    except Exception as e:
        print(f"Error processing Date column: {e}")
        return pd.DataFrame() # Return empty if Date is unusable

    last_date = df['Date'].iloc[-1]
    start_date = last_date - pd.DateOffset(years=plot_period_years)

    # Ensure start_date doesn't go before the first date in the data
    first_date = df['Date'].iloc[0]
    start_date = max(start_date, first_date)

    return df[df['Date'] >= start_date].copy()


# --- Plotly Helper for common layout elements (Keep as before - Used by Plotly functions) ---
def _configure_indicator_layout(fig, title):
    fig.update_layout(
        title=dict(
            text=title, y=0.98, x=0.5, xanchor='center', yanchor='top', font=dict(size=14)
        ),
        legend=dict(
            orientation="h", yanchor="top", y=0.92, xanchor="center", x=0.5, font=dict(size=10)
        ),
        margin=dict(l=35, r=25, t=100, b=40),
        yaxis=dict(domain=[0, 0.78]), # Adjusted domain to leave space for range selector
        template="plotly_white",
        autosize=True,
        height=450,
        xaxis_rangeslider_visible=False,
        xaxis_automargin=True,
        yaxis_automargin=True
    )
    fig.update_xaxes(
        domain=[0, 1],
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1M", step="month", stepmode="backward"),
                dict(count=3, label="3M", step="month", stepmode="backward"),
                dict(count=6, label="6M", step="month", stepmode="backward"),
                dict(count=1, label="YTD", step="year", stepmode="todate"),
                dict(count=1, label="1Y", step="year", stepmode="backward"),
                dict(count=3, label="3Y", step="year", stepmode="backward"),
                dict(step="all", label="All") # Add 'All' button back
            ]),
            yanchor='top',
            y=0.84, # Position below legend
            xanchor='left',
            x=0.01,
            font_size=10
        ),
        tickfont=dict(size=10)
    )
    fig.update_yaxes(tickfont=dict(size=10))

    # Set initial visible range (last 1 year) - Keep this logic for initial zoom
    if fig.data and len(fig.data[0].x) > 0:
         last_date_in_data = fig.data[0].x[-1]
         first_date_in_data = fig.data[0].x[0]
         # Ensure last_date is a Timestamp for comparison/offset
         if not isinstance(last_date_in_data, pd.Timestamp):
             try: last_date_in_data = pd.to_datetime(last_date_in_data)
             except: pass # Keep original if conversion fails

         default_start = first_date_in_data # Default to start of data
         if isinstance(last_date_in_data, pd.Timestamp):
             one_year_back = last_date_in_data - pd.DateOffset(years=1)
             # Make sure one_year_back is not before first_date
             default_start = max(first_date_in_data, one_year_back)

         # Ensure start date is not after end date
         if default_start > last_date_in_data:
             default_start = first_date_in_data

         fig.update_xaxes(range=[default_start, last_date_in_data])

    return fig


# --- Plotly Plotting Functions (Keep as before - Used by Full Report) ---

def plot_price_bollinger(df, ticker, plot_period_years=3):
    """Plots Price and Bollinger Bands for the specified period."""
    if len(df) < 20: return None, "Insufficient data for Bollinger Bands."
    # Calculate on full df first
    df['BB_Upper'], df['BB_Middle'], df['BB_Lower'] = calculate_bollinger_bands(df['Close'])
    # Get data for the plotting period
    df_plot_range = _get_plot_data(df, plot_period_years)
    df_plot = df_plot_range.dropna(subset=['BB_Upper', 'BB_Middle', 'BB_Lower', 'Close']) # Ensure all needed cols are present

    if df_plot.empty: return None, "Bollinger Bands could not be calculated for the selected period."

    fig = go.Figure()
    # Plot using df_plot (limited range)
    fig.add_trace(go.Scatter(x=df_plot['Date'], y=df_plot['BB_Upper'], line=dict(color='rgba(211, 211, 211, 0.8)', width=1.5), name='Upper Band'))
    fig.add_trace(go.Scatter(x=df_plot['Date'], y=df_plot['BB_Lower'], line=dict(color='rgba(211, 211, 211, 0.8)', width=1.5), fill='tonexty', fillcolor='rgba(211, 211, 211, 0.1)', name='Lower Band'))
    fig.add_trace(go.Scatter(x=df_plot['Date'], y=df_plot['BB_Middle'], name='SMA20', line=dict(color='#ff7f0e', width=1.5, dash='dash')))
    fig.add_trace(go.Scatter(x=df_plot['Date'], y=df_plot['Close'], name='Close', line=dict(color='#00008B', width=2)))

    fig = _configure_indicator_layout(fig, f'{ticker} Price & Bollinger Bands ({plot_period_years}Y)')
    fig.update_yaxes(title_text="Price")

    # Conclusion based on the LATEST value from the original df
    latest_valid_data = df.dropna(subset=['Close', 'BB_Upper', 'BB_Lower', 'BB_Middle']).iloc[-1] if not df.dropna(subset=['Close', 'BB_Upper', 'BB_Lower', 'BB_Middle']).empty else None
    conclusion = get_bb_conclusion(
        latest_valid_data['Close'] if latest_valid_data is not None else None,
        latest_valid_data['BB_Upper'] if latest_valid_data is not None else None,
        latest_valid_data['BB_Lower'] if latest_valid_data is not None else None,
        latest_valid_data['BB_Middle'] if latest_valid_data is not None else None
    ) if latest_valid_data is not None else "Bollinger Band conclusion requires more data."

    return fig, conclusion

def plot_rsi(df, ticker, plot_period_years=3):
    """Plots RSI for the specified period."""
    if len(df) < 15: return None, "Insufficient data for RSI (14)."
    # Calculate on full df
    df['RSI'] = calculate_rsi(df['Close'])
     # Get data for the plotting period
    df_plot_range = _get_plot_data(df, plot_period_years)
    df_plot = df_plot_range.dropna(subset=['RSI']) # Ensure RSI is present

    if df_plot.empty: return None, "RSI could not be calculated for the selected period."

    fig = go.Figure()
    # Plot using df_plot
    fig.add_trace(go.Scatter(x=df_plot['Date'], y=df_plot['RSI'], name='RSI', line=dict(color='#8A2BE2', width=2)))
    fig.add_hline(y=70, line_dash="dash", line_color="#DC143C", opacity=0.8, annotation_text="Overbought (70)", annotation_position="bottom right")
    fig.add_hline(y=30, line_dash="dash", line_color="#228B22", opacity=0.8, annotation_text="Oversold (30)", annotation_position="bottom right")

    fig = _configure_indicator_layout(fig, f'Relative Strength Index (RSI 14) ({plot_period_years}Y)')
    fig.update_yaxes(title_text="RSI", range=[0, 100])

    # Conclusion based on LATEST value from original df
    latest_valid_data = df.dropna(subset=['RSI']).iloc[-1] if not df.dropna(subset=['RSI']).empty else None
    conclusion = get_rsi_conclusion(latest_valid_data['RSI'] if latest_valid_data is not None else None) if latest_valid_data is not None else "RSI conclusion requires more data."

    return fig, conclusion

def plot_macd_lines(df, ticker, plot_period_years=3):
    """Plots MACD Line vs Signal Line for the specified period."""
    if len(df) < 35: return None, "Insufficient data for MACD (12, 26, 9)."
    # Calculate on full df
    df['MACD_Line'], df['MACD_Signal'], df['MACD_Hist'] = calculate_macd(df['Close'])
    # Get data for plotting period
    df_plot_range = _get_plot_data(df, plot_period_years)
    df_plot = df_plot_range.dropna(subset=['MACD_Line', 'MACD_Signal']) # Ensure lines are present

    if len(df_plot) < 2: return None, "Insufficient valid MACD Line/Signal data for the selected period."

    fig = go.Figure()
    # Plot using df_plot
    fig.add_trace(go.Scatter(x=df_plot['Date'], y=df_plot['MACD_Line'], name='MACD Line', line=dict(color='#191970', width=2)))
    fig.add_trace(go.Scatter(x=df_plot['Date'], y=df_plot['MACD_Signal'], name='Signal Line', line=dict(color='#FF4500', width=2)))
    fig.add_hline(y=0, line_dash="dash", line_color="grey", opacity=0.5)

    fig = _configure_indicator_layout(fig, f'MACD Line vs Signal Line ({plot_period_years}Y)')
    fig.update_yaxes(title_text="MACD Value")

    # Conclusion based on LATEST values from original df
    df_full_hist = df.dropna(subset=['MACD_Line', 'MACD_Signal', 'MACD_Hist'])
    if len(df_full_hist) < 2: conclusion = "MACD conclusion requires more data."
    else:
        latest = df_full_hist.iloc[-1]; prev = df_full_hist.iloc[-2]
        conclusion = get_macd_conclusion(latest.get('MACD_Line'), latest.get('MACD_Signal'), latest.get('MACD_Hist'), prev.get('MACD_Hist'))

    return fig, conclusion

def plot_macd_histogram(df, ticker, plot_period_years=3):
    """Plots MACD Histogram for the specified period."""
    if len(df) < 35: return None, "Insufficient data for MACD (12, 26, 9)."
    # Ensure MACD is calculated on full df
    if 'MACD_Hist' not in df.columns:
        df['MACD_Line'], df['MACD_Signal'], df['MACD_Hist'] = calculate_macd(df['Close'])

    # Get data for plotting period
    df_plot_range = _get_plot_data(df, plot_period_years)
    df_plot = df_plot_range.dropna(subset=['MACD_Hist']) # Ensure histogram is present

    if len(df_plot) < 2: return None, "Insufficient valid MACD Histogram data for the selected period."

    fig = go.Figure()
    # Plot using df_plot
    colors = np.where(df_plot['MACD_Hist'] < 0, '#DC143C', '#228B22') # Red / Green
    fig.add_trace(go.Bar(x=df_plot['Date'], y=df_plot['MACD_Hist'], name='MACD Hist', marker_color=colors))

    fig = _configure_indicator_layout(fig, f'MACD Histogram ({plot_period_years}Y)')
    fig.update_yaxes(title_text="Histogram Value")

    # Conclusion based on LATEST values from original df (same as plot_macd_lines)
    df_full_hist = df.dropna(subset=['MACD_Line', 'MACD_Signal', 'MACD_Hist'])
    if len(df_full_hist) < 2: conclusion = "MACD conclusion requires more data."
    else:
        latest = df_full_hist.iloc[-1]; prev = df_full_hist.iloc[-2]
        conclusion = get_macd_conclusion(latest.get('MACD_Line'), latest.get('MACD_Signal'), latest.get('MACD_Hist'), prev.get('MACD_Hist'))

    return fig, conclusion

def plot_historical_line_chart(df, ticker):
    """Plots Historical Price and Volume."""
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date')
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(go.Scatter(x=df['Date'], y=df['Close'], name='Close', line=dict(color='#00008B', width=2)), secondary_y=False)

    # Calculate Volume SMA on the full df
    if 'Volume' in df.columns and not df['Volume'].isnull().all():
        df['Volume_SMA20'] = calculate_volume_sma(df, 20) # Calculate on full df
        fig.add_trace(go.Bar(x=df['Date'], y=df['Volume'], name='Volume', marker_color='#FF8C00', opacity=0.35), secondary_y=True)
        fig.add_trace(go.Scatter(x=df['Date'], y=df['Volume_SMA20'], name='Volume SMA20', line=dict(color='#8B4513', width=1.5, dash='dot')), secondary_y=True)
        fig.update_yaxes(title_text="Volume", secondary_y=True, domain=[0, 0.78], showgrid=False, title_font_size=10, tickfont_size=10, automargin=True)

    fig.update_yaxes(title_text="Price ($)", secondary_y=False, domain=[0, 0.78], title_font_size=10, tickfont_size=10, automargin=True)

    # Apply REVISED layout structure (uses _configure_indicator_layout which handles range selector etc.)
    fig = _configure_indicator_layout(fig, f'{ticker} Historical Price & Volume')
    # No specific period in title as range selector handles it

    return fig

# --- Function to calculate additional indicators for summary (Keep as before) ---
def calculate_detailed_ta(df):
    """Calculates additional indicators for the summary report."""
    if df is None or df.empty: return {}
    ta_summary = {}; df = df.copy(); df['Date'] = pd.to_datetime(df['Date']); df = df.sort_values('Date')
    last_row = None
    if not df.empty:
        last_row = df.iloc[-1]
        
    # 15-session return: (Close[-1] / Close[-15]) - 1
    # Uses the same window as generate_historical_performance_html's tail(15):
    #   start = row[-15], end = row[-1]  →  14 price steps = 15 rows.
    # pct_change(15) would compare row[-1] vs row[-16] (16th from end) — off by one.
    if len(df) >= 15:
        start_close = df['Close'].iloc[-15]
        end_close = df['Close'].iloc[-1]
        change_15d = (end_close - start_close) / start_close if start_close != 0 else None
        ta_summary['change_15d_pct'] = change_15d * 100 if change_15d is not None else None
    else:
        ta_summary['change_15d_pct'] = None

    # SMAs
    for period in [20, 50, 100, 200]:
        sma_col = f'SMA_{period}'
        if len(df) >= period:
            df[sma_col] = calculate_sma(df['Close'], period);
            sma_val = df[sma_col].iloc[-1] # Get latest SMA
            ta_summary[sma_col] = sma_val if not pd.isna(sma_val) else None
        else: ta_summary[sma_col] = None

    # Volume Analysis
    if 'Volume' in df.columns:
        latest_volume = df['Volume'].iloc[-1] if not df.empty else None
        if len(df) >= 20:
             df['Volume_SMA20'] = calculate_volume_sma(df, 20);
             latest_vol_sma = df['Volume_SMA20'].iloc[-1]
             ta_summary['Volume_SMA20'] = latest_vol_sma if not pd.isna(latest_vol_sma) else None
             if not pd.isna(latest_volume) and not pd.isna(latest_vol_sma) and latest_vol_sma > 0:
                 ta_summary['Volume_vs_SMA20_Ratio'] = latest_volume / latest_vol_sma
             else: ta_summary['Volume_vs_SMA20_Ratio'] = None
        else:
             ta_summary['Volume_vs_SMA20_Ratio'] = None; ta_summary['Volume_SMA20'] = None

        if len(df) >= 6: # Need at least 6 days for a 5-day lookback plus the current day
            vol_slice = df['Volume'].iloc[-5:] # Last 5 days volume
            if not vol_slice.empty and not pd.isna(latest_volume):
                mean_vol_5d = vol_slice.mean()
                if not pd.isna(mean_vol_5d) and mean_vol_5d > 0:
                    if latest_volume > mean_vol_5d * 1.1: ta_summary['Volume_Trend_5D'] = "Increasing"
                    elif latest_volume < mean_vol_5d * 0.9: ta_summary['Volume_Trend_5D'] = "Decreasing"
                    else: ta_summary['Volume_Trend_5D'] = "Mixed"
                else: ta_summary['Volume_Trend_5D'] = None
            else: ta_summary['Volume_Trend_5D'] = None
        else: ta_summary['Volume_Trend_5D'] = None
    else:
         ta_summary['Volume_vs_SMA20_Ratio'] = None; ta_summary['Volume_SMA20'] = None
         ta_summary['Volume_Trend_5D'] = None

    # Support & Resistance — IMPROVED: Clustering-based method
    # 20 trading sessions ≈30 calendar days (1 month) — the industry-standard "1-month" window.
    # NEW METHOD: Find the most frequently "touched" price levels within $2 tolerance band
    # this identifies real support/resistance where price clusters multiple times.
    # OLD METHOD (commented out): Used simple min/max which was inaccurate for isolated wicks.
    #
    # Example: If price Low touches 387-389 range 5 times in 20 days, that's the real support.
    # The rare spike down to 380 is ignored as it's an outlier/wick, not true support level.
    lookback = 20
    if len(df) >= lookback:
        support, support_touches, resistance, resistance_touches = calculate_price_cluster_levels(df, lookback=lookback, tolerance=2.0)
        ta_summary['Support_30D'] = support if support is not None else None
        ta_summary['Resistance_30D'] = resistance if resistance is not None else None
        ta_summary['Support_30D_Touches'] = support_touches  # Track touch count for diagnostics
        ta_summary['Resistance_30D_Touches'] = resistance_touches
    else:
        ta_summary['Support_30D'] = None
        ta_summary['Resistance_30D'] = None
        ta_summary['Support_30D_Touches'] = 0
        ta_summary['Resistance_30D_Touches'] = 0

    # RSI
    if len(df) >= 15:
         df['RSI_14'] = calculate_rsi(df['Close'], 14)
         rsi_val = df['RSI_14'].iloc[-1]
         ta_summary['RSI_14'] = rsi_val if not pd.isna(rsi_val) else None
    else:
         ta_summary['RSI_14'] = None

    # MACD (needed for conclusion later)
    if len(df) >= 35:
        df['MACD_Line'], df['MACD_Signal'], df['MACD_Hist'] = calculate_macd(df['Close'])
        df_macd_valid = df.dropna(subset=['MACD_Line', 'MACD_Signal', 'MACD_Hist'])
        if len(df_macd_valid) >= 2:
            latest_macd = df_macd_valid.iloc[-1]
            prev_macd = df_macd_valid.iloc[-2]
            ta_summary['MACD_Line'] = latest_macd['MACD_Line']
            ta_summary['MACD_Signal'] = latest_macd['MACD_Signal']
            ta_summary['MACD_Hist'] = latest_macd['MACD_Hist']
            ta_summary['MACD_Hist_Prev'] = prev_macd['MACD_Hist']
        else:
            ta_summary['MACD_Line'] = ta_summary['MACD_Signal'] = ta_summary['MACD_Hist'] = ta_summary['MACD_Hist_Prev'] = None
    else:
        ta_summary['MACD_Line'] = ta_summary['MACD_Signal'] = ta_summary['MACD_Hist'] = ta_summary['MACD_Hist_Prev'] = None


    # BB (needed for conclusion later)
    if len(df) >= 20:
        df['BB_Upper'], df['BB_Middle'], df['BB_Lower'] = calculate_bollinger_bands(df['Close'])
        df_bb_valid = df.dropna(subset=['Close', 'BB_Upper', 'BB_Middle', 'BB_Lower'])
        if not df_bb_valid.empty:
            latest_bb = df_bb_valid.iloc[-1]
            ta_summary['BB_Upper'] = latest_bb['BB_Upper']
            ta_summary['BB_Middle'] = latest_bb['BB_Middle']
            ta_summary['BB_Lower'] = latest_bb['BB_Lower']
        else:
            ta_summary['BB_Upper'] = ta_summary['BB_Middle'] = ta_summary['BB_Lower'] = None
    else:
        ta_summary['BB_Upper'] = ta_summary['BB_Middle'] = ta_summary['BB_Lower'] = None

    # ── Historical Volatility cones ─────────────────────────────────────────
    # All use log-returns × sqrt(252) (trading-day/CBOE/iVolatility convention).
    #
    # Key naming: HV_N means N log-return observations (trading sessions).
    # "30-Day HV" by iVolatility convention = 20 returns (30 cal days ≈20 sessions)
    # which is stored as HV_C2C_30d and labelled "HV C2C (30d)" in the report.
    #
    # Verified iVolatility reference (TSLA 2026-02-23):
    #   HV Close-to-Close 30d = 0.3406  ⇒ HV_C2C_30d  with sqrt(252) = 0.3406 ✓
    #   Parkinson         30d = 0.3430  ⇒ HV_Parkinson_30d with sqrt(252) = 0.3430 ✓
    log_returns_full = np.log(df['Close'] / df['Close'].shift(1)).dropna()

    # Close-to-Close HV for standard lookback windows
    # Key naming convention: HV_C2C_<calendar_label>
    #   30d  = 20 trading sessions  (30 cal days)
    #   90d  = 63 trading sessions  (3 months)
    #   1yr  = 252 trading sessions (1 trading year)
    for hv_period, key in [(20, 'HV_C2C_30d'), (63, 'HV_C2C_90d'), (252, 'HV_C2C_1yr')]:
        if len(log_returns_full) >= hv_period:
            hv_val = log_returns_full.iloc[-hv_period:].std() * np.sqrt(252) * 100
            ta_summary[key] = float(hv_val) if not pd.isna(hv_val) else None
        else:
            ta_summary[key] = None

    # Parkinson HV (High/Low based) — 30-day / 20-session window
    # Formula: sqrt( 1 / (4n·ln2) × Σ ln(H_i / L_i)^2 ) × sqrt(252)
    # Slightly more efficient estimator than Close-to-Close; often ~1% higher.
    # Key: HV_Parkinson_30d  (30 calendar days = 20 trading sessions)
    ta_summary['HV_Parkinson_30d'] = None
    if 'High' in df.columns and 'Low' in df.columns and len(df) >= 21:
        try:
            h_arr = df['High'].iloc[-20:].values.astype(float)
            l_arr = df['Low'].iloc[-20:].values.astype(float)
            if np.all(l_arr > 0):
                pk_var = (1.0 / (4 * 20 * np.log(2))) * np.sum(np.log(h_arr / l_arr) ** 2)
                pk_val = float(np.sqrt(pk_var) * np.sqrt(252) * 100)
                ta_summary['HV_Parkinson_30d'] = pk_val if not np.isnan(pk_val) else None
        except Exception:
            pass

    # Add current price for convenience
    ta_summary['Current_Price'] = df['Close'].iloc[-1] if not df.empty else None

    return ta_summary

# ==============================================================================
# --- NEW Matplotlib Plotting Functions (For WordPress Static Images) ---
# ==============================================================================

def plot_historical_mpl(df, ticker, plot_period_years=3):
    """Plots Historical Price and Volume using Matplotlib."""
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date')

    # Get data for the plotting period
    df_plot = _get_plot_data(df.copy(), plot_period_years) # Operate on a copy

    if df_plot.empty:
        print(f"Warning: No data available for {ticker} in the last {plot_period_years} years.")
        return None # Return None if no data

    fig, ax1 = plt.subplots(figsize=(12, 6)) # Create figure and primary axes

    # Plot Close Price on primary axis
    color_price = 'navy' # Dark blue
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Price ($)', color=color_price)
    ax1.plot(df_plot['Date'], df_plot['Close'], color=color_price, linewidth=1.5, label='Close Price')
    ax1.tick_params(axis='y', labelcolor=color_price)
    ax1.grid(True, axis='y', linestyle='--', alpha=0.6)

    # Create secondary axis for Volume
    ax2 = ax1.twinx()
    color_volume = 'darkorange'
    ax2.set_ylabel('Volume', color=color_volume)

    # Calculate Volume SMA on the plotted data range
    if 'Volume' in df_plot.columns and not df_plot['Volume'].isnull().all():
        df_plot['Volume_SMA20'] = calculate_volume_sma(df_plot, 20) # Calculate on plot data
        ax2.bar(df_plot['Date'], df_plot['Volume'], color=color_volume, alpha=0.3, width=1.0, label='Volume') # Adjust width as needed
        # Plot Volume SMA only if calculated
        if not df_plot['Volume_SMA20'].isnull().all():
             ax2.plot(df_plot['Date'], df_plot['Volume_SMA20'], color='saddlebrown', linewidth=1, linestyle='dotted', label='Volume SMA20')
    ax2.tick_params(axis='y', labelcolor=color_volume)
    ax2.set_ylim(bottom=0) # Volume starts at 0

    # Formatting
    fig.suptitle(f'{ticker} Historical Price & Volume ({plot_period_years}Y)', fontsize=14)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y')) # Format date axis
    fig.autofmt_xdate() # Auto-rotate date labels

    # Combine legends
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc='upper left', fontsize='small')

    fig.tight_layout(rect=[0, 0, 1, 0.96]) # Adjust layout to prevent title overlap

    return fig # Return the Matplotlib figure object

def plot_bollinger_mpl(df, ticker, plot_period_years=3):
    """Plots Price and Bollinger Bands using Matplotlib."""
    if len(df) < 20: return None
    # Calculate on full df first
    df['BB_Upper'], df['BB_Middle'], df['BB_Lower'] = calculate_bollinger_bands(df['Close'])
    # Get data for the plotting period
    df_plot = _get_plot_data(df.copy(), plot_period_years)
    df_plot = df_plot.dropna(subset=['BB_Upper', 'BB_Middle', 'BB_Lower', 'Close'])

    if df_plot.empty: return None

    fig, ax = plt.subplots(figsize=(12, 6))

    # Plot Bands and Fill
    ax.plot(df_plot['Date'], df_plot['BB_Upper'], color='darkgrey', linewidth=1, label='Upper Band')
    ax.plot(df_plot['Date'], df_plot['BB_Lower'], color='darkgrey', linewidth=1, label='Lower Band')
    ax.fill_between(df_plot['Date'], df_plot['BB_Lower'], df_plot['BB_Upper'], color='lightgrey', alpha=0.3)

    # Plot Middle Band (SMA20) and Close Price
    ax.plot(df_plot['Date'], df_plot['BB_Middle'], color='darkorange', linewidth=1, linestyle='--', label='SMA20')
    ax.plot(df_plot['Date'], df_plot['Close'], color='navy', linewidth=1.5, label='Close Price')

    # Formatting
    ax.set_title(f'{ticker} Price & Bollinger Bands ({plot_period_years}Y)', fontsize=14)
    ax.set_ylabel('Price ($)')
    ax.legend(loc='upper left', fontsize='small')
    ax.grid(True, linestyle='--', alpha=0.6)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    fig.autofmt_xdate()
    fig.tight_layout()

    return fig

def plot_rsi_mpl(df, ticker, plot_period_years=3):
    """Plots RSI using Matplotlib."""
    if len(df) < 15: return None
    # Calculate on full df
    df['RSI'] = calculate_rsi(df['Close'])
    # Get data for the plotting period
    df_plot = _get_plot_data(df.copy(), plot_period_years)
    df_plot = df_plot.dropna(subset=['RSI'])

    if df_plot.empty: return None

    fig, ax = plt.subplots(figsize=(12, 4)) # Smaller height for indicator

    # Plot RSI line
    ax.plot(df_plot['Date'], df_plot['RSI'], color='purple', linewidth=1.5, label='RSI (14)')

    # Plot Overbought/Oversold lines
    ax.axhline(70, color='red', linestyle='--', linewidth=1, alpha=0.8, label='Overbought (70)')
    ax.axhline(30, color='green', linestyle='--', linewidth=1, alpha=0.8, label='Oversold (30)')

    # Formatting
    ax.set_title(f'{ticker} Relative Strength Index (RSI 14) ({plot_period_years}Y)', fontsize=14)
    ax.set_ylabel('RSI')
    ax.set_ylim(0, 100) # RSI range is 0-100
    ax.legend(loc='upper left', fontsize='small')
    ax.grid(True, linestyle='--', alpha=0.6)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    fig.autofmt_xdate()
    fig.tight_layout()

    return fig

def plot_macd_lines_mpl(df, ticker, plot_period_years=3):
    """Plots MACD Line vs Signal Line using Matplotlib."""
    if len(df) < 35: return None
    # Calculate on full df
    df['MACD_Line'], df['MACD_Signal'], _ = calculate_macd(df['Close'])
    # Get data for plotting period
    df_plot = _get_plot_data(df.copy(), plot_period_years)
    df_plot = df_plot.dropna(subset=['MACD_Line', 'MACD_Signal'])

    if len(df_plot) < 2: return None

    fig, ax = plt.subplots(figsize=(12, 4)) # Smaller height

    # Plot MACD and Signal lines
    ax.plot(df_plot['Date'], df_plot['MACD_Line'], color='navy', linewidth=1.5, label='MACD Line')
    ax.plot(df_plot['Date'], df_plot['MACD_Signal'], color='orangered', linewidth=1.5, label='Signal Line')

    # Plot Zero line
    ax.axhline(0, color='grey', linestyle='--', linewidth=1, alpha=0.5)

    # Formatting
    ax.set_title(f'{ticker} MACD Line vs Signal Line ({plot_period_years}Y)', fontsize=14)
    ax.set_ylabel('MACD Value')
    ax.legend(loc='upper left', fontsize='small')
    ax.grid(True, linestyle='--', alpha=0.6)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    fig.autofmt_xdate()
    fig.tight_layout()

    return fig

def plot_macd_hist_mpl(df, ticker, plot_period_years=3):
    """Plots MACD Histogram using Matplotlib."""
    if len(df) < 35: return None
    # Ensure MACD is calculated on full df
    if 'MACD_Hist' not in df.columns:
        _, _, df['MACD_Hist'] = calculate_macd(df['Close'])

    # Get data for plotting period
    df_plot = _get_plot_data(df.copy(), plot_period_years)
    df_plot = df_plot.dropna(subset=['MACD_Hist'])

    if len(df_plot) < 2: return None

    fig, ax = plt.subplots(figsize=(12, 4)) # Smaller height

    # Plot MACD Histogram bars
    colors = ['green' if val >= 0 else 'red' for val in df_plot['MACD_Hist']]
    # Use date index directly for bars if 'Date' is datetime
    if pd.api.types.is_datetime64_any_dtype(df_plot['Date']):
        # Estimate bar width based on date frequency (might need adjustment)
        date_diffs = df_plot['Date'].diff().median()
        bar_width = date_diffs.days * 0.8 if date_diffs else 0.8 # Adjust factor as needed
        ax.bar(df_plot['Date'], df_plot['MACD_Hist'], color=colors, width=bar_width, label='MACD Histogram')
    else: # Fallback if 'Date' is not datetime
        ax.bar(range(len(df_plot)), df_plot['MACD_Hist'], color=colors, label='MACD Histogram')

    # Plot Zero line
    ax.axhline(0, color='grey', linestyle='--', linewidth=1, alpha=0.5)

    # Formatting
    ax.set_title(f'{ticker} MACD Histogram ({plot_period_years}Y)', fontsize=14)
    ax.set_ylabel('Histogram Value')
    ax.legend(loc='upper left', fontsize='small')
    ax.grid(True, axis='y', linestyle='--', alpha=0.6) # Grid only on y-axis for bars

    # Format X axis only if 'Date' is datetime
    if pd.api.types.is_datetime64_any_dtype(df_plot['Date']):
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        fig.autofmt_xdate()
    else:
        ax.set_xlabel('Index') # Label index if not dates

    fig.tight_layout()

    return fig

# --- NEW: Matplotlib Forecast Plot ---
def plot_forecast_mpl(rdata, ticker):
    """
    Plots Actual vs Forecast data using Matplotlib.
    Accepts the prepared report data dictionary (rdata).
    """
    print(f"  Generating Matplotlib forecast chart for {ticker}...")

    actual_data = rdata.get('actual_data')
    forecast_data = rdata.get('monthly_forecast_table_data') # Use the table data for consistency
    period_label = rdata.get('period_label', 'Period')
    time_col = rdata.get('time_col', 'Period')
    overall_pct_change = rdata.get('overall_pct_change', 0.0)
    forecast_1y = rdata.get('forecast_1y')


    # Validate data
    if forecast_data is None or forecast_data.empty or time_col not in forecast_data.columns:
        print(f"    Warning: Forecast data for {ticker} is missing or invalid for plotting.")
        return None

    fig, ax = plt.subplots(figsize=(12, 6))

    try:
        # Prepare x-axis labels (treat 'Period' as categorical for now)
        x_labels_actual = []
        y_values_actual = []
        if actual_data is not None and not actual_data.empty and 'Average' in actual_data.columns and time_col in actual_data.columns:
            plot_actual = actual_data.tail(6) # Show last 6 actual periods
            x_labels_actual = plot_actual[time_col].tolist()
            y_values_actual = plot_actual['Average'].tolist()
            ax.plot(x_labels_actual, y_values_actual, marker='o', linestyle='-', color='blue', linewidth=1.5, label='Actual Avg')

        x_labels_forecast = forecast_data[time_col].tolist()
        y_values_forecast_avg = forecast_data['Average'].tolist()

        # Combine labels for the axis, ensuring order and uniqueness
        all_x_labels = sorted(list(set(x_labels_actual + x_labels_forecast)))
        ax.set_xticks(range(len(all_x_labels))) # Set ticks based on combined length
        ax.set_xticklabels(all_x_labels, rotation=45, ha='right') # Rotate labels

        # Plot forecast average
        ax.plot(x_labels_forecast, y_values_forecast_avg, marker='.', linestyle='--', color='green', linewidth=1.5, label='Forecast Avg')

        # Plot forecast range (Low/High) if available
        if 'Low' in forecast_data.columns and 'High' in forecast_data.columns:
            y_values_forecast_low = forecast_data['Low'].tolist()
            y_values_forecast_high = forecast_data['High'].tolist()
            ax.fill_between(x_labels_forecast, y_values_forecast_low, y_values_forecast_high,
                            color='palegreen', alpha=0.4, label='Forecast Range (Low-High)')

        # Add annotation for the 1-year forecast point
        if forecast_1y is not None and x_labels_forecast:
            last_period_label = x_labels_forecast[-1]
            annotation_text = f"{forecast_1y:.2f}\n({overall_pct_change:+.1f}% 1Y)"
            # Find index of last period label in the combined axis
            try:
                 last_period_index = all_x_labels.index(last_period_label)
                 ax.annotate(annotation_text,
                           xy=(last_period_index, forecast_1y), # Use index for position
                           xytext=(15, -15), # Offset text
                           textcoords='offset points',
                           ha='center', va='top',
                           fontsize=9, color='darkgreen',
                           bbox=dict(boxstyle='round,pad=0.3', fc='white', alpha=0.7),
                           arrowprops=dict(arrowstyle='->', color='darkgreen'))
            except ValueError:
                 print(f"Warning: Could not find last forecast period '{last_period_label}' in combined axis for annotation.")

        # Formatting
        num_forecast_periods = len(forecast_data)
        ax.set_title(f'{ticker} Price Forecast ({num_forecast_periods} {period_label}s)', fontsize=14)
        ax.set_ylabel('Price ($)')
        ax.set_xlabel(period_label)
        ax.legend(loc='upper left', fontsize='small')
        ax.grid(True, linestyle='--', alpha=0.6)

        # Adjust x-axis tick frequency if too many labels
        if len(all_x_labels) > 20:
            step = max(1, len(all_x_labels) // 15) # Show ~15 labels max
            ax.set_xticks(np.arange(0, len(all_x_labels), step))

        fig.tight_layout()
        print(f"  Successfully generated Matplotlib forecast figure for {ticker}")
        return fig

    except Exception as e:
        print(f"  ERROR generating Matplotlib forecast chart for {ticker}: {e}")
        import traceback
        traceback.print_exc() # Print detailed error
        plt.close(fig) # Ensure figure is closed
        return None # Return None on error


# ==============================================================================
# --- FEATURE 1: Enhanced Risk Factors (Quantitative + Industry-Specific) ---
# ==============================================================================

# Industry-specific risk library keyed by yfinance industry/sector strings
INDUSTRY_RISKS: dict = {
    "Semiconductors": [
        {"category": "Geopolitical / Export Controls", "severity": "high",
         "text": "U.S. export restrictions on advanced chips to China directly impact revenue. China represents a significant portion of global semiconductor demand."},
        {"category": "Customer Concentration", "severity": "high",
         "text": "Heavy reliance on a small number of hyperscaler customers (Microsoft, Google, Amazon, Meta) means losing one major contract could materially impact revenue."},
        {"category": "Competitive Disruption", "severity": "medium",
         "text": "Custom silicon development by major cloud providers (Google TPUs, Amazon Trainium, Microsoft Maia) could reduce dependency on third-party GPUs over time."},
        {"category": "AI Cycle Risk", "severity": "medium",
         "text": "Current revenue is heavily tied to AI infrastructure buildout. If enterprise AI ROI disappoints, capex spending could contract sharply, disproportionately impacting GPU demand."},
        {"category": "Supply Chain Dependency", "severity": "high",
         "text": "Reliance on leading-edge foundries (e.g. TSMC) for advanced node manufacturing creates concentration risk. Any geopolitical disruption or capacity constraint could affect supply."},
    ],
    "Technology": [
        {"category": "Regulatory / Antitrust Risk", "severity": "medium",
         "text": "Increased global regulatory scrutiny (EU Digital Markets Act, U.S. antitrust investigations) can disrupt business models and impose material compliance costs."},
        {"category": "Cybersecurity & Data Privacy", "severity": "medium",
         "text": "A significant data breach or prolonged system outage could erode user trust, invite regulatory fines, and trigger customer churn."},
        {"category": "Talent Competition", "severity": "low",
         "text": "Competition for skilled engineers in AI, cloud, and software remains intense, increasing compensation costs and the risk of key-person departures."},
        {"category": "Rapid Technology Obsolescence", "severity": "medium",
         "text": "Fast-moving innovation cycles can render existing products or platforms less competitive, requiring continuous heavy R&D investment to maintain market position."},
    ],
    "Software—Application": [
        {"category": "Customer Churn / NRR Pressure", "severity": "medium",
         "text": "SaaS businesses depend on net revenue retention. A slowdown in enterprise IT spending or increased competition can compress NRR and slow ARR growth."},
        {"category": "AI Disruption to SaaS", "severity": "high",
         "text": "Generative AI tools are commoditising certain software categories. Incumbents face the risk of being displaced by AI-native competitors offering similar functionality at lower cost."},
        {"category": "Concentration in SMB vs. Enterprise", "severity": "low",
         "text": "Heavy exposure to SMB customers increases churn risk during economic downturns, as smaller businesses cut software spend faster than large enterprises."},
    ],
    "Software—Infrastructure": [
        {"category": "Pricing Pressure from Hyperscalers", "severity": "high",
         "text": "AWS, Azure, and Google Cloud bundle competing infrastructure services, creating ongoing price competition that can compress margins for independent infrastructure software vendors."},
        {"category": "Open-Source Competition", "severity": "medium",
         "text": "The rise of viable open-source alternatives can commoditise commercial infrastructure software, making it harder to maintain premium pricing and customer lock-in."},
    ],
    "Internet Content & Information": [
        {"category": "Digital Advertising Cyclicality", "severity": "high",
         "text": "Digital ad revenue is highly correlated with macroeconomic conditions. An economic slowdown or recession typically triggers sharp cuts in advertiser budgets, directly impacting revenue."},
        {"category": "Regulatory / Privacy Risk", "severity": "high",
         "text": "Privacy regulations (GDPR, CCPA) and browser cookie deprecation limit data collection capabilities, reducing ad targeting precision and monetisation potential."},
        {"category": "AI Search Disruption", "severity": "medium",
         "text": "AI-powered answer engines could reduce traditional search query volume, structurally threatening the search advertising business model over the medium term."},
    ],
    "Consumer Electronics": [
        {"category": "Hardware Upgrade Cycle Risk", "severity": "medium",
         "text": "Lengthening device replacement cycles, especially in smartphones, can slow hardware revenue growth even when the installed base is large."},
        {"category": "China Revenue Dependency", "severity": "high",
         "text": "Significant revenue exposure to China creates dual risks: U.S.-China trade tensions can trigger tariffs or market-access restrictions."},
        {"category": "Supply Chain Concentration", "severity": "medium",
         "text": "Manufacturing concentration in Asia (particularly China and Taiwan) creates vulnerability to geopolitical disruptions, natural disasters, and regional lockdowns."},
    ],
    "Biotechnology": [
        {"category": "Clinical Trial Failure", "severity": "high",
         "text": "Failure of a key clinical trial can trigger dramatic valuation declines. The drug development pipeline is inherently uncertain and capital-intensive."},
        {"category": "Regulatory Approval Risk", "severity": "high",
         "text": "FDA/EMA approval decisions involve significant uncertainty. A Complete Response Letter or approval delay can materially set back revenue timelines."},
        {"category": "Patent Cliff / Generic Competition", "severity": "medium",
         "text": "Approaching patent expirations invite generic and biosimilar competition that can rapidly erode pricing power and market share on blockbuster drugs."},
    ],
    "Drug Manufacturers—General": [
        {"category": "Drug Pricing / Political Risk", "severity": "high",
         "text": "Government pressure on drug pricing (IRA provisions, Medicare negotiation) and PBM dynamics can structurally compress pharmaceutical revenue and margins."},
        {"category": "Patent Cliff", "severity": "high",
         "text": "Loss of exclusivity on major revenue-generating drugs exposes the company to generic competition that can erode revenues sharply within 12–24 months post-patent expiry."},
        {"category": "Pipeline Execution Risk", "severity": "medium",
         "text": "Late-stage pipeline failures can destroy years of R&D investment and delay revenue diversification strategies."},
    ],
    "Financial Services": [
        {"category": "Interest Rate Sensitivity", "severity": "high",
         "text": "Financial companies' net interest margins are directly affected by central bank policy. Rapid rate changes can compress margins or create credit loss spikes."},
        {"category": "Credit / Default Risk", "severity": "medium",
         "text": "An economic downturn may increase loan delinquencies and credit losses, particularly in consumer or commercial real estate portfolios."},
        {"category": "Regulatory Capital Requirements", "severity": "medium",
         "text": "Evolving Basel III endgame rules and stress-test requirements could force higher capital retention, reducing capital returns to shareholders."},
    ],
    "Banks—Diversified": [
        {"category": "Net Interest Margin Compression", "severity": "high",
         "text": "Rate cuts erode NIM on floating-rate assets faster than on fixed-rate liabilities, directly pressuring bank profitability."},
        {"category": "Commercial Real Estate Exposure", "severity": "high",
         "text": "Significant CRE loan books face elevated risk as office vacancy rates remain elevated and cap rates rise in a high-rate environment."},
    ],
    "Oil & Gas Exploration & Production": [
        {"category": "Commodity Price Volatility", "severity": "high",
         "text": "Revenue and cash flow are directly linked to oil and natural gas prices, which are inherently volatile and driven by geopolitical events, OPEC decisions, and demand cycles."},
        {"category": "Energy Transition Risk", "severity": "medium",
         "text": "Long-term shift to renewables and EV adoption could structurally reduce demand for oil and gas over the next decade, pressuring asset valuations."},
        {"category": "Regulatory / Environmental Liability", "severity": "medium",
         "text": "Climate-related regulations, carbon taxes, and environmental compliance costs continue to increase, adding to operating expenses and capex burdens."},
    ],
    "Retail—Cyclical": [
        {"category": "Consumer Spending Sensitivity", "severity": "high",
         "text": "Discretionary retailers are highly sensitive to consumer confidence and disposable income. Economic downturns can trigger sharp revenue declines."},
        {"category": "Inventory Management Risk", "severity": "medium",
         "text": "Misjudging demand can result in excess inventory requiring markdowns, compressing gross margins and tying up working capital."},
        {"category": "E-Commerce Competition", "severity": "medium",
         "text": "Ongoing shift of consumer spending to e-commerce (especially Amazon) creates sustained pressure on brick-and-mortar retail traffic and pricing power."},
    ],
    "Aerospace & Defense": [
        {"category": "Government Budget Dependency", "severity": "high",
         "text": "Defense contractors rely heavily on government appropriations. Budget cuts or continuing resolutions can delay programme funding and deliveries."},
        {"category": "Supply Chain Disruption", "severity": "medium",
         "text": "Complex, multi-tier aerospace supply chains are vulnerable to single-source supplier disruptions, raw material shortages, and skilled-labour constraints."},
    ],
    "Auto Manufacturers": [
        {"category": "EV Transition Execution Risk", "severity": "high",
         "text": "The capital-intensive pivot to electric vehicles carries significant execution risk — from battery supply to software integration and consumer adoption pace."},
        {"category": "UAW / Labour Strike Risk", "severity": "medium",
         "text": "Unionised workforces can disrupt production through strikes or work-to-rule actions, creating significant revenue gaps and inventory disruptions at key plants."},
        {"category": "China Competition", "severity": "high",
         "text": "Low-cost Chinese EV manufacturers are expanding globally, putting structural pricing pressure on incumbent automakers, particularly in Europe."},
    ],
    "Technology": [  # Fallback sector-level entry kept intentionally short
        {"category": "Macro Cyclicality", "severity": "medium",
         "text": "Technology spending tends to be among the first areas cut during economic slowdowns as enterprises defer capital and software investments."},
    ],
}

# Generic / universal risks appended when no industry match is found
_GENERIC_RISKS: list = [
    {"category": "Market / Macro Risk", "severity": "low",
     "text": "Broad market downturns driven by recession fears, rising interest rates, or geopolitical shocks can weigh on the stock regardless of company fundamentals."},
    {"category": "Liquidity Risk", "severity": "low",
     "text": "In volatile or bear markets, lower-liquidity stocks may experience outsized price moves due to wider bid-ask spreads and reduced market-maker support."},
    {"category": "Currency / FX Risk", "severity": "low",
     "text": "Companies with significant international revenue face earnings translation risk from USD strengthening, which can reduce reported results for non-US operations."},
]


_AI_CLOUD_INFRA_RISKS: list = [
    {"category": "GPU Supply & Capacity Constraints", "severity": "high",
     "text": "AI cloud providers depend on constrained high-end GPU supply. Delays in allocation or pricing spikes can impair growth plans and gross margins."},
    {"category": "Hyperscaler Competition", "severity": "high",
     "text": "Competition from AWS, Azure, and Google Cloud can pressure pricing, customer acquisition costs, and retention for smaller AI infrastructure platforms."},
    {"category": "Capex Intensity", "severity": "high",
     "text": "AI infrastructure expansion is capital-intensive. Sustained capex without matching utilization can weigh on free cash flow and increase financing risk."},
    {"category": "Customer Concentration", "severity": "medium",
     "text": "Revenue concentration among a limited number of enterprise AI customers can create volatility if one or two large accounts reduce workloads."},
    {"category": "Utilization & Unit Economics", "severity": "medium",
     "text": "Under-utilized GPU clusters can quickly compress margins. Profitability depends on maintaining high utilization and disciplined workload pricing."},
]


def generate_quantitative_risks(info: dict) -> list:
    """
    Derive structured risk factors directly from fundamental ticker data.
    Returns a list of dicts with keys: category, severity, text.
    Severity levels: 'high' | 'medium' | 'low'
    """
    risks = []

    # --- Debt / Leverage Risk ---
    de_ratio = info.get('debtToEquity')
    if de_ratio is not None:
        try:
            de_val = float(de_ratio) / 100  # yfinance reports D/E as percentage points
            if de_val > 2.0:
                risks.append({
                    "category": "Financial Leverage",
                    "severity": "high",
                    "text": f"Debt-to-Equity ratio of {de_val:.2f}x significantly elevates financial risk, "
                            f"especially in a rising-rate environment where refinancing costs increase."
                })
            elif de_val > 1.0:
                risks.append({
                    "category": "Financial Leverage",
                    "severity": "medium",
                    "text": f"Debt-to-Equity ratio of {de_val:.2f}x reflects meaningful leverage. "
                            f"Investors should monitor free cash flow coverage of debt obligations."
                })
        except (TypeError, ValueError):
            pass

    # --- Valuation Risk ---
    trailing_pe = info.get('trailingPE')
    forward_pe  = info.get('forwardPE')
    pe = forward_pe if forward_pe is not None else trailing_pe
    if pe is not None:
        try:
            pe_val = float(pe)
            if pe_val > 50:
                risks.append({
                    "category": "Valuation Risk",
                    "severity": "high",
                    "text": f"P/E ratio of {pe_val:.1f}x prices in very high growth expectations. "
                            f"Any earnings miss or guidance cut could trigger a sharp de-rating."
                })
            elif pe_val > 30:
                risks.append({
                    "category": "Valuation Risk",
                    "severity": "medium",
                    "text": f"Elevated P/E of {pe_val:.1f}x leaves limited room for disappointment. "
                            f"Sustained growth execution is required to justify the current multiple."
                })
        except (TypeError, ValueError):
            pass

    # --- Market Sensitivity / Beta Risk ---
    beta = info.get('beta')
    if beta is not None:
        try:
            beta_val = float(beta)
            if beta_val > 2.0:
                risks.append({
                    "category": "Market Sensitivity",
                    "severity": "high",
                    "text": f"Beta of {beta_val:.2f}x means the stock amplifies broad market moves substantially. "
                            f"A 10% market decline could produce a ~{beta_val * 10:.0f}% drawdown."
                })
            elif beta_val > 1.5:
                risks.append({
                    "category": "Market Sensitivity",
                    "severity": "medium",
                    "text": f"Beta of {beta_val:.2f}x indicates above-average sensitivity to market swings, "
                            f"amplifying both upside and downside moves relative to the index."
                })
        except (TypeError, ValueError):
            pass

    # --- Profitability Risk ---
    profit_margins = info.get('profitMargins')
    if profit_margins is not None:
        try:
            pm = float(profit_margins)
            if pm < 0:
                risks.append({
                    "category": "Profitability Deficit",
                    "severity": "high",
                    "text": f"Negative net profit margin ({pm * 100:.1f}%) indicates the company is currently burning cash at the operating level, "
                            f"requiring either a path to profitability or continued external financing."
                })
        except (TypeError, ValueError):
            pass

    # --- Short Interest Risk ---
    short_ratio = info.get('shortRatio')
    if short_ratio is not None:
        try:
            sr = float(short_ratio)
            if sr > 7:
                risks.append({
                    "category": "Short Interest / Squeeze Risk",
                    "severity": "medium",
                    "text": f"Short ratio of {sr:.1f} days-to-cover reflects elevated bearish positioning. "
                            f"While a high short ratio can trigger squeeze rallies, it also signals meaningful "
                            f"institutional skepticism about near-term fundamentals."
                })
        except (TypeError, ValueError):
            pass

    # --- Revenue Growth Risk ---
    revenue_growth = info.get('revenueGrowth')
    if revenue_growth is not None:
        try:
            rg = float(revenue_growth)
            if rg < -0.05:
                risks.append({
                    "category": "Revenue Contraction",
                    "severity": "high",
                    "text": f"Year-over-year revenue growth of {rg * 100:.1f}% signals declining top-line momentum, "
                            f"which may challenge margin expansion and free cash flow generation."
                })
        except (TypeError, ValueError):
            pass

    # --- Earnings Growth Risk ---
    earnings_growth = info.get('earningsGrowth')
    if earnings_growth is not None:
        try:
            eg = float(earnings_growth)
            if eg < -0.10:
                risks.append({
                    "category": "Earnings Deterioration",
                    "severity": "high",
                    "text": f"Earnings growth of {eg * 100:.1f}% YoY raises concerns about cost structure, "
                            f"pricing power, or market-share loss. Sustained negative EPS growth "
                            f"typically leads to multiple compression."
                })
        except (TypeError, ValueError):
            pass

    # --- Liquidity Risk ---
    current_ratio = info.get('currentRatio')
    if current_ratio is not None:
        try:
            cr = float(current_ratio)
            if cr < 1.0:
                risks.append({
                    "category": "Short-Term Liquidity",
                    "severity": "high",
                    "text": f"Current ratio of {cr:.2f}x (below 1.0) means current liabilities exceed current assets, "
                            f"suggesting potential working capital strain or reliance on revolving credit facilities."
                })
        except (TypeError, ValueError):
            pass

    return risks


def get_industry_risks(sector: str, industry: str, company_summary: str = "") -> list:
    """
    Return structured industry-specific risk factors from the INDUSTRY_RISKS library.
    Falls back to sector-level risks, then generic risks. Returns up to 5 entries.
    """
    sector_l = (sector or "").lower()
    industry_l = (industry or "").lower()
    summary_l = (company_summary or "").lower()

    ai_infra_keywords = [
        'ai infrastructure', 'gpu cloud', 'gpu clusters', 'cloud infrastructure', 'infrastructure-as-a-service',
        'data center', 'compute platform', 'ai cloud', 'model training', 'inference workloads', 'neocloud'
    ]
    adtech_keywords = ['advertising', 'ad targeting', 'search advertising', 'cookie', 'digital ad']
    looks_like_ai_infra = any(k in f"{industry_l} {summary_l}" for k in ai_infra_keywords)
    looks_like_adtech = any(k in summary_l for k in adtech_keywords)

    if looks_like_ai_infra and not looks_like_adtech:
        return list(_AI_CLOUD_INFRA_RISKS)[:5]

    risks = INDUSTRY_RISKS.get(industry or "", [])
    if not risks and sector:
        risks = INDUSTRY_RISKS.get(sector, [])
    if not risks:
        risks = _GENERIC_RISKS
    return list(risks)[:5]  # Cap at 5 industry risks per report


# ==============================================================================
# --- FEATURE 2: Implied Volatility & Options Data (via yfinance) ---
# ==============================================================================

def interpret_iv(iv: float, iv_rank: float | None, is_proxy: bool = False) -> tuple[str, str]:
    """Return a short signal label and plain-English interpretation."""
    if iv_rank is None:
        # Fallback when true IV history is unavailable from the data source.
        if iv >= 70:
            label = "Very High"
            detail = "Current ATM IV is very high in absolute terms."
        elif iv >= 45:
            label = "Elevated"
            detail = "Current ATM IV is elevated in absolute terms."
        elif iv >= 25:
            label = "Normal"
            detail = "Current ATM IV is within a typical absolute range."
        else:
            label = "Low"
            detail = "Current ATM IV is low in absolute terms."
        return label, detail

    if iv_rank > 80:
        label = "Very High"
        detail = "Options are expensive (top 20% of the reference range). Premium sellers historically have an edge."
    elif iv_rank > 50:
        label = "Elevated"
        detail = "Above-average uncertainty is priced in. Slightly favours premium sellers."
    elif iv_rank > 20:
        label = "Normal"
        detail = "Options are fairly priced relative to the reference range."
    else:
        label = "Low"
        detail = "Options are cheap (bottom 20% of the reference range). Premium buyers may find value."

    if is_proxy:
        detail = f"{detail} Signal is based on an RV-derived proxy because historical per-stock IV is not available from yfinance."
    return label, detail


def get_options_data(ticker_symbol: str) -> dict | None:
    """
    Fetch ATM options data and compute Implied Volatility metrics using yfinance.
    Returns a dict of options metrics, or None if data is unavailable.

    Keys returned include:
        nearest_expiry, expiry_dte, implied_move_pct, implied_move_dollars,
        atm_iv, iv_rank, iv_rank_method, iv_52w_high, iv_52w_low,
        call_iv, put_iv, atm_strike, current_price, expiry_count,
        signal_label, interpretation, data_warnings
    """
    try:
        import yfinance as yf
        ticker = yf.Ticker(ticker_symbol)

        expiry_dates = ticker.options
        if not expiry_dates:
            print(f"get_options_data: no options expiries available for {ticker_symbol}")
            return None

        expiry_count = len(expiry_dates)

        # --- Current price: prefer info, fallback to fast_info ---
        info = ticker.info or {}
        current_price = (
            info.get('currentPrice') or
            info.get('regularMarketPrice') or
            getattr(ticker.fast_info, 'last_price', None)
        )
        if current_price is None or float(current_price) <= 0:
            print(f"get_options_data: could not determine current price for {ticker_symbol}")
            return None
        current_price = float(current_price)

        from datetime import date as _date, datetime as _datetime, timedelta as _timedelta
        today      = _date.today()
        today_str  = str(today)

        future_expiries = []
        for exp in expiry_dates:
            exp_date = _datetime.strptime(exp, '%Y-%m-%d').date()
            days_out = (exp_date - today).days
            if days_out >= 7:
                future_expiries.append((exp, days_out))

        if not future_expiries:
            print(f"get_options_data: no expiry >=7 DTE for {ticker_symbol}")
            return None

        # Sort candidate expiries by DTE proximity to 30 days
        sorted_candidates = sorted(future_expiries, key=lambda item: abs(item[1] - 30))

        chosen_expiry = None
        chosen_dte = None
        chosen_chain = None
        data_warnings = []

        # Find the first expiry that has overlapping strikes within 15% of current price
        for exp, days_out in sorted_candidates:
            try:
                chain = ticker.option_chain(exp)
                calls = chain.calls.copy()
                puts = chain.puts.copy()
                if calls.empty or puts.empty:
                    continue
                calls['strike'] = pd.to_numeric(calls['strike'], errors='coerce')
                puts['strike'] = pd.to_numeric(puts['strike'], errors='coerce')
                calls = calls.dropna(subset=['strike'])
                puts = puts.dropna(subset=['strike'])
                if calls.empty or puts.empty:
                    continue
                
                common_strikes = set(calls['strike']).intersection(set(puts['strike']))
                if common_strikes:
                    closest_strike = min(common_strikes, key=lambda k: abs(k - current_price))
                    if abs(closest_strike - current_price) / current_price <= 0.15:
                        chosen_expiry = exp
                        chosen_dte = days_out
                        chosen_chain = chain
                        break
            except Exception as e:
                print(f"Error checking option chain for expiry {exp}: {e}")
                continue

        if chosen_expiry is None:
            # Fall back to picking the closest expiry by DTE
            chosen = min(future_expiries, key=lambda item: abs(item[1] - 30))
            chosen_expiry, chosen_dte = chosen
            print(f"get_options_data: fallback to closest expiry {chosen_expiry} ({chosen_dte} DTE) for {ticker_symbol}")
            try:
                chosen_chain = ticker.option_chain(chosen_expiry)
            except Exception as e:
                print(f"Error loading fallback option chain for {chosen_expiry}: {e}")
                return None
        else:
            print(f"get_options_data: selected high-quality expiry {chosen_expiry} ({chosen_dte} DTE) for {ticker_symbol}")

        nearest_expiry = chosen_expiry
        exp_days_out = chosen_dte

        calls = chosen_chain.calls.copy()
        puts = chosen_chain.puts.copy()
        calls['strike'] = pd.to_numeric(calls['strike'], errors='coerce')
        puts['strike'] = pd.to_numeric(puts['strike'], errors='coerce')
        calls = calls.dropna(subset=['strike'])
        puts = puts.dropna(subset=['strike'])
        if calls.empty or puts.empty:
            return None

        # Filter ATM strike (overlapping common strike closest to current price)
        common_strikes = set(calls['strike']).intersection(set(puts['strike']))
        if common_strikes:
            atm_strike = min(common_strikes, key=lambda k: abs(k - current_price))
        else:
            # Absolute fallback: find closest strikes independently
            atm_call_idx = (calls['strike'] - current_price).abs().idxmin()
            atm_put_idx  = (puts['strike']  - current_price).abs().idxmin()
            atm_strike = float(calls.loc[atm_call_idx]['strike'])

        calls_filtered = calls
        puts_filtered = puts

        import math as _math

        # ----------------------------------------------------------------
        # Black-Scholes IV solver.
        # yfinance often returns bid=0 / ask=0 for options more than a
        # week out, making its built-in impliedVolatility field useless
        # (it falls back to near-zero placeholder values).
        # We compute IV from lastPrice using the BS formula directly.
        # ----------------------------------------------------------------
        def _bs_option_price(flag, S, K, T, r, sigma):
            """Return Black-Scholes price for 'call' or 'put'."""
            if T <= 0 or sigma <= 0:
                return 0.0
            d1 = (_math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * _math.sqrt(T))
            d2 = d1 - sigma * _math.sqrt(T)
            def _ncdf(x):
                return 0.5 * (1.0 + _math.erf(x / _math.sqrt(2.0)))
            if flag == 'call':
                return S * _ncdf(d1) - K * _math.exp(-r * T) * _ncdf(d2)
            else:
                return K * _math.exp(-r * T) * _ncdf(-d2) - S * _ncdf(-d1)

        def _compute_iv(flag, S, K, T, r, market_price):
            """
            Compute IV (as a decimal, e.g. 0.46 for 46%) using bisection.
            Returns None if IV cannot be solved.
            """
            if market_price is None or market_price <= 0 or T <= 0 or S <= 0 or K <= 0:
                return None
            intrinsic = max(0.0, (S - K) if flag == 'call' else (K - S))
            if market_price <= intrinsic * 1.001:
                return None  # price at/below intrinsic — unsolvable
            lo, hi = 1e-4, 20.0  # search 0.01% to 2000% IV
            f_lo = _bs_option_price(flag, S, K, T, r, lo) - market_price
            f_hi = _bs_option_price(flag, S, K, T, r, hi) - market_price
            if f_lo * f_hi > 0:
                return None  # no sign change — can't bisect
            for _ in range(100):
                mid = (lo + hi) / 2.0
                f_mid = _bs_option_price(flag, S, K, T, r, mid) - market_price
                if abs(f_mid) < 1e-5 or (hi - lo) < 1e-6:
                    return mid
                if f_lo * f_mid < 0:
                    hi = mid
                else:
                    lo = mid
                    f_lo = f_mid
            return (lo + hi) / 2.0

        T         = exp_days_out / 365.0   # time to expiry in years
        r         = 0.043                  # approximate risk-free rate

        def _coerce_price(v):
            try:
                f = float(v)
                return f if _math.isfinite(f) and f > 0 else None
            except (TypeError, ValueError):
                return None

        def _is_stale_trade(last_trade_raw, max_age_days=2):
            if last_trade_raw is None or (isinstance(last_trade_raw, float) and _math.isnan(last_trade_raw)):
                return True
            try:
                ts = pd.to_datetime(last_trade_raw, utc=True, errors='coerce')
                if pd.isna(ts):
                    return True
                now = pd.Timestamp.utcnow()
                return (now - ts).total_seconds() > (max_age_days * 86400)
            except Exception:
                return True

        def _extract_option_price(row, allow_stale_last=False):
            bid = _coerce_price(row.get('bid'))
            ask = _coerce_price(row.get('ask'))
            if bid is not None and ask is not None and ask >= bid:
                return (bid + ask) / 2.0, 'mid'

            last = _coerce_price(row.get('lastPrice'))
            if last is not None:
                last_trade = row.get('lastTradeDate')
                if allow_stale_last or not _is_stale_trade(last_trade):
                    return last, 'last'
            return None, 'none'

        def _row_iv_pct(row, flag):
            """
            Get IV% for a single option row.
            Priority: yfinance impliedVolatility (if plausible) -> BS using
            the same market-price extraction logic used for implied move.
            """
            # --- try yfinance IV first ---
            raw_iv = row.get('impliedVolatility')
            if raw_iv is not None:
                try:
                    f = float(raw_iv)
                    if not (_math.isnan(f) or _math.isinf(f)) and f > 0.10:
                        # > 10% annualised — treat as plausible (raised from 5%)
                        return f * 100.0
                except (TypeError, ValueError):
                    pass

            # --- fallback: BS IV from lastPrice ---
            K = float(row['strike'])
            price, _ = _extract_option_price(row, allow_stale_last=True)
            if price is None:
                return None

            sigma = _compute_iv(flag, current_price, K, T, r, price)
            return sigma * 100.0 if sigma is not None else None

        # --- Find ATM strike (closest to current_price) from FULL chain ---
        # Ensure Call and Put are selected at the exact same strike for a valid straddle
        if atm_strike in calls_filtered['strike'].values:
            atm_call = calls_filtered[calls_filtered['strike'] == atm_strike].iloc[0]
        else:
            atm_call_idx = (calls_filtered['strike'] - current_price).abs().idxmin()
            atm_call = calls_filtered.loc[atm_call_idx]
            
        if atm_strike in puts_filtered['strike'].values:
            atm_put = puts_filtered[puts_filtered['strike'] == atm_strike].iloc[0]
        else:
            atm_put_idx = (puts_filtered['strike'] - current_price).abs().idxmin()
            atm_put = puts_filtered.loc[atm_put_idx]
            
        atm_strike = float(atm_call['strike'])

        # Straddle pricing: midpoint first, then non-stale last trade.
        # If only stale prints exist, allow stale as last resort with warning.
        atm_call_price, call_price_src = _extract_option_price(atm_call)
        atm_put_price, put_price_src = _extract_option_price(atm_put)
        if atm_call_price is None:
            atm_call_price, call_price_src = _extract_option_price(atm_call, allow_stale_last=True)
            if atm_call_price is not None:
                data_warnings.append("Call leg used stale last trade due to missing live quote.")
        if atm_put_price is None:
            atm_put_price, put_price_src = _extract_option_price(atm_put, allow_stale_last=True)
            if atm_put_price is not None:
                data_warnings.append("Put leg used stale last trade due to missing live quote.")

        if atm_call_price is None or atm_put_price is None:
            data_warnings.append("Unable to price both ATM legs from quotes/trades.")
            straddle_price = None
            implied_move_pct = None
        else:
            straddle_price = atm_call_price + atm_put_price
            implied_move_pct = (straddle_price / current_price) * 100 if current_price != 0 else None

        # Make pricing basis explicit for downstream disclosure.
        if call_price_src == 'mid' and put_price_src == 'mid':
            pricing_basis = 'midpoint_from_bid_ask'
        elif call_price_src == 'none' or put_price_src == 'none':
            pricing_basis = 'unavailable'
        else:
            pricing_basis = 'mixed_mid_and_last_trade'

        # --- Compute IV for ATM call and put ---
        call_iv = _row_iv_pct(atm_call, 'call')
        put_iv  = _row_iv_pct(atm_put,  'put')

        # If ATM has no solv-able IV, walk to adjacent strikes
        if call_iv is None:
            sorted_calls = calls_filtered.iloc[(calls_filtered['strike'] - current_price).abs().argsort()]
            for _, row in sorted_calls.iloc[1:5].iterrows():
                v = _row_iv_pct(row, 'call')
                if v is not None:
                    call_iv = v
                    break

        if put_iv is None:
            sorted_puts = puts_filtered.iloc[(puts_filtered['strike'] - current_price).abs().argsort()]
            for _, row in sorted_puts.iloc[1:5].iterrows():
                v = _row_iv_pct(row, 'put')
                if v is not None:
                    put_iv = v
                    break

        # Average IV
        valid_ivs = [v for v in [call_iv, put_iv] if v is not None]
        avg_iv = sum(valid_ivs) / len(valid_ivs) if valid_ivs else 0.0

        # --- IV context score via rolling 30-day realized vol proxy ---
        # yfinance does not provide a reliable per-stock historical IV time series,
        # so this remains a proxy and is explicitly labelled downstream.
        hist = ticker.history(period="1y", auto_adjust=True)
        iv_rank = None
        iv_52w_high = None
        iv_52w_low  = None
        iv_rank_method = "unavailable"
        if not hist.empty and len(hist) >= 30:
            hist['returns'] = hist['Close'].pct_change()
            rolling_vol = hist['returns'].rolling(30).std() * np.sqrt(252) * 100
            rolling_vol = rolling_vol.dropna()
            if len(rolling_vol) >= 20 and avg_iv > 0:
                iv_52w_low = float(rolling_vol.quantile(0.10))
                iv_52w_high = float(rolling_vol.quantile(0.90))
                if iv_52w_high > iv_52w_low:
                    iv_rank = ((avg_iv - iv_52w_low) / (iv_52w_high - iv_52w_low)) * 100
                    iv_rank = max(0.0, min(100.0, iv_rank))
                    iv_rank_method = "rv30_proxy"

        signal_label, signal_detail = interpret_iv(avg_iv, iv_rank, is_proxy=(iv_rank_method == "rv30_proxy"))

        implied_move_scaled_30d = False
        implied_move_pct_30d = implied_move_pct
        implied_move_dollars_30d = straddle_price
        if straddle_price is not None and exp_days_out > 0 and exp_days_out > 60:
            scale = _math.sqrt(30.0 / exp_days_out)
            implied_move_pct_30d = implied_move_pct * scale if implied_move_pct is not None else None
            implied_move_dollars_30d = straddle_price * scale
            implied_move_scaled_30d = True
            data_warnings.append(
                f"Nearest usable expiry is {exp_days_out} DTE; implied move is scaled to 30-day equivalent."
            )

        return {
            "snapshot_timestamp_utc": pd.Timestamp.utcnow().isoformat(),
            "options_chain_source":  "yfinance",
            "implied_move_methodology": "ATM straddle from option quotes; midpoint from bid/ask when available, with last-trade fallback when needed.",
            "implied_move_pricing_basis": pricing_basis,
            "nearest_expiry":        nearest_expiry,
            "expiry_dte":            exp_days_out,
            "expiry_count":          expiry_count,
            "atm_strike":            round(atm_strike, 2),
            "current_price":         round(current_price, 2),
            "implied_move_pct":      round(implied_move_pct_30d, 2) if implied_move_pct_30d is not None else None,
            "implied_move_dollars":  round(implied_move_dollars_30d, 2) if implied_move_dollars_30d is not None else None,
            "implied_move_pct_raw":  round(implied_move_pct, 2) if implied_move_pct is not None else None,
            "implied_move_dollars_raw": round(straddle_price, 2) if straddle_price is not None else None,
            "implied_move_is_30d_scaled": implied_move_scaled_30d,
            "call_iv":               round(call_iv, 1) if call_iv is not None else None,
            "put_iv":                round(put_iv, 1)  if put_iv  is not None else None,
            "atm_iv":                round(avg_iv, 1),
            "iv_rank":               round(iv_rank, 1) if iv_rank is not None else None,
            "iv_rank_method":        iv_rank_method,
            "iv_52w_high":           round(iv_52w_high, 1) if iv_52w_high is not None else None,
            "iv_52w_low":            round(iv_52w_low, 1)  if iv_52w_low  is not None else None,
            "signal_label":          signal_label,
            "interpretation":        signal_detail,
            "call_price_source":     call_price_src,
            "put_price_source":      put_price_src,
            "data_warnings":         data_warnings,
        }

    except Exception as e:
        print(f"get_options_data error for {ticker_symbol}: {e}")
        return None


def validate_options_data(options_result: dict, stock_price: float) -> bool:
    """
    Sanity-check the output of get_options_data() and print warnings for
    likely data errors.  Returns True if data looks plausible, False otherwise.
    """
    if not options_result:
        print("validate_options_data: result is None/empty")
        return False

    warnings = []

    avg_iv = options_result.get('atm_iv', 0) or 0
    if avg_iv < 15:
        warnings.append(f"CRITICAL: ATM IV {avg_iv:.1f}% is below 15% — likely wrong expiry selected (check DTE)")
    if avg_iv > 200:
        warnings.append(f"CRITICAL: ATM IV {avg_iv:.1f}% is above 200% — likely a data error")

    call_iv = options_result.get('call_iv')
    if call_iv is not None and call_iv < 1:
        warnings.append(f"WARNING: Call IV {call_iv:.1f}% is near zero — check data source")

    strike = options_result.get('atm_strike', 0) or 0
    if stock_price:
        stock_price = float(stock_price)
        strike_gap_pct = abs(strike - stock_price) / stock_price
        if stock_price < 10:
            strike_gap_limit = 0.20
        elif stock_price < 50:
            strike_gap_limit = 0.10
        else:
            strike_gap_limit = 0.05
        if strike_gap_pct > strike_gap_limit:
            warnings.append(
                f"WARNING: ATM strike ${strike} is >{strike_gap_limit * 100:.0f}% from current price ${stock_price:.2f}"
            )

    iv_rank = options_result.get('iv_rank', None)
    if iv_rank == 0 and avg_iv > 20:
        warnings.append(f"CRITICAL: IV Rank=0 but ATM IV={avg_iv:.1f}% — IV Rank calculation bug")

    implied_move_pct = options_result.get('implied_move_pct', None)
    if implied_move_pct is None:
        warnings.append("WARNING: Implied move could not be computed from ATM option prices")

    if warnings:
        print("=== OPTIONS DATA VALIDATION WARNINGS ===")
        for w in warnings:
            print(f"  {w}")
        print("=========================================")
        return False

    iv_rank_msg = f"{iv_rank:.0f}/100" if iv_rank is not None else "N/A"
    print(f"validate_options_data: OK — expiry {options_result.get('nearest_expiry')}, "
          f"ATM IV={avg_iv:.1f}%, strike=${strike}, IV Rank={iv_rank_msg}")
    return True


# ==============================================================================
# --- FEATURE 3: Sector & Market Context (Benchmark Comparison) ---
# ==============================================================================

# Map yfinance sector names → most relevant benchmark ETF tickers
_SECTOR_ETF_MAP: dict = {
    "Technology":              "XLK",
    "Communication Services":  "XLC",
    "Consumer Cyclical":       "XLY",
    "Consumer Defensive":      "XLP",
    "Energy":                  "XLE",
    "Financial Services":      "XLF",
    "Healthcare":              "XLV",
    "Industrials":             "XLI",
    "Basic Materials":         "XLB",
    "Real Estate":             "XLRE",
    "Utilities":               "XLU",
}


def get_sector_health(sector: str) -> dict:
    """
    Assess the health of the stock's sector using the corresponding sector ETF.
    Returns a dict with 3-month return, trend vs 50-day MA, and a label.
    """
    try:
        import yfinance as yf
        etf_symbol = _SECTOR_ETF_MAP.get(sector, "SPY")
        etf = yf.Ticker(etf_symbol)
        hist = etf.history(period="3mo", auto_adjust=True)

        if hist.empty or len(hist) < 5:
            return {"etf_symbol": etf_symbol, "sector_trend": "Data Unavailable",
                    "sox_3m_return": None, "sox_vs_50ma": "Unknown"}

        close = hist["Close"].dropna()
        if len(close) < 2:
            return {"etf_symbol": etf_symbol, "sector_trend": "Data Unavailable",
                    "etf_3m_return": None, "sox_vs_50ma": "Unknown"}
        etf_3m_return = ((close.iloc[-1] / close.iloc[0]) - 1) * 100

        # 50-day MA relative to current price (need wider window)
        hist_50 = etf.history(period="6mo", auto_adjust=True)
        trend = "Unknown"
        if not hist_50.empty and len(hist_50) >= 50:
            close_50 = hist_50["Close"].dropna()
            if len(close_50) >= 50:
                sma50 = close_50.rolling(50).mean().iloc[-1]
                current = close_50.iloc[-1]
                trend = "bullish" if current > sma50 else "bearish"

        # Derive sector trend label using BOTH 3-month return AND 50-day MA signal.
        # Using return alone creates contradictions (e.g. -2.9% return + bearish MA
        # would wrongly show "In-line").  The MA signal acts as a tiebreaker.
        above_50ma = (trend == "bullish")
        if above_50ma and etf_3m_return > 2:
            sector_trend = "Outperforming"
        elif (not above_50ma) or etf_3m_return < -2:
            sector_trend = "Underperforming"
        else:
            sector_trend = "In-line"

        return {
            "etf_symbol":    etf_symbol,
            "etf_3m_return": round(float(etf_3m_return), 2),
            "sox_vs_50ma":   trend,
            "sector_trend":  sector_trend,
        }
    except Exception as e:
        print(f"get_sector_health error for sector '{sector}': {e}")
        return {"etf_symbol": "N/A", "sector_trend": "Data Unavailable",
                "etf_3m_return": None, "sox_vs_50ma": "Unknown"}


def get_market_context(ticker_symbol: str, period: str = "1y") -> dict | None:
    """
    Compare the stock's performance against key market benchmarks and its sector ETF.
    Returns a dict with benchmark comparison rows and sector health summary.

    Uses yfinance — no additional API key required.  Wraps failures gracefully.
    """
    try:
        import yfinance as yf

        benchmarks = {
            "S&P 500":    "^GSPC",
            "NASDAQ 100": "^NDX",
            "Dow Jones":  "^DJI",
        }

        results = {}

        stock = yf.Ticker(ticker_symbol)
        stock_hist = stock.history(period=period, auto_adjust=True)

        if stock_hist.empty:
            print(f"get_market_context: no history for {ticker_symbol}")
            return None

        stock_close = stock_hist["Close"]

        for name, symbol in benchmarks.items():
            try:
                bench      = yf.Ticker(symbol)
                bench_hist = bench.history(period=period, auto_adjust=True)
                if bench_hist.empty:
                    continue

                bench_close = bench_hist["Close"]

                # Align by index (date)
                combined = pd.DataFrame({"stock": stock_close, "bench": bench_close}).dropna()
                if len(combined) < 2:
                    continue

                stock_ret = ((combined["stock"].iloc[-1] / combined["stock"].iloc[0]) - 1) * 100
                bench_ret = ((combined["bench"].iloc[-1] / combined["bench"].iloc[0]) - 1) * 100
                outperf   = stock_ret - bench_ret

                # YTD
                ytd_mask  = combined.index >= f"{pd.Timestamp.now().year}-01-01"
                ytd_df    = combined[ytd_mask]
                ytd_stock = ytd_bench = ytd_outperf = None
                if len(ytd_df) > 1:
                    ytd_stock  = ((ytd_df["stock"].iloc[-1] / ytd_df["stock"].iloc[0]) - 1) * 100
                    ytd_bench  = ((ytd_df["bench"].iloc[-1] / ytd_df["bench"].iloc[0]) - 1) * 100
                    ytd_outperf = ytd_stock - ytd_bench

                results[name] = {
                    "symbol":              symbol,
                    "period_return_stock": round(float(stock_ret), 2),
                    "period_return_bench": round(float(bench_ret), 2),
                    "outperformance":      round(float(outperf), 2),
                    "ytd_stock":           round(float(ytd_stock), 2)   if ytd_stock  is not None else None,
                    "ytd_bench":           round(float(ytd_bench), 2)   if ytd_bench  is not None else None,
                    "ytd_outperformance":  round(float(ytd_outperf), 2) if ytd_outperf is not None else None,
                    "outperforming":       outperf > 0,
                    "ytd_outperforming":   (ytd_outperf > 0) if ytd_outperf is not None else None,
                }
            except Exception as be:
                print(f"get_market_context benchmark error ({symbol}): {be}")
                continue

        # Sector ETF (optional extra row)
        try:
            info = stock.info or {}
            sector = info.get("sector", "")
            sector_etf = _SECTOR_ETF_MAP.get(sector)
            if sector_etf:
                etf = yf.Ticker(sector_etf)
                etf_hist = etf.history(period=period, auto_adjust=True)
                if not etf_hist.empty:
                    etf_close = etf_hist["Close"]
                    combined_etf = pd.DataFrame({"stock": stock_close, "etf": etf_close}).dropna()
                    if len(combined_etf) >= 2:
                        etf_ret   = ((combined_etf["etf"].iloc[-1]   / combined_etf["etf"].iloc[0])   - 1) * 100
                        stock_ret_etf = ((combined_etf["stock"].iloc[-1] / combined_etf["stock"].iloc[0]) - 1) * 100
                        outperf_etf = stock_ret_etf - etf_ret

                        ytd_mask = combined_etf.index >= f"{pd.Timestamp.now().year}-01-01"
                        ytd_etf_df = combined_etf[ytd_mask]
                        ytd_stock_etf = ytd_etf_v = ytd_out_etf = None
                        if len(ytd_etf_df) > 1:
                            ytd_stock_etf = ((ytd_etf_df["stock"].iloc[-1] / ytd_etf_df["stock"].iloc[0]) - 1) * 100
                            ytd_etf_v     = ((ytd_etf_df["etf"].iloc[-1]   / ytd_etf_df["etf"].iloc[0])   - 1) * 100
                            ytd_out_etf   = ytd_stock_etf - ytd_etf_v

                        results[f"Sector ETF ({sector_etf})"] = {
                            "symbol":              sector_etf,
                            "period_return_stock": round(float(stock_ret_etf), 2),
                            "period_return_bench": round(float(etf_ret), 2),
                            "outperformance":      round(float(outperf_etf), 2),
                            "ytd_stock":           round(float(ytd_stock_etf), 2) if ytd_stock_etf is not None else None,
                            "ytd_bench":           round(float(ytd_etf_v), 2)     if ytd_etf_v     is not None else None,
                            "ytd_outperformance":  round(float(ytd_out_etf), 2)   if ytd_out_etf   is not None else None,
                            "outperforming":       outperf_etf > 0,
                            "ytd_outperforming":   (ytd_out_etf > 0) if ytd_out_etf is not None else None,
                        }
        except Exception as se:
            print(f"get_market_context sector ETF error: {se}")

        # Sector health analysis
        try:
            info = stock.info or {}
            sector = info.get("sector", "")
            sector_health = get_sector_health(sector)
        except Exception:
            sector_health = {}

        if not results:
            return None

        return {
            "period":         period,
            "ticker":         ticker_symbol,
            "benchmarks":     results,
            "sector_health":  sector_health,
        }

    except Exception as e:
        print(f"get_market_context error for {ticker_symbol}: {e}")
        return None