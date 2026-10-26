"""News relevance, trading-day dates and the short-interest date (NVDA audit, 26 Oct 2026)."""
import pandas as pd
import pytest

from data_processing_scripts.trading_sessions import market_date, range_52w
from database.data_mapper import DataMapper, issuer_mentioned

NVDA = ('NVDA', 'NVIDIA Corporation')
MARKET_WRAP_TAGS = ['NVDA', 'AAPL', 'MSFT', '^GSPC']


@pytest.mark.parametrize('title', [
    'Flagstar Financial, Inc. Q3 2026 Earnings Call Summary',
    'Preferred Bank Q3 2026 Earnings Call Summary',
    'Is Elon Musk Coming for Verizon?',
    'Will AI Fiber Demand Change Corning (GLW) Narrative',
    "Not Starlink. Not Starship. Not Grok. $561 Million of SpaceX's $4.4 Billion Profit Came From",
    'Why Comfort Systems Plunged Today',
])
def test_stories_about_other_companies_are_not_nvidia_news(title):
    # These were all stored as NVDA news because Yahoo tagged NVDA among their related tickers.
    assert not issuer_mentioned(title, None, *NVDA, related=MARKET_WRAP_TAGS)


@pytest.mark.parametrize('title', [
    'Prediction: Nvidia Will Outperform Broadcom Over the Next 5 Years',
    'NVDA stock slips as chip names fall',
    'NVIDIA Corporation to present at conference',
])
def test_stories_that_name_the_company_or_ticker_are_kept(title):
    assert issuer_mentioned(title, None, *NVDA, related=MARKET_WRAP_TAGS)


def test_generic_name_words_and_lower_case_tickers_do_not_match():
    assert not issuer_mentioned('A big week for every corporation', None, *NVDA)
    assert not issuer_mentioned('Stocks fall across all sectors', None, 'ALL', 'The Allstate Corporation')
    assert issuer_mentioned('Allstate raises its dividend', None, 'ALL', 'The Allstate Corporation')
    assert issuer_mentioned('Bank of America beats estimates', None, 'BAC', 'Bank of America Corporation')
    assert not issuer_mentioned('Preferred Bank Q3 call', None, 'BAC', 'Bank of America Corporation')


def test_a_story_tagged_only_with_this_ticker_is_kept():
    assert issuer_mentioned('Chip demand stays strong', None, *NVDA, related=['NVDA'])


def test_news_rows_carry_no_invented_relevance():
    rows = DataMapper().map_stock_news_data(19, [
        {'title': 'Nvidia raises guidance', 'link': 'https://x/1', 'providerPublishTime': 1793000000, 'relatedTickers': ['NVDA']},
        {'title': 'Preferred Bank Q3 2026 Earnings Call Summary', 'link': 'https://x/2', 'providerPublishTime': 1793000000, 'relatedTickers': MARKET_WRAP_TAGS},
    ], 'NVDA', company_name='NVIDIA Corporation')
    assert [r['title'] for r in rows] == ['Nvidia raises guidance']
    assert rows[0]['relevance_score'] is None


def test_rows_are_dated_by_the_trading_day_not_the_workers_clock():
    sessions = pd.DataFrame({'Date': pd.to_datetime(['2026-10-22', '2026-10-23'])})
    # Friday 23 Oct close, written at 21:51 UTC (03:21 on Saturday in India): the row is Friday's.
    assert market_date(sessions) == '2026-10-23'
    # The quote's own time wins: 16:00 New York on Fri 23 Oct 2026.
    assert market_date(sessions, {'market_time': int(pd.Timestamp('2026-10-23 20:00', tz='UTC').timestamp())}) == '2026-10-23'
    assert len(market_date(None)) == 10


def test_market_snapshot_row_date_is_the_price_trading_day():
    days = pd.date_range('2025-09-01', '2026-10-23', freq='D')
    frame = pd.DataFrame({'Date': days, 'Open': 1.0, 'High': 1.0, 'Low': 1.0, 'Close': 1.0, 'Volume': 1.0})
    frame.loc[frame['Date'].dt.dayofweek < 5, ['Open', 'High', 'Low', 'Close', 'Volume']] = None
    sessions_only = pd.bdate_range('2025-09-01', '2026-10-23')
    frame = frame[frame['Date'].isin(sessions_only)].reset_index(drop=True)
    frame[['Open', 'High', 'Low', 'Close']] = [[100.0 + i, 101.0 + i, 99.0 + i, 100.0 + i] for i in range(len(frame))]
    frame['Volume'] = 1000.0
    record = DataMapper().map_market_snapshot(19, {'current_price': 400.0}, {}, frame)
    assert record['date'] == '2026-10-23'


def test_52_week_range_comes_from_a_year_of_sessions():
    s = pd.DataFrame({'Date': pd.bdate_range('2025-09-01', periods=300)})
    s['High'] = [200.0 + (i % 40) for i in range(300)]
    s['Low'] = [150.0 + (i % 40) for i in range(300)]
    # NVDA: vendor kept 236.54 after new highs, and an expired low of 138.83
    assert range_52w(s, 236.54, 138.83) == (239.0, 150.0)
    assert range_52w(s.tail(50), 236.54, 138.83) == (239.0, 138.83)  # short history: vendor value, widened


def test_short_interest_row_is_dated_by_the_settlement_date():
    record = DataMapper().map_ownership_data(19, {'dateShortInterest': 1790726400, 'sharesShort': 311389688})
    assert record['report_date'] == '2026-09-30'
