#!/usr/bin/env python3
"""
Create and Seed Monitored Tickers Table
======================================
Utility script to create the 'monitored_tickers' table in the database
and seed it with symbols from 'stock_tickers_list.json'.
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

def main():
    logger.info("Connecting to database...")
    try:
        db = AzurePostgresClient()
        logger.info("✓ Connected successfully.")
        
        # Create table DDL
        create_sql = """
        CREATE TABLE IF NOT EXISTS monitored_tickers (
            symbol VARCHAR(10) PRIMARY KEY,
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
        logger.info("Creating 'monitored_tickers' table if it does not exist...")
        with db.connection.cursor() as cursor:
            cursor.execute(create_sql)
            db.connection.commit()
        logger.info("✓ Table created or verified successfully.")

        # Seed with initial tickers from JSON
        json_path = Path(__file__).parent / "stock_tickers_list.json"
        tickers = []
        if json_path.exists():
            try:
                import json
                with open(json_path, "r") as f:
                    data = json.load(f)
                    tickers = data.get("tickers", [])
                logger.info(f"Found {len(tickers)} tickers in stock_tickers_list.json for seeding.")
            except Exception as e:
                logger.warning(f"Could not parse stock_tickers_list.json: {e}")
        
        if not tickers:
            tickers = ["AAPL", "MSFT", "TSLA", "NVDA", "AMZN", "IREN"]
            logger.info("Using default fallback ticker list for seeding.")
            
        logger.info(f"Seeding monitored_tickers table with {len(tickers)} tickers...")
        
        inserted = 0
        with db.connection.cursor() as cursor:
            for t in tickers:
                symbol = t.upper().strip()
                if not symbol:
                    continue
                cursor.execute("""
                    INSERT INTO monitored_tickers (symbol, is_active)
                    VALUES (%s, true)
                    ON CONFLICT (symbol) DO NOTHING;
                """, (symbol,))
                inserted += cursor.rowcount
            db.connection.commit()
        
        logger.info(f"✓ Seeding completed. {inserted} new tickers inserted into 'monitored_tickers'.")
        
        # Print status of active tickers
        with db.connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM monitored_tickers WHERE is_active = true;")
            active_count = cursor.fetchone()[0]
            logger.info(f"Total active tickers in DB: {active_count}")
            
    except Exception as e:
        logger.error(f"❌ Failed to setup monitored_tickers table: {e}")
        logger.exception("Traceback:")
        sys.exit(1)

if __name__ == "__main__":
    main()
