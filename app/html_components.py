def get_currency_symbol(ticker):
    """Determine the appropriate currency symbol based on ticker suffix."""
    if not ticker:
        return "$"  # Default to USD
    
    ticker = ticker.upper()
    
    # Indian exchanges (.NS, .BO)
    if ticker.endswith('.NS') or ticker.endswith('.BO'):
        return "₹"
    
    # European exchanges
    elif ticker.endswith('.PA') or ticker.endswith('.F') or ticker.endswith('.DE') or \
         ticker.endswith('.BE') or ticker.endswith('.DU') or ticker.endswith('.HM') or \
         ticker.endswith('.HA') or ticker.endswith('.MU'):
        return "€"
    
    # UK exchanges
    elif ticker.endswith('.L'):
        return "£"
    
    # Canadian exchanges
    elif ticker.endswith('.TO') or ticker.endswith('.V'):
        return "C$"
    
    # Australian exchanges
    elif ticker.endswith('.AX'):
        return "A$"
    
    # Singapore exchanges
    elif ticker.endswith('.SI'):
        return "S$"
    
    # Nordic exchanges (using local currencies)
    elif ticker.endswith('.ST'):  # Stockholm
        return "kr"  # Swedish Krona
    elif ticker.endswith('.CO'):  # Copenhagen
        return "kr"  # Danish Krone
    elif ticker.endswith('.HE'):  # Helsinki
        return "€"   # Euro
    elif ticker.endswith('.OL'):  # Oslo
        return "kr"  # Norwegian Krone
    elif ticker.endswith('.IC'):  # Iceland
        return "kr"  # Icelandic Krona
    
    # New Zealand
    elif ticker.endswith('.NZ'):
        return "NZ$"
    
    # Default to USD for US stocks and others
    else:
        return "$"
