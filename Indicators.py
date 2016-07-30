import pandas as pd


def simple_moving_average(prices, num_days, symbol):
    """

    :param prices:
    :param num_days:
    :param symbol:
    :return:
    """
    prices["SMA"] = pd.rolling_mean(prices[symbol], num_days)
    return prices["SMA"]


def exponentially_weighted_sma(prices, num_days, symbol):
    """

    :param prices:
    :param num_days:
    :param symbol:
    :return:
    """
    prices["EMA"] = pd.ewma(prices[symbol], span=num_days, min_periods=num_days - 1)
    return prices["EMA"]


def bollinger_bands(prices, num_days, symbol):
    """

    :param prices: Pandas dataframe containing prices for a security.
    :param num_days: Number of days for Simple Moving Average.
    :param symbol: The symbol to calculate with.
    :return: Returns prices dataframe with "BB" column, with upper BB band at 1.0, lower at -1.0, and SMA at 0.0
    """
    pd.options.mode.chained_assignment = None  # default='warn'
    # Calculate num-days Simple Moving Average
    sma = pd.rolling_mean(prices[symbol], num_days)

    # Rolling Standard Deviation
    rstd = pd.rolling_std(prices[symbol], num_days)

    # Upper Bollinger Band
    ubb = sma + (2 * rstd)

    # Lower Bollinger Band
    lbb = sma - (2 * rstd)

    # Price relative to Bollinger Bands. Bands are at -1.0 and 1.0, SMA is at 0.0
    prices["BB"] = (prices[symbol] - sma) / (2 * rstd)

    return prices["BB"]


def momentum(prices, num_days, symbol):
    """

    :param prices:
    :param num_days:
    :param symbol:
    :return: Returns prices dataframe with "MM" column containing momentum indicator.
    """
    prices["MM"] = (prices[symbol] / prices[symbol].shift(num_days - 1)) - 1
    return prices["MM"]


def volatility(prices, num_days, symbol):
    """

    :param prices:
    :param num_days:
    :param symbol:
    :return:
    """
    # Daily Returns
    daily_returns = (prices[symbol] / prices[symbol].shift(1)) - 1
    # daily_returns = daily_returns[1:]
    daily_returns = daily_returns.fillna(0.0)

    # Standard Deviation
    return pd.rolling_std(daily_returns, num_days)

# Moving Average

# Exponential Moving Average

# Momentum

# Rate of Change

# Average True Range

# Bollinger Bands

# Pivot Points, Supports and Resistances

# Stochastic oscillator %K

# Stochastic oscillator %D

# Trix

# Average Directional Movement Index

# MACD, MACD Signal and MACD difference

# Mass Index

# Vortex Indicator: http://www.vortexindicator.com/VFX_VORTEX.PDF

# KST Oscillator

# Relative Strength Index

# True Strength Index

# Accumulation/Distribution

# Chaikin Oscillator

# Money Flow Index and Ratio

# On-balance Volume

# Force Index

# Ease of Movement

# Commodity Channel Index

# Coppock Curve

# Keltner Channel

# Ultimate Oscillator

# Donchian Channel

# Standard Deviation
