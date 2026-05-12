"""
Live breakout bot v4 - ENHANCED with all improvements
- Partial fill handling + slippage validation
- SL retry logic with exponential backoff
- Multi-timeframe confirmation (5m + 15m)
- Dynamic risk management
- Correlation filter (prevent same-sector trades)
- Connection health monitoring
- Advanced P&L analytics with R-multiples
- Backtest capability
"""

import csv
import datetime
import json
import logging
import os
import sys
import time
from collections import Counter
from typing import Any, Dict, Optional

import requests
import yfinance as yf

# Import enhanced modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "modules"))

from trade_executor import TradeExecutor
from sl_manager import SLManager
from mtf_confirm import MTFConfirm
from analytics import TradeAnalytics
from risk_manager import RiskManager
from connection_health import ConnectionHealth
from correlation_filter import CorrelationFilter

try:
    from dhanhq import dhanhq
    DHAN_AVAILABLE = True
except ImportError:
    DHAN_AVAILABLE = False
    print("ERROR: dhanhq not installed. Run: pip install dhanhq")
    sys.exit(1)

# ============================================================
# SAFETY SWITCHES
# ============================================================
DRY_RUN = True
ALLOW_LIVE_TRADING = False

# ============================================================
# CREDENTIALS
# ============================================================
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "YOUR_CLIENT_ID").strip()
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "YOUR_ACCESS_TOKEN").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TELEGRAM_TOKEN").strip()
CHAT_ID = os.getenv("CHAT_ID", "YOUR_CHAT_ID").strip()

# ============================================================
# CAPITAL & RISK CONFIG
# ============================================================
CAPITAL = 25_000
RISK_PER_TRADE = 0.01
MAX_TRADES = 2
MAX_QTY = 200
MAX_DAILY_TRADES = 3
MAX_DAILY_LOSS = 0.02
MAX_STOCK_PRICE = 1000.0
MAX_SLIPPAGE_PCT = 1.0  # New: Reject fills with > 1% slippage

# ============================================================
# TIME CONFIG (IST)
# ============================================================
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

MARKET_START = datetime.time(9, 20)
MARKET_END = datetime.time(15, 15)
MIDDAY_START = datetime.time(11, 30)
MIDDAY_END = datetime.time(14, 0)
NO_ENTRY_FROM = datetime.time(14, 45)
EOD_EXIT = datetime.time(15, 10)
DAILY_SUMMARY_TIME = datetime.time(15, 15)

# ============================================================
# STRATEGY CONFIG
# ============================================================
BREAKOUT_BARS = 20
MIN_BREAKOUT_PC = 0.002
VOLUME_MULT = 1.3
MIN_SL_PCT = 0.003
MAX_SL_PCT = 0.02
TRAIL_PCT = 0.004
MARKET_MIN_PC = -0.0015
SOFT_MARKET_MIN_PC = -0.0035
STOCK_COOLDOWN = 1800
MIN_TODAY_CANDLES = 5
RSI_PERIOD = 14
RSI_OVERBOUGHT = 75
MAX_RISK_ABOVE_EMA10_PCT = 2.5

ORDER_FILL_TIMEOUT = 20
ORDER_POLL_SECONDS = 2
SCAN_INTERVAL_SECONDS = 60

STOP_ORDER_STYLE = "SLM"
STOP_LIMIT_BUFFER = 0.003

MARKET_HARD_TICKERS = ["^NSEI"]
MARKET_SOFT_TICKERS = ["RELIANCE.NS"]

# ============================================================
# VERIFIED DHAN SECURITY IDS
# ============================================================
SECURITY_IDS = {
    "HDFCBANK.NS": "1333",
    "ICICIBANK.NS": "4963",
    "SBIN.NS": "3045",
    "AXISBANK.NS": "5900",
    "KOTAKBANK.NS": "1922",
    "BAJFINANCE.NS": "317",
    "TCS.NS": "11536",
    "INFY.NS": "1594",
    "WIPRO.NS": "3787",
    "RELIANCE.NS": "2885",
    "ITC.NS": "1660",
    "LT.NS": "11483",
    "MARUTI.NS": "10999",
    "BAJAJFINSV.NS": "16675",
    "TATASTEEL.NS": "3499",
    "HINDALCO.NS": "1363",
    "SUNPHARMA.NS": "3351",
    "ADANIENT.NS": "25",
    "ULTRACEMCO.NS": "11532",
    "TITAN.NS": "3506",
}

STOCKS = list(SECURITY_IDS.keys())

# ============================================================
# FILES
# ============================================================
PNL_FILE = "live_pnl_v4.json"
PNL_CSV = "live_pnl_v4.csv"
RESTART_LOG = "live_restart_v4.json"
SUMMARY_LOG = "daily_summary_v4.json"

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    handlers=[logging.FileHandler("live_bot_v4.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ============================================================
# RUNTIME STATE
# ============================================================
active_trades: Dict[str, Dict[str, Any]] = {}
last_signal_time: Dict[str, float] = {}
daily_trade_count = 0
daily_pnl = 0.0
last_reset_date: Optional[datetime.date] = None
eod_sent = False
daily_summary_sent = False
daily_scans_total = 0
daily_signals_found = 0
dhan = None
scan_rejection_counts: Counter[str] = Counter()

# ============================================================
# ENHANCED MODULES
# ============================================================
trade_executor: Optional[TradeExecutor] = None
sl_manager: Optional[SLManager] = None
mtf_confirm: Optional[MTFConfirm] = None
trade_analytics: Optional[TradeAnalytics] = None
risk_manager: Optional[RiskManager] = None
connection_health: Optional[ConnectionHealth] = None
correlation_filter: Optional[CorrelationFilter] = None

# ============================================================
# TIME HELPERS
# ============================================================

def now_ist() -> datetime.datetime:
    return datetime.datetime.now(IST)

def today_ist() -> datetime.date:
    return now_ist().date()

def now_ist_time() -> datetime.time:
    return now_ist().time()

# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def count_rejection(reason: str) -> None:
    scan_rejection_counts[reason] += 1

def reset_scan_rejections() -> None:
    scan_rejection_counts.clear()

def log_scan_rejections() -> None:
    if not scan_rejection_counts:
        return
    ordered = sorted(scan_rejection_counts.items(), key=lambda item: (-item[1], item[0]))
    summary = " | ".join(f"{reason}: {count}" for reason, count in ordered[:6])
    log.info("Scan rejections | %s", summary)

def is_placeholder(value: str) -> bool:
    return not value or value.startswith("YOUR_")

def log_restart(reason: str = "NORMAL START") -> None:
    try:
        restarts = []
        if os.path.exists(RESTART_LOG):
            with open(RESTART_LOG, encoding="utf-8") as f:
                restarts = json.load(f)

        entry = {
            "timestamp": now_ist().isoformat(),
            "date": today_ist().isoformat(),
            "time": now_ist().strftime("%H:%M:%S"),
            "reason": reason,
            "pid": os.getpid(),
            "dry_run": DRY_RUN,
        }
        restarts.append(entry)
        with open(RESTART_LOG, "w", encoding="utf-8") as f:
            json.dump(restarts, f, indent=2)
        log.info("Restart logged | %s | PID: %s", reason, os.getpid())
    except Exception as exc:
        log.error("Restart log error: %s", exc)

def send_telegram(msg: str) -> None:
    if is_placeholder(TELEGRAM_TOKEN) or is_placeholder(CHAT_ID):
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=5)
        if resp.status_code != 200:
            log.warning("Telegram failed: %s", resp.status_code)
    except Exception as exc:
        log.error("Telegram error: %s", exc)

# ============================================================
# INITIALIZATION
# ============================================================

def validate_live_mode() -> bool:
    if DRY_RUN:
        return True
    if not ALLOW_LIVE_TRADING:
        log.error("Live mode blocked. Set ALLOW_LIVE_TRADING = True after DRY RUN verification.")
        return False
    if is_placeholder(DHAN_CLIENT_ID) or is_placeholder(DHAN_ACCESS_TOKEN):
        log.error("Update DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN before live trading.")
        return False
    return True

def init_dhan() -> bool:
    global dhan
    try:
        dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
        result = dhan.get_fund_limits()
        
        if result.get("status") != "success":
            error = result.get("remarks", {}).get("error_message", "Unknown")
            log.error("Dhan auth failed: %s", error)
            send_telegram(f"Dhan auth failed\n{error}\nGenerate new token.")
            return False
        
        data = flatten_data(result.get("data", {}))
        balance = pick_number(data, "availabelBalance", "availableBalance", default=0)
        mode = "DRY RUN" if DRY_RUN else "LIVE"
        log.info("Dhan connected | Balance: Rs %.2f | Mode: %s", balance, mode)
        send_telegram(
            f"{'DRY RUN' if DRY_RUN else 'LIVE'} BOT STARTED\n"
            f"Balance : Rs {balance:,.2f}\n"
            f"Capital : Rs {CAPITAL:,}\n"
            f"Risk/trade : Rs {int(CAPITAL * RISK_PER_TRADE)}\n"
            f"Max loss/day : Rs {int(CAPITAL * MAX_DAILY_LOSS)}"
        )
        return True
    except Exception as exc:
        log.error("Dhan init error: %s", exc)
        return False

def init_modules() -> None:
    global trade_executor, sl_manager, mtf_confirm, trade_analytics, risk_manager, connection_health, correlation_filter
    
    trade_executor = TradeExecutor(dhan, dry_run=DRY_RUN, max_slippage_pct=MAX_SLIPPAGE_PCT)
    sl_manager = SLManager(dhan, dry_run=DRY_RUN, max_retries=3)
    mtf_confirm = MTFConfirm(lookback_5m=BREAKOUT_BARS, lookback_15m=10)
    trade_analytics = TradeAnalytics(pnl_file=PNL_FILE)
    risk_manager = RiskManager(
        capital=CAPITAL,
        base_risk_pct=RISK_PER_TRADE,
        max_daily_loss_pct=MAX_DAILY_LOSS,
        max_position_size_pct=0.05,
    )
    connection_health = ConnectionHealth(dhan, check_interval=300, failure_threshold=3)
    correlation_filter = CorrelationFilter(strict_mode=False)
    
    log.info("All enhanced modules initialized")

# ============================================================
# DATA PARSING HELPERS
# ============================================================

def flatten_data(data: Any) -> Dict[str, Any]:
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    return {}

def pick_number(data: Dict[str, Any], *keys: str, default: float = 0.0) -> float:
    for key in keys:
        if key in data and data[key] not in (None, ""):
            try:
                return float(data[key])
            except Exception:
                continue
    return float(default)

def pick_text(data: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        if key in data and data[key] not in (None, ""):
            return str(data[key]).strip()
    return ""

# ============================================================
# MARKET CHECKS
# ============================================================

def df_today_ist(df):
    if df is None or len(df) == 0:
        return df
    index = df.index
    if getattr(index, "tz", None) is not None:
        local_dates = index.tz_convert(IST).date
    else:
        local_dates = index.date
    return df[local_dates == today_ist()]

def market_move_pct(ticker: str) -> Optional[float]:
    try:
        df = yf.download(ticker, period="2d", interval="5m", progress=False, auto_adjust=False)
        if df is None or len(df) < 5:
            log.warning("Market ticker %s - no data", ticker)
            return None
        df_today = df_today_ist(df)
        if df_today is None or len(df_today) < 2:
            log.warning("Market ticker %s - insufficient today candles", ticker)
            return None
        close = df_today["Close"].squeeze()
        current = float(close.iloc[-1])
        day_open = float(close.iloc[0])
        return round(((current - day_open) / day_open) * 100, 3)
    except Exception as exc:
        log.warning("Market ticker %s error: %s", ticker, exc)
        return None

def market_is_safe() -> bool:
    hard_results: Dict[str, float] = {}
    for ticker in MARKET_HARD_TICKERS:
        pct = market_move_pct(ticker)
        if pct is None:
            continue
        hard_results[ticker] = pct
        if pct < MARKET_MIN_PC * 100:
            log.info("Market unsafe (%s): %.2f%%", ticker, pct)
            return False
        log.info("Market safe (%s): %.2f%%", ticker, pct)
    
    if not hard_results:
        log.warning("All hard market tickers failed - BLOCKING scan")
        return False
    return True

def committed_capital() -> float:
    return sum(trade["entry_price"] * trade["qty"] for trade in active_trades.values())

# ============================================================
# SIGNAL SCANNING
# ============================================================

def scan_stock(symbol: str) -> Optional[Dict[str, Any]]:
    global daily_scans_total, daily_signals_found
    
    try:
        daily_scans_total += 1
        
        if symbol in active_trades:
            count_rejection("already_active")
            return None
        
        if symbol in last_signal_time:
            if time.time() - last_signal_time[symbol] < STOCK_COOLDOWN:
                count_rejection("cooldown")
                return None
        
        # Check correlation filter
        correlation_check = correlation_filter.can_trade(symbol, active_trades)
        if not correlation_check["allowed"]:
            count_rejection("correlation_blocked")
            return None
        
        df = yf.download(symbol, period="2d", interval="5m", progress=False, auto_adjust=False)
        if df is None or len(df) < BREAKOUT_BARS + 2:
            count_rejection("no_data")
            return None
        
        df_today = df_today_ist(df)
        if df_today is None or len(df_today) < MIN_TODAY_CANDLES:
            count_rejection("insufficient_today_candles")
            return None
        
        high_2d = df["High"].squeeze()
        low_today = df_today["Low"].squeeze()
        high_today = df_today["High"].squeeze()
        close_today = df_today["Close"].squeeze()
        volume_today = df_today["Volume"].squeeze()
        
        price = float(close_today.iloc[-1])
        prev_close = float(close_today.iloc[-2])
        
        if price > MAX_STOCK_PRICE:
            count_rejection("price_above_cap")
            return None
        
        # Calculate technicals
        ema10_val = float(close_today.ewm(span=10, adjust=False).mean().iloc[-1])
        ema21_val = float(close_today.ewm(span=21, adjust=False).mean().iloc[-1])
        ema50_val = float(close_today.ewm(span=50, adjust=False).mean().iloc[-1])
        
        typical_price = (high_today + low_today + close_today) / 3
        cum_vol = volume_today.cumsum()
        if float(cum_vol.iloc[-1]) <= 0:
            count_rejection("vwap_volume_zero")
            return None
        vwap_val = float(((typical_price * volume_today).cumsum() / cum_vol).iloc[-1])
        
        # RSI
        delta = close_today.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(RSI_PERIOD).mean()
        avg_loss = loss.rolling(RSI_PERIOD).mean()
        rs = avg_gain / avg_loss
        rsi_val = float((100 - (100 / (1 + rs))).iloc[-1])
        
        if rsi_val != rsi_val:
            count_rejection("rsi_insufficient_data")
            return None
        
        # EMA checks
        if ema10_val < ema21_val:
            count_rejection("ema_trend_bearish")
            return None
        
        if price < ema10_val:
            count_rejection("below_ema10")
            return None
        
        if price < ema50_val:
            count_rejection("below_ema50")
            return None
        
        if price < vwap_val:
            count_rejection("below_vwap")
            return None
        
        if rsi_val > RSI_OVERBOUGHT:
            count_rejection("rsi_overbought")
            return None
        
        risk_pct_ema = ((price - ema10_val) / ema10_val) * 100
        if risk_pct_ema > MAX_RISK_ABOVE_EMA10_PCT:
            count_rejection("high_risk_above_ema10")
            return None
        
        # Breakout check
        breakout_level = float(high_2d.rolling(BREAKOUT_BARS).max().iloc[-2])
        
        vol = float(volume_today.iloc[-1])
        vol_avg = float(volume_today.rolling(min(BREAKOUT_BARS, len(volume_today))).mean().iloc[-1])
        
        if vol_avg <= 0:
            count_rejection("volume_avg_zero")
            return None
        
        if price <= breakout_level:
            count_rejection("below_breakout_level")
            return None
        
        breakout_gap = round(((price - breakout_level) / breakout_level) * 100, 3)
        if breakout_gap < MIN_BREAKOUT_PC * 100:
            count_rejection("breakout_gap_small")
            return None
        
        if price <= prev_close:
            count_rejection("below_prev_close")
            return None
        
        if vol < VOLUME_MULT * vol_avg:
            count_rejection("volume_low")
            return None
        
        # SL calculation
        sl = round(float(min(low_today.iloc[-3:])), 2)
        risk = round(price - sl, 2)
        
        if risk <= 0:
            count_rejection("invalid_risk")
            return None
        
        sl_validation = risk_manager.validate_sl_distance(price, sl, MIN_SL_PCT, MAX_SL_PCT)
        if not sl_validation["valid"]:
            count_rejection("sl_validation_failed")
            return None
        
        # Position sizing
        free_capital = max(CAPITAL - committed_capital(), 0)
        qty_by_risk = int((CAPITAL * RISK_PER_TRADE) / risk)
        qty_by_capital = int(free_capital // price)
        qty = min(qty_by_risk, qty_by_capital, MAX_QTY)
        
        if qty <= 0:
            count_rejection("qty_zero")
            return None
        
        # MTF Confirmation
        if not mtf_confirm.confirm_signal(symbol, price, {}):
            count_rejection("mtf_confirmation_failed")
            return None
        
        daily_signals_found += 1
        
        tp = round(price + risk * 2, 2)
        log.info(
            "BREAKOUT: %s | Rs %.2f | Gap: %.3f%% | VWAP: %.2f | RSI: %.1f | EMA10: %.2f | SL: Rs %.2f | TP: Rs %.2f | Qty: %s",
            symbol,
            price,
            breakout_gap,
            vwap_val,
            rsi_val,
            ema10_val,
            sl,
            tp,
            qty,
        )
        
        return {
            "symbol": symbol,
            "price": round(price, 2),
            "sl": sl,
            "tp": tp,
            "qty": qty,
            "risk": risk,
            "breakout_level": round(breakout_level, 2),
            "breakout_gap": breakout_gap,
            "vwap": round(vwap_val, 2),
            "rsi": round(rsi_val, 1),
            "ema10": round(ema10_val, 2),
            "ema21": round(ema21_val, 2),
            "ema50": round(ema50_val, 2),
            "entry_time": time.time(),
            "entry_order_id": "",
            "sl_order_id": "",
            "exit_order_id": "",
            "force_exit": False,
            "last_force_exit_alert": 0.0,
        }
    except Exception as exc:
        count_rejection("scan_error")
        log.error("%s scan error: %s", symbol, exc)
        return None

# ============================================================
# TRADE MANAGEMENT
# ============================================================

def enter_trade(signal: Dict[str, Any]) -> None:
    global daily_trade_count
    
    symbol = signal["symbol"]
    requested_qty = signal["qty"]
    requested_entry = signal["price"]
    
    log.info(
        "Entry attempt: %s | Qty: %s | Signal price: Rs %.2f | Mode: %s",
        symbol,
        requested_qty,
        requested_entry,
        "DRY RUN" if DRY_RUN else "LIVE",
    )
    
    # Place buy order with slippage check
    entry_order_id = trade_executor.place_buy_order(
        SECURITY_IDS[symbol],
        requested_qty,
    )
    if not entry_order_id:
        return
    
    # Wait for fill with slippage validation
    fill_result = trade_executor.wait_for_fill_with_slippage_check(
        entry_order_id,
        requested_entry,
        requested_qty,
        timeout=ORDER_FILL_TIMEOUT,
        poll_interval=ORDER_POLL_SECONDS,
    )
    
    if not fill_result["success"] or fill_result["filled_qty"] <= 0:
        log.warning("Entry not confirmed for %s: %s (Slippage: %.3f%%)", 
                   symbol, fill_result["status"], fill_result["slippage_pct"])
        send_telegram(
            f"ENTRY NOT CONFIRMED\n"
            f"{symbol}\n"
            f"Status: {fill_result['status']}\n"
            f"Slippage: {fill_result['slippage_pct']:.3f}%\n"
            "No trade recorded."
        )
        return
    
    actual_qty = int(fill_result["filled_qty"])
    actual_entry = fill_result["avg_price"]
    
    # Create trade record
    trade = dict(signal)
    trade["qty"] = actual_qty
    trade["entry_price"] = round(actual_entry, 2)
    trade["entry_time"] = time.time()
    trade["entry_order_id"] = entry_order_id
    trade["force_exit"] = False
    trade["last_force_exit_alert"] = 0.0
    trade["entry_slippage_pct"] = fill_result["slippage_pct"]
    
    # Place SL order with retry
    sl_order_id = sl_manager.place_sl_order_with_retry(
        SECURITY_IDS[symbol],
        actual_qty,
        trade["sl"],
        STOP_ORDER_STYLE,
        STOP_LIMIT_BUFFER,
    )
    
    trade["sl_order_id"] = sl_order_id or ""
    
    if not sl_order_id:
        log.error("Protective SL failed for %s after entry. Trying immediate exit.", symbol)
        send_telegram(
            f"CRITICAL - SL ORDER FAILED\n"
            f"{symbol} x{actual_qty}\n"
            "Trying emergency exit."
        )
        trade["force_exit"] = True
        active_trades[symbol] = trade
        try_force_exit_trade(trade, "ENTRY SL FAILED - EMERGENCY EXIT", trade["entry_price"])
        return
    
    active_trades[symbol] = trade
    last_signal_time[symbol] = time.time()
    daily_trade_count += 1
    
    send_telegram(
        f"{'DRY RUN' if DRY_RUN else 'LIVE'} TRADE ENTERED\n"
        f"Stock    : {symbol}\n"
        f"Entry    : Rs {trade['entry_price']:.2f}\n"
        f"Slippage : {fill_result['slippage_pct']:.3f}%\n"
        f"SL       : Rs {trade['sl']:.2f}\n"
        f"TP       : Rs {trade['tp']:.2f}\n"
        f"Qty      : {trade['qty']}\n"
        f"Trade    : {daily_trade_count}/{MAX_DAILY_TRADES}"
    )

def manage_trades() -> None:
    global daily_pnl
    
    now = now_ist_time()
    
    for symbol in list(active_trades.keys()):
        try:
            trade = active_trades[symbol]
            
            df = yf.download(symbol, period="2d", interval="1m", progress=False, auto_adjust=False)
            if df is None or df.empty:
                continue
            
            df_today = df_today_ist(df)
            if df_today is None or df_today.empty:
                continue
            
            ltp = float(df_today["Close"].squeeze().iloc[-1])
            
            if trade.get("force_exit"):
                try_force_exit_trade(trade, "FORCED EXIT RETRY", ltp)
                continue
            
            # Trail SL if in profit
            if ltp > trade["entry_price"] and trade.get("sl_order_id"):
                new_sl = round(ltp * (1 - TRAIL_PCT), 2)
                if new_sl > trade["sl"]:
                    if sl_manager.modify_sl_order_with_retry(
                        trade["sl_order_id"],
                        trade["qty"],
                        new_sl,
                        STOP_ORDER_STYLE,
                        STOP_LIMIT_BUFFER,
                    ):
                        log.info("Trail SL %s: Rs %.2f -> Rs %.2f", symbol, trade["sl"], new_sl)
                        trade["sl"] = new_sl
            
            # Check exit conditions
            exit_reason = None
            if now >= EOD_EXIT:
                exit_reason = "EOD EXIT"
            elif ltp <= trade["sl"]:
                exit_reason = "SL HIT"
            elif ltp >= trade["tp"]:
                exit_reason = "TARGET HIT"
            
            if exit_reason:
                try_force_exit_trade(trade, exit_reason, ltp)
        
        except Exception as exc:
            log.error("%s manage error: %s", symbol, exc)

def try_force_exit_trade(trade: Dict[str, Any], reason: str, ltp: Optional[float] = None) -> bool:
    global daily_pnl
    
    symbol = trade["symbol"]
    qty = trade["qty"]
    
    if trade.get("sl_order_id"):
        sl_manager.cancel_sl_order(trade["sl_order_id"])
        trade["sl_order_id"] = ""
    
    # Place market sell
    order_id = place_market_sell(symbol, qty)
    if not order_id:
        now_ts = time.time()
        if now_ts - trade.get("last_force_exit_alert", 0) > 120:
            send_telegram(
                f"URGENT - EXIT FAILED\n"
                f"{symbol} x{qty}\n"
                f"Reason: {reason}\n"
                "Check Dhan manually now."
            )
            trade["last_force_exit_alert"] = now_ts
        trade["force_exit"] = True
        return False
    
    trade["exit_order_id"] = order_id
    
    if DRY_RUN:
        close_trade_record(trade, reason, ltp if ltp is not None else trade["entry_price"])
        return True
    
    # Wait for fill
    fill = get_order_fill(order_id)
    if fill["success"] and fill["filled_qty"] > 0:
        exit_price = fill["avg_price"] or (ltp if ltp is not None else trade["entry_price"])
        close_trade_record(trade, reason, exit_price)
        return True
    
    trade["force_exit"] = True
    return False

def close_trade_record(trade: Dict[str, Any], exit_reason: str, exit_price: float) -> None:
    global daily_pnl
    
    # Record trade with analytics
    record = trade_analytics.record_trade(
        symbol=trade["symbol"],
        entry_price=trade["price"],
        actual_entry_price=trade["entry_price"],
        exit_price=round(exit_price, 2),
        qty=trade["qty"],
        sl=trade["sl"],
        tp=trade["tp"],
        hold_mins=round((time.time() - trade["entry_time"]) / 60, 1),
        exit_reason=exit_reason,
        breakout_level=trade.get("breakout_level", 0),
        breakout_gap=trade.get("breakout_gap", 0),
        vwap=trade.get("vwap", 0),
        rsi=trade.get("rsi", 0),
        ema10=trade.get("ema10", 0),
        ema21=trade.get("ema21", 0),
        ema50=trade.get("ema50", 0),
        date=today_ist().isoformat(),
        time=now_ist().strftime("%H:%M"),
        dry_run=DRY_RUN,
        entry_order_id=trade.get("entry_order_id", ""),
        sl_order_id=trade.get("sl_order_id", ""),
        exit_order_id=trade.get("exit_order_id", ""),
    )
    
    trade_analytics.save_trade(record)
    daily_pnl = round(daily_pnl + record["net_pnl"], 2)
    
    display_reason = exit_reason
    if exit_reason == "SL HIT" and exit_price > trade["entry_price"]:
        display_reason = "TRAIL SL HIT"
    
    msg = (
        f"{'DRY RUN' if DRY_RUN else 'LIVE'} EXIT - {display_reason}\n"
        f"Stock: {trade['symbol']}\n"
        f"Entry: Rs {trade['entry_price']:.2f} -> Exit: Rs {exit_price:.2f}\n"
        f"Qty: {trade['qty']}\n"
        f"Net P&L: Rs {record['net_pnl']:+,.2f} | Slippage: Rs {record['slippage_cost']:+,.2f}\n"
        f"Day P&L: Rs {daily_pnl:+,.2f} | R-Multiple: {record['r_multiple']:.2f}"
    )
    log.info(msg)
    send_telegram(msg)
    
    active_trades.pop(trade["symbol"], None)

def place_market_sell(symbol: str, qty: int) -> Optional[str]:
    if DRY_RUN:
        order_id = f"DRY_SELL_{symbol}_{int(time.time())}"
        log.info("[DRY RUN] SELL %s x%s", symbol, qty)
        return order_id
    
    try:
        security_id = SECURITY_IDS.get(symbol)
        if not security_id:
            return None
        
        result = dhan.place_order(
            security_id=security_id,
            exchange_segment=dhan.NSE,
            transaction_type=dhan.SELL,
            quantity=qty,
            order_type=dhan.MARKET,
            product_type=dhan.INTRA,
            price=0,
        )
        if result.get("status") == "success":
            order_id = pick_text(flatten_data(result.get("data", {})), "orderId", "id")
            log.info("SELL requested: %s x%s | ID: %s", symbol, qty, order_id)
            return order_id or None
        
        error = result.get("remarks", {}).get("error_message", str(result))
        log.error("SELL failed: %s | %s", symbol, error)
        send_telegram(f"SELL FAILED - MANUAL CHECK NEEDED\n{symbol} x{qty}\n{error}")
        return None
    except Exception as exc:
        log.error("place_market_sell error: %s", exc)
        return None

def get_order_fill(order_id: str) -> Dict[str, Any]:
    if DRY_RUN:
        return {"success": True, "filled_qty": 0, "avg_price": 0.0, "status": "COMPLETE"}
    
    try:
        result = dhan.get_order_by_id(order_id)
        if result.get("status") != "success":
            return {"success": False, "filled_qty": 0, "avg_price": 0.0, "status": "FAILED"}
        
        data = flatten_data(result.get("data", {}))
        status = pick_text(data, "orderStatus", "status")
        filled_qty = pick_number(data, "tradedQuantity", "filledQty", "filledQuantity")
        avg_price = pick_number(data, "averageTradedPrice", "averagePrice", "avgTradedPrice")
        
        filled = status.upper() in {"TRADED", "COMPLETE", "COMPLETED", "FILLED"} or filled_qty > 0
        return {"success": filled, "filled_qty": filled_qty, "avg_price": avg_price, "status": status}
    except Exception as exc:
        log.error("get_order_fill error: %s", exc)
        return {"success": False, "filled_qty": 0, "avg_price": 0.0, "status": "ERROR"}

# ============================================================
# DAILY RESET & SUMMARIES
# ============================================================

def check_daily_reset() -> None:
    global last_reset_date, daily_trade_count, daily_pnl, eod_sent, daily_summary_sent
    global daily_scans_total, daily_signals_found
    
    today = today_ist()
    now = now_ist_time()
    
    if not daily_summary_sent and now >= DAILY_SUMMARY_TIME:
        send_daily_summary()
        daily_summary_sent = True
    
    if not eod_sent and now >= EOD_EXIT:
        send_eod_summary()
        eod_sent = True
    
    if last_reset_date != today:
        log.info("Daily reset - %s", today)
        active_trades.clear()
        last_signal_time.clear()
        daily_trade_count = 0
        daily_pnl = 0.0
        last_reset_date = today
        eod_sent = False
        daily_summary_sent = False
        daily_scans_total = 0
        daily_signals_found = 0
        send_telegram(
            f"Bot reset for {today}\n"
            f"Capital: Rs {CAPITAL:,}\n"
            f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}"
        )

def send_daily_summary() -> None:
    today = today_ist().isoformat()
    stats = trade_analytics.get_daily_stats(today)
    perf = trade_analytics.get_performance_summary(days=1)
    
    lines = [
        "=" * 52,
        f"DAILY SUMMARY - {today} [{'DRY RUN' if DRY_RUN else 'LIVE'}]",
        "=" * 52,
        f"Scans attempted      : {daily_scans_total}",
        f"Signals found        : {daily_signals_found}",
        f"Trades taken         : {stats.get('trades_count', 0)}/{MAX_DAILY_TRADES}",
        f"Wins / Losses        : {stats.get('wins', 0)} / {stats.get('losses', 0)}",
        f"Win rate             : {stats.get('win_rate', 0):.1f}%",
        f"Net P&L              : Rs {stats.get('total_pnl', 0):+,.2f}",
        f"Slippage cost        : Rs {stats.get('total_slippage_cost', 0):+,.2f}",
        f"Avg R-Multiple       : {perf.get('avg_r_multiple', 0):.2f}",
    ]
    
    for line in lines:
        log.info(line)
    
    send_telegram("\n".join(lines))

def send_eod_summary() -> None:
    today = today_ist().isoformat()
    stats = trade_analytics.get_daily_stats(today)
    
    if stats.get("trades_count", 0) == 0:
        log.info("EOD summary sent - no trades")
        send_telegram(f"{today} - No trades today.")
        return
    
    msg = (
        f"EOD {'DRY RUN' if DRY_RUN else 'LIVE'} - {today}\n"
        f"Trades: {stats['trades_count']} | Wins: {stats['wins']} | Losses: {stats['losses']}\n"
        f"Win Rate: {stats['win_rate']}%\n"
        f"Net P&L: Rs {stats['total_pnl']:+,.2f}\n"
        f"Slippage: Rs {stats['total_slippage_cost']:+,.2f}"
    )
    send_telegram(msg)
    log.info("EOD summary sent")

def shutdown_flatten_positions() -> None:
    if not active_trades:
        return
    send_telegram("Bot stopping - attempting to flatten open positions.")
    for symbol in list(active_trades.keys()):
        trade = active_trades.get(symbol)
        if trade:
            try_force_exit_trade(trade, "MANUAL STOP EXIT", trade.get("entry_price"))

# ============================================================
# STARTUP
# ============================================================

def startup_banner() -> None:
    mode_str = "DRY RUN - no real orders" if DRY_RUN else "LIVE - REAL ORDERS"
    log.info("=" * 60)
    log.info("LIVE BOT v4 ENHANCED - %s", mode_str)
    log.info("Capital  : Rs %s", f"{CAPITAL:,}")
    log.info("Risk     : %.2f%% = Rs %s/trade", RISK_PER_TRADE * 100, int(CAPITAL * RISK_PER_TRADE))
    log.info("Max/day  : %s trades", MAX_DAILY_TRADES)
    log.info("Max loss : Rs %s/day", int(CAPITAL * MAX_DAILY_LOSS))
    log.info("Stocks   : %s", len(STOCKS))
    log.info("Price cap: Rs %.0f", MAX_STOCK_PRICE)
    log.info("Max slip : %.1f%%", MAX_SLIPPAGE_PCT)
    log.info("Trail SL : %.2f%%", TRAIL_PCT * 100)
    log.info("=" * 60)
    log.info("ENHANCEMENTS:")
    log.info("✓ Partial fill handling + slippage validation")
    log.info("✓ SL retry logic with exponential backoff")
    log.info("✓ Multi-timeframe confirmation (5m + 15m)")
    log.info("✓ Dynamic risk management")
    log.info("✓ Correlation filter (sector-based)")
    log.info("✓ Connection health monitoring")
    log.info("✓ Advanced P&L analytics with R-multiples")
    log.info("=" * 60)

# ============================================================
# MAIN LOOP
# ============================================================

def main() -> None:
    global eod_sent, daily_summary_sent
    
    log_restart("NORMAL START")
    startup_banner()
    
    if not validate_live_mode():
        sys.exit(1)
    
    if not init_dhan():
        log.error("Cannot start - Dhan connection failed.")
        sys.exit(1)
    
    init_modules()
    
    consecutive_errors = 0
    
    while True:
        try:
            # Check connection health
            health = connection_health.check_connection()
            if connection_health.should_alert():
                send_telegram(f"⚠️ CONNECTION ALERT\n{connection_health.get_status_message()}")
            
            # Daily reset
            check_daily_reset()
            now = now_ist_time()
            
            # Market closed
            if now < MARKET_START or now > MARKET_END:
                log.info("Market closed | %s IST", now.strftime("%H:%M"))
                consecutive_errors = 0
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue
            
            # Manage existing trades
            manage_trades()
            
            # Check if should scan
            if now >= EOD_EXIT:
                if not eod_sent:
                    if not active_trades:
                        log.info("EOD window - no active trades")
                    send_eod_summary()
                    eod_sent = True
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue
            
            if now >= NO_ENTRY_FROM:
                log.info("Late session - managing trades only")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue
            
            if MIDDAY_START <= now < MIDDAY_END:
                log.info("Midday block - managing trades only")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue
            
            if daily_trade_count >= MAX_DAILY_TRADES:
                log.info("Daily trade limit hit (%s/%s)", daily_trade_count, MAX_DAILY_TRADES)
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue
            
            if len(active_trades) >= MAX_TRADES:
                log.info("Max concurrent trades already open")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue
            
            # Check daily loss limit
            trade_check = risk_manager.should_trade_today(daily_pnl)
            if not trade_check["should_trade"]:
                log.warning("Daily loss limit hit: Rs %.2f", daily_pnl)
                send_telegram(f"DAILY LOSS LIMIT HIT\nLoss: Rs {daily_pnl:,.2f}\nNo more trades today.")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue
            
            # Market safety check
            if not market_is_safe():
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue
            
            # Scan stocks
            pnl_str = f"{daily_pnl:+,.2f}"
            log.info(
                "Scanning %s | %s IST | Active: %s | Trades: %s/%s | PnL: Rs %s",
                len(STOCKS),
                now.strftime("%H:%M"),
                len(active_trades),
                daily_trade_count,
                MAX_DAILY_TRADES,
                pnl_str,
            )
            
            reset_scan_rejections()
            candidates = []
            for symbol in STOCKS:
                result = scan_stock(symbol)
                if result:
                    candidates.append(result)
            
            if candidates:
                best = sorted(candidates, key=lambda item: item["breakout_gap"], reverse=True)[0]
                log.info(
                    "Candidates found: %s | Best: %s %.3f%%",
                    len(candidates),
                    best["symbol"],
                    best["breakout_gap"],
                )
                enter_trade(best)
            else:
                log_scan_rejections()
            
            consecutive_errors = 0
            time.sleep(SCAN_INTERVAL_SECONDS)
        
        except KeyboardInterrupt:
            log.info("Bot stopped by user")
            log_restart("MANUAL STOP")
            shutdown_flatten_positions()
            if not daily_summary_sent:
                send_daily_summary()
                daily_summary_sent = True
            send_eod_summary()
            send_telegram("Bot stopped.")
            break
        
        except Exception as exc:
            consecutive_errors += 1
            log.error("Main loop error #%s: %s", consecutive_errors, exc)
            if consecutive_errors >= 5:
                log_restart(f"AUTO RESTART - {exc}")
                send_telegram(f"Bot restarting\n{exc}")
                time.sleep(5)
                os.execv(sys.executable, [sys.executable] + sys.argv)
            time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
