import streamlit as st
import pandas as pd

# =========================
# LOAD DATA
# =========================
df = pd.read_csv("trade_log.csv")

if df.empty:
    st.title("No trades yet")
    st.stop()

# =========================
# PREPARE DATA
# =========================
df["Time"] = pd.to_datetime(df["Time"])

buy_df = df[df["Type"] == "BUY"]
sell_df = df[df["Type"] == "SELL"]

total_trades = len(sell_df)
total_pnl = sell_df["P&L"].sum()

wins = sell_df[sell_df["P&L"] > 0]
losses = sell_df[sell_df["P&L"] <= 0]

win_rate = (len(wins) / total_trades * 100) if total_trades > 0 else 0

# =========================
# DASHBOARD
# =========================
st.title("📊 Algo Trading Dashboard")

col1, col2, col3 = st.columns(3)

col1.metric("Total Trades", total_trades)
col2.metric("Total P&L", f"₹{round(total_pnl,2)}")
col3.metric("Win Rate", f"{round(win_rate,2)}%")

# =========================
# P&L CHART
# =========================
st.subheader("P&L Over Time")

sell_df["CumPnL"] = sell_df["P&L"].cumsum()
st.line_chart(sell_df.set_index("Time")["CumPnL"])

# =========================
# TRADE TABLE
# =========================
st.subheader("Trade Log")
st.dataframe(df.sort_values(by="Time", ascending=False))

# =========================
# WIN / LOSS
# =========================
st.subheader("Win / Loss Distribution")

st.bar_chart({
    "Wins": len(wins),
    "Losses": len(losses)
})