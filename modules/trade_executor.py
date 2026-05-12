"""Enhanced trade execution with partial fill handling and slippage guards."""

import logging
import time
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)


class TradeExecutor:
    """Handles trade entry with partial fill protection and slippage validation."""

    def __init__(self, dhan_client, dry_run: bool = True, max_slippage_pct: float = 1.0):
        self.dhan = dhan_client
        self.dry_run = dry_run
        self.max_slippage_pct = max_slippage_pct

    def place_buy_order(self, security_id: str, qty: int) -> Optional[str]:
        """Place market buy order."""
        if self.dry_run:
            order_id = f"DRY_BUY_{security_id}_{int(time.time())}"
            log.info("[DRY RUN] BUY order placed | Security ID: %s | Qty: %s", security_id, qty)
            return order_id

        try:
            result = self.dhan.place_order(
                security_id=security_id,
                exchange_segment=self.dhan.NSE,
                transaction_type=self.dhan.BUY,
                quantity=qty,
                order_type=self.dhan.MARKET,
                product_type=self.dhan.INTRA,
                price=0,
            )
            if result.get("status") == "success":
                order_id = self._extract_order_id(result)
                log.info("BUY order placed | Security ID: %s | Qty: %s | Order ID: %s", security_id, qty, order_id)
                return order_id

            error = result.get("remarks", {}).get("error_message", str(result))
            log.error("BUY order failed: %s", error)
            return None
        except Exception as exc:
            log.error("place_buy_order error: %s", exc)
            return None

    def wait_for_fill_with_slippage_check(
        self,
        order_id: str,
        signal_price: float,
        requested_qty: int,
        timeout: int = 20,
        poll_interval: int = 2,
    ) -> Dict[str, Any]:
        """
        Wait for order fill and validate slippage.

        Returns:
            {
                "success": bool,
                "filled_qty": int,
                "avg_price": float,
                "slippage_pct": float,
                "status": str,
                "message": str,
            }
        """
        if self.dry_run:
            return {
                "success": True,
                "filled_qty": requested_qty,
                "avg_price": signal_price,
                "slippage_pct": 0.0,
                "status": "COMPLETE",
                "message": "DRY RUN fill",
            }

        deadline = time.time() + timeout
        last_snapshot = {
            "success": False,
            "filled_qty": 0,
            "avg_price": 0.0,
            "slippage_pct": 0.0,
            "status": "UNKNOWN",
            "message": "Timeout waiting for fill",
        }

        while time.time() < deadline:
            try:
                snapshot = self._get_order_snapshot(order_id)
                if not snapshot["ok"]:
                    time.sleep(poll_interval)
                    continue

                status = snapshot["status"]
                filled_qty = snapshot["filled_qty"]
                avg_price = snapshot["avg_price"]

                # Check if order is filled
                if self._is_order_filled(status, filled_qty):
                    slippage_pct = round(((avg_price - signal_price) / signal_price) * 100, 3)

                    # Validate slippage
                    if abs(slippage_pct) > self.max_slippage_pct:
                        log.warning(
                            "Slippage exceeded | Order: %s | Slippage: %.3f%% | Max: %.1f%%",
                            order_id,
                            slippage_pct,
                            self.max_slippage_pct,
                        )
                        return {
                            "success": False,
                            "filled_qty": int(filled_qty),
                            "avg_price": avg_price,
                            "slippage_pct": slippage_pct,
                            "status": status,
                            "message": f"Slippage {slippage_pct:.3f}% exceeds limit {self.max_slippage_pct}%",
                        }

                    last_snapshot = {
                        "success": True,
                        "filled_qty": int(filled_qty),
                        "avg_price": avg_price,
                        "slippage_pct": slippage_pct,
                        "status": status,
                        "message": "Order filled",
                    }
                    return last_snapshot

                # Check if order is final but not filled
                if self._is_order_final(status):
                    return {
                        "success": False,
                        "filled_qty": int(filled_qty),
                        "avg_price": avg_price,
                        "slippage_pct": 0.0,
                        "status": status,
                        "message": f"Order {status} with 0 fill",
                    }

                time.sleep(poll_interval)

            except Exception as exc:
                log.error("Error waiting for fill on order %s: %s", order_id, exc)

        return last_snapshot

    def _get_order_snapshot(self, order_id: str) -> Dict[str, Any]:
        """Get current order status snapshot."""
        try:
            result = self.dhan.get_order_by_id(order_id)
            if result.get("status") != "success":
                return {"ok": False, "status": "UNKNOWN", "filled_qty": 0, "avg_price": 0.0}

            data = self._flatten_data(result.get("data", {}))
            status = self._pick_text(
                data,
                "orderStatus",
                "status",
                "orderStatusText",
                "transactionStatus",
            )
            filled_qty = self._pick_number(
                data,
                "tradedQuantity",
                "filledQty",
                "filledQuantity",
                "executedQuantity",
                "tradedQty",
            )
            avg_price = self._pick_number(
                data,
                "averageTradedPrice",
                "averagePrice",
                "avgTradedPrice",
                "tradedPrice",
                "price",
            )
            return {
                "ok": True,
                "status": status or "UNKNOWN",
                "filled_qty": filled_qty,
                "avg_price": avg_price,
            }
        except Exception as exc:
            log.error("get_order_snapshot error for %s: %s", order_id, exc)
            return {"ok": False, "status": "UNKNOWN", "filled_qty": 0, "avg_price": 0.0}

    @staticmethod
    def _flatten_data(data: Any) -> Dict[str, Any]:
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
        return {}

    @staticmethod
    def _pick_number(data: Dict[str, Any], *keys: str, default: float = 0.0) -> float:
        for key in keys:
            if key in data and data[key] not in (None, ""):
                try:
                    return float(data[key])
                except Exception:
                    continue
        return float(default)

    @staticmethod
    def _pick_text(data: Dict[str, Any], *keys: str) -> str:
        for key in keys:
            if key in data and data[key] not in (None, ""):
                return str(data[key]).strip()
        return ""

    @staticmethod
    def _normalize_status(value: str) -> str:
        return value.strip().upper().replace("-", "").replace("_", "").replace(" ", "")

    def _is_order_final(self, status: str) -> bool:
        final_statuses = {
            "TRADED",
            "COMPLETE",
            "COMPLETED",
            "FILLED",
            "EXECUTED",
            "CANCELLED",
            "CANCELED",
            "REJECTED",
            "EXPIRED",
        }
        return self._normalize_status(status) in final_statuses

    def _is_order_filled(self, status: str, filled_qty: float) -> bool:
        filled_statuses = {"TRADED", "COMPLETE", "COMPLETED", "FILLED", "EXECUTED"}
        return self._normalize_status(status) in filled_statuses or filled_qty > 0

    @staticmethod
    def _extract_order_id(result: Dict[str, Any]) -> Optional[str]:
        data = TradeExecutor._flatten_data(result.get("data", {}))
        return TradeExecutor._pick_text(data, "orderId", "id") or None
