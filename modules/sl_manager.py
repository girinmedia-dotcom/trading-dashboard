"""SL order management with retry logic and exponential backoff."""

import logging
import time
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)


class SLManager:
    """Manages stop-loss orders with retry logic and backoff strategy."""

    def __init__(self, dhan_client, dry_run: bool = True, max_retries: int = 3):
        self.dhan = dhan_client
        self.dry_run = dry_run
        self.max_retries = max_retries

    def place_sl_order_with_retry(
        self,
        security_id: str,
        qty: int,
        sl_price: float,
        order_style: str = "SLM",
        stop_limit_buffer: float = 0.003,
    ) -> Optional[str]:
        """
        Place SL order with exponential backoff retry.

        Args:
            security_id: Dhan security ID
            qty: Quantity
            sl_price: Stop-loss price level
            order_style: "SLM" (market) or "SL" (limit)
            stop_limit_buffer: Limit price buffer if using SL style

        Returns:
            Order ID if successful, None otherwise
        """
        for attempt in range(self.max_retries):
            try:
                order_id = self._place_sl_order_single(
                    security_id, qty, sl_price, order_style, stop_limit_buffer
                )
                if order_id:
                    log.info("SL order placed successfully | Attempt: %s/%s | Order ID: %s", 
                             attempt + 1, self.max_retries, order_id)
                    return order_id

                # Exponential backoff: 2s, 4s, 8s
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** (attempt + 1)
                    log.warning(
                        "SL placement failed | Attempt %s/%s | Retrying in %ss",
                        attempt + 1,
                        self.max_retries,
                        wait_time,
                    )
                    time.sleep(wait_time)

            except Exception as exc:
                log.error("SL retry attempt %s error: %s", attempt + 1, exc)
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** (attempt + 1)
                    time.sleep(wait_time)

        log.error("SL order failed after %s retries | Security ID: %s", self.max_retries, security_id)
        return None

    def _place_sl_order_single(
        self,
        security_id: str,
        qty: int,
        sl_price: float,
        order_style: str = "SLM",
        stop_limit_buffer: float = 0.003,
    ) -> Optional[str]:
        """Single attempt to place SL order."""
        if self.dry_run:
            order_id = f"DRY_SL_{security_id}_{int(time.time())}"
            log.info("[DRY RUN] SL order | Security ID: %s | Qty: %s | Price: Rs %.2f", 
                     security_id, qty, sl_price)
            return order_id

        try:
            trigger = round(sl_price, 2)

            if order_style.upper() == "SLM":
                order_type = self.dhan.SLM
                price = 0
                trigger_price = trigger
            else:
                order_type = self.dhan.SL
                limit_price = round(sl_price * (1 - stop_limit_buffer), 2)
                price = limit_price
                trigger_price = trigger

            result = self.dhan.place_order(
                security_id=security_id,
                exchange_segment=self.dhan.NSE,
                transaction_type=self.dhan.SELL,
                quantity=qty,
                order_type=order_type,
                product_type=self.dhan.INTRA,
                price=price,
                trigger_price=trigger_price,
            )

            if result.get("status") == "success":
                order_id = self._extract_order_id(result)
                log.info("SL placed | Security ID: %s | Qty: %s | Trigger: Rs %.2f | Order ID: %s",
                         security_id, qty, trigger, order_id)
                return order_id

            error = result.get("remarks", {}).get("error_message", str(result))
            log.error("SL placement failed: %s", error)
            return None

        except Exception as exc:
            log.error("_place_sl_order_single error: %s", exc)
            return None

    def modify_sl_order_with_retry(
        self,
        order_id: str,
        qty: int,
        sl_price: float,
        order_style: str = "SLM",
        stop_limit_buffer: float = 0.003,
    ) -> bool:
        """
        Modify SL order with retry logic.

        Args:
            order_id: Existing order ID to modify
            qty: New quantity
            sl_price: New stop-loss price
            order_style: "SLM" or "SL"
            stop_limit_buffer: Limit price buffer

        Returns:
            True if successful, False otherwise
        """
        if not order_id:
            return False

        if self.dry_run:
            log.info("[DRY RUN] SL modify | Order ID: %s | New Price: Rs %.2f", order_id, sl_price)
            return True

        for attempt in range(self.max_retries):
            try:
                if self._modify_sl_order_single(order_id, qty, sl_price, order_style, stop_limit_buffer):
                    log.info("SL modified successfully | Attempt: %s/%s | New Price: Rs %.2f",
                             attempt + 1, self.max_retries, sl_price)
                    return True

                if attempt < self.max_retries - 1:
                    wait_time = 2 ** (attempt + 1)
                    log.warning(
                        "SL modify failed | Attempt %s/%s | Retrying in %ss",
                        attempt + 1,
                        self.max_retries,
                        wait_time,
                    )
                    time.sleep(wait_time)

            except Exception as exc:
                log.error("SL modify retry attempt %s error: %s", attempt + 1, exc)
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** (attempt + 1)
                    time.sleep(wait_time)

        log.error("SL modify failed after %s retries | Order ID: %s", self.max_retries, order_id)
        return False

    def _modify_sl_order_single(
        self,
        order_id: str,
        qty: int,
        sl_price: float,
        order_style: str = "SLM",
        stop_limit_buffer: float = 0.003,
    ) -> bool:
        """Single attempt to modify SL order."""
        try:
            trigger = round(sl_price, 2)

            if order_style.upper() == "SLM":
                order_type = self.dhan.SLM
                price = 0
                trigger_price = trigger
            else:
                order_type = self.dhan.SL
                limit_price = round(sl_price * (1 - stop_limit_buffer), 2)
                price = limit_price
                trigger_price = trigger

            result = self.dhan.modify_order(
                order_id=order_id,
                order_type=order_type,
                leg_name="",
                quantity=qty,
                price=price,
                trigger_price=trigger_price,
                disclosed_quantity=0,
                validity=self.dhan.DAY,
            )

            if result.get("status") == "success":
                log.info("SL order modified | Order ID: %s | New Price: Rs %.2f", order_id, sl_price)
                return True

            error = result.get("remarks", {}).get("error_message", str(result))
            log.error("SL modify failed: %s", error)
            return False

        except Exception as exc:
            log.error("_modify_sl_order_single error: %s", exc)
            return False

    def cancel_order_with_retry(self, order_id: str) -> bool:
        """Cancel order with retry logic."""
        if not order_id or self.dry_run:
            return True

        for attempt in range(self.max_retries):
            try:
                result = self.dhan.cancel_order(order_id)
                if result.get("status") == "success":
                    log.info("Order cancelled | Order ID: %s", order_id)
                    return True

                if attempt < self.max_retries - 1:
                    time.sleep(2 ** (attempt + 1))

            except Exception as exc:
                log.error("Cancel retry attempt %s error: %s", attempt + 1, exc)

        log.error("Cancel failed after retries | Order ID: %s", order_id)
        return False

    @staticmethod
    def _extract_order_id(result: Dict[str, Any]) -> Optional[str]:
        """Extract order ID from API response."""
        data = SLManager._flatten_data(result.get("data", {}))
        return SLManager._pick_text(data, "orderId", "id") or None

    @staticmethod
    def _flatten_data(data: Any) -> Dict[str, Any]:
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
        return {}

    @staticmethod
    def _pick_text(data: Dict[str, Any], *keys: str) -> str:
        for key in keys:
            if key in data and data[key] not in (None, ""):
                return str(data[key]).strip()
        return ""
