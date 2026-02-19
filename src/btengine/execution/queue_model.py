from __future__ import annotations

import math
from dataclasses import dataclass

from ..types import Side, Trade


@dataclass(slots=True)
class MakerQueueOrder:
    """Approximate maker fill model using visible book qty + trade tape.

    Model:
    - When the order is placed, assume we're behind the currently visible
      quantity at our price level (queue_ahead_qty).
    - We only reduce queue_ahead_qty when:
      - trades execute at our exact price level and against our side, or
      - the visible quantity at our price level decreases (cancels/executions).
    - We do not increase queue_ahead_qty on visible quantity increases
      (assume new liquidity joins behind us).
    """

    symbol: str
    side: Side  # "buy" -> resting on bid; "sell" -> resting on ask
    price: float
    quantity: float

    queue_ahead_qty: float
    filled_qty: float = 0.0
    trade_participation: float = 1.0  # 0..1. Conservative if < 1.
    priority_seq: int = 0  # lower => older maker priority

    def remaining_qty(self) -> float:
        rem = self.quantity - self.filled_qty
        return rem if rem > 0.0 else 0.0

    def is_filled(self) -> bool:
        return self.remaining_qty() <= 0.0

    def on_book_qty_update(self, new_visible_qty: float) -> None:
        """Update visible queue ahead from an orderbook level update."""

        if new_visible_qty < 0:
            raise ValueError("new_visible_qty must be >= 0")
        # Only allow decreases to help us; increases assumed behind us.
        if new_visible_qty < self.queue_ahead_qty:
            self.queue_ahead_qty = new_visible_qty

    def _matches_trade(self, trade: Trade) -> bool:
        if trade.symbol != self.symbol:
            return False
        if self.is_filled():
            return False

        # Price must match the level we are resting on.
        if not math.isclose(trade.price, self.price, rel_tol=0.0, abs_tol=1e-9):
            return False

        # Binance semantics:
        # - is_buyer_maker=True  => sell aggressor, trade hits bids.
        # - is_buyer_maker=False => buy aggressor, trade hits asks.
        if self.side == "buy":
            if not trade.is_buyer_maker:
                return False  # buy aggressor doesn't fill our bid
        else:  # sell
            if trade.is_buyer_maker:
                return False  # sell aggressor doesn't fill our ask
        return True

    def on_trade_budgeted(self, trade: Trade, *, max_trade_qty: float | None = None) -> tuple[float, float]:
        """Consume trade tape with an optional per-trade volume budget.

        Returns:
        - filled quantity credited to this order
        - consumed trade quantity at this price (queue + fill)
        """

        if not self._matches_trade(trade):
            return 0.0, 0.0

        if not (0.0 < self.trade_participation <= 1.0):
            raise ValueError("trade_participation must be in (0, 1]")

        v = float(trade.quantity) * float(self.trade_participation)
        if max_trade_qty is not None:
            v = min(v, max(0.0, float(max_trade_qty)))
        if v <= 0.0:
            return 0.0, 0.0

        # First consume the queue ahead.
        queue_before = float(self.queue_ahead_qty)
        queue_consumed = min(queue_before, v)

        if queue_before >= v:
            self.queue_ahead_qty -= v
            return 0.0, float(v)

        # The excess trades can fill us.
        remaining_after_queue = v - queue_before
        self.queue_ahead_qty = 0.0

        fill = min(self.remaining_qty(), remaining_after_queue)
        self.filled_qty += fill
        consumed = queue_consumed + fill
        return fill, consumed

    def on_trade(self, trade: Trade) -> float:
        """Consume trade tape to progress queue/fills.

        Returns filled quantity (base) from this trade.
        """

        fill, _ = self.on_trade_budgeted(trade, max_trade_qty=None)
        return fill
