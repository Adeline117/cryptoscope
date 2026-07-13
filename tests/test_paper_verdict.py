"""Paper PnL alone must never authorize real-money trading."""


def test_positive_paper_summary_does_not_claim_positive_ev(monkeypatch):
    import src.trading.paper_trader as pt
    monkeypatch.setattr(pt, "get_performance_summary", lambda: {
        "balance_sol": 12, "initial_sol": 10, "total_pnl_sol": 2,
        "total_pnl_pct": 20, "closed_trades": 20, "open_positions": 0,
        "winners": 14, "losers": 6, "win_rate": 70, "avg_win_pct": 8,
        "avg_loss_pct": -4, "total_trades": 20,
    })
    message = pt.format_performance_message()
    assert "可以考虑实盘" not in message
    assert "不可据此授权实盘" in message
