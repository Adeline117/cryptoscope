from __future__ import annotations

from collections import defaultdict

import pytest


class _Socket:
    def __init__(self, *, send_error: Exception | None = None, replies=None):
        self.send_error = send_error
        self.replies = iter(replies or [])
        self.closed = 0

    def send(self, _payload):
        if self.send_error:
            raise self.send_error

    def recv(self):
        return next(self.replies)

    def close(self):
        self.closed += 1


def test_failed_websocket_subscription_closes_partial_connection():
    from src.pipeline import realtime_ws

    ws = _Socket(send_error=RuntimeError("subscription write failed"))

    with pytest.raises(RuntimeError, match="subscription write failed"):
        realtime_ws._open_subscription(
            "wss://rpc.invalid", ["0xtoken"], lambda *_args, **_kwargs: ws,
        )

    assert ws.closed == 1


def test_empty_websocket_frame_forces_reconnect_instead_of_busy_loop():
    from src.pipeline import realtime_ws

    ws = _Socket(replies=[""])

    with pytest.raises(ConnectionError, match="peer closed"):
        realtime_ws._receive_one(ws, defaultdict(lambda: {"buy": 0.0, "sell": 0.0}), {})


@pytest.mark.asyncio
@pytest.mark.parametrize("send_fails", [False, True])
async def test_realtime_telegram_bot_closes_on_success_and_failure(monkeypatch, send_fails):
    import telegram

    from src.distribution import telegram_sender

    class Bot:
        instances = []

        def __init__(self, token):
            self.token = token
            self.entered = False
            self.exited = False
            self.sent = 0
            self.__class__.instances.append(self)

        async def __aenter__(self):
            self.entered = True
            return self

        async def __aexit__(self, *_args):
            self.exited = True

        async def send_message(self, **_kwargs):
            self.sent += 1
            if send_fails:
                raise RuntimeError("telegram closed connection")

    monkeypatch.setattr(telegram, "Bot", Bot)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TG_REVIEW_CHANNEL", "123")

    assert await telegram_sender.send_alert("test") is (not send_fails)
    bot = Bot.instances[-1]
    assert bot.entered and bot.exited
    assert bot.sent == 1
