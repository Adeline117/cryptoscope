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
            self.initialized = False
            self.shutdown_called = False
            self.sent = 0
            self.__class__.instances.append(self)

        async def initialize(self):
            self.initialized = True

        async def shutdown(self):
            self.shutdown_called = True

        async def send_message(self, **_kwargs):
            self.sent += 1
            if send_fails:
                raise RuntimeError("telegram closed connection")

    monkeypatch.setattr(telegram, "Bot", Bot)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TG_REVIEW_CHANNEL", "123")

    assert await telegram_sender.send_alert("test") is (not send_fails)
    bot = Bot.instances[-1]
    assert bot.initialized and bot.shutdown_called
    assert bot.sent == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sender", "expected_messages"),
    [
        (lambda module: module.send_daily_digest("summary", 1, ["topic"]), 1),
        (lambda module: module.send_meme_alert("meme"), 1),
        (lambda module: module.send_critical_alert("critical"), 1),
        (lambda module: module.send_thread_for_review(
            "topic", 80, ["source"], "english", "中文"), 3),
    ],
)
async def test_every_telegram_sender_owns_and_shuts_down_its_client(
        monkeypatch, sender, expected_messages):
    import telegram

    from src.distribution import telegram_sender

    class Bot:
        instances = []

        def __init__(self, token):
            self.token = token
            self.initialized = False
            self.shutdown_called = False
            self.sent = 0
            self.__class__.instances.append(self)

        async def initialize(self):
            self.initialized = True

        async def shutdown(self):
            self.shutdown_called = True

        async def send_message(self, **_kwargs):
            self.sent += 1

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(telegram, "Bot", Bot)
    monkeypatch.setattr(telegram_sender.asyncio, "sleep", no_sleep)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TG_REVIEW_CHANNEL", "123")

    assert await sender(telegram_sender) is True
    bot = Bot.instances[-1]
    assert bot.initialized and bot.shutdown_called
    assert bot.sent == expected_messages


@pytest.mark.asyncio
async def test_telegram_initialization_failure_still_shuts_down_client(monkeypatch):
    import telegram

    from src.distribution import telegram_sender

    class Bot:
        instance = None

        def __init__(self, token):
            self.token = token
            self.shutdown_called = False
            self.__class__.instance = self

        async def initialize(self):
            raise RuntimeError("initialization rejected")

        async def shutdown(self):
            self.shutdown_called = True

    monkeypatch.setattr(telegram, "Bot", Bot)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TG_REVIEW_CHANNEL", "123")

    assert await telegram_sender.send_critical_alert("critical") is False
    assert Bot.instance.shutdown_called
