#!/bin/bash
# CryptoScope Telegram interactive bot (long-running)
cd /Users/adelinewen/cryptoscope-1
source .venv/bin/activate
export $(grep -v '^#' .env | grep -v '^$' | xargs)

# Check if already running
if pgrep -f "telegram_bot_runner" > /dev/null; then
    exit 0
fi

exec python3 -c "
import asyncio, os, sys
sys.path.insert(0, '.')
os.environ.setdefault('PYTHONDONTWRITEBYTECODE', '1')

async def main():
    try:
        from src.distribution.telegram_bot import create_bot_application
        app = create_bot_application()
        if app:
            print('CryptoScope Telegram Bot started')
            await app.initialize()
            await app.start()
            await app.updater.start_polling()
            # Keep running
            while True:
                await asyncio.sleep(3600)
    except Exception as e:
        print(f'Bot failed: {e}')

asyncio.run(main())
" >> /Users/adelinewen/cryptoscope-1/output/bot.log 2>&1
