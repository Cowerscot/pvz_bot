# run_bot.py — Точка входа для запуска VK-бота

from bot_engine import create_vk_bot

if __name__ == '__main__':
    run_bot = create_vk_bot()
    run_bot()
