"""Точка входа: Telegram-сессия, клиент ВК и общий event loop."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from telethon import TelegramClient

from config import ConfigError, Settings, load_settings
from tg_handler import TelegramReposter
from vk_poster import VKPoster

logger = logging.getLogger("reposter")


def main() -> None:
    _configure_logging("INFO")
    try:
        settings = load_settings()
    except ConfigError as exc:
        print(f"Ошибка конфигурации: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    _configure_logging(settings.log_level)
    try:
        asyncio.run(amain(settings))
    except KeyboardInterrupt:
        logger.info("Остановлено")
    except RuntimeError as exc:
        logger.error("%s", exc)
        raise SystemExit(1) from exc


async def amain(settings: Settings) -> None:
    _log_startup(settings)
    poster = VKPoster(settings.vk_token, settings.vk_group_id)
    await poster.check_access()

    if not settings.session_file.exists() and not sys.stdin.isatty():
        raise RuntimeError(
            "Сессия Telegram ещё не создана, а терминал неинтерактивный. "
            "Один раз войдите командой: docker compose run --rm -it reposter"
        )

    client = TelegramClient(str(settings.session_base), settings.api_id, settings.api_hash)
    await client.start(phone=settings.phone, password=settings.tg_2fa_password)
    _restrict_session_file(settings)

    try:
        chat = await client.get_entity(settings.tg_chat_id)
    except Exception as exc:
        await client.disconnect()
        raise RuntimeError(
            f"Не вижу чат {settings.tg_chat_id}. Аккаунт PHONE должен состоять "
            "в закрытой группе, а TG_CHAT_ID — быть id супергруппы (обычно -100...)."
        ) from exc

    title = getattr(chat, "title", None) or settings.tg_chat_id
    logger.info("Слушаю чат «%s» (%s)", title, settings.tg_chat_id)

    stop_event = asyncio.Event()
    _install_stop_signals(stop_event)
    reposter = TelegramReposter(client, settings, poster, stop_event)
    reposter.register(chat)

    logger.info("Репостер запущен. История чата не копируется, только новые сообщения.")
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        # Ctrl+C на Python 3.11 отменяет главную задачу. Снимаем отмену,
        # чтобы успеть дослать альбом, который ещё ждёт свою паузу.
        _clear_cancellation()
    await _shutdown(stop_event, reposter, client)


async def _shutdown(
    stop_event: asyncio.Event,
    reposter: TelegramReposter,
    client: TelegramClient,
) -> None:
    stop_event.set()
    logger.info("Останавливаюсь")
    try:
        await reposter.drain()
    except Exception:
        logger.exception("Не удалось дослать накопленные сообщения")
    try:
        if client.is_connected():
            await client.disconnect()
    except Exception:
        logger.exception("Не удалось отключить Telegram")


def _install_stop_signals(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def _stop() -> None:
        logger.info("Получен сигнал остановки")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            # Windows не умеет add_signal_handler. Ctrl+C приходит как KeyboardInterrupt.
            return


def _clear_cancellation() -> None:
    current = asyncio.current_task()
    cancelling = getattr(current, "cancelling", None)
    uncancel = getattr(current, "uncancel", None)
    if current is None or cancelling is None or uncancel is None:
        return
    while cancelling():
        uncancel()


def _restrict_session_file(settings: Settings) -> None:
    try:
        if settings.session_file.exists():
            settings.session_file.chmod(0o600)
    except OSError:
        logger.debug("Не удалось сузить права на файл сессии")


def _log_startup(settings: Settings) -> None:
    logger.info(
        "Кураторов в списке: %s. Чат: %s. Группа VK: %s. Сессия: %s",
        len(settings.allowed_tg_user_ids),
        settings.tg_chat_id,
        settings.vk_group_id,
        settings.session_dir,
    )


def _configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("vk_api").setLevel(logging.WARNING)


if __name__ == "__main__":
    main()
