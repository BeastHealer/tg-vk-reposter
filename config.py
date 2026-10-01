"""Загрузка и проверка настроек репостера из окружения и файла .env."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# Пауза после последнего куска альбома Telegram, прежде чем слать один пост в ВК.
DEFAULT_ALBUM_QUIET_SECONDS = 2.5
# Если части альбома продолжают приходить, всё равно публикуем не позже этого срока.
DEFAULT_ALBUM_MAX_WAIT_SECONDS = 12.0


class ConfigError(Exception):
    """В окружении не хватает данных или они противоречат друг другу."""


@dataclass(frozen=True)
class Settings:
    """Проверенные настройки процесса. Секреты не попадают в repr."""

    api_id: int
    api_hash: str = field(repr=False)
    phone: str = field(repr=False)
    tg_2fa_password: str | None = field(repr=False)
    tg_chat_id: int
    allowed_tg_user_ids: frozenset[int]
    vk_token: str = field(repr=False)
    vk_group_id: int
    session_dir: Path
    album_quiet_seconds: float = DEFAULT_ALBUM_QUIET_SECONDS
    album_max_wait_seconds: float = DEFAULT_ALBUM_MAX_WAIT_SECONDS
    log_level: str = "INFO"

    @property
    def vk_owner_id(self) -> int:
        """id стены сообщества. У групп ВК он всегда отрицательный."""
        return -self.vk_group_id

    @property
    def session_base(self) -> Path:
        """Путь без суффикса: Telethon сам дописывает .session."""
        return self.session_dir / "reposter"

    @property
    def session_file(self) -> Path:
        return self.session_dir / "reposter.session"

    @property
    def posted_store_path(self) -> Path:
        return self.session_dir / "posted_ids.json"


def load_settings() -> Settings:
    """Читает .env рядом с проектом. Уже заданные переменные окружения важнее файла.

    Так Docker может передать секреты через env_file, а локальный запуск
    по-прежнему берёт их из .env. Пустые значения считаются отсутствующими.
    """
    env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(env_path, override=False)

    api_id = _parse_int("API_ID", _required("API_ID"))
    if api_id <= 0:
        raise ConfigError("API_ID должен быть положительным числом с my.telegram.org")

    api_hash = _required("API_HASH")
    phone = _required("PHONE")
    if not phone.startswith("+"):
        raise ConfigError("PHONE нужен в международном формате, например +79991234567")

    tg_chat_id = _parse_int("TG_CHAT_ID", _required("TG_CHAT_ID"))
    if tg_chat_id >= 0:
        raise ConfigError(
            "TG_CHAT_ID закрытой супергруппы начинается с -100. "
            "Положительный id — это личный чат или устаревший формат."
        )

    allowed_ids = _parse_id_list(_required("ALLOWED_TG_USER_IDS"))
    vk_token = _required("VK_TOKEN")
    if len(vk_token) < 20:
        raise ConfigError("VK_TOKEN слишком короткий, вставьте полный ключ доступа")

    vk_group_raw = _required("VK_GROUP_ID")
    if vk_group_raw.startswith("-"):
        raise ConfigError(
            "VK_GROUP_ID указывается без минуса (минус добавляется только в owner_id стены)"
        )
    vk_group_id = _parse_int("VK_GROUP_ID", vk_group_raw)
    if vk_group_id <= 0:
        raise ConfigError("VK_GROUP_ID должен быть положительным числом")

    session_dir = Path(os.getenv("SESSION_DIR", "sessions").strip() or "sessions")
    if not session_dir.is_absolute():
        session_dir = Path(__file__).resolve().parent / session_dir
    try:
        session_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ConfigError(f"Не удалось создать каталог сессии {session_dir}: {exc}") from exc

    album_quiet_seconds = _parse_float("ALBUM_QUIET_SECONDS", DEFAULT_ALBUM_QUIET_SECONDS)
    album_max_wait_seconds = _parse_float(
        "ALBUM_MAX_WAIT_SECONDS",
        DEFAULT_ALBUM_MAX_WAIT_SECONDS,
    )
    if not 0.5 <= album_quiet_seconds <= 30:
        raise ConfigError("ALBUM_QUIET_SECONDS должен быть в диапазоне от 0.5 до 30")
    if album_max_wait_seconds < album_quiet_seconds or album_max_wait_seconds > 120:
        raise ConfigError(
            "ALBUM_MAX_WAIT_SECONDS должен быть не меньше ALBUM_QUIET_SECONDS и не больше 120"
        )

    log_level = (os.getenv("LOG_LEVEL", "INFO") or "INFO").strip().upper()
    if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ConfigError(f"Неизвестный LOG_LEVEL: {log_level}")

    password = os.getenv("TG_2FA_PASSWORD", "").strip() or None

    return Settings(
        api_id=api_id,
        api_hash=api_hash,
        phone=phone,
        tg_2fa_password=password,
        tg_chat_id=tg_chat_id,
        allowed_tg_user_ids=allowed_ids,
        vk_token=vk_token,
        vk_group_id=vk_group_id,
        session_dir=session_dir,
        album_quiet_seconds=album_quiet_seconds,
        album_max_wait_seconds=album_max_wait_seconds,
        log_level=log_level,
    )


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"Не задана переменная окружения {name}")
    return value


def _parse_int(name: str, value: str) -> int:
    try:
        return int(value.strip())
    except ValueError as exc:
        raise ConfigError(f"{name} должно быть целым числом, сейчас: {value}") from exc


def _parse_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} должно быть числом, сейчас: {raw}") from exc


def _parse_id_list(raw: str) -> frozenset[int]:
    parts = [item.strip() for item in raw.split(",")]
    parts = [item for item in parts if item]
    if not parts:
        raise ConfigError("ALLOWED_TG_USER_IDS пуст: укажите хотя бы один id куратора")

    parsed: list[int] = []
    for item in parts:
        try:
            value = int(item)
        except ValueError as exc:
            raise ConfigError(
                f"ALLOWED_TG_USER_IDS: «{item}» не является числовым id"
            ) from exc
        if value == 0:
            raise ConfigError("ALLOWED_TG_USER_IDS не может содержать 0")
        parsed.append(value)
    return frozenset(parsed)
