"""Приём сообщений закрытого чата Telegram и подготовка постов для ВК.

Альбом (несколько фото с одним grouped_id) Telegram присылает отдельными
событиями с небольшой задержкой. Их нельзя публиковать по одному: студенты
увидят пачку записей вместо одного материала. Куски копятся в словаре,
отдельная задача ждёт тишину через asyncio.sleep и только потом вызывает ВК.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
import tempfile
import time
from collections.abc import Coroutine
from dataclasses import dataclass, field
from pathlib import Path

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.tl.custom import Message

from config import Settings
from vk_poster import LocalMedia, MediaKind, VKPoster

logger = logging.getLogger(__name__)

_IGNORED_WEBPAGE = {"MessageMediaWebPage", "MessageMediaEmpty"}


@dataclass
class _AlbumBucket:
    messages: list[Message] = field(default_factory=list)
    started_at: float = 0.0
    last_event_at: float = 0.0


class PostedStore:
    """Журнал уже отправленных id. Переживает рестарт контейнера вместе с сессией.

    Telethon иногда повторно отдаёт апдейт, если процесс умер до сохранения
    session-файла. Без журнала материал ушёл бы в ВК второй раз.
    """

    def __init__(self, path: Path, limit: int = 5000) -> None:
        self.path = path
        self._limit = limit
        self._order: list[str] = []
        self._ids: set[str] = set()
        self._load()

    def has(self, key: str) -> bool:
        return key in self._ids

    def add_many(self, keys: list[str]) -> None:
        changed = False
        for key in keys:
            if key in self._ids:
                continue
            self._ids.add(key)
            self._order.append(key)
            changed = True
        if not changed:
            return
        overflow = len(self._order) - self._limit
        if overflow > 0:
            for key in self._order[:overflow]:
                self._ids.discard(key)
            del self._order[:overflow]
        self._save()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            broken = self.path.with_name(self.path.name + ".broken")
            logger.warning("Журнал %s повреждён (%s), переименовываю в %s", self.path, exc, broken)
            try:
                self.path.replace(broken)
            except OSError:
                logger.warning("Не удалось переименовать повреждённый журнал")
            return
        if not isinstance(data, list):
            return
        for item in data[-self._limit :]:
            key = str(item)
            if key not in self._ids:
                self._ids.add(key)
                self._order.append(key)

    def _save(self) -> None:
        temporary = self.path.with_suffix(".json.tmp")
        try:
            temporary.write_text(
                json.dumps(self._order, ensure_ascii=False),
                encoding="utf-8",
            )
            temporary.replace(self.path)
        except OSError:
            logger.exception("Не удалось записать журнал %s", self.path)


class TelegramReposter:
    def __init__(
        self,
        client: TelegramClient,
        settings: Settings,
        poster: VKPoster,
        stop_event: asyncio.Event,
    ) -> None:
        self._client = client
        self._settings = settings
        self._poster = poster
        self._stop = stop_event
        self._store = PostedStore(settings.posted_store_path)
        self._albums: dict[int, _AlbumBucket] = {}
        self._album_tasks: dict[int, asyncio.Task[None]] = {}
        self._jobs: set[asyncio.Task[None]] = set()
        self._lock = asyncio.Lock()
        # Сколько обработчиков сейчас внутри _handle. drain ждёт ноль,
        # иначе остановка может оборвать альбом, который ещё только кладут в словарь.
        self._active = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self._quiet = settings.album_quiet_seconds
        self._max_wait = settings.album_max_wait_seconds

    def register(self, chat: object) -> None:
        self._client.add_event_handler(
            self._on_message,
            events.NewMessage(chats=chat),
        )

    async def drain(self) -> None:
        """Досылает уже начатые посты. Новые сообщения после сигнала не берём."""
        self._stop.set()
        while True:
            await self._idle.wait()
            async with self._lock:
                album_tasks = [task for task in self._album_tasks.values() if not task.done()]
            jobs = [task for task in list(self._jobs) if not task.done()]
            pending = [*album_tasks, *jobs]
            if self._active == 0 and not pending:
                return
            if not pending:
                await asyncio.sleep(0.05)
                continue
            logger.info("Перед остановкой досылаю задач: %s", len(pending))
            await asyncio.gather(*pending, return_exceptions=True)

    async def _on_message(self, event: events.NewMessage.Event) -> None:
        self._active += 1
        self._idle.clear()
        try:
            try:
                await self._handle(event)
            except Exception:
                logger.exception("Не обработано сообщение %s", getattr(event, "id", "?"))
        finally:
            self._active -= 1
            if self._active == 0:
                self._idle.set()

    async def _handle(self, event: events.NewMessage.Event) -> None:
        if self._stop.is_set():
            return
        message = event.message
        if message is None or message.action:
            return

        sender_id = event.sender_id
        if sender_id is None or sender_id not in self._settings.allowed_tg_user_ids:
            logger.debug(
                "Пропуск сообщения %s от %s: отправитель не в списке кураторов",
                message.id,
                sender_id,
            )
            return

        # Альбом нельзя ждать внутри этого обработчика. Пока он не вернётся,
        # следующее фото с тем же grouped_id не попадёт в буфер, если апдейты
        # идут по очереди. Поэтому здесь только запись в словарь, а sleep — в задаче.
        if message.grouped_id is not None:
            await self._buffer_album(message)
            return
        self._spawn(self._publish_batch([message]))

    async def _buffer_album(self, message: Message) -> None:
        grouped_id = message.grouped_id
        if grouped_id is None:
            return
        async with self._lock:
            now = time.monotonic()
            bucket = self._albums.get(grouped_id)
            if bucket is None:
                bucket = _AlbumBucket(messages=[], started_at=now, last_event_at=now)
                self._albums[grouped_id] = bucket
                # create_task не выполняет тело, пока мы держим lock и не отдали
                # управление. Сообщение добавляется ниже до отпускания lock,
                # так что задача не увидит пустой альбом.
                self._album_tasks[grouped_id] = asyncio.create_task(
                    self._flush_when_quiet(grouped_id),
                    name=f"album-{grouped_id}",
                )
            bucket.messages.append(message)
            bucket.last_event_at = now
            logger.info(
                "Альбом %s: часть %s, собрано %s, жду %.1f с тишины",
                grouped_id,
                message.id,
                len(bucket.messages),
                self._quiet,
            )

    async def _flush_when_quiet(self, grouped_id: int) -> None:
        """Ждёт, пока grouped_id перестанет пополняться, и публикует один пост.

        Каждая новая часть сдвигает last_event_at. Задача спит остаток паузы
        (по умолчанию 2.5 с) через asyncio.sleep. Если за это время ничего не
        пришло — альбом полный. Если части идут слишком долго, срабатывает
        album_max_wait_seconds, чтобы материал не завис в буфере.
        """
        messages: list[Message] | None = None
        try:
            while True:
                async with self._lock:
                    bucket = self._albums.get(grouped_id)
                    if bucket is None:
                        return
                    now = time.monotonic()
                    idle = now - bucket.last_event_at
                    elapsed = now - bucket.started_at
                    ready = (
                        self._stop.is_set()
                        or elapsed >= self._max_wait
                        or idle >= self._quiet
                    )
                    if ready:
                        messages = sorted(bucket.messages, key=lambda item: item.id)
                        self._albums.pop(grouped_id, None)
                        break
                    delay = min(self._quiet - idle, self._max_wait - elapsed)
                await asyncio.sleep(max(delay, 0.05))
            if messages:
                logger.info(
                    "Альбом %s собран (%s сообщений), отправляю одним постом",
                    grouped_id,
                    len(messages),
                )
                await self._publish_batch(messages)
        except asyncio.CancelledError:
            logger.info("Сбор альбома %s отменён", grouped_id)
            raise
        except Exception:
            logger.exception("Альбом %s не опубликован", grouped_id)
        finally:
            async with self._lock:
                self._album_tasks.pop(grouped_id, None)

    def _spawn(self, coro: Coroutine[object, object, None]) -> None:
        task: asyncio.Task[None] = asyncio.create_task(coro)
        self._jobs.add(task)
        task.add_done_callback(self._jobs.discard)

    async def _publish_batch(self, messages: list[Message]) -> None:
        messages = sorted(messages, key=lambda item: item.id)
        workdir = Path(tempfile.mkdtemp(prefix="reposter_"))
        media: list[LocalMedia] = []
        download_failed = False
        try:
            if _already_posted(self._store, messages):
                logger.info(
                    "Сообщения %s уже публиковались, повторно не отправляю",
                    [item.id for item in messages],
                )
                return

            # Пересылку публикуем от имени куратора, который нажал «переслать»
            # в учебный чат. Исходный автор из fwd_from в шапку не подставляется.
            if any(item.fwd_from for item in messages):
                logger.info(
                    "Сообщения %s пересланы: автор поста — отправитель в чате",
                    [item.id for item in messages],
                )

            sender = await messages[0].get_sender()
            author = format_author(sender)
            body_parts: list[str] = []
            for index, message in enumerate(messages, start=1):
                text = (message.raw_text or "").strip()
                if text and (not body_parts or body_parts[-1] != text):
                    body_parts.append(text)
                kind = _classify(message)
                if kind is None:
                    continue
                try:
                    item = await _download_media(message, workdir, index, kind)
                except Exception:
                    download_failed = True
                    logger.exception("Не скачалось медиа сообщения %s", message.id)
                    continue
                if item is None:
                    download_failed = True
                    continue
                media.append(item)

            body = "\n\n".join(body_parts)
            if download_failed and not media and not body:
                logger.error(
                    "Медиа сообщений %s не скачалось, пост без текста не создан",
                    [item.id for item in messages],
                )
                return

            result = await self._poster.publish(
                render_post(author, body),
                media,
                allow_text_only=bool(body),
            )
            if result.failed:
                logger.error(
                    "Сообщения %s остались неопубликованными",
                    [item.id for item in messages],
                )
                return
            self._store.add_many(_dedup_keys(messages))
            if result.skip:
                logger.info(
                    "Сообщения %s без текста и вложений, которые можно перенести в ВК",
                    [item.id for item in messages],
                )
        except Exception:
            logger.exception(
                "Сбой обработки сообщений %s",
                [item.id for item in messages],
            )
        finally:
            # Файлы живут до конца to_thread внутри publish(). Удалять их раньше нельзя:
            # поток ВК ещё читает путь. Здесь publish уже вернулся.
            shutil.rmtree(workdir, ignore_errors=True)


def format_author(sender: object) -> str:
    """«Имя Фамилия». Без фамилии остаётся только имя."""
    if sender is None:
        return "Неизвестный автор"
    first = _clean(getattr(sender, "first_name", None))
    last = _clean(getattr(sender, "last_name", None))
    if first and last:
        return f"{first} {last}"
    if first:
        return first
    if last:
        return last
    title = _clean(getattr(sender, "title", None))
    if title:
        return title
    username = _clean(getattr(sender, "username", None))
    if username:
        return f"@{username}"
    return "Неизвестный автор"


def render_post(author: str, body: str) -> str:
    """Шаблон поста. Медиа без подписи оставляют только строку автора."""
    header = f"Автор: {author}"
    body = body.strip()
    if not body:
        return header
    return f"{header}\n\n{body}"


def _classify(message: Message) -> MediaKind | None:
    """Возвращает photo/document/video либо None, если вложение переносить не нужно.

    Голосовые и видео-кружки специально отбрасываются: в учебной стене они
    неудобны, а ВК не принимает их как обычные видео. Проверять их нужно
    раньше document: в Telegram и голос, и кружок, и стикер лежат в документе.
    """
    if message.voice:
        logger.warning("Голосовое сообщение %s пропущено", message.id)
        return None
    if message.video_note:
        logger.warning("Видео-кружок %s пропущен", message.id)
        return None
    if message.sticker:
        logger.warning("Стикер %s пропущен", message.id)
        return None
    # Ссылка с превью тоже отдаёт .photo/.document, но это не файл сообщения.
    # В пост пойдёт только текст, в котором уже есть URL.
    if message.web_preview:
        return None
    if message.photo:
        return "photo"
    if message.video:
        return "video"
    if message.document or message.gif:
        return "document"
    if message.audio:
        logger.warning("Аудио %s пропущено", message.id)
        return None

    media = message.media
    if media is None:
        return None
    media_name = type(media).__name__
    if media_name in _IGNORED_WEBPAGE:
        return None
    if media_name == "MessageMediaPoll":
        logger.warning("Опрос %s не переносится в ВК", message.id)
        return None
    if media_name in {"MessageMediaGeo", "MessageMediaGeoLive", "MessageMediaVenue"}:
        logger.warning("Геоточка %s не переносится в ВК", message.id)
        return None
    logger.warning("Медиа %s типа %s не поддерживается", message.id, media_name)
    return None


async def _download_media(
    message: Message,
    workdir: Path,
    index: int,
    kind: MediaKind,
) -> LocalMedia | None:
    extension = _extension(message, kind)
    filename = _safe_filename(message, kind, extension)
    target = workdir / f"{index:02d}_{filename}"

    downloaded: object | None = None
    for attempt in (1, 2):
        try:
            downloaded = await message.download_media(file=str(target))
            break
        except FloodWaitError as exc:
            target.unlink(missing_ok=True)
            if attempt == 2:
                raise
            wait = int(exc.seconds) + 1
            logger.warning(
                "Telegram просит подождать %s с перед скачиванием сообщения %s",
                wait,
                message.id,
            )
            await asyncio.sleep(wait)

    saved = Path(downloaded) if isinstance(downloaded, (str, Path)) else target
    if not saved.is_file() or saved.stat().st_size <= 0:
        logger.warning("Пустое скачивание медиа сообщения %s", message.id)
        return None

    size = saved.stat().st_size
    if kind == "photo" and size > 50 * 1024 * 1024:
        logger.warning("Фото %s больше 50 МБ, ВК может отклонить загрузку", filename)
    logger.info(
        "Скачано %s (%s, %s байт) из сообщения %s",
        filename,
        kind,
        size,
        message.id,
    )
    return LocalMedia(kind=kind, path=saved, filename=filename)


def _extension(message: Message, kind: MediaKind) -> str:
    extension = ""
    if message.file and message.file.ext:
        extension = message.file.ext.lower()
    if extension and not extension.startswith("."):
        extension = f".{extension}"
    if extension:
        return extension
    return {"photo": ".jpg", "video": ".mp4", "document": ".bin"}[kind]


def _safe_filename(message: Message, kind: MediaKind, extension: str) -> str:
    raw_name = ""
    if message.file and message.file.name:
        raw_name = Path(message.file.name).name
    if not raw_name:
        raw_name = f"{kind}_{message.id}{extension}"
    cleaned = re.sub(r"[^\w.\- ()]+", "_", raw_name, flags=re.UNICODE).strip(" .")
    if not cleaned:
        cleaned = f"{kind}_{message.id}{extension}"
    if extension and not cleaned.lower().endswith(extension.lower()):
        cleaned = f"{cleaned}{extension}"
    return cleaned[:120]


def _already_posted(store: PostedStore, messages: list[Message]) -> bool:
    grouped_id = messages[0].grouped_id
    chat_id = messages[0].chat_id
    if grouped_id is not None and store.has(f"album:{chat_id}:{grouped_id}"):
        return True
    return all(store.has(f"msg:{chat_id}:{message.id}") for message in messages)


def _dedup_keys(messages: list[Message]) -> list[str]:
    chat_id = messages[0].chat_id
    keys = [f"msg:{chat_id}:{message.id}" for message in messages]
    grouped_id = messages[0].grouped_id
    if grouped_id is not None:
        keys.append(f"album:{chat_id}:{grouped_id}")
    return keys


def _clean(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())
