"""Публикация текста и вложений на стену сообщества ВКонтакте.

vk_api — синхронная библиотека. Все сетевые вызовы из event loop Telethon
уходят в рабочий поток через asyncio.to_thread, иначе загрузка файла
остановит приём новых сообщений Telegram.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, TypeVar

import requests
import vk_api
from requests.adapters import HTTPAdapter
from vk_api.exceptions import VkApiError

logger = logging.getLogger(__name__)

# У стены ВК не больше 10 вложений на запись и около 16 384 символов текста.
VK_WALL_ATTACHMENTS_LIMIT = 10
VK_WALL_TEXT_LIMIT = 16_000

# Коды, которые имеет смысл повторить: внутренний сбой, слишком часто, flood, timeout.
_RETRYABLE_API_CODES = {1, 6, 9, 10}
_MAX_ATTEMPTS = 3

MediaKind = Literal["photo", "document", "video"]
T = TypeVar("T")


class _NothingToPost(Exception):
    """В сообщении не осталось ни текста, ни вложений. Это не ошибка доставки."""


class _DeliveryError(Exception):
    """Вложения были, но ни одно не загрузилось, а текста для отдельного поста нет."""


@dataclass(frozen=True)
class LocalMedia:
    """Файл уже скачан на диск. Удаляет его вызывающий код после publish()."""

    kind: MediaKind
    path: Path
    filename: str


@dataclass(frozen=True)
class PublishResult:
    post_ids: tuple[int, ...]
    failed: bool
    skip: bool


class _TimeoutAdapter(HTTPAdapter):
    """У requests нет таймаута по умолчанию: без него зависшая загрузка держит поток."""

    def __init__(self, timeout: tuple[float, float], **kwargs: object) -> None:
        self._timeout = timeout
        super().__init__(**kwargs)

    def send(self, request, **kwargs):  # type: ignore[no-untyped-def]
        kwargs.setdefault("timeout", self._timeout)
        return super().send(request, **kwargs)


class VKPoster:
    """Один автор сессии ВК. Публикации идут строго по одной."""

    def __init__(self, token: str, group_id: int) -> None:
        self.group_id = abs(group_id)
        self.owner_id = -self.group_id
        self._session = vk_api.VkApi(token=token)
        self.vk = self._session.get_api()
        self.upload = vk_api.VkUpload(self._session)
        self._lock = asyncio.Lock()
        self._install_timeouts()

    def _install_timeouts(self) -> None:
        # (соединение, чтение). Видео учебной группы может быть крупным.
        adapter = _TimeoutAdapter(timeout=(15, 300))
        for session in (self._session.http, self.upload.http):
            session.mount("http://", adapter)
            session.mount("https://", adapter)

    async def check_access(self) -> None:
        """Проверяет токен до входа в Telegram, чтобы контейнер не молчал вхолостую."""
        async with self._lock:
            await asyncio.to_thread(self._check_access_sync)

    def _check_access_sync(self) -> None:
        try:
            try:
                payload = self.vk.groups.getById(group_id=self.group_id)
            except VkApiError as exc:
                # Новые версии API принимают group_ids вместо одного group_id.
                if getattr(exc, "code", None) != 100:
                    raise
                payload = self.vk.groups.getById(group_ids=str(self.group_id))
        except VkApiError as exc:
            if getattr(exc, "code", None) == 5:
                raise RuntimeError(
                    "VK_TOKEN отклонён (ошибка 5). Выпустите новый ключ сообщества."
                ) from exc
            logger.warning("Предпроверка сообщества VK не удалась: %s", exc)
            return
        except requests.RequestException as exc:
            logger.warning("Сеть VK недоступна при старте: %s. Продолжаю запуск.", exc)
            return

        logger.info(
            "VK: стена «%s», owner_id=%s, from_group=1",
            _group_name(payload, self.group_id),
            self.owner_id,
        )

    async def publish(
        self,
        text: str,
        media: list[LocalMedia],
        *,
        allow_text_only: bool,
    ) -> PublishResult:
        """Грузит медиа и создаёт запись. Ошибка одного поста не выходит наружу.

        Lock держится на всё время синхронной работы: у VkApi одна requests-сессия,
        параллельные потоки её портят. Сам HTTP при этом выполняется в to_thread
        и event loop Telethon не блокирует.
        """
        async with self._lock:
            try:
                post_ids = await asyncio.to_thread(
                    self._publish_sync,
                    text,
                    media,
                    allow_text_only,
                )
            except _NothingToPost:
                return PublishResult(post_ids=(), failed=False, skip=True)
            except _DeliveryError as exc:
                logger.error("%s", exc)
                return PublishResult(post_ids=(), failed=True, skip=False)
            except Exception:
                logger.exception("Публикация в ВК не удалась")
                return PublishResult(post_ids=(), failed=True, skip=False)
        return PublishResult(post_ids=tuple(post_ids), failed=False, skip=False)

    def _publish_sync(
        self,
        text: str,
        media: list[LocalMedia],
        allow_text_only: bool,
    ) -> list[int]:
        attachments: list[str] = []
        errors = 0
        for item in media:
            try:
                attachments.append(self._upload_one(item))
            except Exception:
                # Один битый файл не должен отменять остальные вложения альбома.
                errors += 1
                logger.exception("Не загрузилось %s (%s)", item.filename, item.kind)

        if not attachments and not allow_text_only:
            if errors:
                raise _DeliveryError(
                    "Вложения не загрузились, а текста сообщения нет — пост не создан"
                )
            raise _NothingToPost()

        if errors and attachments:
            logger.warning("Часть вложений пропущена из-за ошибок: %s", errors)
        return self._send_posts(text, attachments)

    def _upload_one(self, item: LocalMedia) -> str:
        if not item.path.is_file() or item.path.stat().st_size <= 0:
            raise RuntimeError(f"Файл пустой или отсутствует: {item.filename}")

        if item.kind == "photo":
            return self._upload_photo(item)
        if item.kind == "document":
            return self._upload_document(item)
        if item.kind == "video":
            return self._upload_video(item)
        raise RuntimeError(f"Неизвестный тип вложения: {item.kind}")

    def _upload_photo(self, item: LocalMedia) -> str:
        saved = self._with_retry(
            lambda: self.upload.photo_wall(str(item.path), group_id=self.group_id),
            f"фото {item.filename}",
        )
        photos = saved if isinstance(saved, list) else [saved]
        if not photos:
            raise RuntimeError(f"VK вернул пустой список для фото {item.filename}")
        return _attachment("photo", photos[0], ("id", "photo_id"))

    def _upload_document(self, item: LocalMedia) -> str:
        # В актуальном vk_api нет document_file. Загрузка «на стену» — это
        # document(..., to_wall=True): внутри вызывается docs.getWallUploadServer,
        # затем docs.save. document_wall делает ровно этот вызов.
        saved = self._with_retry(
            lambda: self.upload.document(
                str(item.path),
                title=item.filename[:128],
                group_id=self.group_id,
                to_wall=True,
            ),
            f"документ {item.filename}",
        )
        if not isinstance(saved, dict):
            raise RuntimeError(f"Неожиданный ответ загрузки документа: {saved!r}")
        document = saved.get("doc") if isinstance(saved.get("doc"), dict) else saved
        return _attachment("doc", document, ("id", "doc_id"))

    def _upload_video(self, item: LocalMedia) -> str:
        # wallpost=0: video.save не должен сам писать на стену, иначе получится
        # вторая запись без нашей шапки «Автор: ...». Вложение добавим в wall.post.
        saved = self._with_retry(
            lambda: self.upload.video(
                video_file=str(item.path),
                name=item.filename[:128],
                group_id=self.group_id,
                wallpost=0,
            ),
            f"видео {item.filename}",
        )
        if not isinstance(saved, dict):
            raise RuntimeError(f"Неожиданный ответ загрузки видео: {saved!r}")
        if saved.get("video_id") is None:
            saved = {**saved, "video_id": saved.get("id")}
        if saved.get("owner_id") is None:
            saved = {**saved, "owner_id": self.owner_id}
        return _attachment("video", saved, ("video_id", "id"))

    def _send_posts(self, text: str, attachments: list[str]) -> list[int]:
        text = _trim_text(text)
        chunks = [
            attachments[index : index + VK_WALL_ATTACHMENTS_LIMIT]
            for index in range(0, len(attachments), VK_WALL_ATTACHMENTS_LIMIT)
        ] or [[]]

        post_ids: list[int] = []
        for index, chunk in enumerate(chunks):
            part = text if index == 0 else _continuation_text(text)
            post_id = self._wall_post(part, chunk)
            post_ids.append(post_id)
            logger.info(
                "Пост опубликован: https://vk.com/wall%s_%s",
                self.owner_id,
                post_id,
            )
        return post_ids

    def _wall_post(self, text: str, attachments: list[str]) -> int:
        def action() -> int:
            params: dict[str, object] = {
                "owner_id": self.owner_id,
                "from_group": 1,
                "message": text,
            }
            if attachments:
                params["attachments"] = ",".join(attachments)
            response = self.vk.wall.post(**params)
            return _extract_post_id(response)

        return self._with_retry(action, "wall.post")

    def _with_retry(self, action: Callable[[], T], description: str) -> T:
        delay = 2.0
        last_error: Exception | None = None
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                return action()
            except VkApiError as exc:
                last_error = exc
                code = getattr(exc, "code", None)
                if code not in _RETRYABLE_API_CODES or attempt == _MAX_ATTEMPTS:
                    raise
                logger.warning(
                    "VK %s: %s. Повтор %s/%s через %.0f с",
                    description,
                    exc,
                    attempt,
                    _MAX_ATTEMPTS,
                    delay,
                )
            except requests.RequestException as exc:
                last_error = exc
                if attempt == _MAX_ATTEMPTS:
                    raise
                logger.warning(
                    "Сеть при %s: %s. Повтор %s/%s через %.0f с",
                    description,
                    exc,
                    attempt,
                    _MAX_ATTEMPTS,
                    delay,
                )
            time.sleep(delay)
            delay *= 2
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Не удалось выполнить {description}")


def _attachment(prefix: str, payload: dict, id_keys: tuple[str, ...]) -> str:
    """Формат ВК: type{owner_id}_{id}[_access_key].

    access_key нужен, чтобы wall.post увидел только что загруженный файл.
    Без него свежие photo/doc часто не прикрепляются.
    """
    owner_id = payload.get("owner_id")
    media_id = next((payload.get(key) for key in id_keys if payload.get(key) is not None), None)
    if owner_id is None or media_id is None:
        raise RuntimeError(f"В ответе VK нет идентификатора вложения: {payload!r}")
    attachment = f"{prefix}{owner_id}_{media_id}"
    access_key = payload.get("access_key")
    if access_key:
        attachment = f"{attachment}_{access_key}"
    return attachment


def _extract_post_id(response: object) -> int:
    if isinstance(response, dict) and response.get("post_id") is not None:
        return int(response["post_id"])
    if isinstance(response, int):
        return response
    raise RuntimeError(f"Неожиданный ответ wall.post: {response!r}")


def _trim_text(text: str) -> str:
    if len(text) <= VK_WALL_TEXT_LIMIT:
        return text
    marker = "\n\n[текст обрезан]"
    return text[: VK_WALL_TEXT_LIMIT - len(marker)] + marker


def _continuation_text(text: str) -> str:
    header = text.split("\n", 1)[0]
    return _trim_text(f"{header}\n\n[продолжение]")


def _group_name(payload: object, fallback: int) -> str:
    groups: object = payload
    if isinstance(payload, dict):
        groups = payload.get("groups") or []
    if isinstance(groups, list) and groups and isinstance(groups[0], dict):
        return str(groups[0].get("name") or fallback)
    return str(fallback)
