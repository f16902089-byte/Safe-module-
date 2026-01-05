# coding: utf-8
"""
Safe — модуль для userbot'а на Telethon.

Изменения:
- Устранена дублирующая отправка: теперь каждое удаление обрабатывается один раз.
  Реализован небольшой TTL-кеш processed_recent для (chat_id, msg_id).
- Текст при наличии прикрепляется к фото/видео/файлу как caption (при отправке в лог),
  при этом сохраняется и отдельный мета-блок (blockquote с жирными метками).
- Поддержка голосовых сообщений и video_note ("кружочки") оставлена — модуль пытается
  детектировать и сохранять корректные расширения/форматы.
- Если сообщение автоматически удалилось после просмотра (view-once / self-destruct) —
  событие MessageDeleted обычно приходит, и модуль обработает его как обычное удаление,
  отправив содержимое в лог (при условии, что оно было закешировано при получении).
- Команда .safe logs для привязки/показа/отключения чата логов (вся логика осталась).
- Создатель: @NFTkarma
"""
import io
import logging
import mimetypes
import time
from collections import OrderedDict, defaultdict

from telethon import events
from telethon.tl.types import User
from telethon.errors import FloodWaitError

from .. import loader, utils

logger = logging.getLogger(__name__)


@loader.tds
class Safe(loader.Module):
    strings = {
        "name": "Safe",
        "usage": (
            "<b>Использование Safe (логи):</b>\n"
            "<code>.safe logs</code> — показать текущий чат логов\n"
            "<code>.safe logs here</code> — привязать текущий чат как чат логов\n"
            "<code>.safe logs &lt;@username|chat_id&gt;</code> — привязать указанный чат как лог\n"
            "<code>.safe logs off</code> — отключить логирование\n"
            "Создатель: @NFTkarma<emoji document_id=5390858914885568318>😷</emoji>"
        ),
        "log_set": "<emoji document_id=5400203303432780864>✅</emoji> <b>Чат логов установлен:</b> <code>{}</code>",
        "log_unset": "<emoji document_id=5400111356772905255>😘</emoji> <b>Чат логов отключён.</b>",
        "log_show": "<emoji document_id=5397610749503765867>😴</emoji> <b>Текущий чат логов:</b> <code>{}</code>",
        "no_log": "<emoji document_id=5326018884539553727>🤣</emoji> <b>Чат логов не настроен. Установи с помощью</b> <code>.safe logs here</code> <b>или</b> <code>.safe logs &lt;@username|chat_id&gt;</code>",
        "saved_deleted_text": (
            "<blockquote>"
            "<b><emoji document_id=5391349962791483884>💀</emoji> Удалено в приватном чате</b>\n"
            "<b>Пользователь:</b> {who}\n"
            "<b>Тип:</b> <b>Текст</b>\n\n"
            "<code>{text}</code>\n\n"
            "<b>Создатель:<emoji document_id=5390858914885568318>😷</emoji></b> @NFTkarma"
            "</blockquote>"
        ),
        "saved_deleted_media": (
            "<blockquote>"
            "<b><emoji document_id=5391349962791483884>💀</emoji> Удалено в приватном чате</b>\n"
            "<b>Пользователь:</b> {who}\n"
            "<b>Тип:</b> <b>Медиа/Файл</b>\n"
            "<b>Имя файла:</b> <b>{filename}</b>\n\n"
            "<b>Создатель:<emoji document_id=5390858914885568318>😷</emoji></b> @NFTkarma"
            "</blockquote>"
        ),
        "not_cached": (
            "<blockquote>"
            "<b>ℹ️ Удалено сообщение, но оно не было закешировано — содержимое недоступно.</b>\n"
            "<b>Создатель:<emoji document_id=5390858914885568318>😷</emoji></b> @NFTkarma"
            "</blockquote>"
        ),
        "error": "<blockquote><b>❌ Ошибка:</b> <code>{}</code></blockquote>"
    }

    def __init__(self):
        # cache per chat: { chat_id: OrderedDict({ msg_id: saved_dict }) }
        # saved_dict: { "text", "media"(bytes), "file_name", "sender_id", "media_type", "ext", "mime" }
        self.cache = {}
        # index msg_id -> set(chat_id) to recover when event lacks peer info
        self.msgid_index = defaultdict(set)

        # processed recent deletions to avoid duplicates: {(chat_id, msg_id): timestamp}
        self.processed_recent = {}
        self.processed_ttl = 10.0  # seconds to keep processed entries

        # limits
        self.max_per_chat = 400
        self.global_max = 3000

        # handlers placeholders
        self._new_handler = None
        self._del_handler = None

        # client/db placeholders
        self.client = None
        self.db = None

    async def client_ready(self, client, db):
        self.client = client
        self.db = db
        # register handlers immediately (module is always active)
        try:
            await self._register_handlers()
            logger.info("Safe: handlers registered on startup")
        except Exception as e:
            logger.exception("Safe: failed to register handlers on startup: %s", e)

    # ---------- internal helpers ----------
    def _ensure_chat_cache(self, chat_id):
        if chat_id not in self.cache:
            self.cache[chat_id] = OrderedDict()

    def _index_message(self, chat_id, msg_id):
        self.msgid_index[msg_id].add(chat_id)

    def _unindex_message(self, chat_id, msg_id):
        s = self.msgid_index.get(msg_id)
        if not s:
            return
        s.discard(chat_id)
        if not s:
            self.msgid_index.pop(msg_id, None)

    def _prune_cache_if_needed(self):
        total = sum(len(v) for v in self.cache.values())
        if total <= self.global_max:
            return
        while total > self.global_max:
            for chat_id, od in list(self.cache.items()):
                if od:
                    old_msg_id, _ = od.popitem(last=False)
                    self._unindex_message(chat_id, old_msg_id)
                    total -= 1
                    break
                else:
                    self.cache.pop(chat_id, None)

    def _prune_processed(self):
        """Remove old processed entries to free memory."""
        now = time.time()
        to_remove = [k for k, ts in self.processed_recent.items() if now - ts > self.processed_ttl]
        for k in to_remove:
            self.processed_recent.pop(k, None)

    # ---------- media detection / naming ----------
    def _detect_media_type(self, msg):
        m = getattr(msg, "media", None)
        if not m:
            return None
        if getattr(m, "photo", None) is not None:
            return "photo"
        doc = getattr(m, "document", None)
        if doc:
            for a in getattr(doc, "attributes", []) or []:
                clsname = a.__class__.__name__.lower()
                if "audio" in clsname or "voice" in clsname:
                    try:
                        if getattr(a, "voice", False) or getattr(a, "voice_note", False):
                            return "voice"
                    except Exception:
                        return "voice"
                if "video" in clsname:
                    try:
                        if getattr(a, "round_message", False):
                            return "video_note"
                    except Exception:
                        return "video"
                if "sticker" in clsname:
                    return "sticker"
            mime = getattr(doc, "mime_type", "") or ""
            if mime.startswith("audio/"):
                return "voice"
            if mime.startswith("video/"):
                return "video"
            if mime.startswith("image/"):
                return "image"
            return "document"
        if getattr(m, "video", None) is not None:
            return "video"
        return None

    def _choose_extension(self, msg, doc):
        if doc is not None:
            for a in getattr(doc, "attributes", []) or []:
                fn = getattr(a, "file_name", None)
                if fn and "." in fn:
                    return "." + fn.split(".")[-1]
            mime = getattr(doc, "mime_type", None)
            if mime:
                try:
                    ext = mimetypes.guess_extension(mime.split(";")[0].strip())
                    if ext:
                        return ext
                except Exception:
                    pass
            fn2 = getattr(doc, "file_name", None)
            if fn2 and "." in fn2:
                return "." + fn2.split(".")[-1]
            for a in getattr(doc, "attributes", []) or []:
                clsname = a.__class__.__name__.lower()
                if "audio" in clsname or "voice" in clsname:
                    return ".ogg"
                if "video" in clsname:
                    return ".mp4"
            return ".bin"
        m = getattr(msg, "media", None)
        if getattr(m, "photo", None) is not None:
            return ".jpg"
        if getattr(m, "video", None) is not None:
            return ".mp4"
        return ".bin"

    # ---------- sending ----------
    async def _send_to_log(self, header_text: str, saved: dict, msg_id: int):
        """
        Send header (blockquote) and the media/text to configured log chat.
        Text (if exists) is attached as caption to media (so it appears with the photo/video).
        """
        log_chat = self.db.get("Safe", "log_chat", None)
        if not log_chat:
            return

        # Resolve log chat entity
        target = log_chat
        if isinstance(target, str) and target.isdigit():
            target = int(target)

        try:
            try:
                target_entity = await self.client.get_entity(target)
            except Exception:
                try:
                    target_entity = int(target)
                except Exception:
                    logger.warning("Safe: cannot resolve log chat: %s", target)
                    return

            # send header (blockquote + bold)
            await self.client.send_message(target_entity, header_text, parse_mode="html")

            # if there is media - send it WITH caption = original text (so it's attached)
            if saved.get("media"):
                bio = io.BytesIO(saved["media"])
                fname = saved.get("file_name") or f"file_{msg_id}"
                if "." not in fname and saved.get("ext"):
                    fname = fname + saved.get("ext")
                bio.name = fname
                caption = None
                if saved.get("text"):
                    caption = utils.escape_html(saved["text"])
                    if len(caption) > 1000:
                        caption = caption[:997] + "..."
                try:
                    await self.client.send_file(target_entity, bio, caption=caption, parse_mode="html", force_document=False)
                except Exception:
                    try:
                        bio.seek(0)
                        await self.client.send_file(target_entity, bio, caption=caption, parse_mode="html", force_document=True)
                    except Exception as e:
                        logger.exception("Safe: failed sending media to log chat: %s", e)
                        try:
                            await self.client.send_message(target_entity, self.strings["error"].format(utils.escape_html(str(e))), parse_mode="html")
                        except Exception:
                            pass
            else:
                # No media — header already contains the text body, nothing else to send.
                pass
        except FloodWaitError as e:
            logger.warning("Safe: FloodWait when sending to log chat: %s", e)
        except Exception as e:
            logger.exception("Safe: error sending to log chat: %s", e)

    # ---------- events ----------
    async def _on_new_message(self, event: events.NewMessage.Event):
        """
        Cache incoming and outgoing messages in private chats only.
        We try to download media immediately so if it later disappears (self-destruct/view-once),
        we have a copy.
        """
        try:
            if not getattr(event, "is_private", False):
                return
        except Exception:
            return

        msg = getattr(event, "message", None)
        if not msg:
            return

        # Determine canonical chat entity
        try:
            chat_entity = await event.get_chat()
            if not isinstance(chat_entity, User):
                return
            chat_id = chat_entity.id
        except Exception:
            return

        saved = {
            "text": None,
            "media": None,
            "file_name": None,
            "sender_id": msg.sender_id,
            "media_type": None,
            "ext": None,
            "mime": None
        }

        if msg.message:
            saved["text"] = msg.message

        if msg.media:
            try:
                doc = getattr(msg.media, "document", None)
                b = await self.client.download_media(msg.media, bytes)
                if b:
                    saved["media"] = b
                    ext = self._choose_extension(msg, doc)
                    saved["ext"] = ext
                    # try to get file name
                    fname = None
                    if doc and getattr(doc, "attributes", None):
                        for a in doc.attributes:
                            fn = getattr(a, "file_name", None)
                            if fn:
                                fname = fn
                                break
                    if not fname and doc:
                        fname = getattr(doc, "file_name", None)
                    if not fname:
                        mtype = self._detect_media_type(msg) or "file"
                        fname = f"{mtype}_{msg.id}{ext or ''}"
                    else:
                        if "." not in fname and ext:
                            fname = fname + ext
                    saved["file_name"] = fname
                    saved["media_type"] = self._detect_media_type(msg)
                    if doc:
                        saved["mime"] = getattr(doc, "mime_type", None)
            except Exception:
                # downloading may fail for view-once until viewed — best effort
                logger.exception("Safe: cannot download media for caching")

        # store in cache and index
        self._ensure_chat_cache(chat_id)
        od = self.cache[chat_id]
        od[msg.id] = saved
        self._index_message(chat_id, msg.id)

        # enforce per-chat limit
        while len(od) > self.max_per_chat:
            old_msg_id, _ = od.popitem(last=False)
            self._unindex_message(chat_id, old_msg_id)

        # global trim
        self._prune_cache_if_needed()

    async def _process_deleted_for_chat(self, chat_id, msg_id):
        """
        When we know chat_id and msg_id, prepare notification and send to log chat.
        Ensures each (chat_id,msg_id) is processed only once (short TTL).
        """
        # prune processed map
        self._prune_processed()
        key = (chat_id, msg_id)
        if key in self.processed_recent:
            # already processed recently -> skip
            return
        # mark as processing/processed now
        self.processed_recent[key] = time.time()

        saved = None
        if chat_id in self.cache:
            saved = self.cache[chat_id].pop(msg_id, None)
            self._unindex_message(chat_id, msg_id)

        # get info about the chat's user (the one who chatted with us)
        try:
            chat_entity = await self.client.get_entity(chat_id)
        except Exception:
            chat_entity = None

        if saved:
            sender_id = saved.get("sender_id")
            # determine user label (who sent the original message)
            who = "Кто-то"
            try:
                if sender_id is not None:
                    if (await self.client.get_me()).id == sender_id:
                        who = "Вы"
                    else:
                        s_ent = await self.client.get_entity(sender_id)
                        name = getattr(s_ent, "first_name", "") or ""
                        last = getattr(s_ent, "last_name", "") or ""
                        username = getattr(s_ent, "username", None)
                        if name or last:
                            who = utils.escape_html((name + " " + last).strip())
                        elif username:
                            who = "@" + utils.escape_html(username)
                        else:
                            who = f"id{sender_id}"
            except Exception:
                who = f"id{sender_id}"

            # Build header and send to configured log chat
            if saved.get("text") and not saved.get("media"):
                escaped = utils.escape_html(saved["text"])
                header = self.strings["saved_deleted_text"].format(who=who, text=escaped)
                await self._send_to_log(header, saved, msg_id)
            else:
                filename = utils.escape_html(saved.get("file_name") or f"file_{msg_id}")
                header = self.strings["saved_deleted_media"].format(who=who, filename=filename)
                # include any accompanying text inside header blockquote as well
                if saved.get("text"):
                    txt = utils.escape_html(saved["text"])
                    header += f"\n<blockquote><b>Сопроводительный текст:</b>\n<code>{txt}</code></blockquote>"
                await self._send_to_log(header, saved, msg_id)
        else:
            # Not cached — report to log that something was deleted but no cache
            log_chat = self.db.get("Safe", "log_chat", None)
            if not log_chat:
                return
            try:
                who_label = "Пользователь"
                if chat_entity:
                    name = getattr(chat_entity, "first_name", "") or ""
                    last = getattr(chat_entity, "last_name", "") or ""
                    username = getattr(chat_entity, "username", None)
                    if name or last:
                        who_label = utils.escape_html((name + " " + last).strip())
                    elif username:
                        who_label = "@" + utils.escape_html(username)
                msg = (
                    "<blockquote>"
                    "<b>ℹ️ Удалено сообщение, но оно не было закешировано — содержимое недоступно.</b>\n"
                    f"<b>Чат:</b> {who_label}\n\n"
                    "<b>Создатель:</b> @NFTkarma"
                    "</blockquote>"
                )
                await self._send_to_log(msg, {}, msg_id)
            except Exception:
                pass

    async def _on_message_deleted(self, event: events.MessageDeleted.Event):
        """
        High-level delete event. Try to resolve peer -> chat_id.
        If peer missing/unresolvable, try to find chat_id via msgid_index.
        """
        deleted_ids = getattr(event, "deleted_ids", None)
        if not deleted_ids:
            return

        # Try peer first
        peer = getattr(event, "peer_id", None) or getattr(event, "chat_id", None)
        if peer is not None:
            try:
                entity = await self.client.get_entity(peer)
                if isinstance(entity, User):
                    chat_id = entity.id
                    for msg_id in deleted_ids:
                        await self._process_deleted_for_chat(chat_id, msg_id)
                    return
            except Exception:
                # cannot resolve peer, fall back to index
                logger.debug("Safe: cannot resolve peer in MessageDeleted, falling back to index")
                pass

        # Fallback: find chat_id(s) by msgid_index
        for msg_id in deleted_ids:
            chat_ids = list(self.msgid_index.get(msg_id, []))
            if chat_ids:
                for chat_id in chat_ids:
                    await self._process_deleted_for_chat(chat_id, msg_id)
            else:
                logger.debug("Safe: deleted msg_id %s not found in index", msg_id)

    # ---------- registration ----------
    async def _register_handlers(self):
        if self._new_handler is None:
            # cache incoming and outgoing new messages in private chats
            # Important: pass function object, not awaited coroutine
            self.client.add_event_handler(self._on_new_message, events.NewMessage(incoming=True, outgoing=True))
            self._new_handler = self._on_new_message
        if self._del_handler is None:
            # high-level deletes
            self.client.add_event_handler(self._on_message_deleted, events.MessageDeleted())
            self._del_handler = self._on_message_deleted

    async def _unregister_handlers(self):
        if self._new_handler is not None:
            try:
                self.client.remove_event_handler(self._new_handler, events.NewMessage(incoming=True, outgoing=True))
            except Exception:
                try:
                    self.client.remove_event_handler(self._new_handler)
                except Exception:
                    pass
            self._new_handler = None
        if self._del_handler is not None:
            try:
                self.client.remove_event_handler(self._del_handler, events.MessageDeleted())
            except Exception:
                try:
                    self.client.remove_event_handler(self._del_handler)
                except Exception:
                    pass
            self._del_handler = None

    # ---------- commands ----------
    @loader.command(ru_doc="Управление лог-чатом: .safe logs ...")
    async def safe(self, message):
        args = utils.get_args_raw(message)
        if not args:
            # show usage
            await utils.answer(message, self.strings["usage"])
            return

        parts = args.split(None, 1)
        cmd = parts[0].lower()

        if cmd != "logs":
            await utils.answer(message, self.strings["usage"])
            return

        sub = parts[1].strip() if len(parts) > 1 else None
        if not sub:
            log_chat = self.db.get("Safe", "log_chat", None)
            if log_chat:
                await utils.answer(message, self.strings["log_show"].format(utils.escape_html(str(log_chat))))
            else:
                await utils.answer(message, self.strings["no_log"])
            return

        sub_l = sub.lower()
        if sub_l == "here":
            target = message.chat_id or getattr(message, "peer_id", None)
            if not target:
                await utils.answer(message, self.strings["error"].format("Не удалось определить текущий чат"))
                return
            # store as string (id)
            self.db.set("Safe", "log_chat", str(target))
            await utils.answer(message, self.strings["log_set"].format(utils.escape_html(str(target))))
            return

        if sub_l == "off":
            self.db.set("Safe", "log_chat", None)
            await utils.answer(message, self.strings["log_unset"])
            return

        # try to resolve provided entity (username or id)
        try:
            tgt = sub
            if tgt.isdigit():
                tgt = int(tgt)
            entity = await self.client.get_entity(tgt)
            # store canonical id or @username
            if getattr(entity, "username", None):
                store = "@" + entity.username
            else:
                store = str(getattr(entity, "id", str(tgt)))
            self.db.set("Safe", "log_chat", store)
            await utils.answer(message, self.strings["log_set"].format(utils.escape_html(store)))
        except Exception as e:
            await utils.answer(message, self.strings["error"].format(utils.escape_html(str(e))))