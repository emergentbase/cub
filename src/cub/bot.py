"""Telegram bot interface."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import signal
import time
from contextlib import asynccontextmanager
from typing import Awaitable, Optional

from telegram import BotCommand, Message, Update
from telegram.constants import ChatAction
from telegram.error import BadRequest
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from telegram.request import HTTPXRequest

from cub.formatting import format_task_list, format_task_status
from cub.reminders import ReminderService
from cub.settings import Settings
from cub.smart_assistant import SmartAssistant
from cub.store import TaskStore
from cub.task_runner import ClaudeTaskRunner
from cub.telegram_render import render_for_telegram, split_message
from cub.text_intents import TextIntent, parse_control_intent
from cub.timeparse import parse_when

TASK_ID_RE = re.compile(r"^[0-9a-f]{8}$", re.IGNORECASE)
STATUS_LINE_RE = re.compile(r"^status(?:\s+([0-9a-f]{8}))?$", re.IGNORECASE)
REMIND_LINE_RE = re.compile(
    r"^check on (?:claude code )?task(?:\s+([0-9a-f]{8}))?\s+(?:in\s+)?(.+?)\s+from\s+now$",
    re.IGNORECASE,
)


class TelegramAssistantBot:
    """Command handlers + text routing for delegated Claude tasks."""

    TYPING_INTERVAL_SECONDS = 4.0
    SHUTDOWN_TIMEOUT_SECONDS = 8.0
    FORCE_EXIT_CODE = 130

    def __init__(self, settings: Settings, store: TaskStore):
        self.settings = settings
        self.store = store
        self.runner: ClaudeTaskRunner | None = None
        self.reminders: ReminderService | None = None
        self.assistant: SmartAssistant | None = None

        request = HTTPXRequest(
            connection_pool_size=settings.telegram_connection_pool_size,
            pool_timeout=settings.telegram_pool_timeout_seconds,
        )
        self.app = (
            Application.builder()
            .token(settings.telegram_bot_token)
            .concurrent_updates(settings.telegram_max_concurrent_updates)
            .request(request)
            .get_updates_request(request)
            .build()
        )
        self.app.post_init = self._post_init
        self.app.post_shutdown = self._post_shutdown

        self._register_handlers()

    def bind_services(
        self,
        runner: ClaudeTaskRunner,
        reminders: ReminderService,
        assistant: SmartAssistant | None = None,
    ) -> None:
        self.runner = runner
        self.reminders = reminders
        self.assistant = assistant
        if (
            assistant
            and assistant.enabled
            and self.settings.progress_formatter == "assistant"
        ):
            runner.set_progress_rewriter(assistant.format_progress)
        else:
            runner.set_progress_rewriter(None)

        if (
            assistant
            and assistant.enabled
            and self.settings.task_result_formatter == "assistant"
        ):
            runner.set_task_result_rewriter(assistant.format_task_result)
        else:
            runner.set_task_result_rewriter(None)

    async def send_text(self, chat_id: int, text: str) -> None:
        await self._send_to_chat(chat_id, text=text, reply_to_message=None)
        self.store.append_chat_message(chat_id, "assistant", text)

    def run(self) -> None:
        previous_handlers: dict[int, object] = {}

        def _handle_interrupt(sig: int, _frame) -> None:
            logging.error("Interrupt received (signal %s). Exiting immediately.", sig)
            os._exit(self.FORCE_EXIT_CODE)

        for sig in _iter_supported_signals():
            try:
                previous_handlers[sig] = signal.getsignal(sig)
                signal.signal(sig, _handle_interrupt)
            except (ValueError, OSError, RuntimeError):
                continue

        try:
            self.app.run_polling(
                allowed_updates=["message"],
                drop_pending_updates=self.settings.telegram_drop_pending_updates,
                stop_signals=(),
            )
        finally:
            for sig, handler in previous_handlers.items():
                try:
                    signal.signal(sig, handler)
                except (ValueError, OSError, RuntimeError):
                    continue

    async def _post_init(self, app: Application) -> None:
        await app.bot.set_my_commands(
            [
                BotCommand("run", "Run a delegated Claude task"),
                BotCommand("probe", "Resume/probe an existing task session"),
                BotCommand("continue", "Continue an existing task session"),
                BotCommand("status", "Show task status"),
                BotCommand("list", "List ongoing tasks"),
                BotCommand("tasks", "List recent tasks"),
                BotCommand("cancel", "Cancel a running task"),
                BotCommand("killall", "Kill all Claude Code processes"),
                BotCommand("mute", "Background tasks are already quiet"),
                BotCommand("unmute", "Background tasks stay quiet"),
                BotCommand("newsession", "Reset front assistant memory session"),
                BotCommand("remind", "Set a status reminder"),
                BotCommand("help", "Show help"),
            ]
        )
        if self.reminders:
            self.reminders.start()

    async def _post_shutdown(self, _app: Application) -> None:
        if self.reminders:
            await self._run_shutdown_step("reminder shutdown", self.reminders.stop())
        if self.runner:
            await self._run_shutdown_step("task runner shutdown", self.runner.close())
        if self.assistant:
            await self._run_shutdown_step("assistant client shutdown", self.assistant.close())

    def _register_handlers(self) -> None:
        self.app.add_handler(CommandHandler("start", self.cmd_help))
        self.app.add_handler(CommandHandler("help", self.cmd_help))
        self.app.add_handler(CommandHandler("run", self.cmd_run))
        self.app.add_handler(CommandHandler("probe", self.cmd_probe))
        self.app.add_handler(CommandHandler("continue", self.cmd_continue))
        self.app.add_handler(CommandHandler("status", self.cmd_status))
        self.app.add_handler(CommandHandler("list", self.cmd_list))
        self.app.add_handler(CommandHandler("tasks", self.cmd_tasks))
        self.app.add_handler(CommandHandler("cancel", self.cmd_cancel))
        self.app.add_handler(CommandHandler("killall", self.cmd_killall))
        self.app.add_handler(CommandHandler("mute", self.cmd_mute))
        self.app.add_handler(CommandHandler("unmute", self.cmd_unmute))
        self.app.add_handler(CommandHandler("newsession", self.cmd_newsession))
        self.app.add_handler(CommandHandler("remind", self.cmd_remind))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.on_text))

    def _allowed(self, user_id: int) -> bool:
        allow = self.settings.allowed_user_ids
        return not allow or user_id in allow

    async def _authorize(self, update: Update) -> bool:
        user = update.effective_user
        message = update.effective_message
        if not user or not message:
            return False

        if self._allowed(user.id):
            self._remember_user_message(message)
            return True

        await message.reply_text("Access denied for this Telegram user id.")
        return False

    async def cmd_help(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return
        message = update.effective_message
        if not message:
            return

        await self._send_reply(
            message,
            message.chat_id,
            "Commands:\n"
            "/run <task> - run task via Claude CLI\n"
            "/probe [task_id] [question] - resume/probe Claude session for a task\n"
            "/continue [task_id] <instruction> - continue task session with new instruction\n"
            "/status [task_id] - show status for task or latest task\n"
            "/list - list ongoing tasks\n"
            "/tasks - list recent tasks\n"
            "/cancel [task_id] [--force] - cancel latest/running task\n"
            "/killall [--graceful] - kill Claude Code processes on this machine\n"
            "/mute - compatibility no-op; tasks are already quiet by default\n"
            "/unmute - compatibility no-op; ask for /status anytime\n"
            "/newsession - reset front assistant memory session\n"
            "/remind <task_id> <when> [note] - schedule reminder\n"
            "\n"
            "Time examples: 5m, 2h, 1d, 2026-02-22T18:30\n"
            "\n"
            "Tips:\n"
            "- Sending plain text is equivalent to /run <text>\n"
            "- Natural language controls work too: 'cancel task', 'kill task ab12cd34', 'check task ab12cd34', 'continue task ab12cd34 add e2e tests'.\n"
            "- Background tasks only send the final result automatically. Ask for /status whenever you want an update.\n"
            "- Use /killall if Claude processes are stuck and you need machine cleanup."
        )

    async def cmd_run(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return

        message = update.effective_message
        chat = update.effective_chat
        if not message or not chat:
            return

        prompt = " ".join(context.args).strip()
        if not prompt:
            await self._send_reply(message, chat.id, "Usage: /run <task description>")
            return

        await self._submit_task(update, prompt)

    async def cmd_probe(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return

        message = update.effective_message
        chat = update.effective_chat
        if not message or not chat:
            return

        if not self.runner:
            await self._send_reply(message, chat.id, "Task runner is not available")
            return

        args = [arg.strip() for arg in context.args if arg.strip()]
        task_id: str | None = None
        if args and TASK_ID_RE.match(args[0]):
            task_id = args.pop(0).lower()

        task = self._lookup_task(chat.id, task_id)
        if not task:
            if task_id:
                await self._send_reply(message, chat.id, "Task not found in this chat")
            else:
                await self._send_reply(message, chat.id, "No task found to probe in this chat")
            return

        query = " ".join(args).strip() or None
        await self._queue_probe_task(update, task, query=query, label_prefix="probe")

    async def cmd_continue(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return

        message = update.effective_message
        chat = update.effective_chat
        if not message or not chat:
            return

        args = [arg.strip() for arg in context.args if arg.strip()]
        task_id: str | None = None
        if args and TASK_ID_RE.match(args[0]):
            task_id = args.pop(0).lower()

        instruction = " ".join(args).strip()
        if not instruction:
            await self._send_reply(
                message,
                chat.id,
                "Usage: /continue [task_id] <instruction>\n"
                "Example: /continue 3ffc93bf add Playwright e2e tests",
            )
            return

        task = self._lookup_task(chat.id, task_id)
        if not task:
            if task_id:
                await self._send_reply(message, chat.id, "Task not found in this chat")
            else:
                await self._send_reply(message, chat.id, "No task found to continue in this chat")
            return

        await self._queue_probe_task(
            update,
            task,
            query=instruction,
            label_prefix="continue",
        )

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return

        message = update.effective_message
        chat = update.effective_chat
        if not message or not chat:
            return

        task_id = context.args[0].strip() if context.args else None
        if task_id and not TASK_ID_RE.match(task_id):
            await self._send_reply(message, chat.id, "Task id must be 8 hex chars, e.g. a1b2c3d4")
            return

        task = self._lookup_task(chat.id, task_id)
        if not task:
            await self._send_reply(message, chat.id, "Task not found in this chat")
            return

        events = self.store.recent_events(task["id"], limit=12)
        await self._send_reply(message, chat.id, format_task_status(task, events))

    async def cmd_tasks(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return

        message = update.effective_message
        chat = update.effective_chat
        if not message or not chat:
            return

        tasks = self.store.list_tasks(chat.id, limit=10)
        await self._send_reply(message, chat.id, format_task_list(tasks))

    async def cmd_list(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return

        message = update.effective_message
        chat = update.effective_chat
        if not message or not chat:
            return

        tasks = self.store.list_tasks(chat.id, limit=20, only_active=True)
        if not tasks:
            await self._send_reply(message, chat.id, "No ongoing tasks in this chat.")
            return
        await self._send_reply(message, chat.id, format_task_list(tasks, title="Ongoing tasks:"))

    async def cmd_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return

        message = update.effective_message
        chat = update.effective_chat
        if not message or not chat:
            return

        if not self.runner:
            await self._send_reply(message, chat.id, "Task runner is not available")
            return

        args = [arg.strip() for arg in context.args if arg.strip()]
        force = any(arg.lower() in {"--force", "-f", "force", "kill"} for arg in args)
        task_id: str | None = None

        for arg in args:
            if TASK_ID_RE.match(arg):
                task_id = arg.lower()
                break

        task = self._lookup_task(chat.id, task_id)
        if not task:
            if task_id:
                await self._send_reply(message, chat.id, "Task not found in this chat")
            else:
                await self._send_reply(message, chat.id, "No task found to cancel in this chat")
            return

        result = await self.runner.cancel_task(task["id"], force=force)
        await self._send_reply(message, chat.id, result)

    async def cmd_killall(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return

        message = update.effective_message
        chat = update.effective_chat
        if not message or not chat:
            return

        if not self.runner:
            await self._send_reply(message, chat.id, "Task runner is not available")
            return

        args = {arg.strip().lower() for arg in context.args if arg.strip()}
        graceful_flags = {"--graceful", "graceful", "--term", "term"}
        force = len(args & graceful_flags) == 0

        result = await self.runner.kill_all_claude_processes(force=force)
        await self._send_reply(message, chat.id, result)

    async def cmd_mute(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return

        message = update.effective_message
        chat = update.effective_chat
        if not message or not chat:
            return

        self.store.set_updates_muted(chat.id, True)
        await self._send_reply(
            message,
            chat.id,
            "Background tasks are already quiet by default. I’ll still send the final result here.",
        )

    async def cmd_unmute(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return

        message = update.effective_message
        chat = update.effective_chat
        if not message or not chat:
            return

        self.store.set_updates_muted(chat.id, False)
        await self._send_reply(
            message,
            chat.id,
            "Auto progress updates stay off. Ask for /status anytime, and I’ll still send the final result here.",
        )

    async def cmd_newsession(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return

        message = update.effective_message
        chat = update.effective_chat
        if not message or not chat:
            return

        if not self.assistant or self.settings.front_assistant_provider != "claude_cli":
            await self._send_reply(
                message,
                chat.id,
                "Front assistant session reset is available only with FRONT_ASSISTANT_PROVIDER=claude_cli.",
            )
            return

        session_id = self.store.reset_front_session_id(chat.id)
        await self._send_reply(
            message,
            chat.id,
            f"Started a new front assistant session for this chat ({session_id[:8]}...).",
        )

    async def cmd_remind(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return

        message = update.effective_message
        user = update.effective_user
        chat = update.effective_chat
        if not message or not user or not chat:
            return

        if not self.reminders:
            await self._send_reply(message, chat.id, "Reminder service is not available")
            return

        if not context.args:
            await self._send_reply(message, chat.id, "Usage: /remind <task_id> <when> [note]")
            return

        args = list(context.args)
        task_id: Optional[str] = None

        if args and TASK_ID_RE.match(args[0]):
            task_id = args.pop(0)

        if not task_id:
            latest = self.store.latest_task_for_chat(chat.id)
            if not latest:
                await self._send_reply(message, chat.id, "No tasks found in this chat")
                return
            task_id = latest["id"]

        if not args:
            await self._send_reply(message, chat.id, "Usage: /remind <task_id> <when> [note]")
            return

        when_spec = args.pop(0)
        note = " ".join(args).strip() or None

        task = self._lookup_task(chat.id, task_id)
        if not task:
            await self._send_reply(message, chat.id, "Task not found in this chat")
            return

        try:
            due_at = parse_when(when_spec)
        except ValueError as exc:
            await self._send_reply(message, chat.id, str(exc))
            return

        reminder_id = self.reminders.schedule_reminder(
            task_id=task_id,
            chat_id=chat.id,
            user_id=user.id,
            due_at=due_at,
            note=note,
        )

        due_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(due_at))
        await self._send_reply(
            message,
            chat.id,
            f"Reminder #{reminder_id} set for task {task_id} at {due_str}"
            + (f" ({note})" if note else "")
        )

    async def on_text(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._authorize(update):
            return

        message = update.effective_message
        chat = update.effective_chat
        if not message or not chat or not message.text:
            return

        text = message.text.strip()

        status_match = STATUS_LINE_RE.match(text)
        if status_match:
            task_id = status_match.group(1)
            task = self._lookup_task(chat.id, task_id)
            if not task:
                await self._send_reply(message, chat.id, "Task not found in this chat")
                return
            events = self.store.recent_events(task["id"], limit=12)
            await self._send_reply(message, chat.id, format_task_status(task, events))
            return

        remind_match = REMIND_LINE_RE.match(text)
        if remind_match:
            task_id = remind_match.group(1)
            when_spec = remind_match.group(2)
            await self._create_inline_reminder(message, chat.id, update.effective_user.id, task_id, when_spec)
            return

        intent = parse_control_intent(text)
        if intent:
            handled = await self._handle_control_intent(update, intent)
            if handled:
                return

        await self._handle_plain_text(update, text)

    async def _handle_plain_text(self, update: Update, text: str) -> None:
        message = update.effective_message
        user = update.effective_user
        chat = update.effective_chat
        if not message or not user or not chat:
            return

        if self.assistant and self.assistant.enabled:
            async with self._typing(chat.id):
                decision = await self.assistant.decide(chat.id, text)
            if decision.action == "reply" and decision.reply:
                await self._send_reply(message, chat.id, decision.reply)
                return
            await self._submit_task(update, decision.delegate_prompt or text, label_override=decision.label)
            return

        await self._submit_task(update, text)

    async def _submit_task(
        self,
        update: Update,
        prompt: str,
        *,
        label_override: str | None = None,
        resume_session_id: str | None = None,
    ) -> None:
        message = update.effective_message
        user = update.effective_user
        chat = update.effective_chat
        if not message or not user or not chat:
            return

        if not self.runner:
            await self._send_reply(message, chat.id, "Task runner is not available")
            return

        label = _label_from_prompt(label_override or prompt)
        task = await self.runner.queue_task(
            chat_id=chat.id,
            user_id=user.id,
            prompt=prompt,
            label=label,
            resume_session_id=resume_session_id,
        )

        session_id = str(task.get("claude_session_id") or "").strip()
        lines = [f"Queued task {task['id']} ({label})."]
        if session_id:
            lines.append(f"Session id: {session_id}")
        lines.append("I’ll send the final result here. If you want an update before then, ask for status.")
        await self._send_reply(
            message,
            chat.id,
            "\n".join(lines),
        )

    def _lookup_task(self, chat_id: int, task_id: str | None) -> dict | None:
        if task_id:
            task = self.store.get_task(task_id)
        else:
            task = self.store.latest_task_for_chat(chat_id)

        if not task:
            return None
        if int(task["chat_id"]) != int(chat_id):
            return None
        return task

    async def _create_inline_reminder(
        self,
        message,
        chat_id: int,
        user_id: int,
        task_id: str | None,
        when_spec: str,
    ) -> None:
        if not self.reminders:
            await self._send_reply(message, chat_id, "Reminder service is not available")
            return

        task = self._lookup_task(chat_id, task_id)
        if not task:
            await self._send_reply(message, chat_id, "Task not found in this chat")
            return

        try:
            due_at = parse_when(when_spec)
        except ValueError as exc:
            await self._send_reply(message, chat_id, str(exc))
            return

        reminder_id = self.reminders.schedule_reminder(
            task_id=task["id"],
            chat_id=chat_id,
            user_id=user_id,
            due_at=due_at,
            note="status check",
        )
        due_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(due_at))
        await self._send_reply(
            message,
            chat_id,
            f"Reminder #{reminder_id} set for task {task['id']} at {due_str}",
        )

    async def _handle_control_intent(self, update: Update, intent: TextIntent) -> bool:
        message = update.effective_message
        chat = update.effective_chat
        if not message or not chat:
            return False

        if intent.action == "mute_updates":
            self.store.set_updates_muted(chat.id, True)
            await self._send_reply(
                message,
                chat.id,
                "Background tasks are already quiet by default. I’ll still send the final result here.",
            )
            return True

        if intent.action == "unmute_updates":
            self.store.set_updates_muted(chat.id, False)
            await self._send_reply(
                message,
                chat.id,
                "Auto progress updates stay off. Ask for /status anytime, and I’ll still send the final result here.",
            )
            return True

        if intent.action == "probe_task":
            task = self._lookup_task(chat.id, intent.task_id)
            if not task:
                if intent.task_id:
                    await self._send_reply(message, chat.id, "Task not found in this chat")
                else:
                    await self._send_reply(message, chat.id, "No task found to probe in this chat")
                return True

            await self._queue_probe_task(update, task, query=intent.query, label_prefix="probe")
            return True

        if intent.action == "continue_task":
            task = self._lookup_task(chat.id, intent.task_id)
            if not task:
                if intent.task_id:
                    await self._send_reply(message, chat.id, "Task not found in this chat")
                else:
                    await self._send_reply(message, chat.id, "No task found to continue in this chat")
                return True

            if not intent.query:
                await self._send_reply(
                    message,
                    chat.id,
                    "Add an instruction after continue, e.g.:\n"
                    "continue task 3ffc93bf add e2e tests",
                )
                return True

            await self._queue_probe_task(update, task, query=intent.query, label_prefix="continue")
            return True

        if intent.action != "cancel_task":
            return False

        if not self.runner:
            await self._send_reply(message, chat.id, "Task runner is not available")
            return True

        task = self._lookup_task(chat.id, intent.task_id)
        if not task:
            if intent.task_id:
                await self._send_reply(message, chat.id, "Task not found in this chat")
            else:
                await self._send_reply(message, chat.id, "No task found to cancel in this chat")
            return True

        result = await self.runner.cancel_task(task["id"], force=intent.force)
        await self._send_reply(message, chat.id, result)
        return True

    async def _queue_probe_task(
        self,
        update: Update,
        task: dict,
        *,
        query: str | None,
        label_prefix: str,
    ) -> None:
        message = update.effective_message
        chat = update.effective_chat
        if not message or not chat:
            return

        session_id = str(task.get("claude_session_id") or "").strip()
        if not session_id:
            await self._send_reply(
                message,
                chat.id,
                "This task does not have a stored Claude session id, so it cannot be probed.",
            )
            return

        prompt = query or _default_probe_prompt(task["id"])
        await self._submit_task(
            update,
            prompt,
            label_override=f"{label_prefix} {task['id']}",
            resume_session_id=session_id,
        )

    def _remember_user_message(self, message: Message) -> None:
        text = (message.text or "").strip()
        if not text:
            return
        self.store.append_chat_message(message.chat_id, "user", text)

    async def _send_reply(self, message: Message | None, chat_id: int, text: str) -> None:
        if not message:
            return
        await self._send_to_chat(chat_id, text=text, reply_to_message=message)
        self.store.append_chat_message(chat_id, "assistant", text)

    async def _send_to_chat(
        self,
        chat_id: int,
        *,
        text: str,
        reply_to_message: Message | None,
    ) -> None:
        for idx, chunk in enumerate(split_message(text)):
            rendered, parse_mode = render_for_telegram(chunk, self.settings.telegram_render_mode)
            try:
                if idx == 0 and reply_to_message:
                    await reply_to_message.reply_text(rendered, parse_mode=parse_mode)
                else:
                    await self.app.bot.send_message(
                        chat_id=chat_id,
                        text=rendered,
                        parse_mode=parse_mode,
                    )
            except BadRequest:
                if idx == 0 and reply_to_message:
                    await reply_to_message.reply_text(chunk)
                else:
                    await self.app.bot.send_message(chat_id=chat_id, text=chunk)

    @asynccontextmanager
    async def _typing(self, chat_id: int):
        async def loop() -> None:
            while True:
                try:
                    await self.app.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
                except Exception:
                    pass
                await asyncio.sleep(self.TYPING_INTERVAL_SECONDS)

        task = asyncio.create_task(loop())
        try:
            yield
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _run_shutdown_step(self, label: str, coro: Awaitable[None]) -> None:
        try:
            await asyncio.wait_for(coro, timeout=self.SHUTDOWN_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            logging.warning("%s timed out after %.1fs", label, self.SHUTDOWN_TIMEOUT_SECONDS)
        except Exception:
            logging.exception("%s failed during shutdown", label)


def _label_from_prompt(prompt: str, max_len: int = 50) -> str:
    text = " ".join(prompt.strip().split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _default_probe_prompt(task_id: str) -> str:
    return (
        f"Check on task {task_id} and provide a concise status update, "
        "what is already done, and the next step."
    )


def _iter_supported_signals() -> tuple[int, ...]:
    signals = [signal.SIGINT]
    if hasattr(signal, "SIGTERM"):
        signals.append(signal.SIGTERM)
    return tuple(signals)
