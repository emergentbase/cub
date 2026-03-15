"""Async Claude CLI task execution with quiet background runs."""

from __future__ import annotations

import asyncio
import os
import shlex
import signal
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable
from uuid import uuid4

from cub.formatting import summarize_events
from cub.models import TASK_CANCELED, TASK_COMPLETED, TASK_FAILED
from cub.settings import Settings
from cub.store import TaskStore
from cub.stream_parse import extract_text_snippet

SendMessage = Callable[[int, str], Awaitable[None]]
ProgressRewriter = Callable[[str, list[str]], Awaitable[str | None]]
TaskResultRewriter = Callable[
    [str, str, int | None, list[str], str | None],
    Awaitable[str | None],
]


@dataclass
class _RunningTask:
    process: asyncio.subprocess.Process
    chat_id: int
    pending_snippets: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ProcessCleanupReport:
    """Summary of machine-level Claude process cleanup."""

    matched: int
    stopped: int
    force_kill_attempts: int
    permission_denied: int
    remaining: int
    term_signals_sent: int


class ClaudeTaskRunner:
    """Background worker that executes Claude CLI tasks."""

    SHUTDOWN_WAIT_SECONDS = 3.0
    FORCE_KILL_WAIT_SECONDS = 3.0

    def __init__(self, store: TaskStore, settings: Settings, send_message: SendMessage):
        self.store = store
        self.settings = settings
        self.send_message = send_message
        self._progress_rewriter: ProgressRewriter | None = None
        self._task_result_rewriter: TaskResultRewriter | None = None

        self._running: dict[str, _RunningTask] = {}
        self._workers: dict[str, asyncio.Task[None]] = {}

    def set_progress_rewriter(self, rewriter: ProgressRewriter | None) -> None:
        """Optional formatter for progress updates (for example, front assistant rewrite)."""
        self._progress_rewriter = rewriter

    def set_task_result_rewriter(self, rewriter: TaskResultRewriter | None) -> None:
        """Optional formatter for final task result messages."""
        self._task_result_rewriter = rewriter

    async def close(self) -> None:
        """Best-effort shutdown: terminate active subprocesses."""
        for task_id, ctx in list(self._running.items()):
            process = ctx.process
            if process.returncode is None:
                await _signal_process_tree(process, force=False)

        if self._workers:
            done, pending = await asyncio.wait(
                self._workers.values(), timeout=self.SHUTDOWN_WAIT_SECONDS
            )
            if pending:
                for task_id, ctx in list(self._running.items()):
                    if ctx.process.returncode is None:
                        await _signal_process_tree(ctx.process, force=True)
                done2, pending = await asyncio.wait(
                    pending, timeout=self.FORCE_KILL_WAIT_SECONDS
                )
                del done2
                for item in pending:
                    item.cancel()
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*pending, return_exceptions=True),
                        timeout=self.FORCE_KILL_WAIT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    pass

        self._workers.clear()
        self._running.clear()

    async def queue_task(
        self,
        *,
        chat_id: int,
        user_id: int,
        prompt: str,
        label: str,
        working_dir: str | None = None,
        resume_session_id: str | None = None,
    ) -> dict:
        """Create and start a new Claude delegated task."""
        cwd = self._resolve_cwd(working_dir)
        resume_id = (resume_session_id or "").strip() or None
        session_id = resume_id or str(uuid4())
        command_parts = [self.settings.claude_command, *self.settings.claude_args]
        if resume_id:
            command_parts.extend(["--resume", session_id])
        else:
            command_parts.extend(["--session-id", session_id])
        command_parts.extend(["-p", prompt])
        command_string = shlex.join(command_parts)

        task = self.store.create_task(
            chat_id=chat_id,
            user_id=user_id,
            prompt=prompt,
            label=label,
            command=command_string,
            cwd=str(cwd),
            claude_session_id=session_id,
        )

        worker = asyncio.create_task(self._run_task(task["id"], command_parts, cwd))
        self._workers[task["id"]] = worker
        worker.add_done_callback(lambda _t, tid=task["id"]: self._workers.pop(tid, None))
        return task

    async def cancel_task(self, task_id: str, *, force: bool = False) -> str:
        task = self.store.get_task(task_id)
        if not task:
            return f"Task {task_id} not found"

        status = task.get("status")
        if status in {TASK_COMPLETED, TASK_FAILED, TASK_CANCELED}:
            return f"Task {task_id} is already {status}"

        self.store.cancel_task(task_id)

        running = self._running.get(task_id)
        killed = False
        if running and running.process.returncode is None:
            if force:
                await _signal_process_tree(running.process, force=True)
                killed = True
            else:
                await _signal_process_tree(running.process, force=False)

            try:
                await asyncio.wait_for(running.process.wait(), timeout=5)
            except asyncio.TimeoutError:
                await _signal_process_tree(running.process, force=True)
                killed = True
                try:
                    await asyncio.wait_for(running.process.wait(), timeout=5)
                except asyncio.TimeoutError:
                    pass

        if force or killed:
            return f"Task {task_id} canceled (process killed)"
        return f"Task {task_id} canceled"

    async def kill_all_claude_processes(self, *, force: bool = True) -> str:
        """Terminate Claude-related processes, including this runner's active tasks."""
        canceled_tasks = 0
        for task_id in list(self._running):
            await self.cancel_task(task_id, force=True)
            canceled_tasks += 1

        report = await cleanup_claude_processes(
            self.settings.claude_command,
            self.settings.front_claude_command,
            force=force,
            exclude_pids={os.getpid()},
        )
        return format_process_cleanup_report(report, force=force, canceled_tasks=canceled_tasks)

    async def _run_task(self, task_id: str, command_parts: list[str], cwd: Path) -> None:
        chat_id = None
        process: asyncio.subprocess.Process | None = None
        try:
            process = await asyncio.create_subprocess_exec(
                *command_parts,
                cwd=str(cwd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        except FileNotFoundError:
            self.store.finish_task(
                task_id,
                status=TASK_FAILED,
                exit_code=None,
                error=f"'{self.settings.claude_command}' command not found on PATH",
            )
            task = self.store.get_task(task_id)
            if task:
                await self.send_message(int(task["chat_id"]), f"Task {task_id} failed: Claude CLI not found")
            return
        except Exception as exc:
            self.store.finish_task(
                task_id,
                status=TASK_FAILED,
                exit_code=None,
                error=f"failed to start process: {exc}",
            )
            task = self.store.get_task(task_id)
            if task:
                await self.send_message(int(task["chat_id"]), f"Task {task_id} failed to start: {exc}")
            return

        task = self.store.get_task(task_id)
        if not task:
            process.terminate()
            return

        chat_id = int(task["chat_id"])
        self.store.set_task_running(task_id, process.pid)

        context = _RunningTask(process=process, chat_id=chat_id)
        self._running[task_id] = context

        reader_out = asyncio.create_task(self._read_stream(task_id, process.stdout, context, "stdout"))
        reader_err = asyncio.create_task(self._read_stream(task_id, process.stderr, context, "stderr"))
        try:
            return_code = await process.wait()
            await asyncio.gather(reader_out, reader_err)
        except asyncio.CancelledError:
            if process.returncode is None:
                await _signal_process_tree(process, force=True)
                try:
                    await asyncio.wait_for(
                        process.wait(), timeout=self.FORCE_KILL_WAIT_SECONDS
                    )
                except asyncio.TimeoutError:
                    pass
            reader_out.cancel()
            reader_err.cancel()
            await asyncio.gather(reader_out, reader_err, return_exceptions=True)
            raise
        finally:
            self._running.pop(task_id, None)

        post = self.store.get_task(task_id)
        if not post:
            return

        if post["status"] == TASK_CANCELED:
            await self.send_message(chat_id, f"Task {task_id} canceled")
            return

        if return_code == 0:
            self.store.finish_task(task_id, status=TASK_COMPLETED, exit_code=return_code, error=None)
        else:
            self.store.finish_task(
                task_id,
                status=TASK_FAILED,
                exit_code=return_code,
                error=f"process exited with code {return_code}",
            )

        final = self.store.get_task(task_id)
        if not final:
            return

        events = self.store.recent_events(task_id, limit=15)
        status = final["status"]
        exit_code = final.get("exit_code")
        error = final.get("error")
        snippets = _collect_final_snippets(events, max_lines=8)
        shell_transcript = _build_shell_transcript(snippets)

        if self._task_result_rewriter:
            rewritten = await self._task_result_rewriter(task_id, status, exit_code, snippets, error)
            if rewritten:
                message = rewritten
                if shell_transcript and not _result_includes_shell_transcript(message, shell_transcript):
                    message = _append_shell_transcript(message, shell_transcript)
                await self.send_message(chat_id, message)
                return

        summary = summarize_events(events, max_lines=6)
        fallback = f"Task {task_id} {status}.\nExit code: {exit_code}\nRecent output:\n{summary}"
        if shell_transcript:
            fallback = _append_shell_transcript(fallback, shell_transcript)
        await self.send_message(chat_id, fallback)

    async def _read_stream(
        self,
        task_id: str,
        stream: asyncio.StreamReader | None,
        context: _RunningTask,
        stream_name: str,
    ) -> None:
        if not stream:
            return

        while True:
            line = await stream.readline()
            if not line:
                break

            text = line.decode("utf-8", errors="replace").rstrip("\n")
            parsed = extract_text_snippet(text)
            self.store.append_event(
                task_id,
                stream=stream_name,
                raw_line=text,
                parsed_text=parsed,
                max_chars=self.settings.max_line_chars,
            )

            if parsed:
                if not context.pending_snippets or context.pending_snippets[-1] != parsed:
                    context.pending_snippets.append(parsed)
                if len(context.pending_snippets) > 40:
                    context.pending_snippets = context.pending_snippets[-20:]

    def _resolve_cwd(self, working_dir: str | None) -> Path:
        if not working_dir:
            return self.settings.workspace_dir

        path = Path(working_dir).expanduser()
        if not path.is_absolute():
            path = (self.settings.workspace_dir / path).resolve()
        else:
            path = path.resolve()

        if not path.exists() or not path.is_dir():
            raise ValueError(f"working directory not found: {path}")
        return path

def _collect_final_snippets(events: list[dict[str, Any]], *, max_lines: int) -> list[str]:
    lines: list[str] = []
    for event in events:
        text = (event.get("parsed_text") or "").strip()
        if not text:
            continue
        if text in lines:
            continue
        lines.append(text)

    if len(lines) > max_lines:
        lines = lines[-max_lines:]
    return lines


def _build_shell_transcript(snippets: list[str], *, max_output_lines: int = 3) -> str | None:
    command: str | None = None
    output_lines: list[str] = []

    for raw in snippets:
        text = raw.strip()
        if not text:
            continue

        if text.lower().startswith("running bash:"):
            command = text.split(":", 1)[1].strip()
            output_lines = []
            continue

        if not command:
            continue

        if _is_shell_output_candidate(text):
            output_lines.append(text)
            if len(output_lines) >= max_output_lines:
                break

    if not command:
        return None

    transcript = [f"$ {command}"]
    transcript.extend(output_lines)
    return "\n".join(transcript)


def _is_shell_output_candidate(text: str) -> bool:
    lowered = text.lower()
    if lowered.startswith(
        (
            "task ",
            "summary",
            "here ",
            "the ",
            "status:",
            "error:",
            "warning:",
            "running bash:",
            "editing files.",
        )
    ):
        return False
    if "```" in text:
        return False
    return True


def _result_includes_shell_transcript(message: str, transcript: str) -> bool:
    lines = transcript.splitlines()
    if not lines:
        return True

    command_line = lines[0].replace("$ ", "", 1).strip()
    if command_line and command_line not in message:
        return False

    output_lines = [line for line in lines[1:] if line.strip()]
    if not output_lines:
        return True

    return any(line in message for line in output_lines)


def _append_shell_transcript(message: str, transcript: str) -> str:
    body = message.rstrip()
    return f"{body}\n\n```shell\n{transcript}\n```"


async def _signal_process_tree(process: asyncio.subprocess.Process, *, force: bool) -> None:
    if process.returncode is not None:
        return

    pid = process.pid
    if pid and hasattr(os, "killpg"):
        sig = signal.SIGKILL if force else signal.SIGTERM
        try:
            os.killpg(pid, sig)
            return
        except ProcessLookupError:
            return
        except Exception:
            pass

    try:
        if force:
            process.kill()
        else:
            process.terminate()
    except ProcessLookupError:
        return


async def cleanup_claude_processes(
    claude_command: str,
    front_claude_command: str,
    *,
    force: bool,
    exclude_pids: set[int] | None = None,
) -> ProcessCleanupReport:
    """Terminate Claude-related processes discovered from command names/paths."""
    target_names, target_paths = _build_claude_process_targets(
        claude_command,
        front_claude_command,
    )
    excluded = set(exclude_pids or set())
    excluded.add(os.getpid())

    matched = await asyncio.to_thread(
        _list_matching_process_ids,
        target_names,
        target_paths,
        excluded,
    )
    if not matched:
        return ProcessCleanupReport(
            matched=0,
            stopped=0,
            force_kill_attempts=0,
            permission_denied=0,
            remaining=0,
            term_signals_sent=0,
        )

    term_sent, term_denied = await asyncio.to_thread(_signal_pids, matched, signal.SIGTERM)
    await asyncio.sleep(0.5)
    remaining = await asyncio.to_thread(_existing_pids, matched)
    kill_sent: set[int] = set()
    kill_denied: set[int] = set()

    if force and remaining:
        kill_sent, kill_denied = await asyncio.to_thread(_signal_pids, remaining, signal.SIGKILL)
        await asyncio.sleep(0.2)
        remaining = await asyncio.to_thread(_existing_pids, remaining)

    denied = term_denied | kill_denied
    stopped = matched - remaining

    return ProcessCleanupReport(
        matched=len(matched),
        stopped=len(stopped),
        force_kill_attempts=len(kill_sent),
        permission_denied=len(denied),
        remaining=len(remaining),
        term_signals_sent=len(term_sent),
    )


def format_process_cleanup_report(
    report: ProcessCleanupReport,
    *,
    force: bool,
    canceled_tasks: int = 0,
) -> str:
    """Render cleanup summary for Telegram and local CLI output."""
    if report.matched == 0:
        if canceled_tasks:
            return (
                f"Stopped {canceled_tasks} running task(s). "
                "No additional Claude processes were found."
            )
        return "No Claude Code processes found."

    lines = [
        "Claude process cleanup complete.",
        f"- Matched: {report.matched}",
        f"- Stopped: {report.stopped}",
    ]
    if canceled_tasks:
        lines.append(f"- Runner tasks canceled: {canceled_tasks}")
    if force:
        lines.append(f"- Force-kill attempts: {report.force_kill_attempts}")
    if report.permission_denied:
        lines.append(f"- Permission denied: {report.permission_denied}")
    if report.remaining:
        lines.append(f"- Still running: {report.remaining}")
    else:
        lines.append("- Remaining: 0")
    if report.term_signals_sent == 0 and report.matched > 0:
        lines.append("- Note: could not send SIGTERM to matched processes.")
    return "\n".join(lines)


def _build_claude_process_targets(*commands: str) -> tuple[set[str], set[str]]:
    names: set[str] = set()
    paths: set[str] = set()
    for raw_command in commands:
        raw = (raw_command or "").strip()
        if not raw:
            continue
        if "claude" not in raw.lower():
            continue

        command_path = Path(raw).expanduser()
        names.add(command_path.name.lower())
        if "/" in raw:
            try:
                resolved = str(command_path.resolve())
            except Exception:
                resolved = str(command_path)
            paths.add(resolved)

    if not names and not paths:
        names.add("claude")

    return names, paths


def _list_matching_process_ids(
    target_names: set[str],
    target_paths: set[str],
    exclude_pids: set[int] | None = None,
) -> set[int]:
    if not target_names and not target_paths:
        return set()

    try:
        output = subprocess.run(
            ["ps", "-axo", "pid=,command="],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return set()

    if output.returncode != 0:
        return set()

    excluded = exclude_pids or set()
    matched: set[int] = set()

    for raw_line in output.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        parts = line.split(None, 1)
        if len(parts) < 2:
            continue

        try:
            pid = int(parts[0])
        except ValueError:
            continue

        if pid in excluded:
            continue

        command_line = parts[1].strip()
        if not command_line:
            continue

        tokens = _split_command_line_tokens(command_line)
        if _matches_target_tokens(tokens, target_names=target_names, target_paths=target_paths):
            matched.add(pid)

    return matched


def _split_command_line_tokens(command_line: str) -> list[str]:
    try:
        return shlex.split(command_line)
    except ValueError:
        return command_line.split()


def _matches_target_tokens(
    tokens: list[str],
    *,
    target_names: set[str],
    target_paths: set[str],
) -> bool:
    if not tokens:
        return False

    for token in tokens:
        normalized = token.strip()
        if not normalized:
            continue

        if Path(normalized).name.lower() in target_names:
            return True

        if target_paths and "/" in normalized:
            candidate = Path(normalized).expanduser()
            try:
                resolved = str(candidate.resolve())
            except Exception:
                resolved = str(candidate)
            if resolved in target_paths:
                return True

    return False


def _signal_pids(pids: set[int], sig: int) -> tuple[set[int], set[int]]:
    sent: set[int] = set()
    denied: set[int] = set()
    for pid in pids:
        try:
            os.kill(pid, sig)
            sent.add(pid)
        except ProcessLookupError:
            continue
        except PermissionError:
            denied.add(pid)
    return sent, denied


def _existing_pids(pids: set[int]) -> set[int]:
    alive: set[int] = set()
    for pid in pids:
        try:
            os.kill(pid, 0)
            alive.add(pid)
        except ProcessLookupError:
            continue
        except PermissionError:
            alive.add(pid)
    return alive
