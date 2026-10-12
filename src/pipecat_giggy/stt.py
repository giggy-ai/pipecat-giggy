"""VAD-delimited live PCM transcription over Giggy's public HTTP/SSE API."""

import asyncio
import base64
import codecs
import json
import re
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from urllib.parse import urlsplit
from uuid import UUID

import aiohttp
from pipecat.frames.frames import (
    CancelFrame,
    EndFrame,
    Frame,
    InputAudioRawFrame,
    InterimTranscriptionFrame,
    STTMuteFrame,
    TranscriptionFrame,
    VADUserStartedSpeakingFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.settings import STTSettings
from pipecat.services.stt_service import STTService
from pipecat.utils.tracing.service_decorators import traced_stt
from pipecat.utils.types import is_given

_PACKET_BYTES = 16000  # 500 ms of mono PCM16 at 16 kHz
_MAX_AUDIO_BYTES = 960000  # 30 seconds, including the real VAD pre-roll
_MAX_RESULT_BYTES = 16 * 1024 * 1024


@dataclass
class _Utterance:
    user_id: str
    audio: bytearray = field(default_factory=bytearray)
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    ended: bool = False
    captured: int = 0


class GiggySTTService(STTService):
    """Live transcription of 0.5–30 second utterances, with local VAD boundaries.

    Requires mono PCM16 LE at 16 kHz and upstream VAD start/stop frames.
    Provisional snapshots are interim frames; only the durable transcript becomes
    a final frame. Admissions and ambiguous input POSTs are never retried.
    """

    def __init__(
        self,
        *,
        api_key: str,
        aiohttp_session: aiohttp.ClientSession,
        base_url: str = "https://giggy.ai",
        sample_rate: int = 16000,
        **kwargs,
    ):
        if not api_key:
            raise ValueError("api_key is required.")
        if sample_rate != 16000:
            raise ValueError("Giggy live STT requires 16000 Hz mono PCM16.")
        origin = urlsplit(base_url)
        if (
            origin.scheme != "https"
            or not origin.hostname
            or origin.username is not None
            or origin.password is not None
            or origin.path not in {"", "/"}
            or origin.query
            or origin.fragment
        ):
            raise ValueError(
                "base_url must be an HTTPS origin without credentials or a path."
            )
        if kwargs.get("keepalive_timeout") is not None:
            raise ValueError("Giggy STT does not accept synthetic keepalive audio.")
        if "settings" in kwargs:
            raise ValueError(
                "Giggy live STT uses a fixed model and automatic language detection."
            )
        super().__init__(
            sample_rate=sample_rate,
            settings=STTSettings(model="giggyspeech-stt", language=None),
            **kwargs,
        )
        self._api_key = api_key
        self._session = aiohttp_session
        self._origin = base_url.rstrip("/")
        self._capture: _Utterance | None = None
        self._pre_roll = bytearray()
        self._turn_tasks: set[asyncio.Task] = set()
        self._previous_task: asyncio.Task | None = None
        self._failed = False

    async def _update_settings(self, delta: STTSettings) -> dict:
        if (
            (is_given(delta.language) and delta.language is not None)
            or (is_given(delta.model) and delta.model != "giggyspeech-stt")
            or delta.extra
        ):
            await self.push_error(
                "Giggy live STT requires automatic language detection and its fixed model."
            )
            return {}
        return await super()._update_settings(delta)

    async def process_audio_frame(
        self, frame: InputAudioRawFrame, direction: FrameDirection
    ):
        if self._muted or self._failed or not self.is_usable:
            return
        if (
            frame.sample_rate != 16000
            or frame.num_channels != 1
            or len(frame.audio) % 2
        ):
            await self._fail("Giggy live STT requires aligned mono PCM16 at 16000 Hz.")
            return
        if self._capture is not None:
            user_id = getattr(frame, "user_id", "")
            if self._capture.user_id and user_id != self._capture.user_id:
                await self._fail(
                    "Giggy STT requires one input speaker per service instance."
                )
                return
            self._capture.user_id = user_id
        await super().process_audio_frame(frame, direction)

    async def run_stt(self, audio: bytes) -> AsyncGenerator[Frame | None, None]:
        """Buffer bounded capture data; network work runs independently of audio frames."""
        if self._capture is None:
            # Retain one second of real input: model detection can lag its
            # configured start window, especially for quiet opening words.
            self._pre_roll.extend(audio[-32000:])
            del self._pre_roll[:-32000]
        else:
            turn = self._capture
            if turn.captured + len(audio) > _MAX_AUDIO_BYTES:
                await self._fail(
                    "Giggy STT utterance exceeds 30 seconds including VAD pre-roll."
                )
            else:
                turn.audio.extend(audio)
                turn.captured += len(audio)
                turn.changed.set()
        yield None

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        if (
            isinstance(frame, VADUserStartedSpeakingFrame)
            and not self._failed
            and not self._muted
        ):
            if self._capture is None:
                if len(self._turn_tasks) >= 2:
                    await self._fail(
                        "Giggy STT cannot keep up with new utterances; capture queue is full."
                    )
                else:
                    turn = _Utterance(
                        self._user_id,
                        audio=self._pre_roll,
                        captured=len(self._pre_roll),
                    )
                    self._pre_roll = bytearray()
                    self._capture = turn
                    previous = self._previous_task
                    task = self.create_task(
                        self._transcribe(turn, previous), name="giggy-stt-utterance"
                    )
                    self._turn_tasks.add(task)
                    task.add_done_callback(self._turn_tasks.discard)
                    self._previous_task = task
        elif isinstance(frame, VADUserStoppedSpeakingFrame):
            self._finish_capture()
        elif isinstance(frame, STTMuteFrame) and frame.mute:
            await self._abort()
        # InterruptionFrame is deliberately passed through. Barge-in interrupts
        # the bot's TTS; it must not cancel transcription of the user's new speech.
        await super().process_frame(frame, direction)

    def _finish_capture(self):
        if self._capture is not None:
            self._capture.ended = True
            self._capture.changed.set()
            self._capture = None

    async def stop(self, frame: EndFrame):
        self._finish_capture()
        if self._turn_tasks:
            await asyncio.gather(*tuple(self._turn_tasks), return_exceptions=True)
        self._pre_roll.clear()
        await super().stop(frame)

    async def cancel(self, frame: CancelFrame):
        await self._abort()
        await super().cancel(frame)

    async def cleanup(self):
        await self._abort()
        await super().cleanup()

    async def _abort(self):
        self._capture = None
        self._pre_roll.clear()
        current = asyncio.current_task()
        tasks = [task for task in self._turn_tasks if task is not current]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._previous_task = None

    async def _fail(self, message: str):
        self._failed = True
        await self._abort()
        await self.push_error(message, fatal=True)

    async def _transcribe(self, turn: _Utterance, previous: asyncio.Task | None):
        try:
            # Finalize in turn order. At most one earlier turn and one new capture
            # can coexist; readiness/finalization cannot grow an unbounded queue.
            async with asyncio.timeout(150):
                if previous is not None:
                    await asyncio.shield(previous)
                while turn.captured < _PACKET_BYTES and not turn.ended:
                    turn.changed.clear()
                    await turn.changed.wait()
                if turn.captured < _PACKET_BYTES:
                    # No paid admission and no fabricated silence for short VAD segments.
                    await self.push_error(
                        "Giggy STT needs at least 0.5 seconds of captured audio."
                    )
                    return
                await self._stream_turn(turn)
        except asyncio.CancelledError:
            raise
        except (TimeoutError, aiohttp.ClientError):
            await self._fail(
                "Giggy STT transport failed or timed out; the request was not retried."
            )
        except (ValueError, UnicodeError, KeyError, TypeError):
            await self._fail(
                "Giggy STT returned an invalid response; the request was not retried."
            )
        finally:
            turn.audio.clear()

    async def _events(self, response):
        decoder = codecs.getincrementaldecoder("utf-8")()
        pending = ""
        async for chunk in response.content.iter_chunked(4096):
            pending += decoder.decode(chunk)
            if len(pending.encode("utf-8")) > _MAX_RESULT_BYTES:
                raise ValueError("Event limit exceeded")
            while boundary := re.search(r"\r?\n\r?\n", pending):
                block, pending = pending[: boundary.start()], pending[boundary.end() :]
                event = "message"
                parts = []
                for line in block.splitlines():
                    if line.startswith("event:"):
                        event = line[6:].strip()
                    elif line.startswith("data:"):
                        parts.append(line[5:].lstrip(" "))
                if parts:
                    payload = json.loads("\n".join(parts))
                    if not isinstance(payload, dict):
                        raise ValueError("Expected an event object")
                    yield event, payload
        pending += decoder.decode(b"", final=True)
        if pending.strip() and not pending.lstrip().startswith(":"):
            raise ValueError("Incomplete event")

    async def _send_audio(self, turn: _Utterance, generation_id: str, token: str):
        sequence = 0
        while True:
            turn.changed.clear()
            if len(turn.audio) < _PACKET_BYTES and not turn.ended:
                await turn.changed.wait()
                continue
            pcm = bytes(turn.audio[:_PACKET_BYTES])
            del turn.audio[: len(pcm)]
            complete = turn.ended and not turn.audio
            async with self._session.post(
                f"{self._origin}/api/generations/{generation_id}/stt-input",
                json={
                    "input_token": token,
                    "sequence": sequence,
                    "pcm_base64": base64.b64encode(pcm).decode("ascii"),
                    "complete": complete,
                },
                timeout=aiohttp.ClientTimeout(total=10),
                allow_redirects=False,
            ) as response:
                if response.status != 200:
                    raise ValueError("Input not acknowledged")
                acknowledgement = await response.json()
                if (
                    not isinstance(acknowledgement, dict)
                    or acknowledgement.get("ok") is not True
                ):
                    raise ValueError("Input not acknowledged")
            sequence += 1
            if complete:
                return

    async def _stream_turn(self, turn: _Utterance):
        headers = {"xi-api-key": self._api_key}
        generation_id = None
        attempt = None
        last_sequence = -1
        sender = None
        read_task = None
        try:
            async with self._session.post(
                f"{self._origin}/v1/audio/transcriptions",
                headers=headers,
                json={
                    "mode": "streaming",
                    "microphone": True,
                },
                timeout=aiohttp.ClientTimeout(total=120, sock_connect=10, sock_read=90),
                allow_redirects=False,
            ) as response:
                if response.status != 200:
                    await self._fail(
                        f"Giggy STT admission returned HTTP {response.status}; it was not retried."
                    )
                    return
                if (
                    response.headers.get("content-type", "")
                    .split(";")[0]
                    .strip()
                    .lower()
                    != "text/event-stream"
                ):
                    raise ValueError("Expected SSE")
                events = self._events(response)
                while True:
                    read_task = asyncio.create_task(anext(events))
                    if sender is not None and not sender.done():
                        done, _ = await asyncio.wait(
                            [read_task, sender], return_when=asyncio.FIRST_COMPLETED
                        )
                        if sender in done:
                            # A rejected/ambiguous POST closes SSE immediately.
                            await sender
                    elif sender is not None:
                        await sender
                    try:
                        event, payload = await read_task
                    except StopAsyncIteration:
                        break
                    read_task = None
                    if event == "generation_created":
                        if generation_id is not None:
                            raise ValueError("Duplicate admission")
                        nested_generation = payload.get("generation", {})
                        if not isinstance(nested_generation, dict):
                            raise TypeError("Invalid admission object")
                        generation_id = payload.get(
                            "generation_uuid"
                        ) or nested_generation.get("generation_uuid")
                        if not isinstance(generation_id, str):
                            raise TypeError("Invalid generation identity")
                        if str(UUID(generation_id)) != generation_id:
                            raise ValueError("Invalid generation identity")
                    elif event == "microphone_ready":
                        token = payload.get("input_token")
                        attempt = payload.get("attempt")
                        if (
                            not generation_id
                            or payload.get("generation_uuid") != generation_id
                            or sender is not None
                            or type(attempt) is not int
                            or attempt < 1
                            or not isinstance(token, str)
                            or not re.fullmatch(r"[a-f0-9]{64}", token)
                        ):
                            raise ValueError("Invalid readiness")
                        sender = asyncio.create_task(
                            self._send_audio(turn, generation_id, token)
                        )
                    elif event == "transcript_chunk":
                        sequence = payload.get("sequence")
                        if (
                            sender is None
                            or payload.get("generation_uuid") != generation_id
                            or payload.get("attempt") != attempt
                            or type(payload.get("attempt")) is not int
                            or type(sequence) is not int
                            or sequence <= last_sequence
                            or payload.get("replace") is not True
                            or not isinstance(payload.get("text"), str)
                        ):
                            raise ValueError("Invalid transcript snapshot")
                        last_sequence = sequence
                        await self._handle_transcription(
                            payload["text"], False, turn.user_id
                        )
                    elif event == "generation_updated":
                        if payload.get("generation_uuid") != generation_id:
                            raise ValueError("Generation identity changed")
                        nested_generation = payload.get("generation", {})
                        if not isinstance(nested_generation, dict):
                            raise TypeError("Invalid status event")
                        status = payload.get("status") or nested_generation.get(
                            "status"
                        )
                        if status in {"failed", "cancelled", "admission_failed"}:
                            raise ValueError("Operation failed")
                    elif event == "error":
                        raise ValueError("Provider stream failed")
                if not generation_id or sender is None:
                    raise ValueError("No ready operation")
                # Never let EOF strand a sender waiting for audio indefinitely.
                if not sender.done() and not turn.ended:
                    raise ValueError("SSE ended before input acknowledgement")
                await sender
                async with self._session.get(
                    f"{self._origin}/v1/generations/{generation_id}",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                    allow_redirects=False,
                ) as status_response:
                    if status_response.status != 200:
                        raise ValueError("Cannot confirm operation")
                    generation = (await status_response.json())["generation"]
                    if (
                        not isinstance(generation, dict)
                        or generation.get("generation_uuid") != generation_id
                        or generation.get("status") != "completed"
                    ):
                        raise ValueError("No durable success")
                    result_url = generation["result"]["url"]
                    if not isinstance(result_url, str):
                        raise TypeError("Invalid artifact URL")
                result_origin = urlsplit(result_url)
                if (
                    result_origin.scheme != "https"
                    or not result_origin.hostname
                    or result_origin.username is not None
                ):
                    raise ValueError("Invalid artifact URL")
                # Signed artifact requests never carry the API key or input token.
                async with self._session.get(
                    result_url,
                    timeout=aiohttp.ClientTimeout(total=10),
                    allow_redirects=False,
                ) as artifact:
                    if artifact.status != 200:
                        raise ValueError("Artifact unavailable")
                    body = bytearray()
                    async for chunk in artifact.content.iter_chunked(4096):
                        body.extend(chunk)
                        if len(body) > _MAX_RESULT_BYTES:
                            raise ValueError("Artifact limit exceeded")
                    result = json.loads(body)
                if not isinstance(result, dict) or not isinstance(
                    result.get("text"), str
                ):
                    raise TypeError("Invalid canonical transcript")
                if (
                    type(result.get("audio_duration_ms")) is not int
                    or result["audio_duration_ms"] != (turn.captured + 31) // 32
                ):
                    raise ValueError("Canonical audio duration mismatch")
                await self._handle_transcription(result["text"], True, turn.user_id)
        finally:
            tasks = [task for task in (read_task, sender) if task is not None]
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    @traced_stt
    async def _handle_transcription(
        self, transcript: str, is_final: bool, user_id: str
    ):
        timestamp = datetime.now(UTC).isoformat()
        if is_final:
            await self.push_frame(
                TranscriptionFrame(transcript, user_id, timestamp, finalized=True)
            )
        else:
            await self.push_frame(
                InterimTranscriptionFrame(transcript, user_id, timestamp)
            )
