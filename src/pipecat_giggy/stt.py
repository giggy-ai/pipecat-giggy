"""VAD-delimited live PCM transcription over Giggy's public HTTP/SSE API."""

import asyncio
import base64
import codecs
import json
import re
from collections.abc import AsyncGenerator, AsyncIterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import aiohttp
from loguru import logger
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

_EXTERNAL_IDLE_SECONDS = 15
_PACKET_BYTES = 3200  # Start forwarding after 100 ms of mono PCM16 at 16 kHz.
_MAX_PACKET_BYTES = 16000  # Coalesce delayed uploads, retaining the 500 ms bound.
_MIN_AUDIO_BYTES = 16000  # Admission/finalization still requires 500 ms
_MAX_AUDIO_BYTES = 960000  # 30 seconds, including the real VAD pre-roll
_MAX_RESULT_BYTES = 16 * 1024 * 1024


@dataclass
class _Utterance:
    user_id: str
    language: str | None = None
    audio: bytearray = field(default_factory=bytearray)
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    ended: bool = False
    captured: int = 0
    speech_end_time: float | None = None
    published: asyncio.Event = field(default_factory=asyncio.Event)
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    generation_id: str | None = None
    final_text: str | None = None
    idle_timer: asyncio.TimerHandle | None = None
    task: asyncio.Task | None = None
    end_requested_time: float | None = None
    ready_deadline: float | None = None
    decode_end_sample: int | None = None
    metric_boundary: asyncio.Event | None = None
    input_transport: str | None = None
    tail_candidate_refreshed: bool = False
    quiet_start_sample: int | None = None
    input_acknowledged: asyncio.Event = field(default_factory=asyncio.Event)


class GiggySTTService(STTService):
    """Live transcription of 0.5–30 second utterances, with local VAD boundaries.

    Requires mono PCM16 LE at 16 kHz and upstream VAD start/stop frames.
    Provisional snapshots are interim frames; the fenced complete snapshot becomes
    a final frame while durable persistence is drained and verified separately. Admissions and ambiguous input POSTs are never retried.
    """

    Settings = STTSettings

    def __init__(
        self,
        *,
        api_key: str,
        aiohttp_session: aiohttp.ClientSession,
        base_url: str = "https://giggy.ai",
        sample_rate: int = 16000,
        endpointing: str = "vad",
        settings: STTSettings | None = None,
        **kwargs,
    ):
        if getattr(aiohttp_session, "auth", None) is not None or any(
            name.lower() in {"authorization", "proxy-authorization", "xi-api-key", "x-giggy-input-token", "cookie"}
            for name in getattr(aiohttp_session, "headers", {})
        ):
            raise ValueError("Use a credential-free aiohttp session; pass the Giggy key through api_key.")
        if endpointing not in {"vad", "external"}:
            raise ValueError("endpointing must be vad or external")
        self._endpointing = endpointing
        self._external_turn = None
        self._finalize_turn = None
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
        requested = settings or STTSettings()
        if (is_given(requested.model) and requested.model != "giggyspeech-stt") or requested.extra:
            raise ValueError("Giggy live STT uses the fixed giggyspeech-stt model and no extra settings.")
        super().__init__(
            sample_rate=sample_rate,
            settings=STTSettings(model="giggyspeech-stt", language=requested.language if is_given(requested.language) else None),
            **kwargs,
        )
        self._api_key = api_key
        self._session = aiohttp_session
        self._origin = base_url.rstrip("/")
        self._capture: _Utterance | None = None
        self._pre_roll = bytearray()
        self._turn_tasks: set[asyncio.Task] = set()
        self._previous_task: asyncio.Event | None = None
        self._pending_audio_bytes = 0
        self._stream_slots = asyncio.Semaphore(8)
        self._failed = False
        self.finalization_timings_ms: list[float] = []

    def can_generate_metrics(self) -> bool:
        return True

    def _begin_external_utterance(self, *, timeout=15.0):
        """Create one bounded external reservation without blocking PCM capture."""
        if self._endpointing != "external" or self._failed or self._muted:
            raise RuntimeError("External endpointing is unavailable")
        if self._capture is not None or len(self._turn_tasks) >= 8:
            raise RuntimeError("An utterance is active or capacity is exhausted")
        turn = _Utterance(self._user_id, language=self._settings.language,
                          ready_deadline=asyncio.get_running_loop().time() + timeout)
        self._capture = self._external_turn = turn
        self._finalize_turn = turn
        task = self.create_task(self._transcribe(turn, self._previous_task), name="giggy-stt-external")
        turn.task = task
        self._turn_tasks.add(task)
        task.add_done_callback(self._turn_tasks.discard)
        self._previous_task = turn.published
        return turn, task

    async def _await_external_readiness(self, turn, task, timeout=15.0):
        ready = asyncio.create_task(turn.ready.wait())
        try:
            done, _ = await asyncio.wait({ready, task}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
            if not done:
                raise TimeoutError("Worker readiness timed out before audio")
            if not turn.ready.is_set() or not turn.generation_id or self._failed:
                raise RuntimeError("Worker did not become ready")
        except BaseException:
            await self._abort()
            raise
        finally:
            ready.cancel()
            await asyncio.gather(ready, return_exceptions=True)

    async def start_utterance(self, *, timeout: float = 15.0):
        """Reserve an external worker and await signed readiness before PCM."""
        turn, task = self._begin_external_utterance(timeout=timeout)
        await self._await_external_readiness(turn, task, timeout)

    async def send_audio(self, pcm: bytes):
        """Queue real mono PCM16/16kHz; retain the existing ordered ACK protocol."""
        turn = self._capture
        if self._endpointing != "external" or turn is None or not turn.ready.is_set() or turn.ended or self._failed:
            raise RuntimeError("Await start_utterance before sending audio")
        if not isinstance(pcm, bytes):
            raise TypeError("PCM must be bytes")
        if len(pcm) % 2:
            raise ValueError("PCM16 must be sample aligned")
        if pcm and turn.idle_timer is not None:
            turn.idle_timer.cancel()
            turn.idle_timer = None
        async for _ in self.run_stt(pcm):
            pass

    async def transcribe_stream(
        self,
        audio: AsyncIterable[bytes],
        *,
        ready_timeout: float = 15.0,
        final_timeout: float = 30.0,
    ) -> str:
        """Transcribe one PCM source, automatically finalizing at clean EOF.

        Requires external endpointing. Await readiness before pulling audio;
        natural iterator exhaustion flushes the exact tail without a VAD wait.
        Source errors and cancellation abort instead of finalizing truncated audio.
        Durable verification continues after the fenced final, as with finalize().
        """
        # A rejected second capture must not abort the existing utterance.
        await self.start_utterance(timeout=ready_timeout)
        try:
            async for pcm in audio:
                await self.send_audio(pcm)
            return await self.finalize(timeout=final_timeout)
        except BaseException:
            await self._abort()
            raise

    async def finalize(self, *, timeout: float = 30.0) -> str:
        """Flush pending real PCM and signal complete immediately, without VAD.

        Works for an external capture or a VAD-managed turn when the caller
        explicitly knows the input has ended. It never waits for a VAD stop.
        Returns the fenced final text; existing durable verification continues
        in the background. Repeated calls await the same final, never replay input.
        """
        turn = self._finalize_turn
        if turn is None or self._failed:
            raise RuntimeError("No utterance available for finalization")
        if turn.captured < _MIN_AUDIO_BYTES:
            raise ValueError("Capture at least 0.5 seconds before finalizing")
        if turn.metric_boundary is not None:
            turn.metric_boundary.set()
        if not turn.ended:
            turn.decode_end_sample = None
            self._finish_capture()
        try:
            await asyncio.wait_for(turn.published.wait(), timeout)
        except (TimeoutError, asyncio.CancelledError):
            await self._abort()
            raise
        if self._failed or not turn.final_text:
            raise RuntimeError("No successful final transcript")
        return turn.final_text

    async def _update_settings(self, delta: STTSettings) -> dict:
        if (
            (is_given(delta.model) and delta.model != "giggyspeech-stt")
            or delta.extra
        ):
            await self.push_error(
                "Giggy live STT requires its fixed model and no extra settings."
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
            if turn.captured + len(audio) > _MAX_AUDIO_BYTES or self._pending_audio_bytes + len(audio) > _MAX_AUDIO_BYTES:
                await self._fail(
                    "Giggy STT utterance exceeds 30 seconds including VAD pre-roll."
                )
            else:
                turn.audio.extend(audio)
                turn.captured += len(audio)
                self._pending_audio_bytes += len(audio)
                turn.changed.set()
        yield None

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        if self._endpointing == "external" and isinstance(frame, (VADUserStartedSpeakingFrame, VADUserStoppedSpeakingFrame)):
            # External caller owns turn boundaries; upstream VAD cannot flush it.
            await super().process_frame(frame, direction)
            return
        if (
            isinstance(frame, VADUserStartedSpeakingFrame)
            and not self._failed
            and not self._muted
        ):
            if self._capture is None:
                if len(self._turn_tasks) >= 32 or self._pending_audio_bytes + len(self._pre_roll) > _MAX_AUDIO_BYTES:
                    await self._fail(
                        "Giggy STT cannot keep up with new utterances; capture queue is full."
                    )
                else:
                    turn = _Utterance(
                        self._user_id,
                        language=self._settings.language,
                        audio=self._pre_roll,
                        captured=len(self._pre_roll),
                    )
                    self._pre_roll = bytearray()
                    self._capture = turn
                    self._finalize_turn = turn
                    self._pending_audio_bytes += turn.captured
                    previous = self._previous_task
                    task = self.create_task(
                        self._transcribe(turn, previous), name="giggy-stt-utterance"
                    )
                    self._turn_tasks.add(task)
                    task.add_done_callback(self._turn_tasks.discard)
                    self._previous_task = turn.published
        elif isinstance(frame, VADUserStoppedSpeakingFrame):
            if self._capture is not None and frame.stop_secs > 0:
                self._capture.speech_end_time = frame.timestamp - frame.stop_secs
            self._finish_capture()
        elif isinstance(frame, STTMuteFrame) and frame.mute:
            await self._abort()
        # InterruptionFrame is deliberately passed through. Barge-in interrupts
        # the bot's TTS; it must not cancel transcription of the user's new speech.
        await super().process_frame(frame, direction)

    def _finish_capture(self):
        if self._capture is not None:
            self._capture.end_requested_time = asyncio.get_running_loop().time()
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
        self._finalize_turn = None
        self._external_turn = None
        self._pre_roll.clear()
        current = asyncio.current_task()
        tasks = [task for task in self._turn_tasks if task is not current]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._previous_task = None
        self._pending_audio_bytes = 0

    async def _fail(self, message: str):
        self._failed = True
        await self._abort()
        await self.push_error(message, fatal=True)

    async def _transcribe(self, turn: _Utterance, previous: asyncio.Event | None):
        try:
            # Network progress is concurrent; only publication waits for turn
            # order. Capture is bounded by actual PCM bytes, independently of
            # the existing eight simultaneous live operations.
            async with asyncio.timeout(150):
                while self._endpointing != "external" and turn.captured < _MIN_AUDIO_BYTES and not turn.ended:
                    turn.changed.clear()
                    await turn.changed.wait()
                if turn.captured < _MIN_AUDIO_BYTES and (self._endpointing != "external" or turn.ended):
                    # No paid admission and no fabricated silence for short VAD segments.
                    await self.push_error(
                        "Giggy STT needs at least 0.5 seconds of captured audio."
                    )
                    return
                async with self._stream_slots:
                    await self._stream_turn(turn, previous)
        except asyncio.CancelledError:
            raise
        except (TimeoutError, aiohttp.ClientError) as error:
            logger.error("Giggy STT transport exception type: {}", type(error).__name__)
            await self._fail(
                "Giggy STT transport failed or timed out; the request was not retried."
            )
        except (ValueError, UnicodeError, KeyError, TypeError) as error:
            logger.error("Giggy STT protocol validation failed: {}",
                str(error) if type(error) is ValueError else type(error).__name__)
            await self._fail(
                "Giggy STT returned an invalid response; the request was not retried."
            )
        finally:
            if turn.idle_timer is not None:
                turn.idle_timer.cancel()
                turn.idle_timer = None
            if self._endpointing == "external" and self._capture is turn:
                self._capture = None
            turn.published.set()
            self._pending_audio_bytes = max(0, self._pending_audio_bytes - turn.captured)
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

    async def _stream_audio_frames(self, turn, token):
        sequence = sent_samples = 0
        while True:
            turn.changed.clear()
            # Start at 20ms, then amortize controller parsing and durable owner
            # validation over 100ms packets. Never hold a final or a newly
            # prepared endpoint candidate behind this batching threshold.
            threshold = 640 if sequence == 0 else _PACKET_BYTES
            candidate_flush = turn.decode_end_sample is not None and bool(turn.audio)
            if len(turn.audio) < threshold and not turn.ended and not candidate_flush:
                await turn.changed.wait()
                continue
            pcm = bytes(turn.audio[:_MAX_PACKET_BYTES])
            del turn.audio[:len(pcm)]
            sent_samples += len(pcm) // 2
            complete = turn.ended and not turn.audio
            frame = {"input_token": token, "sequence": sequence,
                     "pcm_base64": base64.b64encode(pcm).decode("ascii"), "complete": complete}
            if turn.decode_end_sample is not None and max(0, sent_samples - 3200) <= turn.decode_end_sample <= sent_samples:
                frame["decode_end_sample"] = turn.decode_end_sample
            yield (json.dumps(frame, separators=(",", ":")) + "\n").encode()
            sequence += 1
            if complete:
                return

    async def _send_audio(self, turn, generation_id, token):
        async with self._session.ws_connect(
            f"{self._origin}/api/generations/{generation_id}/stt-input",
            headers={"x-giggy-input-token": token}, protocols=("giggy-stt-v1",),
            max_msg_size=1024, compress=0,
        ) as socket:
            if socket.protocol != "giggy-stt-v1":
                raise ValueError("Invalid microphone socket protocol")
            sequence = -1
            async for wire in self._stream_audio_frames(turn, token):
                await socket.send_str(wire.decode("utf8"))
                sequence += 1
            acknowledgement = await socket.receive_json(timeout=10)
            if (not isinstance(acknowledgement, dict) or acknowledgement.get("ok") is not True
                    or type(acknowledgement.get("sequence")) is not int
                    or acknowledgement["sequence"] != sequence):
                raise ValueError("Input socket final not acknowledged")
            turn.input_acknowledged.set()

    async def _await_input_acknowledgement(self, turn, sender):
        # WebSocket __aexit__ waits for the peer close handshake. It is cleanup,
        # not input acceptance; the validated final ACK is the publication gate.
        acknowledged = asyncio.create_task(turn.input_acknowledged.wait())
        try:
            await asyncio.wait({acknowledged, sender}, return_when=asyncio.FIRST_COMPLETED)
            if sender.done() or not turn.input_acknowledged.is_set():
                await sender  # Legacy transports and all sender errors still validate.
        finally:
            acknowledged.cancel()
            await asyncio.gather(acknowledged, return_exceptions=True)



    async def _stream_turn(self, turn: _Utterance, previous: asyncio.Event | None = None):
        headers = {"xi-api-key": self._api_key}
        generation_id = None
        attempt = None
        last_sequence = -1
        sender = None
        read_task = None
        phase = "admission"
        final_text = None
        try:
            async with asyncio.timeout_at(turn.ready_deadline) as readiness_timeout:
                async with self._session.post(
                    f"{self._origin}/v1/audio/transcriptions",
                    headers=headers,
                    json={
                        "mode": "streaming",
                        "microphone": True,
                        "language": turn.language,
                        **({"endpointing": "external"} if self._endpointing == "external" else {}),
                        "client_request_id": str(uuid4()),
                    },
                    timeout=aiohttp.ClientTimeout(total=120, sock_connect=10, sock_read=90),
                    allow_redirects=False,
                ) as response:
                    phase = "stream_or_input"
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
                            logger.debug(f"{self}: canonical STT generation {generation_id}")
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
                            if payload.get("input_transport") != "websocket-v1":
                                raise ValueError("V21 requires websocket-v1 input")
                            turn.input_transport = payload.get("input_transport")
                            sender = asyncio.create_task(
                                self._send_audio(turn, generation_id, token)
                            )
                            turn.generation_id = generation_id
                            readiness_timeout.reschedule(None)
                            if self._endpointing == "external" and not turn.captured:
                                turn.idle_timer = asyncio.get_running_loop().call_later(
                                    _EXTERNAL_IDLE_SECONDS, turn.task.cancel)
                            turn.ready.set()
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
                                or type(payload.get("complete")) is not bool
                                or final_text is not None
                            ):
                                raise ValueError("Invalid transcript snapshot")
                            last_sequence = sequence
                            if payload["complete"]:
                                if not turn.ended:
                                    raise ValueError("Final transcript arrived before input ended")
                                final_receipt_time = asyncio.get_running_loop().time()
                                await self._await_input_acknowledgement(turn, sender)
                                if turn.metric_boundary is not None:
                                    await turn.metric_boundary.wait()
                                if previous is not None:
                                    await previous.wait()
                                if self._failed:
                                    return
                                final_text = payload["text"]
                                if turn.end_requested_time is not None:
                                    self.finalization_timings_ms.append(
                                        (final_receipt_time - turn.end_requested_time) * 1000)
                                    del self.finalization_timings_ms[:-32]
                                await self._handle_transcription(final_text, True, turn.user_id,
                                    speech_end_time=turn.speech_end_time)
                                turn.final_text = final_text
                                turn.published.set()
                                self._on_fenced_final(turn)
                            else:
                                await self._handle_transcription(payload["text"], False, turn.user_id)
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
                    if not generation_id or sender is None or final_text is None:
                        raise ValueError("No ready operation")
                    # Never let EOF strand a sender waiting for audio indefinitely.
                    if not sender.done() and not turn.ended:
                        raise ValueError("SSE ended before input acknowledgement")
                    await sender
                    phase = "canonical_status"
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
                    # Recheck mutable defaults immediately before crossing origins.
                    if getattr(self._session, "auth", None) is not None or any(
                        name.lower() in {"authorization", "proxy-authorization", "xi-api-key", "x-giggy-input-token", "cookie"}
                        for name in getattr(self._session, "headers", {})
                    ):
                        raise ValueError("Use a credential-free aiohttp session for signed artifacts.")
                    # Signed artifact requests never carry the API key or input token.
                    phase = "canonical_artifact"
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
                    if result["text"] != final_text:
                        raise ValueError("Saved transcript differs from the fenced final snapshot")
                    return final_text
        except (TimeoutError, aiohttp.ClientError) as error:
            logger.error("Giggy STT transport phase={} type={} generation={}",
                phase, type(error).__name__, generation_id)
            raise
        finally:
            tasks = [task for task in (read_task, sender) if task is not None]
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    def _on_fenced_final(self, turn):
        """Optional Pipecat preparation hook, after final publication only."""

    @traced_stt
    async def _handle_transcription(
        self, transcript: str, is_final: bool, user_id: str,
        speech_end_time: float | None = None,
    ):
        timestamp = datetime.now(UTC).isoformat()
        if is_final:
            if speech_end_time is not None:
                # Restore this capture's real VAD boundary. Later VAD frames
                # must not attribute an earlier result to a newer utterance.
                await self.start_ttfb_metrics(start_time=speech_end_time)
            await self.push_frame(
                TranscriptionFrame(transcript, user_id, timestamp, finalized=True)
            )
        else:
            await self.push_frame(
                InterimTranscriptionFrame(transcript, user_id, timestamp)
            )


def _create_provider_vad(stop_secs, continuation_confidence):
    from pipecat.audio.vad.silero import SileroVADAnalyzer
    from pipecat.audio.vad.vad_analyzer import VADParams, VADState

    class ContinuationSileroVAD(SileroVADAnalyzer):
        def voice_confidence(self, buffer):
            confidence = super().voice_confidence(buffer)
            # Start speech at the original threshold. Once speaking, weaker
            # phonemes can continue the same turn without restarting context.
            # Volume gating and the actual silence duration remain unchanged.
            if (self._vad_state in (VADState.SPEAKING, VADState.STOPPING)
                    and confidence >= continuation_confidence):
                return max(confidence, self.params.confidence)
            return confidence

    detector = ContinuationSileroVAD(sample_rate=16000, params=VADParams(stop_secs=stop_secs))
    detector.set_sample_rate(16000)
    return detector


class GiggyV21PipecatSTTService(GiggySTTService):
    """Pipecat bridge for the qualified V21 external-endpoint lifecycle.

    Reserve the first worker during StartFrame, before consuming capture PCM.
    A provider Silero detector confirms live endpoints after 100ms and prepares
    an uncommitted candidate at quiet onset. The upstream VAD and native metric
    stay unchanged; publication waits for its boundary if inference finishes first.
    No future audio or benchmark EOF knowledge is used.
    Use the public GiggyExternalSTTService for caller-owned or file/stream EOF.
    """

    def __init__(self, *, provider_vad_stop_secs: float = 0.1,
                 minimum_context_secs: float = 0.0, short_context_stop_secs: float = 0.4,
                 continuation_confidence: float = 0.35, **kwargs):
        if "endpointing" in kwargs:
            raise ValueError("The V21 Pipecat bridge owns external endpointing")
        if (isinstance(continuation_confidence, bool) or not isinstance(continuation_confidence, (int, float))
                or not 0 < continuation_confidence <= .7):
            raise ValueError("continuation_confidence must be greater than0 and at most0.7")
        if not isinstance(provider_vad_stop_secs, (int, float)) or isinstance(provider_vad_stop_secs, bool) or not 0.1 <= provider_vad_stop_secs <= 1.0:
            raise ValueError("provider_vad_stop_secs must be between 0.1 and 1.0 seconds")
        if (isinstance(minimum_context_secs, bool) or not isinstance(minimum_context_secs, (int, float))
                or not 0 <= minimum_context_secs <= 4):
            raise ValueError("minimum_context_secs must be between0 and4 seconds")
        if (isinstance(short_context_stop_secs, bool) or not isinstance(short_context_stop_secs, (int, float))
                or not .1 <= short_context_stop_secs <= 1):
            raise ValueError("short_context_stop_secs must be between0.1 and1 seconds")
        super().__init__(endpointing="external", **kwargs)
        self._provider_vad_stop_secs = provider_vad_stop_secs
        self._minimum_context_secs = minimum_context_secs
        self._short_context_stop_secs = short_context_stop_secs
        self._continuation_confidence = continuation_confidence
        self._v21_short_pending = False
        self._v21_speaking = False
        self._v21_stopping = False
        self.turn_readiness_wait_ms: list[float] = []
        self._native_vad = None
        self._native_vad_state = None
        self._native_boundary_turns = []

    async def start(self, frame):
        self._v21_stopping = False
        await super().start(frame)
        await self.start_utterance()

    async def drain_pending_turns(self, *, timeout: float = 30.0):
        """Finish clean input and await every captured turn before collection.

        Call only after the input queue has drained. This is distinct from
        cancellation, which must still abort. A failed or timed-out drain raises
        rather than accepting an earlier partial transcript as sample success.
        Native transcript timestamps and metric collection are unchanged.
        """
        short = self._capture is not None and 0 < self._capture.captured < _MIN_AUDIO_BYTES
        try:
            async with asyncio.timeout(timeout):
                await self.stop(EndFrame())
            if self._failed or short:
                raise RuntimeError("V21 input did not complete every captured turn")
        except BaseException:
            await self._abort()
            raise

    def _context_endpoint_secs(self, turn):
        context_samples = turn.quiet_start_sample if turn.quiet_start_sample is not None else turn.captured // 2
        if context_samples < self._minimum_context_secs * 16000:
            return max(self._provider_vad_stop_secs, self._short_context_stop_secs)
        return self._provider_vad_stop_secs

    async def run_stt(self, audio: bytes):
        # Provider endpointing is independent from the upstream 200ms VAD.
        # Use an actual speech model, not zero samples/RMS, to prepare a tail
        # on STOPPING and confirm it after the configured silence window.
        # Any resumed speech withdraws the candidate.
        stream = self._capture is not None and self._capture.input_transport in {"ndjson-v1", "websocket-v1"}
        # Once initialized, the detector must see the continuous PCM timeline,
        # including gaps and a later reservation's readiness wait. Otherwise a
        # short resumed word may never take it through SPEAKING -> STOPPING.
        native = stream or self._native_vad is not None or bool(self._native_boundary_turns)
        state = None
        if native:
            from pipecat.audio.vad.vad_analyzer import VADParams, VADState
            if self._native_vad is None:
                self._native_vad = _create_provider_vad(self._provider_vad_stop_secs, self._continuation_confidence)
            state = await self._native_vad.analyze_audio(audio)
            self._native_vad_state = state
            if state == VADState.SPEAKING and not self._v21_speaking:
                await self._begin_v21_speech()
        if not self._v21_speaking:
            self._pre_roll.extend(audio[-32000:])
            del self._pre_roll[:-32000]
            yield None
            return
        turn = self._capture
        async for frame in super().run_stt(audio):
            yield frame
        if native and turn is not None and not self._failed:
            if turn.metric_boundary is None:
                turn.metric_boundary = asyncio.Event()
                self._native_boundary_turns.append(turn)
            if state == VADState.STOPPING and turn.quiet_start_sample is None:
                turn.quiet_start_sample = turn.captured // 2
            endpoint_secs = self._context_endpoint_secs(turn)
            if state == VADState.STOPPING and turn.decode_end_sample is None:
                # Include the first model-classified quiet window. Only later
                # confirmed quiet tail can be excluded from the model request.
                turn.decode_end_sample = turn.captured // 2
                turn.tail_candidate_refreshed = False
                turn.changed.set()
            elif (state in (VADState.STOPPING, VADState.QUIET) and endpoint_secs > .2
                  and turn.decode_end_sample is not None
                  and not turn.tail_candidate_refreshed
                  and turn.captured // 2 - turn.decode_end_sample
                      >= int((endpoint_secs - .2) * 16000)):
                # The server permits exclusion of at most 200ms. For a longer
                # confirmation window, prepare one later fixed candidate while
                # still STOPPING; reuse it through final rather than dropping
                # an expired candidate and decoding the whole quiet tail again.
                turn.decode_end_sample = turn.captured // 2
                turn.tail_candidate_refreshed = True
                turn.changed.set()
            elif state in (VADState.SPEAKING, VADState.STARTING):
                turn.decode_end_sample = None
                turn.tail_candidate_refreshed = False
                turn.quiet_start_sample = None
                self._resume_short_capture()
            elif state == VADState.QUIET and (turn.decode_end_sample is not None or self._v21_short_pending):
                context_wait_complete = (endpoint_secs <= self._provider_vad_stop_secs
                    or turn.quiet_start_sample is not None and
                       turn.captured // 2 - turn.quiet_start_sample >= endpoint_secs * 16000)
                if turn.captured >= _MIN_AUDIO_BYTES and context_wait_complete:
                    self._finish_capture()
                    self._v21_speaking = False
                    self._v21_short_pending = False
        elif self._v21_short_pending and turn is not None and turn.captured >= _MIN_AUDIO_BYTES:
            # Legacy POST transport has only upstream VAD. Real subsequent PCM,
            # including real quiet frames, satisfies admission without padding.
            self._finish_capture()
            self._v21_speaking = self._v21_short_pending = False

    def _resume_short_capture(self, *, clear_metric_boundary=False):
        turn = self._capture
        # Only an actual upstream start supersedes its previous timing boundary.
        # Provider hysteresis can legitimately continue while upstream is quiet.
        if clear_metric_boundary and turn is not None and (self._v21_short_pending or turn.speech_end_time is not None):
            turn.speech_end_time = None
            if turn.metric_boundary is not None:
                turn.metric_boundary.clear()
                if turn not in self._native_boundary_turns:
                    self._native_boundary_turns.append(turn)
        self._v21_short_pending = False

    async def _begin_v21_speech(self, *, upstream_start=False):
        if self._v21_speaking:
            self._resume_short_capture(clear_metric_boundary=upstream_start)
            return
        if self._failed or self._muted:
            return
        if self._pending_audio_bytes + len(self._pre_roll) > _MAX_AUDIO_BYTES:
            await self._fail("Giggy STT cannot keep up with new utterances; capture queue is full.")
            return
        readiness_started = asyncio.get_running_loop().time()
        if self._capture is None:
            self._begin_external_utterance()
        self.turn_readiness_wait_ms.append((asyncio.get_running_loop().time() - readiness_started) * 1000)
        del self.turn_readiness_wait_ms[:-32]
        turn = self._capture
        if turn is not None:
            self._finalize_turn = turn
            turn.audio.extend(self._pre_roll)
            turn.captured += len(self._pre_roll)
            self._pending_audio_bytes += len(self._pre_roll)
            self._pre_roll.clear()
            if turn.idle_timer is not None:
                turn.idle_timer.cancel()
                turn.idle_timer = None
            turn.changed.set()
            self._v21_speaking = True

    async def _discard_v21_reservation(self, *, short=False):
        turn = self._capture
        self._capture = None
        if turn is None:
            return
        if self._finalize_turn is turn:
            self._finalize_turn = None
        if self._external_turn is turn:
            self._external_turn = None
        if turn.task is not None:
            turn.task.cancel()
            await asyncio.gather(turn.task, return_exceptions=True)
        if short:
            await self.push_error("Giggy STT needs at least 0.5 seconds of captured audio.")

    def _prepare_v21_next_turn(self):
        # One idle reservation, bounded by the existing 15-second deadline.
        # No PCM is sent until VAD actually starts another utterance.
        if self._capture is None and not self._failed and not self._muted and not self._v21_stopping and len(self._turn_tasks) < 8:
            previous_final = self._finalize_turn
            self._begin_external_utterance()
            self._finalize_turn = previous_final

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        if isinstance(frame, VADUserStartedSpeakingFrame):
            await self._begin_v21_speech(upstream_start=True)
        elif isinstance(frame, VADUserStoppedSpeakingFrame):
            native_holds_turn = False
            # Preserve the upstream native metric boundary. A result already
            # computed/fenced during VAD may publish here; the metric can never
            # credit a final before its real publication.
            for turn in self._native_boundary_turns:
                if frame.stop_secs > 0:
                    turn.speech_end_time = frame.timestamp - frame.stop_secs
                turn.metric_boundary.set()
            self._native_boundary_turns.clear()
            turn = self._capture
            if self._v21_speaking and turn is not None:
                if frame.stop_secs > 0:
                    turn.speech_end_time = frame.timestamp - frame.stop_secs
                native_provider = self._native_vad is not None and turn.input_transport in {"ndjson-v1", "websocket-v1"}
                native_holds_turn = native_provider and (
                    self._context_endpoint_secs(turn) > frame.stop_secs
                    or getattr(self._native_vad_state, "name", None) in ("SPEAKING", "STARTING", "STOPPING"))
                if native_holds_turn:
                    # Upstream VAD defines the metric boundary, not this
                    # provider's endpoint. Retain the same decoder context
                    # until its own detector confirms quiet or speech resumes.
                    if turn.captured < _MIN_AUDIO_BYTES:
                        turn.decode_end_sample = None
                        self._v21_short_pending = True
                elif turn.captured < _MIN_AUDIO_BYTES:
                    # Retain a short utterance until real quiet PCM satisfies
                    # the API minimum, or merge resumed speech into this turn.
                    # Never discard speech just because VAD split it early.
                    self._v21_short_pending = True
                    turn.decode_end_sample = None
                else:
                    self._finish_capture()
                    self._v21_short_pending = False
            self._v21_speaking = bool(self._capture is not None and (self._v21_short_pending or native_holds_turn))
        await super().process_frame(frame, direction)

    def _on_fenced_final(self, turn):
        # Match V21's fast-final boundary: the next admission may overlap
        # persistence, never compete with the current final decode/delivery.
        if not self._v21_speaking:
            self._prepare_v21_next_turn()

    async def stop(self, frame: EndFrame):
        self._v21_stopping = True
        self._v21_short_pending = False
        for turn in self._native_boundary_turns:
            turn.metric_boundary.set()
        self._native_boundary_turns.clear()
        if self._capture is not None:
            self._capture.decode_end_sample = None
            if self._capture.captured < _MIN_AUDIO_BYTES:
                await self._discard_v21_reservation(short=self._v21_speaking)
        self._v21_speaking = False
        await super().stop(frame)

    async def cancel(self, frame: CancelFrame):
        self._v21_stopping = True
        await super().cancel(frame)

    async def _abort(self):
        self._v21_speaking = self._v21_short_pending = False
        self._native_boundary_turns.clear()
        await super()._abort()

    async def cleanup(self):
        self._v21_stopping = True
        if self._native_vad is not None:
            await self._native_vad.cleanup()
        self._native_boundary_turns.clear()
        await super().cleanup()
