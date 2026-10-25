"""Cloud-only Giggy sandbox using Pipecat services, frames, and WebSocket transport."""
import asyncio
import base64
import json
import os
import secrets
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4
from urllib.parse import urlsplit

import aiohttp
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import FileResponse, JSONResponse
from loguru import logger
from pipecat.frames.frames import (
    EndFrame, ErrorFrame, Frame, InputAudioRawFrame, InputTransportMessageFrame,
    InterimTranscriptionFrame, InterruptionFrame, LLMFullResponseStartFrame,
    LLMFullResponseEndFrame, LLMTextFrame, OutputAudioRawFrame,
    OutputTransportMessageFrame, OutputTransportMessageUrgentFrame, STTMuteFrame,
    TTSSpeakFrame, TTSUpdateSettingsFrame, TranscriptionFrame,
    UserStartedSpeakingFrame, UserStoppedSpeakingFrame, ProposedUserStoppedSpeakingFrame,
    VADUserStartedSpeakingFrame, VADUserStoppedSpeakingFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair, LLMUserAggregatorParams
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.serializers.base_serializer import FrameSerializer
from pipecat.services.deepseek.llm import DeepSeekLLMService
from pipecat.transports.websocket.fastapi import FastAPIWebsocketParams, FastAPIWebsocketTransport
from pipecat.turns.user_start import ExternalUserTurnStartStrategy
from pipecat.turns.user_stop import ExternalUserTurnStopStrategy
from pipecat.turns.user_turn_strategies import UserTurnStrategies
from pipecat.workers.runner import WorkerRunner
from pipecat_giggy import GiggyHttpTTSService, GiggyExternalSTTService
from sandbox_config import VOICES, GREETINGS

# Provider/library debug logs can contain prompts or upstream error bodies.
# The example emits only sanitized errors over its browser protocol.
logger.remove()
PUBLIC = Path(__file__).parent / "public"
ALLOWED_TYPES = {"voice_agent_session_start", "voice_agent_session_end", "voice_agent_interrupt",
                 "voice_agent_audio_start", "voice_agent_audio_chunk", "voice_agent_audio_end", "voice_agent_user_text"}


@dataclass
class Account:
    giggy_key: str
    deepseek_key: str
    expires: float
    socket: WebSocket | None = None


@dataclass
class Conversation:
    websocket: WebSocket
    account: Account
    current: str = ""
    output_turn: str = ""
    sequence: int = 0
    input_sequence: int = 0
    input_bytes: int = 0
    capture: str = ""
    text: str = ""
    initialized: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def emit(self, kind, **values):
        async with self.lock:
            await self.websocket.send_json({"type": kind, **values})


class SandboxSerializer(FrameSerializer):
    """Adapt the sandbox browser protocol to Pipecat transport messages and PCM."""
    def __init__(self, state):
        super().__init__()
        self.state = state
        self.window = time.monotonic()
        self.messages = self.bytes = 0

    async def deserialize(self, data):
        if time.monotonic() - self.window > 60:
            self.window = time.monotonic(); self.messages = self.bytes = 0
        self.messages += 1; self.bytes += len(data)
        if len(data) > 65536 or self.messages > 1800 or self.bytes > 16 * 1024 * 1024:
            raise ValueError("Message limit exceeded")
        value = json.loads(data)
        if not isinstance(value, dict) or value.get("type") not in ALLOWED_TYPES or any(k.startswith("_") for k in value):
            raise ValueError("Invalid sandbox message")
        return InputTransportMessageFrame(message=value)

    async def serialize(self, frame):
        if self.should_ignore_frame(frame):
            return None
        if isinstance(frame, OutputAudioRawFrame) and self.state.output_turn:
            result = {"type": "voice_agent_audio_chunk", "turn_id": self.state.output_turn,
                      "sequence": self.state.sequence, "audio_format": "audio/pcm;rate=24000;channels=1;bits=16",
                      "chunk_base64": base64.b64encode(frame.audio).decode("ascii")}
            self.state.sequence += 1
            return json.dumps(result)
        if isinstance(frame, (OutputTransportMessageFrame, OutputTransportMessageUrgentFrame)):
            return json.dumps(frame.message)
        return None


class BrowserIngress(FrameProcessor):
    def __init__(self, state):
        super().__init__(); self.state = state

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if direction != FrameDirection.DOWNSTREAM or not isinstance(frame, InputTransportMessageFrame):
            await self.push_frame(frame, direction); return
        message = frame.message; kind = message["type"]; state = self.state
        if kind == "voice_agent_session_start":
            if state.initialized:
                return
            voice_id = message.get("voice", {}).get("voice_library_uuid")
            if voice_id not in {v["id"] for v in VOICES}:
                raise ValueError("Choose a demo voice")
            state.initialized = True
            await self.push_frame(TTSUpdateSettingsFrame(delta=GiggyHttpTTSService.Settings(voice=voice_id)))
            await state.emit("voice_agent_session_ready", runtime="cloud", worker_available=True)
            state.current = state.output_turn = "greeting-" + str(uuid4())
            state.text = secrets.choice(GREETINGS)
            await state.emit("voice_agent_intro", turn_id=state.current, text=state.text)
            await self.push_frame(TTSSpeakFrame(state.text))
            return
        if not state.initialized:
            raise ValueError("Start the session first")
        if kind in {"voice_agent_interrupt", "voice_agent_session_end"}:
            state.output_turn = ""; state.capture = ""
            await self.broadcast_interruption()
            await self.push_frame(STTMuteFrame(mute=True)); await self.push_frame(STTMuteFrame(mute=False))
            if kind == "voice_agent_session_end":
                await self.push_frame(EndFrame())
            return
        turn = str(message.get("turn_id", ""))
        if not turn or len(turn) > 100:
            raise ValueError("Invalid turn")
        if kind == "voice_agent_audio_start":
            state.current = state.capture = turn; state.input_sequence = state.input_bytes = 0
            await self.push_frame(UserStartedSpeakingFrame())
            await self.push_frame(VADUserStartedSpeakingFrame())
        elif kind == "voice_agent_audio_chunk":
            if turn != state.capture or message.get("sequence") != state.input_sequence or message.get("sample_rate") != 16000:
                raise ValueError("Invalid microphone sequence")
            pcm = base64.b64decode(message.get("chunk_base64", ""), validate=True)
            if not pcm or len(pcm) % 2 or len(pcm) > 64000 or state.input_bytes + len(pcm) > 960000:
                raise ValueError("Invalid microphone audio")
            state.input_sequence += 1; state.input_bytes += len(pcm)
            audio = InputAudioRawFrame(pcm, 16000, 1); audio.user_id = turn
            await self.push_frame(audio)
        elif kind == "voice_agent_audio_end":
            if turn != state.capture:
                raise ValueError("No active microphone turn")
            state.capture = ""
            await self.push_frame(VADUserStoppedSpeakingFrame())
        elif kind == "voice_agent_user_text":
            text = str(message.get("text", "")).strip()[:1200]
            if not text:
                return
            state.current = turn
            await self.push_frame(UserStartedSpeakingFrame())
            transcription = TranscriptionFrame(text, turn, datetime.now(UTC).isoformat())
            transcription.sandbox_typed = True
            await self.push_frame(transcription)


class TranscriptEvents(FrameProcessor):
    def __init__(self, state):
        super().__init__(); self.state = state

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if direction == FrameDirection.DOWNSTREAM and isinstance(frame, (TranscriptionFrame, InterimTranscriptionFrame)):
            if frame.user_id != self.state.current:
                return
            kind = "voice_agent_transcript_final" if isinstance(frame, TranscriptionFrame) else "voice_agent_transcript_partial"
            if not getattr(frame, "sandbox_typed", False):
                await self.state.emit(kind, turn_id=frame.user_id, text=frame.text)
            await self.push_frame(frame, direction)
            if isinstance(frame, TranscriptionFrame):
                await self.push_frame(ProposedUserStoppedSpeakingFrame())
            return
        await self.push_frame(frame, direction)


class ResponseEvents(FrameProcessor):
    def __init__(self, state):
        super().__init__(); self.state = state

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        state = self.state
        if direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, LLMFullResponseStartFrame):
                state.output_turn = state.current; state.text = ""
            elif isinstance(frame, LLMTextFrame):
                state.text += frame.text
                await state.emit("voice_agent_assistant_text_delta", turn_id=state.output_turn, delta=frame.text)
            elif isinstance(frame, InterruptionFrame):
                state.output_turn = ""
        await self.push_frame(frame, direction)


async def run_conversation(websocket, account, *, services=None):
    """The production pipeline uses Giggy STT, native DeepSeek, and Giggy TTS."""
    state = Conversation(websocket, account)
    async with aiohttp.ClientSession() as session:
        stt, llm, tts = services or (
            GiggyExternalSTTService(api_key=account.giggy_key, aiohttp_session=session, endpointing="external"),
            # Pipecat 1.12.0 needs provider options passed through its active base request builder.
            DeepSeekLLMService(api_key=account.deepseek_key, settings=DeepSeekLLMService.Settings(model="deepseek-flash", max_tokens=160,
                extra={"extra_body": {"thinking": {"type": "disabled"}}})),
            GiggyHttpTTSService(api_key=account.giggy_key, voice_id=VOICES[7]["id"], aiohttp_session=session),
        )
        transport = FastAPIWebsocketTransport(websocket, FastAPIWebsocketParams(
            audio_in_enabled=False, audio_out_enabled=True, audio_out_sample_rate=24000,
            serializer=SandboxSerializer(state), allowed_origins=[os.getenv("APP_ORIGIN", "http://localhost:3003")],
            session_timeout=min(900, max(1, int(account.expires - time.time()))),
        ))
        context = LLMContext([{"role": "system", "content": "You are Giggy, a friendly voice assistant. Answer in one or two concise natural sentences. No markdown or stage directions."}])
        aggregators = LLMContextAggregatorPair(context, user_params=LLMUserAggregatorParams(
            user_turn_strategies=UserTurnStrategies(start=[ExternalUserTurnStartStrategy(enable_interruptions=False)],
                                                  stop=[ExternalUserTurnStopStrategy(timeout=0.05)]),
            empty_user_turn=None, audio_idle_timeout=0,
        ))
        worker = PipelineWorker(Pipeline([transport.input(), BrowserIngress(state), stt, TranscriptEvents(state),
            aggregators.user(), llm, ResponseEvents(state), tts, transport.output(), aggregators.assistant()]),
            params=PipelineParams(audio_in_sample_rate=16000, audio_out_sample_rate=24000), enable_rtvi=False)
        runner = WorkerRunner(handle_sigint=False)

        @transport.event_handler("on_client_disconnected")
        async def on_disconnected(transport, client):
            await runner.cancel()

        @transport.event_handler("on_client_timeout")
        async def on_timeout(transport, client):
            await runner.cancel()

        @worker.event_handler("on_pipeline_error")
        async def on_error(worker, frame):
            try:
                await state.emit("voice_agent_error", turn_id=state.current, message="Pipecat voice request failed. Check your keys and credits.")
            except Exception:
                pass

        # Pipecat's native context/audio events report completion after playback.
        @aggregators.assistant().event_handler("on_assistant_turn_stopped")
        async def on_assistant_stopped(aggregator, message):
            if state.output_turn and not message.interrupted:
                await state.emit("voice_agent_turn_done", turn_id=state.output_turn, text=state.text)
                state.output_turn = ""
            messages = context.get_messages()
            if len(messages) > 33:
                context.set_messages([messages[0], *messages[-32:]])

        await runner.add_workers(worker)
        await runner.run()


def create_app(*, origin=None, validate_keys=None, conversation=run_conversation):
    origin = origin or os.getenv("APP_ORIGIN", "http://localhost:3003")
    parsed = urlsplit(origin)
    if origin != f"{parsed.scheme}://{parsed.netloc}" or (parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1", "::1"})):
        raise ValueError("Use an exact HTTPS origin, or HTTP loopback for development")
    secure = parsed.scheme == "https"; accounts = {}; attempts = {}

    async def revoke(token):
        account = accounts.pop(token, None)
        if account:
            if account.socket:
                try: await account.socket.close(code=1000)
                except Exception: pass
            account.giggy_key = account.deepseek_key = ""

    @asynccontextmanager
    async def lifespan(app):
        async def cleanup():
            while True:
                await asyncio.sleep(30)
                for token, account in list(accounts.items()):
                    if account.expires <= time.time(): await revoke(token)
                for ip, value in list(attempts.items()):
                    if value[0] < time.time(): attempts.pop(ip, None)
        task = asyncio.create_task(cleanup())
        yield
        task.cancel()
        for token in list(accounts): await revoke(token)
    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def security(request: Request, call_next):
        if request.method not in {"GET", "HEAD"} and request.headers.get("origin") != origin:
            return JSONResponse({"error": "Origin not allowed."}, 403)
        try:
            size = int(request.headers.get("content-length", "0"))
        except ValueError:
            return JSONResponse({"error": "Invalid request size."}, 400)
        if size > 8192:
            return JSONResponse({"error": "Request too large."}, 413)
        response = await call_next(request)
        response.headers.update({"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer",
            "Content-Security-Policy": "default-src 'self'; connect-src 'self'; img-src 'self'; script-src 'self'; style-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"})
        return response

    def account_for(request):
        account = accounts.get(request.cookies.get("giggy_pipecat_session"))
        return account if account and account.expires > time.time() else None

    @app.post("/api/session")
    async def connect(request: Request):
        ip = request.client.host
        until, count = attempts.get(ip, (time.time() + 60, 0))
        if until < time.time(): until, count = time.time() + 60, 0
        if len(attempts) >= 1000 and ip not in attempts or count >= 10:
            return JSONResponse({"error": "Too many attempts. Try again shortly."}, 429)
        attempts[ip] = (until, count + 1)
        if len(accounts) >= 100: return JSONResponse({"error": "Demo is at capacity."}, 503)
        try:
            raw = bytearray()
            async for chunk in request.stream():
                raw.extend(chunk)
                if len(raw) > 8192: return JSONResponse({"error": "Request too large."}, 413)
            body = json.loads(raw)
            keys = [str(body.get(name, "")).strip() for name in ["giggyKey", "deepseekKey"]]
            if not all(10 <= len(key) <= 512 and not any(c.isspace() for c in key) for key in keys):
                return JSONResponse({"error": "Enter your own Giggy and DeepSeek keys."}, 400)
            if validate_keys:
                valid = await validate_keys(*keys)
            else:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                    async def check(url, headers):
                        async with session.get(url, headers=headers, allow_redirects=False) as response: return response.status == 200
                    valid = all(await asyncio.gather(check("https://giggy.ai/v1/voices", {"xi-api-key": keys[0]}),
                        check("https://api.deepseek.com/models", {"Authorization": "Bearer " + keys[1]})))
            if not valid: return JSONResponse({"error": "Keys could not be validated. Sign up for Giggy and use your own keys."}, 401)
            await revoke(request.cookies.get("giggy_pipecat_session"))
            token = secrets.token_hex(32); accounts[token] = Account(*keys, time.time() + 1800)
            response = JSONResponse({"voices": VOICES, "expiresIn": 1800})
            response.set_cookie("giggy_pipecat_session", token, httponly=True, samesite="strict", secure=secure, max_age=1800)
            return response
        except Exception:
            return JSONResponse({"error": "Account setup could not complete; check your keys."}, 400)

    @app.get("/api/session")
    async def current(request: Request):
        return JSONResponse({"voices": VOICES}) if account_for(request) else JSONResponse({"error": "Connect your own keys first."}, 401)

    @app.delete("/api/session")
    async def disconnect(request: Request):
        await revoke(request.cookies.get("giggy_pipecat_session"))
        response = JSONResponse({"ok": True}); response.delete_cookie("giggy_pipecat_session", httponly=True, samesite="strict", secure=secure)
        return response

    @app.websocket("/api/voice-agent/realtime")
    async def realtime(websocket: WebSocket):
        account = account_for(websocket)
        if websocket.headers.get("origin") != origin or websocket.url.query or not account or account.socket:
            await websocket.close(code=1008); return
        account.socket = websocket
        try:
            await websocket.accept()
            await conversation(websocket, account)
        except Exception:
            try: await websocket.close(code=1011, reason="Voice session failed")
            except Exception: pass
        finally:
            account.socket = None

    @app.get("/{requested:path}")
    async def static(requested: str):
        allowed = {"": "index.html", "app/voice-agents/demo": "index.html", **{name: name for name in
            ["app.js", "style.css", "microphone-worklet.js", "giggy-avatar.webp", "giggy-logo.svg"]}}
        if requested not in allowed: return JSONResponse({"error": "Not found."}, 404)
        return FileResponse(PUBLIC / allowed[requested])
    return app


app = create_app()
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.getenv("HOST", "127.0.0.1"), port=int(os.getenv("PORT", "3003")),
                access_log=False, ws_max_size=65536)
