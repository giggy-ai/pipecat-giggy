"""Offline protocol and real Pipecat pipeline qualification; no billable API calls."""
import asyncio
import base64
import json
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pipecat.frames.frames import (
    CancelFrame, EndFrame, InputAudioRawFrame, InterimTranscriptionFrame,
    InterruptionFrame, STTMuteFrame, TranscriptionFrame,
    VADUserStartedSpeakingFrame, VADUserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.settings import STTSettings
from pipecat_giggy import GiggySTTService

KEY = "offline-secret-key"
TOKEN = "a" * 64
PCM = b"\x01\x02" * 8000
DOWN = FrameDirection.DOWNSTREAM


class Response:
    def __init__(self, body=None, status=200, stream=False):
        self.body = body
        self.status = status
        self.headers = {"content-type": "text/event-stream" if stream else "application/json"}
        self.content = self
        self.queue = asyncio.Queue()
        self.closed = False
        self.stream = stream

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True

    async def json(self):
        return self.body

    def event(self, event, body):
        self.queue.put_nowait((f"event: {event}\r\ndata: " + json.dumps(body, ensure_ascii=False) + "\r\n\r\n").encode())

    async def iter_chunked(self, size):
        if not self.stream:
            yield json.dumps(self.body).encode()
            return
        while True:
            item = await self.queue.get()
            if item is None:
                return
            # Split UTF-8 and CRLF boundaries, independently of event boundaries.
            for offset in range(0, len(item), 7):
                yield item[offset:offset + 7]


class Provider:
    def __init__(self, fault=None, hold=False):
        self.calls = []
        self.turns = {}
        self.fault = fault
        self.hold = hold
        self.closed = False
        self.first_packet = asyncio.Event()
        self.admitted = asyncio.Event()

    def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        if url.endswith("/v1/audio/transcriptions"):
            identity = str(uuid4())
            response = Response(stream=True, status=self.fault if isinstance(self.fault, int) else 200)
            turn = {"response": response, "pcm": bytearray(), "sequence": 0}
            self.turns[identity] = turn
            response.queue.put_nowait(b": heartbeat\r\n\r\n")
            response.event("generation_created", {"generation_uuid": identity} if self.fault != "admission-object" else {"generation":[]})
            ready_id = str(uuid4()) if self.fault == "identity" else identity
            if self.fault != "no-ready":
                response.event("microphone_ready", {"generation_uuid": ready_id, "attempt": 1, "input_token": TOKEN})
            if self.fault == "no-ready":
                response.queue.put_nowait(None)
            self.admitted.set()
            return response
        assert "/stt-input" in url
        identity = url.split("/")[-2]
        turn = self.turns[identity]
        payload = kwargs["json"]
        assert payload["sequence"] == turn["sequence"]
        turn["sequence"] += 1
        turn["pcm"].extend(base64.b64decode(payload["pcm_base64"]))
        response = turn["response"]
        if turn["sequence"] == 1:
            self.first_packet.set()
            transcript = {"generation_uuid": identity, "attempt": 1, "sequence": 0,
                          "replace": True, "text": "Hello"}
            if self.fault == "attempt":
                transcript["attempt"] = 2
            if self.fault == "replace":
                transcript["replace"] = False
            response.event("transcript_chunk", transcript)
            if self.fault == "malformed":
                response.queue.put_nowait(b"event: transcript_chunk\ndata: nope\n\n")
            if self.fault == "duplicate":
                response.event("transcript_chunk", transcript)
        if payload["complete"] and not self.hold:
            response.event("transcript_chunk", {"generation_uuid": identity, "attempt": 1, "sequence": 1,
                                                "replace": True, "text": "こんにちは"})
            response.event("generation_updated", {"generation_uuid": identity, "status": "completed"})
            response.queue.put_nowait(None)
        return Response([] if self.fault == "ack-object" else {"ok": self.fault != "ack"}, status=410 if self.fault == "input-http" else 200)

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        identity = url.split("/")[-1]
        turn = self.turns[identity]
        if "/v1/generations/" in url:
            status = "failed" if self.fault == "durable" else "completed"
            if self.fault == "status-object":
                return Response({"generation":[]})
            return Response({"generation": {"generation_uuid": identity, "status": status,
                                            "result": {"url": f"https://artifacts.test/{identity}"}}})
        duration = (len(turn["pcm"]) + 31) // 32
        if self.fault == "duration":
            duration += 1
        return Response({"text": "Canonical final text", "audio_duration_ms": duration})


def unit_service(provider):
    service = GiggySTTService(api_key=KEY, aiohttp_session=provider)
    service._sample_rate = 16000
    service.create_task = lambda coro, **kwargs: asyncio.create_task(coro)
    service.push_frame = AsyncMock()
    service.push_error = AsyncMock()
    return service


async def begin(service, audio=PCM):
    await service.process_frame(VADUserStartedSpeakingFrame(), DOWN)
    await service.process_audio_frame(InputAudioRawFrame(audio, 16000, 1), DOWN)


async def finish(service):
    service._finish_capture()
    await asyncio.wait_for(asyncio.gather(*tuple(service._turn_tasks)), 2)


@pytest.mark.asyncio
async def test_live_partial_rewrites_and_only_durable_final():
    provider = Provider()
    service = unit_service(provider)
    await begin(service)
    await asyncio.wait_for(provider.first_packet.wait(), 1)
    assert service._capture is not None and not service._capture.ended
    await service.process_audio_frame(InputAudioRawFrame(PCM[:66], 16000, 1), DOWN)
    await finish(service)
    frames = [call.args[0] for call in service.push_frame.call_args_list]
    partials = [f.text for f in frames if isinstance(f, InterimTranscriptionFrame)]
    finals = [f for f in frames if isinstance(f, TranscriptionFrame)]
    assert partials == ["Hello", "こんにちは"]
    assert len(finals) == 1 and finals[0].text == "Canonical final text" and finals[0].finalized
    admission = provider.calls[0][2]
    assert admission["json"]["mode"] == "streaming" and admission["json"]["microphone"]
    assert admission["headers"] == {"xi-api-key": KEY}
    posts = [c[2]["json"] for c in provider.calls if c[1].endswith("/stt-input")]
    assert [p["sequence"] for p in posts] == [0, 1]
    assert len(base64.b64decode(posts[-1]["pcm_base64"])) == 66
    assert posts[-1]["complete"]
    assert b"".join(base64.b64decode(p["pcm_base64"]) for p in posts) == PCM + PCM[:66]
    assert all(c[2]["allow_redirects"] is False for c in provider.calls)
    assert "headers" not in provider.calls[-1][2]
    assert all(t["response"].closed for t in provider.turns.values())
    assert not provider.closed
    service.push_error.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [301, 401, 402, 429, 503, "identity", "no-ready", "attempt", "replace", "malformed", "duplicate", "ack", "input-http", "durable", "duration", "admission-object", "ack-object", "status-object"])
async def test_failures_safe_no_retries_or_false_final(fault):
    provider = Provider(fault=fault)
    service = unit_service(provider)
    await begin(service)
    await finish(service)
    assert service.push_error.called
    assert KEY not in str(service.push_error.call_args) and TOKEN not in str(service.push_error.call_args)
    assert not any(isinstance(c.args[0], TranscriptionFrame) for c in service.push_frame.call_args_list)
    assert len(provider.turns) == 1
    assert all(t["response"].closed for t in provider.turns.values())
    assert not service._turn_tasks


@pytest.mark.asyncio
async def test_short_capture_never_admits_or_pads():
    provider = Provider()
    service = unit_service(provider)
    await begin(service, audio=PCM[:1000])
    await finish(service)
    assert not provider.calls and service.push_error.called


@pytest.mark.asyncio
async def test_empty_final_packet_and_interruption_preserves_stt():
    provider = Provider()
    service = unit_service(provider)
    await begin(service)
    await provider.first_packet.wait()
    await service.process_frame(InterruptionFrame(), DOWN)
    await finish(service)
    posts = [c[2]["json"] for c in provider.calls if c[1].endswith("/stt-input")]
    assert posts[-1]["pcm_base64"] == "" and posts[-1]["complete"]
    assert any(isinstance(c.args[0], TranscriptionFrame) for c in service.push_frame.call_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["cancel", "mute", "cleanup"])
async def test_cancel_mute_cleanup_close_sse_and_sender(action):
    provider = Provider(hold=True)
    service = unit_service(provider)
    await begin(service)
    await provider.first_packet.wait()
    if action == "cancel":
        await service.cancel(CancelFrame())
    elif action == "mute":
        await service.process_frame(STTMuteFrame(mute=True), DOWN)
    else:
        # Exercise adapter cleanup without tearing down uninitialized base internals.
        await service._abort()
    assert all(t["response"].closed for t in provider.turns.values())
    assert not service._turn_tasks and service._capture is None
    assert not any(isinstance(c.args[0], TranscriptionFrame) for c in service.push_frame.call_args_list)


@pytest.mark.asyncio
async def test_repeated_turns_ordered_with_bounded_pending_capture():
    provider = Provider()
    service = unit_service(provider)
    await begin(service)
    await provider.first_packet.wait()
    service._finish_capture()
    await begin(service)
    await finish(service)
    finals = [c.args[0] for c in service.push_frame.call_args_list if isinstance(c.args[0], TranscriptionFrame)]
    assert len(finals) == 2 and len(provider.turns) == 2
    admissions = [c[2]["json"] for c in provider.calls if c[1].endswith("/transcriptions")]
    assert admissions == [{"mode": "streaming", "microphone": True}] * 2
    assert len(provider.turns) == 2


@pytest.mark.asyncio
async def test_turn_queue_overflow_closes_prior_capture():
    provider = Provider(hold=True)
    service = unit_service(provider)
    await begin(service)
    await provider.first_packet.wait()
    service._finish_capture()
    await begin(service)
    service._finish_capture()
    await begin(service)
    assert service._failed and service.push_error.called
    assert not service._turn_tasks


@pytest.mark.asyncio
async def test_audio_limit_fails_without_rolling_or_retrying():
    provider = Provider(hold=True)
    service = unit_service(provider)
    await begin(service)
    await provider.first_packet.wait()
    await service.process_audio_frame(InputAudioRawFrame(PCM * 60, 16000, 1), DOWN)
    assert service._failed and not service._turn_tasks
    assert all(t["response"].closed for t in provider.turns.values())


@pytest.mark.asyncio
@pytest.mark.parametrize("audio,rate,channels", [(b"x",16000,1),(PCM,24000,1),(PCM,16000,2)], ids=["odd-sample", "wrong-rate", "stereo"])
async def test_bad_audio_never_admits(audio, rate, channels):
    provider = Provider()
    service = unit_service(provider)
    await service.process_audio_frame(InputAudioRawFrame(audio, rate, channels), DOWN)
    assert service._failed and not provider.calls


@pytest.mark.asyncio
async def test_settings_reject_unsupported_changes_without_mutation():
    service = unit_service(Provider())
    await service._update_settings(STTSettings(language="en"))
    await service._update_settings(STTSettings(model="another-model"))
    assert service._settings.language is None and service._settings.model == "giggyspeech-stt"
    assert service.push_error.call_count == 2


@pytest.mark.asyncio
async def test_real_pipeline_vad_frames_and_caller_session_ownership():
    from pipecat.pipeline.worker import PipelineParams
    from pipecat.tests.utils import SleepFrame, run_test
    provider = Provider()
    service = GiggySTTService(api_key=KEY, aiohttp_session=provider)
    down, up = await asyncio.wait_for(run_test(
        service,
        frames_to_send=[VADUserStartedSpeakingFrame(), SleepFrame(sleep=.02),
                        InputAudioRawFrame(PCM,16000,1), SleepFrame(sleep=.05),
                        VADUserStoppedSpeakingFrame(), SleepFrame(sleep=.1)],
        pipeline_params=PipelineParams(audio_in_sample_rate=16000), enable_rtvi=False,
    ), 5)
    finals = [f for f in down if isinstance(f,TranscriptionFrame)]
    assert len(finals) == 1 and finals[0].finalized
    assert len(provider.turns) == 1 and not provider.closed and not service._turn_tasks
    assert not any(getattr(f,"error",None) for f in up)


@pytest.mark.parametrize("kwargs", [{"api_key":""},{"sample_rate":24000},{"base_url":"http://example.com"},
                                      {"base_url":"https://example.com/path"},{"base_url":"https://secret@example.com"},
                                      {"keepalive_timeout":1},{"settings":STTSettings()}])
def test_invalid_configuration(kwargs):
    params = {"api_key":KEY,"aiohttp_session":Provider(),**kwargs}
    with pytest.raises(ValueError):
        GiggySTTService(**params)


@pytest.mark.asyncio
async def test_real_http_sse_ordered_input_and_durable_status():
    import aiohttp
    from aiohttp import web
    identity = str(uuid4())
    ready = asyncio.Event()
    completed = asyncio.Event()
    events = asyncio.Queue()
    received = bytearray()
    sequences = []
    admission_calls = []

    async def admit(request):
        admission_calls.append(await request.json())
        assert request.headers["xi-api-key"] == KEY
        response = web.StreamResponse(headers={"Content-Type":"text/event-stream"})
        await response.prepare(request)
        for event, payload in [("generation_created",{"generation_uuid":identity}),
                               ("microphone_ready",{"generation_uuid":identity,"attempt":1,"input_token":TOKEN})]:
            await response.write((f"event: {event}\ndata: {json.dumps(payload)}\n\n").encode())
        ready.set()
        while True:
            payload = await events.get()
            if payload is None:
                break
            await response.write((f"event: transcript_chunk\ndata: {json.dumps(payload)}\n\n").encode())
        await response.write_eof()
        return response

    async def receive(request):
        payload = await request.json()
        assert "xi-api-key" not in request.headers
        assert payload["input_token"] == TOKEN
        assert payload["sequence"] == len(sequences)
        sequences.append(payload["sequence"])
        received.extend(base64.b64decode(payload["pcm_base64"]))
        await events.put({"generation_uuid":identity,"attempt":1,"sequence":len(sequences)-1,"replace":True,"text":"live text"})
        if payload["complete"]:
            completed.set()
            await events.put(None)
        return web.json_response({"ok":True})

    async def status(request):
        assert request.headers["xi-api-key"] == KEY and completed.is_set()
        return web.json_response({"generation":{"generation_uuid":identity,"status":"completed", "result":{"url":f"https://artifacts.test/{identity}"}}})

    app = web.Application()
    app.router.add_post("/v1/audio/transcriptions",admit)
    app.router.add_post("/api/generations/{identity}/stt-input",receive)
    app.router.add_get("/v1/generations/{identity}",status)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner,"127.0.0.1",0)
        await site.start()
        async with aiohttp.ClientSession() as session:
            class Transport:
                def post(self,*args,**kwargs):
                    return session.post(*args,**kwargs)
                def get(self,url,**kwargs):
                    if url.startswith("https://artifacts.test/"):
                        assert "headers" not in kwargs
                        return Response({"text":"durable text","audio_duration_ms":(len(received)+31)//32})
                    return session.get(url,**kwargs)
            service = unit_service(Transport())
            service._origin = f"http://127.0.0.1:{runner.addresses[0][1]}"
            await begin(service)
            await asyncio.wait_for(ready.wait(),1)
            await service.process_audio_frame(InputAudioRawFrame(PCM[:3200],16000,1),DOWN)
            await finish(service)
            assert bytes(received) == PCM + PCM[:3200] and sequences == [0,1]
            assert len(admission_calls) == 1 and not session.closed
            assert any(isinstance(c.args[0],TranscriptionFrame) for c in service.push_frame.call_args_list)
            service.push_error.assert_not_called()
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_transcription_replay_example_offline(monkeypatch,tmp_path):
    import importlib.util
    import sys
    import wave
    from pathlib import Path
    path = Path(__file__).resolve().parents[1] / "examples" / "transcribe.py"
    spec = importlib.util.spec_from_file_location("stt_example",path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    recording = tmp_path / "fixture.wav"
    with wave.open(str(recording),"wb") as wav:
        wav.setnchannels(1); wav.setsampwidth(2); wav.setframerate(16000); wav.writeframes(PCM)
    monkeypatch.setattr(sys,"argv",["transcribe.py","--input",str(recording)])
    monkeypatch.delenv("GIGGY_ALLOW_BILLABLE_EXAMPLE",raising=False)
    with pytest.raises(RuntimeError,match="authorize one transcription request"):
        await module.main()
    provider = Provider()
    class Context:
        async def __aenter__(self):
            return provider
        async def __aexit__(self,*args):
            provider.closed = True
    monkeypatch.setattr(module.aiohttp,"ClientSession",lambda:Context())
    monkeypatch.setenv("GIGGY_API_KEY",KEY)
    monkeypatch.setenv("GIGGY_ALLOW_BILLABLE_EXAMPLE","1")
    await asyncio.wait_for(module.main(),5)
    assert len(provider.turns) == 1 and provider.closed
    assert next(iter(provider.turns.values()))["pcm"] == PCM


@pytest.mark.asyncio
async def test_combined_stt_and_tts_pipeline_offline():
    from pipecat.frames.frames import TTSAudioRawFrame, TTSSpeakFrame
    from pipecat.pipeline.pipeline import Pipeline
    from pipecat.pipeline.worker import PipelineParams
    from pipecat.processors.frame_processor import FrameProcessor
    from pipecat.tests.utils import SleepFrame, run_test
    from pipecat_giggy import GiggyHttpTTSService
    provider = Provider()
    original_post = provider.post
    class AudioResponse(Response):
        async def iter_chunked(self,size):
            yield PCM
    def post(url,**kwargs):
        if url.endswith("/v1/audio/speech"):
            provider.calls.append(("POST",url,kwargs))
            result = AudioResponse()
            result.headers["content-type"] = "application/octet-stream"
            return result
        return original_post(url,**kwargs)
    provider.post = post
    class Reply(FrameProcessor):
        async def process_frame(self,frame,direction):
            await super().process_frame(frame,direction)
            if isinstance(frame,TranscriptionFrame):
                await self.push_frame(TTSSpeakFrame("Reply to the canonical transcript."))
            await self.push_frame(frame,direction)
    stt = GiggySTTService(api_key=KEY,aiohttp_session=provider)
    tts = GiggyHttpTTSService(api_key=KEY,voice_id=str(uuid4()),aiohttp_session=provider)
    down,up = await asyncio.wait_for(run_test(
        Pipeline([stt,Reply(),tts]),
        frames_to_send=[VADUserStartedSpeakingFrame(),SleepFrame(sleep=.02),
                        InputAudioRawFrame(PCM,16000,1),SleepFrame(sleep=.05),
                        VADUserStoppedSpeakingFrame(),SleepFrame(sleep=.15)],
        pipeline_params=PipelineParams(audio_in_sample_rate=16000,audio_out_sample_rate=24000),
        enable_rtvi=False,
    ),5)
    assert any(isinstance(f,TranscriptionFrame) and f.finalized for f in down)
    assert any(isinstance(f,TTSAudioRawFrame) and f.sample_rate == 24000 for f in down)
    assert len(provider.turns) == 1 and len([c for c in provider.calls if c[1].endswith("/v1/audio/speech")]) == 1
    assert not any(getattr(f,"error",None) for f in up)
