"""External endpoint lifecycle and real WebSocket tests; no provider credentials."""
import asyncio,json,unittest
from unittest.mock import patch
from pipecat_giggy import stt as m
async def check():
 svc=m.GiggySTTService(api_key='unit',aiohttp_session=object(),endpointing='external');svc.create_task=lambda c,**kw:asyncio.create_task(c);svc._user_id='unit';calls=[];release=asyncio.Event()
 async def stream(self,turn,previous=None):
  calls.append(turn);await release.wait();turn.generation_id='ready';turn.ready.set()
  while not turn.ended:turn.changed.clear();await turn.changed.wait()
  turn.final_text='complete';turn.published.set()
 with patch.object(m.GiggySTTService,'_stream_turn',stream):
  try:
   begin=asyncio.create_task(svc.start_utterance(timeout=1));await asyncio.sleep(0)
   for _ in range(3):await asyncio.sleep(0)
   assert len(calls)==1 and not begin.done() and calls[0].captured==0
   try:await svc.send_audio(b'\x01\x00'*8000)
   except RuntimeError:pass
   else:raise AssertionError('Audio accepted before signed readiness')
   release.set();await begin
   for data in [b'x',object()]:
    try:await svc.send_audio(data)
    except (TypeError,ValueError):pass
    else:raise AssertionError('Invalid PCM accepted')
   try:await svc.finalize(timeout=1)
   except ValueError:pass
   else:raise AssertionError('Short final accepted')
   pcm=b'\xe8\x03'*10000;await svc.send_audio(pcm);turn=calls[0];assert bytes(turn.audio)==pcm
   assert await asyncio.wait_for(svc.finalize(),.05)=='complete','Finalize must not wait for local VAD'
   assert turn.ended and await svc.finalize()=='complete'
   try:await svc.send_audio(pcm)
   except RuntimeError:pass
   else:raise AssertionError('Audio accepted after final')
   await svc.start_utterance(timeout=1);assert len(calls)==2 and calls[1] is not turn
   await svc.send_audio(pcm);assert await svc.finalize()=='complete'
  finally:await svc._abort()
 print(json.dumps({'passed':True,'cases':['ready_before_audio_each_turn','no_synthetic_audio','invalid_PCM','minimum_duration','immediate_finalize_no_VAD','idempotent_final','post_final_rejected','fresh_next_turn'],'network_GPU_calls':0}))


async def lifecycle():
 async def hang(self,turn,previous=None):
  turn.generation_id='ready';turn.ready.set();turn.idle_timer=asyncio.get_running_loop().call_later(m._EXTERNAL_IDLE_SECONDS,asyncio.current_task().cancel);await asyncio.Event().wait()
 async def never_ready(self,turn,previous=None):await asyncio.Event().wait()
 for kind in ['readiness-timeout','final-timeout','idle-expiry']:
  svc=m.GiggySTTService(api_key='unit',aiohttp_session=object(),endpointing='external');svc.create_task=lambda c,**kw:asyncio.create_task(c);svc._user_id='unit'
  with patch.object(m.GiggySTTService,'_stream_turn',never_ready if kind=='readiness-timeout' else hang):
   try:
    if kind=='readiness-timeout':
     try:await svc.start_utterance(timeout=.02)
     except TimeoutError:pass
     else:raise AssertionError('Missing readiness timeout')
    elif kind=='final-timeout':
     await svc.start_utterance();await svc.send_audio(b'\x01\x00'*8000)
     try:await svc.finalize(timeout=.02)
     except TimeoutError:pass
     else:raise AssertionError('Missing final timeout')
    else:
     m._EXTERNAL_IDLE_SECONDS=.02;await svc.start_utterance();await asyncio.wait_for(asyncio.gather(*tuple(svc._turn_tasks),return_exceptions=True),1);await asyncio.sleep(0)
    assert not svc._turn_tasks and svc._capture is None,'Timed out or idle operation must release local session: '+kind
   finally:await svc._abort()
 print(json.dumps({'passed':True,'cases':['readiness-timeout','final-timeout','bounded-idle-reservation'],'CPU_only':True}))


from aiohttp import web,ClientSession
from uuid import uuid4
import base64
async def http_check():
    await finite_http_check()


from pipecat.frames.frames import VADUserStartedSpeakingFrame,VADUserStoppedSpeakingFrame
from pipecat.processors.frame_processor import FrameDirection
async def vad_check():
 calls=[];release=asyncio.Event()
 async def stream(self,turn,previous=None):calls.append(turn);await release.wait()
 async def base(*args,**kw):pass
 svc=m.GiggySTTService(api_key='unit',aiohttp_session=object());svc.create_task=lambda c,**kw:asyncio.create_task(c);svc._user_id='unit'
 with patch.object(m.GiggySTTService,'_stream_turn',stream),patch.object(m.STTService,'process_frame',base):
  try:
   try:await svc.start_utterance()
   except RuntimeError:pass
   else:raise AssertionError('External methods must be opt in')
   await svc.process_frame(VADUserStartedSpeakingFrame(),FrameDirection.DOWNSTREAM)
   async for _ in svc.run_stt(b'\x01\x00'*4000):pass
   for _ in range(3):await asyncio.sleep(0)
   assert not calls,'Default VAD must retain half-second admission minimum'
   async for _ in svc.run_stt(b'\x01\x00'*4000):pass
   for _ in range(3):await asyncio.sleep(0)
   assert len(calls)==1 and calls[0].captured==16000
   await svc.process_frame(VADUserStoppedSpeakingFrame(),FrameDirection.DOWNSTREAM);assert calls[0].ended
  finally:release.set();await svc._abort()
 print(json.dumps({'passed':True,'default_VAD_minimum_and_auto_endpoint_unchanged':True,'external_mode_opt_in':True}))



# Finite-input regressions extend the existing aiohttp/WebSocket fixture above.
import hashlib
import importlib.util
import os
from pathlib import Path
import sys
import time
from contextlib import AsyncExitStack, asynccontextmanager
from pipecat.frames.frames import EndFrame, TranscriptionFrame, InterimTranscriptionFrame
from pipecat.processors.frame_processor import FrameProcessor


def assert_exact_final_audio(frames, expected_pcm):
    assert frames, "No PCM frames sent"
    assert [frame["sequence"] for frame in frames] == list(range(len(frames)))
    assert sum(frame["complete"] is True for frame in frames) == 1
    assert all(frame["complete"] is False for frame in frames[:-1])
    assert frames[-1]["complete"] is True
    actual_pcm = b"".join(base64.b64decode(frame["pcm_base64"]) for frame in frames)
    assert actual_pcm == expected_pcm
    assert hashlib.sha256(actual_pcm).digest() == hashlib.sha256(expected_pcm).digest()


def benchmark_adapter():
    path = Path(os.environ.get("GIGGY_BENCHMARK_STT_PATH", str(
        Path(__file__).resolve().parents[1] / "src/pipecat_giggy/stt.py")))
    spec = importlib.util.spec_from_file_location("finite_benchmark_adapter", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


async def wait_for_condition(predicate, timeout=2):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(.001)


class FiniteHTTPFixture:
    """Existing local admit/audio/status/artifact fixture, with test-only capture."""
    def __init__(self, pcm, *, disconnect=False, wrong_fence=None, verification_gate=None, final_gate=None, artifact_text=None, artifact_duration=None, transport="websocket-v25", artifact_status=200, artifact_error_body=b"", artifact_headers=None):
        self.pcm, self.disconnect, self.wrong_fence = pcm, disconnect, wrong_fence
        self.artifact_requests = 0
        self.artifact_status, self.artifact_error_body = artifact_status, artifact_error_body
        self.artifact_headers = artifact_headers or {}
        self.transport = transport
        self.verification_gate, self.final_gate = verification_gate, final_gate
        self.artifact_text, self.artifact_duration = artifact_text, artifact_duration
        self.verification_started = asyncio.Event()
        self.gid, self.attempt, self.token = str(uuid4()), 1, 'a' * 64
        self.text = 'CafÃƒÂ© Ã¦â€”Â¥Ã¦Å“Â¬Ã¨ÂªÅ¾ complete finite recording'
        self.frames, self.admissions, self.trace = [], [], []
        self.final = asyncio.Event()
        self.eof = asyncio.Event()
        self.disconnect_seen = asyncio.Event()

    def mark(self, event):
        self.trace.append({'event': event, 'monotonic_ms': time.perf_counter_ns() / 1e6})

    async def emit(self, response, event, payload):
        if event == 'microphone_ready':
            payload = {'generation_uuid': self.gid, 'attempt': self.attempt,
                       'sampleRateHz': 16000 if self.transport in {'websocket-v25'} else 8000}
            event = 'ready'
        elif event == 'transcript_chunk':
            payload = {**payload, 'is_final': payload['complete'],
                       'alternatives': [{'transcript': payload['text']}]}
            event = 'transcription'
        await response.send_json({'type': event, **payload})

    async def admit(self, req):
        assert req.headers['Authorization'] == 'Bearer unit'
        response = web.WebSocketResponse()
        await response.prepare(req)
        start = await response.receive_json()
        assert start['type'] == 'start' and start['format'] == 'raw'
        assert start['encoding'] == 'LINEAR16' and start['sampleRateHz'] == 16000
        assert start['options']['endpointing'] == 'external'
        self.admissions.append(start)
        await self.emit(response, 'generation_created', {'generation_uuid': self.gid})
        await self.emit(response, 'microphone_ready', {'generation_uuid': self.gid, 'attempt': self.attempt, 'input_token': self.token, **({'input_transport': self.transport} if self.transport else {})})
        self.response = response
        self.mark('readiness_issued')
        final_task, eof_task = asyncio.create_task(self.final.wait()), asyncio.create_task(self.eof.wait())
        audio_task = asyncio.create_task(self.audio(response))
        try:
            done, _ = await asyncio.wait([final_task, eof_task], return_when=asyncio.FIRST_COMPLETED)
            if eof_task in done:
                self.mark('unexpected_SSE_EOF')
                if self.disconnect == 'abrupt-disconnect':
                    req.transport.abort()
                return response
            payload = {'generation_uuid': self.gid, 'attempt': self.attempt, 'sequence': 1, 'replace': True, 'complete': True, 'text': self.text}
            if self.final_gate is not None:
                await self.final_gate.wait()
            if self.wrong_fence == 'complete-type':
                payload['complete'] = 'true'
            if self.wrong_fence == 'attempt-bool':
                payload['attempt'] = True
            if self.wrong_fence == 'generation':
                payload['generation_uuid'] = str(uuid4())
            elif self.wrong_fence == 'attempt':
                payload['attempt'] = 2
            await self.emit(response, 'transcript_chunk', payload)
            if self.wrong_fence == 'duplicate-final':
                await self.emit(response, 'transcript_chunk', {**payload, 'sequence': 2})
            await self.emit(response, 'generation_updated', {'generation_uuid': self.gid, 'status': 'completed'})
            return response
        finally:
            for task in [final_task, eof_task, audio_task]:
                task.cancel()
            await asyncio.gather(final_task, eof_task, audio_task, return_exceptions=True)
            await response.close()
            self.disconnect_seen.set()

    async def audio(self, socket):
        async for message in socket:
            if message.type == m.aiohttp.WSMsgType.BINARY:
                self.frames.append({'sequence': len(self.frames), 'complete': False,
                                    'pcm_base64': base64.b64encode(message.data).decode()})
                if len(self.frames) == 1:
                    await self.emit_interim()
                    if self.disconnect:
                        self.eof.set()
            else:
                control = json.loads(message.data)
                if control.get('type') == 'prepare':
                    samples = sum(len(base64.b64decode(frame['pcm_base64'])) // 2 for frame in self.frames)
                    assert control['decode_end_sample'] is None or type(control['decode_end_sample']) is int and max(0, samples - 3200) <= control['decode_end_sample'] <= samples
                    self.mark('canonical_preparation')
                    continue
                assert control == {'type': 'stop'}
                sequence = len(self.frames)
                self.frames.append({'sequence': sequence, 'complete': True, 'pcm_base64': ''})
                await socket.send_json({'type': 'input_complete', 'generation_uuid': self.gid,
                                        'sequence': sequence + (1 if self.wrong_fence == 'ack' else 0)})
                self.final.set()
                break

    async def emit_interim(self):
        await self.emit(self.response, 'transcript_chunk', {'generation_uuid': self.gid, 'attempt': self.attempt, 'sequence': 0, 'replace': True, 'complete': False, 'text': 'interim only'})

    async def status(self, req):
        assert req.match_info['id'] == self.gid and self.final.is_set()
        self.verification_started.set()
        if self.verification_gate is not None:
            await self.verification_gate.wait()
        return web.json_response({'generation': {'generation_uuid': self.gid, 'status': 'completed', 'result': {'url': 'https://artifact.invalid/fixture'}}})

    async def artifact(self, req):
        self.artifact_requests += 1
        assert 'Authorization' not in req.headers
        assert 'xi-api-key' not in req.headers
        if self.artifact_status == 408:
            response = web.StreamResponse(status=408, headers={**self.artifact_headers, 'Content-Length': '1024'})
            await response.prepare(req)
            await response.write(self.artifact_error_body)
            req.transport.close()
            return response
        if self.artifact_status != 200:
            return web.Response(status=self.artifact_status, body=self.artifact_error_body, headers=self.artifact_headers)
        return web.json_response({'text': self.text if self.artifact_text is None else self.artifact_text, 'audio_duration_ms': (len(self.pcm) + 31) // 32 if self.artifact_duration is None else self.artifact_duration})

    async def __aenter__(self):
        app = web.Application()
        app.router.add_get('/v1/audio/transcriptions', self.admit)
        app.router.add_get('/v1/generations/{id}', self.status)
        app.router.add_get('/artifact', self.artifact)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, '127.0.0.1', 0)
        await site.start()
        self.origin = 'http://127.0.0.1:' + str(site._server.sockets[0].getsockname()[1])
        self.client = ClientSession()
        fixture = self
        class Session:
            headers = fixture.client.headers
            auth = fixture.client.auth
            def post(self, url, **kw):
                if url.endswith('/stt-input') and kw.get('json', {}).get('complete'):
                    fixture.mark('final_marker_handed_to_HTTP')
                return fixture.client.post(url, **kw)
            ws_connect = fixture.client.ws_connect
            def get(self, url, **kw):
                return fixture.client.get(fixture.origin + '/artifact' if url == 'https://artifact.invalid/fixture' else url, **kw)
        self.session = Session()
        return self

    async def __aexit__(self, *exc):
        self.eof.set()
        await self.client.close()
        await self.runner.cleanup()


async def finite_http_check(module=m, *, ending='external', seconds=1.3, negative=None, transport="websocket-v25"):
    # Patterned, aligned PCM is local transport test data, not a changed corpus.
    pcm = bytes(range(256)) * (int(seconds * 32000) // 256)
    pcm += b'\x12\x34' * ((int(seconds * 32000) - len(pcm)) // 2)
    async with FiniteHTTPFixture(pcm, disconnect=negative if negative in {'disconnect', 'abrupt-disconnect'} else False, wrong_fence=negative if negative in {'generation', 'attempt', 'ack'} else None, transport=transport) as fixture:
        kwargs = {'endpointing': 'external'} if ending == 'external' else {}
        svc = module.GiggySTTService(api_key='unit', aiohttp_session=fixture.session, **kwargs)
        svc._origin, svc._user_id = fixture.origin, 'finite-speaker'
        output, errors, tasks = [], [], []
        def task_factory(coro, **kw):
            task = asyncio.create_task(coro)
            tasks.append(task)
            return task
        svc.create_task = task_factory
        async def push(frame, *args, **kw):
            output.append(frame)
            if isinstance(frame, TranscriptionFrame) and not isinstance(frame, InterimTranscriptionFrame):
                fixture.mark('final_TranscriptionFrame_received')
        async def error(message, *args, **kw):
            errors.append(str(message))
        async def base(*args, **kw):
            pass
        svc.push_frame, svc.push_error = push, error
        async def feed(data):
            if ending == 'external':
                await svc.send_audio(data)
            else:
                async for _ in svc.run_stt(data):
                    pass
        async with AsyncExitStack() as stack:
            # Exercise real FrameProcessor, STTService and AIService lifecycle
            # dispatch. Only task scheduling and downstream output are captured.
            try:
                if ending == 'external':
                    await svc.start_utterance(timeout=2)
                    assert svc._capture.ready.is_set() and svc._capture.generation_id == fixture.gid
                    assert not fixture.frames
                    await feed(pcm)
                else:
                    # Real pre-roll belongs to this captured window. No bytes
                    # outside the intended 1.0/1.3s window are included.
                    await feed(pcm[:3200])
                    await svc.process_frame(VADUserStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)
                    await feed(pcm[3200:])
                fixture.mark('last_audio_sample_submitted')
                await wait_for_condition(lambda: len(fixture.frames) >= (1 if negative in {'disconnect', 'abrupt-disconnect'} else 2))
                finals = lambda: [x for x in output if isinstance(x, TranscriptionFrame) and not isinstance(x, InterimTranscriptionFrame)]
                if negative in {'no-endpoint', 'disconnect', 'abrupt-disconnect'}:
                    if negative in {'disconnect', 'abrupt-disconnect'}:
                        await wait_for_condition(lambda: svc._failed)
                        assert errors and fixture.disconnect_seen.is_set()
                    else:
                        with __import__('contextlib').suppress(TimeoutError):
                            async with asyncio.timeout(.08):
                                while not finals():
                                    await asyncio.sleep(.005)
                        assert not svc._failed and not errors
                    assert not finals() and not any(x['complete'] for x in fixture.frames)
                    if negative == 'no-endpoint':
                        assert any(isinstance(x, InterimTranscriptionFrame) for x in output)
                else:
                    fixture.mark('finalize_invoked' if ending == 'external' else ending + '_supplied')
                    if ending == 'external':
                        if negative in {'generation', 'attempt', 'ack'}:
                            try:
                                await svc.finalize(timeout=2)
                            except RuntimeError:
                                pass
                            else:
                                raise AssertionError('Invalid fence accepted')
                        else:
                            assert await svc.finalize(timeout=2) == fixture.text
                            assert await svc.finalize(timeout=2) == fixture.text
                    elif ending == 'vad':
                        await svc.process_frame(VADUserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
                    else:
                        await svc.process_frame(EndFrame(), FrameDirection.DOWNSTREAM)
                    await asyncio.wait_for(asyncio.gather(*tuple(svc._turn_tasks)), 2)
                    assert_exact_final_audio(fixture.frames, pcm)
                    if negative in {'generation', 'attempt', 'ack'}:
                        assert svc._failed and errors and not finals()
                    else:
                        assert not svc._failed and not errors, errors
                        assert len(finals()) == 1 and finals()[0].finalized is True and finals()[0].text == fixture.text
                        assert all(len(base64.b64decode(frame['pcm_base64'])) <= m._MAX_PACKET_BYTES for frame in fixture.frames)
                        assert svc._capture is None
                record = {'adapter': module.__file__, 'ending': ending, 'seconds': seconds, 'negative': negative, 'generation_uuid': fixture.gid, 'attempt': fixture.attempt, 'PCM_sha256': hashlib.sha256(pcm).hexdigest(), 'samples': len(pcm) // 2, 'PCM_POSTs': len(fixture.frames), 'final_markers': sum(x['complete'] is True for x in fixture.frames), 'final_frames': len(finals()), 'trace': fixture.trace, 'passed': True}
                if path := os.environ.get('FINITE_LOCAL_REPORT'):
                    with open(path, 'a', encoding='utf-8') as out:
                        out.write(json.dumps(record, ensure_ascii=False) + '\n')
                return record
            finally:
                await svc._abort()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)


class FiniteStreamFinalizationTests(unittest.IsolatedAsyncioTestCase):
    async def test_benchmark_VAD_stop(self):
        for seconds in [1.3, 1.0]:
            with self.subTest(seconds=seconds):
                await finite_http_check(benchmark_adapter(), ending='vad', seconds=seconds)

    async def test_benchmark_EndFrame(self):
        for seconds in [1.3, 1.0]:
            with self.subTest(seconds=seconds):
                await finite_http_check(benchmark_adapter(), ending='EndFrame', seconds=seconds)

    async def test_external_finalize_tail_and_empty_final(self):
        for seconds in [1.3, 1.0]:
            with self.subTest(seconds=seconds):
                await finite_http_check(seconds=seconds)

    async def test_current_SDK_VAD_and_EndFrame(self):
        for ending in ['vad', 'EndFrame']:
            for seconds in [1.3, 1.0]:
                with self.subTest(ending=ending, seconds=seconds):
                    await finite_http_check(ending=ending, seconds=seconds, transport='websocket-v25')

    async def test_finite_audio_without_endpoint_has_no_final(self):
        for module, ending in [(m, 'external'), (m, 'vad'), (benchmark_adapter(), 'vad')]:
            with self.subTest(adapter=module.__file__, ending=ending):
                await finite_http_check(module, ending=ending, negative='no-endpoint')

    async def test_unexpected_network_EOF_has_no_success(self):
        for module, ending in [(m, 'external'), (benchmark_adapter(), 'vad')]:
            with self.subTest(adapter=module.__file__):
                await finite_http_check(module, ending=ending, negative='disconnect')

    async def test_abrupt_network_disconnect_has_no_success(self):
        for module, ending in [(m, 'external'), (benchmark_adapter(), 'vad')]:
            with self.subTest(adapter=module.__file__):
                await finite_http_check(module, ending=ending, negative='abrupt-disconnect')

    async def test_benchmark_stale_generation_and_attempt_are_not_final(self):
        for fence in ['generation', 'attempt']:
            with self.subTest(fence=fence):
                await finite_http_check(benchmark_adapter(), ending='vad', negative=fence)

    async def test_stale_generation_and_attempt_are_not_final(self):
        for fence in ['generation', 'attempt', 'ack']:
            with self.subTest(fence=fence):
                await finite_http_check(negative=fence)


@asynccontextmanager
async def benchmark_capture(session, origin):
    module = benchmark_adapter()
    svc = module.GiggySTTService(api_key='unit', aiohttp_session=session)
    svc._origin, svc._user_id = origin, 'fast-final-test'
    frames, errors, tasks = [], [], []
    def create(coro, **kw):
        task = asyncio.create_task(coro)
        tasks.append(task)
        return task
    async def push(frame, *args, **kw):
        frames.append(frame)
    async def error(message, *args, **kw):
        errors.append(str(message))
    svc.create_task, svc.push_frame, svc.push_error = create, push, error
    def finals():
        return [f for f in frames if isinstance(f, TranscriptionFrame) and not isinstance(f, InterimTranscriptionFrame)]
    try:
        yield svc, finals, errors
    finally:
        await svc._abort()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def end_benchmark_capture(svc, pcm):
    await svc.process_frame(VADUserStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)
    async for _ in svc.run_stt(pcm):
        pass
    await svc.process_frame(VADUserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)


class BenchmarkFastFinalTests(unittest.IsolatedAsyncioTestCase):
    async def test_final_precedes_durability_and_stop_still_drains(self):
        pcm = b'\x10\x20' * 16000
        gate = asyncio.Event()
        async with FiniteHTTPFixture(pcm, verification_gate=gate) as http:
            async with benchmark_capture(http.session, http.origin) as (svc, finals, errors):
                try:
                    await end_benchmark_capture(svc, pcm)
                    await asyncio.wait_for(http.verification_started.wait(), 2)
                    assert len(finals()) == 1, 'Fenced final must be delivered before durable verification finishes'
                    assert finals()[0].finalized and finals()[0].text == http.text
                    assert not errors and svc._turn_tasks
                    assert_exact_final_audio(http.frames, pcm)
                    stopping = asyncio.create_task(svc.process_frame(EndFrame(), FrameDirection.DOWNSTREAM))
                    await asyncio.sleep(.01)
                    assert not stopping.done(), 'Normal stop must drain background verification'
                    gate.set()
                    await asyncio.wait_for(stopping, 2)
                    assert len(finals()) == 1 and not errors and not svc._failed and not svc._turn_tasks
                finally:
                    gate.set()

    async def test_late_artifact_mismatch_is_reported_without_second_final(self):
        # Storage failures retain actionable metadata, never bodies or signed URLs.
        for status, body, code in [
            (403, b'<Error><Code>SignatureDoesNotMatch</Code><Message>unit-private-secret https://artifact.invalid/?X-Amz-Signature=private</Message></Error>', 'SignatureDoesNotMatch'),
            (404, b'{"code":"NoSuchKey","message":"unit-private-secret"}', 'NoSuchKey'),
            (503, b'<html>unit-private-secret</html>', 'unknown'),
            (502, b'<Error><Code>unitPrivateSecret</Code></Error>', 'unknown'),
            (302, b'unit-private-secret', 'unknown'),
            (500, b'x' * 8192 + b'<Code>InternalError</Code>', 'unknown'),
            (400, b'{"code":["unit-private-secret"]}', 'unknown'),
            (408, b'<Code>InternalError</Code>', 'InternalError'),
        ]:
            with self.subTest(artifact_status=status):
                pcm = b'\x10\x20' * 16000
                headers = {'x-amz-request-id': '0123456789abcdef', 'CF-Ray': '0123456789abcdef-LAX',
                           'Location': 'https://artifact.invalid/?X-Amz-Signature=private'}
                if status == 400:
                    headers.update({'x-amz-request-id': 'unit-private-secret', 'CF-Ray': 'unit-private-secret'})
                async with FiniteHTTPFixture(pcm, artifact_status=status, artifact_error_body=body, artifact_headers=headers) as http:
                    with patch.object(m.logger, 'error') as logged:
                        async with benchmark_capture(http.session, http.origin) as (svc, finals, errors):
                            await end_benchmark_capture(svc, pcm)
                            await wait_for_condition(lambda: svc._failed)
                            calls = [c for c in logged.call_args_list if 'artifact download failed' in c.args[0]]
                            assert len(calls) == 1, 'Missing artifact failure diagnostic'
                            args = calls[0].args
                            assert args[1:8] == ('canonical_artifact', http.gid, 1, status, code,
                                                None if status == 400 else '0123456789abcdef',
                                                None if status == 400 else '0123456789abcdef-LAX')
                            assert args[8] == ('ClientPayloadError' if status == 408 else None)
                            assert 'unit-private-secret' not in str(logged.call_args_list)
                            assert 'unitPrivateSecret' not in str(logged.call_args_list)
                            assert 'X-Amz-Signature' not in str(logged.call_args_list)
                            assert 'artifact.invalid' not in str(logged.call_args_list)
                            assert errors and len(finals()) == 1
                            assert http.artifact_requests == len(http.admissions) == 1

        for overrides in [{'artifact_text': 'different saved transcript'}, {'artifact_duration': 1}]:
            with self.subTest(overrides=overrides):
                pcm, gate = b'\x10\x20' * 16000, asyncio.Event()
                async with FiniteHTTPFixture(pcm, verification_gate=gate, **overrides) as http:
                    async with benchmark_capture(http.session, http.origin) as (svc, finals, errors):
                        try:
                            await end_benchmark_capture(svc, pcm)
                            await asyncio.wait_for(http.verification_started.wait(), 2)
                            assert len(finals()) == 1 and finals()[0].text == http.text
                            gate.set()
                            await wait_for_condition(lambda: svc._failed)
                            assert errors and len(finals()) == 1
                        finally:
                            gate.set()

        # Defaults can be mutated after construction; reject before artifact I/O.
        pcm, gate = b'\x10\x20' * 16000, asyncio.Event()
        async with FiniteHTTPFixture(pcm, verification_gate=gate) as http:
            async with benchmark_capture(http.session, http.origin) as (svc, finals, errors):
                try:
                    await end_benchmark_capture(svc, pcm)
                    await asyncio.wait_for(http.verification_started.wait(), 2)
                    http.session.headers['Authorization'] = 'unit-shared-credential'
                    gate.set()
                    await wait_for_condition(lambda: svc._failed)
                    assert errors and len(finals()) == 1
                    assert http.artifact_requests == 0, 'Default credentials reached signed artifact origin'
                finally:
                    gate.set()

    async def test_final_metrics_use_the_captured_VAD_boundary(self):
        from unittest.mock import AsyncMock
        pcm = b'\x10\x20' * 16000
        async with FiniteHTTPFixture(pcm) as http:
            async with benchmark_capture(http.session, http.origin) as (svc, finals, errors):
                async def base(*args, **kw):
                    pass
                # Isolate the adapter's restoration from Pipecat's initial
                # VAD metric call; real HTTP and final frame creation remain.
                with patch.object(benchmark_adapter().STTService, 'process_frame', base):
                    svc.start_ttfb_metrics = AsyncMock()
                    await svc.process_frame(VADUserStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)
                    async for _ in svc.run_stt(pcm):
                        pass
                    await svc.process_frame(VADUserStoppedSpeakingFrame(stop_secs=.1, timestamp=1234), FrameDirection.DOWNSTREAM)
                    await asyncio.wait_for(asyncio.gather(*tuple(svc._turn_tasks)), 2)
                    assert len(finals()) == 1 and not errors
                    svc.start_ttfb_metrics.assert_awaited_once_with(start_time=1233.9)

    async def test_non_boolean_and_duplicate_complete_are_rejected(self):
        for fault, expected_finals in [('complete-type', 0), ('attempt-bool', 0), ('duplicate-final', 1)]:
            with self.subTest(fault=fault):
                pcm = b'\x10\x20' * 16000
                async with FiniteHTTPFixture(pcm, wrong_fence=fault) as http:
                    async with benchmark_capture(http.session, http.origin) as (svc, finals, errors):
                        await end_benchmark_capture(svc, pcm)
                        await asyncio.wait_for(asyncio.gather(*tuple(svc._turn_tasks)), 2)
                        assert svc._failed and errors, 'Invalid final snapshot must fail the stream'
                        assert len(finals()) == expected_finals

    async def test_next_turn_publication_is_ordered_without_durable_blocking(self):
        from contextvars import ContextVar
        pcm = b'\x10\x20' * 16000
        first_final, first_durable = asyncio.Event(), asyncio.Event()
        async with FiniteHTTPFixture(pcm, final_gate=first_final, verification_gate=first_durable) as one, FiniteHTTPFixture(pcm) as two:
            one.text, two.text = 'first turn', 'second turn'
            selected, admissions = ContextVar('finite_selected'), []
            class Session:
                def post(self, url, **kw):
                    if url.endswith('/v1/audio/transcriptions'):
                        chosen = [one, two][len(admissions)]
                        admissions.append(chosen)
                        selected.set(chosen)
                    else:
                        chosen = selected.get()
                    request = chosen.session.ws_connect
                    return request(url.replace(one.origin, chosen.origin), **kw)
                ws_connect = post  # Reuse the existing context-scoped request proxy.
                def get(self, url, **kw):
                    chosen = selected.get()
                    return chosen.session.get(url.replace(one.origin, chosen.origin), **kw)
            async with benchmark_capture(Session(), one.origin) as (svc, finals, errors):
                try:
                    await end_benchmark_capture(svc, pcm)
                    await asyncio.wait_for(one.final.wait(), 2)
                    await end_benchmark_capture(svc, pcm)
                    await asyncio.wait_for(two.final.wait(), 2)
                    await asyncio.sleep(.01)
                    assert not finals(), 'A later final must not overtake the earlier final'
                    first_final.set()
                    await wait_for_condition(lambda: len(finals()) == 2)
                    assert [f.text for f in finals()] == ['first turn', 'second turn']
                    assert not first_durable.is_set() and svc._turn_tasks
                    first_durable.set()
                    await asyncio.wait_for(svc.process_frame(EndFrame(), FrameDirection.DOWNSTREAM), 2)
                    assert not errors and not svc._failed and not svc._turn_tasks
                    assert_exact_final_audio(one.frames, pcm)
                    assert_exact_final_audio(two.frames, pcm)
                finally:
                    first_final.set()
                    first_durable.set()

        # A following reservation can still be awaiting signed readiness when
        # upstream VAD stops. Its continuous provider detector remains authoritative.
        from unittest.mock import AsyncMock
        from pipecat.audio.vad.vad_analyzer import VADState
        adapter = benchmark_adapter()
        for transport in (None, 'websocket-v25'):
            for state in (VADState.STARTING, VADState.SPEAKING, VADState.STOPPING, VADState.QUIET):
                with self.subTest(transport=transport, provider_state=state):
                    svc = adapter.GiggyV21PipecatSTTService(api_key='unit', aiohttp_session=object())
                    turn = adapter._Utterance('pending-next', captured=32000, input_transport=transport)
                    svc._capture = turn
                    svc._v21_speaking = True
                    svc._native_vad = object()
                    svc._native_vad_state = state
                    boundary = VADUserStoppedSpeakingFrame(timestamp=time.time(), stop_secs=.2)
                    with patch.object(adapter.GiggySTTService, 'process_frame', new=AsyncMock()):
                        await svc.process_frame(boundary, FrameDirection.DOWNSTREAM)
                    self.assertEqual(turn.speech_end_time, boundary.timestamp - .2)
                    if state == VADState.QUIET:
                        self.assertTrue(turn.ended)
                        self.assertIsNone(svc._capture)
                    else:
                        self.assertFalse(turn.ended)
                        self.assertIs(svc._capture, turn)
                        self.assertTrue(svc._v21_speaking)
                    await svc._abort()


class ExternalEndpointingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._old_idle = m._EXTERNAL_IDLE_SECONDS
    async def asyncTearDown(self):
        m._EXTERNAL_IDLE_SECONDS = self._old_idle
    async def test_default_vad_unchanged(self):
        await vad_check()
        from aiohttp import BasicAuth
        from pipecat_giggy import GiggyHttpTTSService
        for name in ['Authorization','Proxy-Authorization','xi-api-key','x-giggy-input-token','Cookie']:
            async with ClientSession(headers={name:'unit-private-credential'}) as session:
                for service in [m.GiggySTTService, GiggyHttpTTSService]:
                    with self.subTest(default_header=name,service=service.__name__):
                        kwargs={'voice_id':'unit'} if service is GiggyHttpTTSService else {}
                        with self.assertRaisesRegex(ValueError,'credential-free'):
                            service(api_key='unit',aiohttp_session=session,**kwargs)
        async with ClientSession(auth=BasicAuth('unit','private')) as session:
            with self.assertRaisesRegex(ValueError,'credential-free'):
                m.GiggySTTService(api_key='unit',aiohttp_session=session)
        for speed in [True,False,'1',None]:
            with self.subTest(speed=speed),self.assertRaises(ValueError):
                GiggyHttpTTSService(api_key='unit',voice_id='unit',aiohttp_session=object(),speed=speed)

    async def test_ready_pcm_finalize_and_next_turn(self):
        await check()
        controls = []
        class Socket:
            closed = False
            async def send_bytes(self, pcm):
                controls.append(('pcm', pcm))
                samples = sum(len(value) // 2 for kind, value in controls if kind == 'pcm')
                hints = [value['decode_end_sample'] for kind, value in controls
                         if kind == 'control' and value['type'] == 'prepare']
                assert not hints or hints[-1] is None or samples - 3200 <= hints[-1] <= samples
            async def send_json(self, body):
                controls.append(('control', body))
                if body['type'] == 'prepare' and body['decode_end_sample'] is not None and getattr(self, 'extra_pcm', None):
                    self.turn.audio.extend(self.extra_pcm)
                    self.turn.captured += len(self.extra_pcm)
                    self.extra_pcm = None
                    self.turn.ended = True
                    self.turn.changed.set()
        turn = m._Utterance('preparation', audio=bytearray(b'\x01\x00' * 8000),
                            captured=16000, ended=True, decode_end_sample=7900)
        turn.input_acknowledged.set()
        svc = m.GiggySTTService(api_key='unit', aiohttp_session=object(), endpointing='external')
        await svc._send_audio(turn, str(uuid4()), Socket())
        assert controls[1] == ('control', {'type': 'prepare', 'decode_end_sample': 7900})
        assert controls[-1] == ('control', {'type': 'stop'})
        assert controls[0][1] == b'\x01\x00' * 8000 and turn.sent_frames == 1

        # Delayed readiness can coalesce real PCM beyond a previously prepared
        # 200ms tail. Withdraw the hint before that PCM; retain every sample.
        controls.clear()
        first, tail = b'\x01\x00' * 3200, b'\x02\x00' * 8000
        turn = m._Utterance('delayed-ready', audio=bytearray(first), captured=len(first),
                            decode_end_sample=2560)
        turn.input_acknowledged.set()
        socket = Socket()
        socket.turn, socket.extra_pcm = turn, tail
        await svc._send_audio(turn, str(uuid4()), socket)
        assert b''.join(value for kind, value in controls if kind == 'pcm') == first + tail
        assert controls[-3][1] == {'type': 'prepare', 'decode_end_sample': None}
        assert controls[-1] == ('control', {'type': 'stop'})

        controls.clear()
        turn = m._Utterance('already-stale', audio=bytearray(first + tail),
                            captured=len(first + tail), ended=True, decode_end_sample=2560)
        turn.input_acknowledged.set()
        await svc._send_audio(turn, str(uuid4()), Socket())
        assert b''.join(value for kind, value in controls if kind == 'pcm') == first + tail
        assert all(kind != 'control' or value['type'] != 'prepare' for kind, value in controls)
    async def test_timeout_and_idle_cleanup(self):
        await lifecycle()
    async def test_real_http_order_utf8_and_artifact(self):
        await http_check()
if __name__ == "__main__":
    unittest.main()
