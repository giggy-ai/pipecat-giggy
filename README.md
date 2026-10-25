# Giggy speech for Pipecat

[Developer tool guide](https://giggy.ai/docs/speech-api#api-developer-tools) · [Python SDK](https://github.com/giggy-ai/giggy-python) · [JavaScript/TypeScript SDK](https://github.com/giggy-ai/giggy-js) · [Runnable examples](https://github.com/giggy-ai/giggy-examples) · [Pricing](https://giggy.ai/pricing)

Giggy-maintained text-to-speech and live speech-to-text integration for Pipecat voice agents. Version `0.3.1` fixes readiness transitions and expired preparation hints during buffered input, and uses the current one-socket WebSocket STT transport; `0.2.0` retains its earlier transport. Start with [TTS pipeline usage](#pipeline-usage), [live STT](#speech-to-text), or the [live-protocol recording replay](#replay-a-recording-through-the-live-input-protocol). STT sessions accept PCM16 mono 16 kHz audio for 0.5–30 seconds per utterance. Live Pipecat use includes a provider endpoint detector and requires upstream VAD for Pipecat turn timing; explicit-boundary recordings use `GiggyExternalSTTService`.

Version `0.3.1` includes live speech-to-text and text-to-speech. Both use Giggy's paid Streaming API and Pipecat 1.12.0.

This Giggy-maintained package provides streaming STT and TTS processors for Pipecat pipelines. TTS uses existing voice UUIDs and runtime voice/speed settings; STT consumes VAD-delimited 16 kHz mono PCM and returns interim snapshots and an ownership-fenced final transcript, then verifies durable completion. Consumers must drain the lifecycle and handle later verification errors. These adapters do not implement voice cloning or local model inference.

Maintained by Giggy, the speech API provider. Community-maintained integrations
are maintained by their authors; this package is not maintained by the Pipecat team.

## Try Giggy in your existing Pipecat pipeline

[Start with the Giggy setup guide](https://giggy.ai/docs/speech-api?utm_source=pipecat&utm_medium=referral&utm_campaign=pipecat_integration&utm_content=github_readme#api-pipecat).
Create an account and API key, choose a voice UUID, add paid Streaming credits,
then install the adapter and replace your pipeline's TTS service using the example
below. Keep your existing STT, LLM, and transport; you do not need another provider
account just to run the foundational WAV example.

[Watch the Angela Fowler demo](https://github.com/giggy-ai/pipecat-giggy/releases/download/v0.1.0/giggy-angela-starts-with-speech.mp4):
real speech synthesis, a simulated user interruption, and resumed speech. The
displayed text is synthesis text, not microphone transcription.

Need help getting your first audio? [Open a setup question](https://github.com/giggy-ai/pipecat-giggy/issues)
with your Pipecat version, transport, and sanitized error details. Never include
your API key. We'd love to hear what you're building and help with integration.

## Installation

Install the Giggy-maintained integration:

```sh
uv add pipecat-giggy==0.3.1
```

Or use `python -m pip install pipecat-giggy==0.3.1`.

For adapter development or the repository tests, install this source checkout
inside an activated virtual environment:

```sh
python -m pip install -e ".[test]"
```

The WebSocket transport, decode preparation and readiness fixes described below
are available in published `0.3.1`. Older `0.2.0` behavior remains documented in its
[tagged README](https://github.com/giggy-ai/pipecat-giggy/blob/v0.2.0/README.md).
Run repository examples from their matching source revision.

Requires Python 3.11 or later. This checkout is tested with Python 3.11.9 and Pipecat 1.12.0;
the dependency is pinned to that Pipecat release until later releases are tested.

## V25 compatibility

This release targets Giggy's canonical V25 speech contract and Pipecat
1.12.0. The public `GiggySTTService` selects the V25 one-socket live bridge;
`GiggyExternalSTTService` provides explicit caller-owned turn boundaries.

| Path | V25 contract |
| --- | --- |
| Live STT | `wss://api.giggy.ai/v1/audio/transcriptions`; Bearer authentication, start control, binary PCM, stop control and transcript events on one socket |
| PCM input | Mono PCM16 LE, 16 kHz; 20 ms first packet, 100 ms ongoing packets, bounded catch-up packets; ordered binary frames and explicit `stop` control |
| STT finalization | Validate final input ACK and generation/attempt-fenced final before publication; drain and verify the completed saved transcript and captured duration |
| Live endpointing | 100 ms provider silence, 0.7 start / 0.35 continuation confidence, no added minimum decoder context; upstream Pipecat measurement VAD is unchanged |
| TTS | `POST /v1/audio/speech`, model `giggyspeech`, voice UUID, `response_format="pcm"`, mono 24 kHz PCM and speed 0.25–4 |
| Cancellation | STT cancellation aborts input without inventing a final; TTS cancellation closes the active response; neither admission is automatically retried |

The adapter requires V25's one-socket protocol and fails closed on invalid
readiness, input acknowledgement or transcript identity. It does not select private worker versions, move GPUs, or depend
on development experiment flags. Language defaults to automatic detection; set
`Settings(language=Language.EN)` for the English benchmark recipe.

Local compatibility verification covers canonical STT regressions, the actual
V25 WebSocket protocol fixtures, and TTS progressive PCM, error handling, runtime
voice/speed updates and response cancellation. This does not qualify microphone
hardware or every newer Pipecat release. Install version 0.3.1 for these features; installing version 0.2.0 does not
install these source changes.

## Prerequisites

- A Giggy account and API key from https://giggy.ai.
- A public or owned Giggy voice UUID, obtained from `GET /v1/voices` or `GET /v1/my-voices`.
- Streaming credits. The compatibility speech endpoint uses paid Streaming admission;
  it is not the free Batch endpoint. See https://giggy.ai/pricing.

## Pipeline usage

```python
import os
import aiohttp
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.workers.runner import WorkerRunner
from pipecat_giggy import GiggyHttpTTSService

async def run_pipeline(transport, stt, llm):
    async with aiohttp.ClientSession() as session:
        tts = GiggyHttpTTSService(
            api_key=os.environ["GIGGY_API_KEY"],
            voice_id=os.environ["GIGGY_VOICE_ID"],
            aiohttp_session=session,
            sample_rate=24000,
            speed=1.0,
        )
        # transport, stt, and llm are supplied by your application.
        worker = PipelineWorker(
            Pipeline([transport.input(), stt, llm, tts, transport.output()]),
            params=PipelineParams(audio_out_sample_rate=24000),
        )
        runner = WorkerRunner()
        await runner.add_workers(worker)
        await runner.run()
```

The application owns and closes the HTTP session. Pipecat initializes the audio
rate when starting the pipeline; do not call `run_tts` on an uninitialized service.

## Run the foundational example

The single-file example runs a real Pipecat pipeline and writes its audio to WAV.
It does not require an STT/LLM provider or a microphone, and makes one billable
Giggy synthesis request. It does not play audio to the speaker or demonstrate
microphone barge-in.

```sh
export GIGGY_API_KEY="your-api-key"
export GIGGY_VOICE_ID="your-voice-uuid"
export GIGGY_ALLOW_BILLABLE_EXAMPLE=1
python examples/basic.py --text "A voice that knows when to listen." --output speech.wav
```

On PowerShell, set variables with `$env:GIGGY_API_KEY = "your-api-key"` and the
same pattern for the voice and billable-example flag. No key is printed.

## Configuration

| Parameter | Required/default | Meaning |
| --- | --- | --- |
| `api_key` | Required | Giggy API key |
| `voice_id` | Required | Public or owned Giggy voice UUID |
| `aiohttp_session` | Required | Caller-owned `aiohttp.ClientSession` |
| `sample_rate` | `24000` | Current Streaming contract supports 24 kHz only |
| `speed` | `1.0` | Speech speed, 0.25–4 inclusive |

| Additional keyword arguments | Pipecat defaults | Options passed to `TTSService` |

Runtime settings use Pipecat's `TTSUpdateSettingsFrame`:

```python
from pipecat.frames.frames import TTSUpdateSettingsFrame

await worker.queue_frame(TTSUpdateSettingsFrame(
    delta=GiggyHttpTTSService.Settings(voice="another-voice-uuid", speed=1.1)
))
```

The model is fixed to `giggyspeech`; changing the runtime model is unsupported.

## Streaming and interruption

The service sends complete text once to `https://api.giggy.ai/v1/audio/speech` and
emits mono PCM16 LE at 24 kHz through `TTSAudioRawFrame`, retaining context IDs.
Pipecat emits the corresponding text/start/stop frames. There are no word
timestamps or microphone transcriptions supplied by this TTS service.

Your transport and turn-detection processors generate Pipecat's
`InterruptionFrame`. Pipecat cancels the active synthesis coroutine; cancellation
propagates and closes the HTTP response. Your output transport must also clear
queued playback. Closing a request does not guarantee immediate preemption of
already-submitted GPU work.

Connection timeout is 10 seconds and stream-read timeout is 90 seconds. The
audio-context idle deadline defaults to 90 seconds so delayed initial audio is
not discarded by Pipecat's shorter default. HTTP completion still closes a
context immediately. HTTP/format/timeout failures produce safe `ErrorFrame`s.
Failed requests are never automatically retried because admission may already
have incurred credits. No alternative synthesis backend is used.

## Speech-to-text

`GiggySTTService` uses the V25 live bridge: readiness before capture,
20 ms first / 100 ms ongoing upload, 100 ms provider silence with 0.7/0.35
confidence hysteresis, local endpoint preparation and ACK-gated final publication.
It connects to Giggy's existing paid live PCM transcription API. The application
still supplies upstream VAD and keeps its measurement timing unchanged. For finite recordings, clean `EndFrame` waits for captured turns. Benchmark collectors can call
`drain_pending_turns()` after their input queue has drained and before collecting a finite transcript; cancellation must still abort.
Cancellation still aborts. `GiggyExternalSTTService` supports caller-owned endpoints
and clean audio-source EOF without the live provider detector.
It sends audio while the user speaks, emits cumulative provisional text through
`InterimTranscriptionFrame`. Version `0.3.1` emits one finalized
`TranscriptionFrame` when the ownership-fenced stream marks its snapshot complete.
It then drains completion and verifies the saved transcript matches that final;
storage or verification failures remain visible even after text delivery.
The older `0.2.0` package waits for completed-operation verification and canonical
transcript retrieval before final delivery; `0.3.1` provides the behavior above.
Display each interim snapshot by replacement; do not append revisions or treat
them as committed user text. No STT voice UUID is required.

Install `pipecat-giggy==0.3.1` for the current WebSocket STT transport and readiness fixes. Version `0.2.0` retains its earlier transport; `0.1.0` contains TTS only.

```sh
python -m pip install "pipecat-giggy==0.3.1"
```

[Watch the two-voice STT conversation demo](https://github.com/giggy-ai/pipecat-giggy/releases/download/v0.2.0/giggy-pipecat-stt-live-chat.mp4).
It uses TTS-generated Aiden utterances through browser PCM capture and real Giggy
STT, with Angela Fowler responses and VAD-driven interruption. User transcripts
are actual provisional/final results; assistant text follows playback with
approximate timing. Replies are scripted. Physical microphone hardware and echo
cancellation are not qualified by this recording.

```python
from pipecat_giggy import GiggySTTService
from pipecat.transcriptions.language import Language

stt = GiggySTTService(
    api_key=os.environ["GIGGY_API_KEY"],
    aiohttp_session=session,
    settings=GiggySTTService.Settings(language=Language.EN),  # Optional; omit for detection.
)
# Configure transport input as mono PCM16 at 16 kHz and use upstream VAD.
# Keep your existing LLM, context aggregators, turn strategies and transport.
# Giggy TTS uses 24 kHz output independently of the 16 kHz STT input.
```

Supply Pipecat's `VADUserStartedSpeakingFrame` and
`VADUserStoppedSpeakingFrame` upstream of STT. Use a VAD onset delay no greater
than one second, covered by the adapter's real-audio pre-roll. Account for the
VAD model's detection delay as well as its configured start window. Input must already be
mono PCM16 LE at 16 kHz; compressed audio and other rates are rejected. The
adapter includes the provider Silero endpoint detector; it does not provide resampling,
speaker diarization or word timestamps. Keep upstream VAD for Pipecat turn timing.

The source adapter uses Giggy's current one-socket WebSocket API. It authenticates
with a Bearer API key at `wss://api.giggy.ai/v1/audio/transcriptions`, sends `start`
with raw mono LINEAR16 at 16 kHz and external endpointing, and waits for canonical
admission and readiness. Real PCM travels as binary messages on that same socket;
`stop` ends input. Input ACK and generation/attempt/sequence-fenced transcript
events are validated before final publication. During candidate silence, the
source adapter relays the existing canonical `decode_end_sample` in a `prepare`
control, so decoding can overlap endpoint confirmation. Resumed speech clears
that hint with `null`. All real PCM is still sent, and only `stop` ends input.
Published `0.3.1` uses the production Giggy WebSocket preparation
handler. There is no transport
fallback or automatic input replay.
Microphone hardware and echo cancellation remain application-owned concerns.

Each completed utterance, including real pre-roll and captured stop silence, must
contain 0.5–30 seconds of audio. The live bridge reserves readiness before PCM,
retains short captures until real audio reaches the half-second minimum, and can
merge resumed speech. A clean EOF below that minimum fails; no synthetic padding
is added. Longer captures fail rather than splitting automatically. The fixed
model is `giggyspeech-stt`. `Settings(language=Language.EN)` supplies English for
known-English audio; omitted or `None` selects automatic detection. Language
settings are snapshotted per reservation, so updates apply to a new reservation,
not an in-flight utterance. Synthetic keepalive audio is unsupported.


One earlier utterance may finalize while the next is captured. In `0.3.1`,
up to eight network operations progress concurrently; final transcripts
retain utterance order without waiting for earlier artifact storage. Pending
capture is capped at 32 utterances and 30 seconds of aggregate real PCM. The
published 0.2.0 package retains its two-utterance limit. Queue overflow, an
invalid protocol response, rejected input, or transport failure fails visibly and
closes unfinished streams. There is no automatic retry, reconnect, WAV upload
substitution or alternative STT provider.

`InterruptionFrame` passes through without cancelling the user's STT capture:
barge-in stops the bot's TTS. `CancelFrame`, mute and cleanup cancel STT network
tasks; `EndFrame` flushes capture and waits for completion. Live API disconnects
use Giggy's existing cancellation/refund rail. Each utterance is a separate paid
Streaming operation: the API reserves its capture ceiling and settles actual
accepted audio duration on successful completion. See [pricing](https://giggy.ai/pricing).

The application owns the HTTP session. Keep your API key on your server, use a
credential-free session without default authentication headers or BasicAuth, and never log API keys or signed artifact URLs. Such defaults are rejected at construction and checked again before signed artifact reads; do not mutate session credentials during active turns. Requests stay on the admitting origin. HTTPS redirects are disabled;
signed transcript artifacts are fetched without sending the Giggy key/token.
The turn lifetime is bounded to 150 seconds; readiness defaults to 15 seconds and socket receives are bounded to 90
seconds and the final WebSocket acknowledgement to 10 seconds. No measured STT latency guarantee is
claimed for this integration; Pipecat's default STT metadata is not a Giggy benchmark.

### Replay a recording through the live input protocol

After the editable installation above, the recording example uses
`GiggyExternalSTTService` with the recording's explicit
start/stop boundaries; it does not run the live provider VAD. It needs your Giggy
account, API key and paid Streaming credits. It needs
no LLM/STT provider account, TTS voice UUID or microphone. It replays an existing
uncompressed mono PCM16 16 kHz WAV at capture cadence and makes one billable live
STT request. This is not an end-to-end microphone or voice-call demo.

```sh
export GIGGY_API_KEY="your-api-key"
export GIGGY_ALLOW_BILLABLE_EXAMPLE=1
python examples/transcribe.py --input your-recording.wav
```

For PowerShell use `$env:GIGGY_API_KEY = "your-api-key"` and
`$env:GIGGY_ALLOW_BILLABLE_EXAMPLE = "1"`. The recording must be 0.5â€“30 seconds.
Output labels provisional replacement snapshots and the canonical final text.

## Maintenance

Compatibility updates are recorded in [CHANGELOG.md](CHANGELOG.md). Report issues
to the Giggy repository after publication. Source and dependencies are released
under their respective licenses; this integration uses the [ISC license](LICENSE).

## External endpointing and readiness before audio

Use `GiggyExternalSTTService(endpointing="external")` when the caller supplies the
end-of-turn decision. The default public `GiggySTTService` owns its provider VAD
and rejects an `endpointing` override. The external helper's own default is
`"vad"`, so explicit-source examples must pass `endpointing="external"`.
External endpointing uses the current V25 production API. After normal
Pipecat service initialization:

```python
from pipecat_giggy import GiggyExternalSTTService

stt = GiggyExternalSTTService(
    api_key=api_key,
    aiohttp_session=session,
    endpointing="external",
    base_url="https://api.giggy.ai",
)

await stt.start_utterance()  # authenticated atomic reservation + signed readiness
for pcm in real_pcm_frames:  # real-time mono PCM16 LE, 16 kHz
    await stt.send_audio(pcm)
text = await stt.finalize()  # flush real tail; no additional local VAD wait
```

Await readiness separately for every utterance. Each turn uses a fresh canonical
claim; a worker slot is not pinned across idle turns. `finalize()` signals the
existing ordered input protocol, then returns the fenced final transcript.
Existing durable transcript/artifact verification continues in the background;
normal stop/cleanup drains it. A downstream application must handle later fatal
verification errors. Repeated finalize calls await the same result without replay.

Audio before readiness, after finalization, or below the existing half-second
minimum at finalization is rejected. PCM is never padded or trimmed. The existing
30-second audio cap and server stream deadline remain. Start audio promptly after
readiness: unused server reservations request cancellation after 20 seconds
(on the existing five-second poll), with a 35-second input watchdog fallback; the audio deadline starts with the first real PCM. Unused client
reservations cancel after 15 seconds; readiness/finalization timeouts abort the
owned session. No admission or ambiguous PCM upload is retried automatically.

### WebSocket integration without Pipecat

1. Open `wss://api.giggy.ai/v1/audio/transcriptions` with `Authorization: Bearer <key>`.
2. Send `{"type":"start","format":"raw","encoding":"LINEAR16","sampleRateHz":16000,"interimResults":true,"options":{"endpointing":"external"}}`.
3. Wait for `generation_created` and matching `ready`; retain the generation ID and attempt.
4. Send real PCM as binary messages. Flush the real tail, then send `{"type":"stop"}`.
5. Validate `input_complete` and its binary-frame count, then the identity/attempt/sequence-fenced final `transcription`. Drain completion and verify the canonical saved transcript and captured duration. Open a new operation for the next utterance.

For latency reporting, distinguish finalize-call→final-text (includes local
upload queues) from final-marker-sent→final-text, and report readiness separately.
Do not drop audio or move a timer after an API processing stage to improve a score.


### Automatic finalization when an audio source ends

For one finite asynchronous PCM source, use external endpointing and let the SDK
send the final marker automatically when the source ends normally:

```python
from pipecat_giggy import GiggyExternalSTTService

stt = GiggyExternalSTTService(
    api_key=api_key,
    aiohttp_session=session,
    endpointing="external",
    base_url="https://api.giggy.ai",
)
text = await stt.transcribe_stream(audio_chunks())
# audio_chunks() is an async iterable of mono PCM16 LE / 16 kHz bytes.
# No separate start_utterance() or finalize() call is needed.
```

The SDK waits for worker readiness before pulling audio, streams through the
existing ordered acknowledgements, and flushes the original remaining PCM with
exactly one final marker at clean iterator EOF. There is no additional VAD wait.
The existing 0.5–30 second bounds remain; an empty/short source is rejected and
its reservation is aborted. Source exceptions and cancellation abort without
publishing a final for truncated input. Rejected overlapping captures do not
cancel an existing utterance. Readiness and final-result timeouts can be supplied
as `ready_timeout` and `final_timeout`.

A source that merely pauses has not reached EOF. A broken network connection or
closing the WebSocket is also not an audio-source EOF. Raw WebSocket
clients must flush their real binary PCM tail, then send `{"type":"stop"}`; an
ordinary frame or a disconnected socket does not mean the source ended successfully. Existing Pipecat `EndFrame`
already flushes the current capture; `CancelFrame` continues to abort it.

The return value uses the existing fenced fast-final path. Durable verification
continues in the background; drain the normal service lifecycle for durable
completion and handle later verification errors. Version 0.3.1 provides this behavior; older releases retain their earlier transport.


## Develop and verify this checkout

The published installation commands above install the stable PyPI release. To
test this release from source, use a virtual environment and install this source:

```sh
python -m pip install -e ".[test]"
python -m pytest tests -q
```

The tests use local protocol fixtures; they do not need an API key, GPU worker,
or paid speech requests. A live benchmark is separate from these regression
tests. Keep the pinned Pipecat version until a newer release has passed the same
lifecycle, interruption, protocol and metrics checks. The project remains
`pipecat-giggy` on GitHub/PyPI and `pipecat_giggy` in Python, following Pipecat's
community naming recommendation. No alias package or automatic provider fallback
is installed.

## Existing browser demos

For an application you can run today, use the [standalone cloud sandbox](https://github.com/giggy-ai/giggy-voice-agent), with its own Giggy/DeepSeek keys and server bridge. For native Pipecat, review [cloud sandbox PR #2](https://github.com/giggy-ai/pipecat-giggy/pull/2) and [SmallWebRTC PR #1](https://github.com/giggy-ai/pipecat-giggy/pull/1). They remain separate unmerged proposals with qualification notes; no additional demo is needed. The standalone sandbox explicitly uses the supported legacy HTTPS/SSE bridge; it does not demonstrate native WebSocket latency. The existing recorded-input STT/TTS video is not physical microphone qualification.

[Create a Giggy account](https://giggy.ai/?login=1&utm_source=github&utm_medium=referral&utm_campaign=developer_onboarding&utm_content=pipecat_giggy_readme) and [read the speech API guide](https://giggy.ai/docs/speech-api?utm_source=github&utm_medium=referral&utm_campaign=developer_onboarding&utm_content=pipecat_giggy_docs). Keep keys server-side. Install published `pipecat-giggy==0.3.1`; selecting a voice, sufficient account credits and the pinned Pipecat version remain prerequisites for a real API run.
