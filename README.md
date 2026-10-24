# Giggy speech for Pipecat

[Developer tool guide](https://giggy.ai/docs/speech-api#api-developer-tools) · [Python SDK](https://github.com/giggy-ai/giggy-python) · [JavaScript/TypeScript SDK](https://github.com/giggy-ai/giggy-js) · [Runnable examples](https://github.com/giggy-ai/giggy-examples) · [Pricing](https://giggy.ai/pricing)

Giggy-maintained text-to-speech and live speech-to-text integration for Pipecat voice agents. This checkout is the unpublished `0.3.0.dev0` candidate; the current PyPI release is `0.2.0`. Start with [TTS pipeline usage](#pipeline-usage), [live STT](#speech-to-text), or the [live-protocol recording replay](#replay-a-recording-through-the-live-input-protocol). STT sessions accept PCM16 mono 16 kHz audio for 0.5–30 seconds per utterance. Live Pipecat use includes a provider endpoint detector and requires upstream VAD for Pipecat turn timing; explicit-boundary recordings use `GiggyExternalSTTService`.

Version `0.2.0` adds live speech-to-text alongside the existing text-to-speech
integration. Both use Giggy's paid Streaming API and Pipecat 1.12.0.

Giggy makes expressive Voice AI accessible to more developers, with natural
speech, expressive control, and voice cloning. Our focus is making high-quality
speech affordable enough for developers to build and scale voice experiences.

This integration brings Giggy's paid streaming speech API into Pipecat pipelines,
with voice selection, speech-speed updates, and interruption handling. Developers
can give their assistants a distinctive voice while keeping their existing
transcription, language model, and transport providers.

Voice cloning and expressive control are Giggy product capabilities. This adapter
uses existing voice UUIDs; its runtime settings expose voice selection and speed.

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
uv add pipecat-giggy
```

Or use `python -m pip install pipecat-giggy`.

For the unpublished V25 behavior shown in this checkout, install the local source
inside an activated virtual environment instead:

```sh
python -m pip install -e ".[test]"
```

Published `0.2.0` behavior is documented in its
[tagged README](https://github.com/giggy-ai/pipecat-giggy/blob/v0.2.0/README.md).
The live and recording examples below describe this checkout; use its editable
installation when running those files.

Requires Python 3.11 or later. This checkout is tested with Python 3.11.9 and Pipecat 1.12.0;
the dependency is pinned to that Pipecat release until later releases are tested.

## V25 compatibility

The source candidate targets Giggy's canonical V25 speech contract and Pipecat
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
hardware or every newer Pipecat release. The unpublished candidate must be
installed from this checkout; installing the existing PyPI release does not
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
`InterimTranscriptionFrame`. This development checkout emits one finalized
`TranscriptionFrame` when the ownership-fenced stream marks its snapshot complete.
It then drains completion and verifies the saved transcript matches that final;
storage or verification failures remain visible even after text delivery.
The published 0.2.0 package still waits for completed-operation verification and
canonical transcript retrieval; this latency change has not been published.
Display each interim snapshot by replacement; do not append revisions or treat
them as committed user text. No STT voice UUID is required.

These V25 integration changes are local and not yet published. Install `pipecat-giggy>=0.2.0` for released STT; version 0.1.0 contains TTS only.

```sh
python -m pip install "pipecat-giggy>=0.2.0"
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
This unreleased source change requires the production Giggy WebSocket preparation
handler; it is not available in the published package. There is no transport
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

One earlier utterance may finalize while the next is captured. In this development
checkout, up to eight network operations progress concurrently; final transcripts
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
`$env:GIGGY_ALLOW_BILLABLE_EXAMPLE = "1"`. The recording must be 0.5–30 seconds.
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
completion and handle later verification errors. This is a source-checkout change
and has not been published to the package registry.


## Develop and verify this checkout

The published installation commands above install the stable PyPI release. To
test these unpublished changes, use a virtual environment and install this source:

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
