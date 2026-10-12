# Giggy speech for Pipecat

**Local preparation:** STT is an unpublished `0.2.0.dev0` candidate. PyPI `0.1.0`
contains TTS only. Live microphone/conversation qualification and an STT demo
remain release gates; no STT support has been submitted to Pipecat yet.

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

Requires Python 3.11 or later. Tested with Python 3.12 and Pipecat 1.12.0;
the dependency is pinned to that Pipecat release until later releases are tested.

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
| `speed` | `1.0` | Speech speed, 0.25â€“4 inclusive |
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

The service sends complete text once to `https://giggy.ai/v1/audio/speech` and
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

## Speech-to-text candidate

`GiggySTTService` connects to Giggy's existing paid live PCM transcription API.
It sends audio while the user speaks, emits cumulative provisional text through
`InterimTranscriptionFrame`, and emits one finalized `TranscriptionFrame` only
after verifying the completed operation and fetching its canonical transcript.
Display each interim snapshot by replacement; do not append revisions or treat
them as committed user text. No STT voice UUID is required.

For local evaluation, install this checkout with `python -m pip install -e .`.
The released `uv add pipecat-giggy` package does not yet include STT.

```python
from pipecat_giggy import GiggySTTService

stt = GiggySTTService(
    api_key=os.environ["GIGGY_API_KEY"],
    aiohttp_session=session,
)
# Configure transport input as mono PCM16 at 16 kHz and use upstream VAD.
# Keep your existing LLM, context aggregators, turn strategies and transport.
# Giggy TTS uses 24 kHz output independently of the 16 kHz STT input.
```

Supply Pipecat's `VADUserStartedSpeakingFrame` and
`VADUserStoppedSpeakingFrame` upstream of STT. Use a VAD onset delay no greater
than 200 ms, covered by the adapter's real-audio pre-roll. Input must already be
mono PCM16 LE at 16 kHz; compressed audio and other rates are rejected. The
adapter does not provide resampling, VAD, speaker diarization or word timestamps.

The live wire format was checked against Giggy's current V17 server handlers on
October 11, 2026. Admission is JSON `mode:"streaming", microphone:true` with
`xi-api-key`. The adapter supplies a fresh valid `client_request_id` UUID for
each utterance; V17 also permits omitting it and then generates the UUID. Neither
form resumes a disconnected session. The admitted UUID and `microphone_ready`
attempt/token fence the ordered `input_token`, `sequence`, `pcm_base64`, and
`complete` frame POSTs. No V17-specific parameter, language override, multipart
upload or internal worker route is required. The contract check uses the actual
V17 admission/input handlers with offline worker and storage stand-ins; real
microphone qualification remains a separate release gate.

Each captured utterance, including pre-roll and VAD stop silence, must contain
0.5–30 seconds of audio. Shorter segments report an error before admission;
longer ones fail and close the active request. No synthetic silence is added.
There is no automatic splitting at 30 seconds. Automatic language detection is
required; model/language changes and synthetic keepalive are unsupported.

One earlier utterance may finalize while the next is captured. Network operations
and final transcripts stay ordered, with at most two bounded utterances buffered.
A slow earlier operation delays admission of the next. A third pending utterance,
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
session without default authentication headers, and never log the live input
capability. Requests stay on the admitting origin. HTTPS redirects are disabled;
signed transcript artifacts are fetched without sending the Giggy key/token.
The turn lifetime is bounded to 150 seconds; admission/SSE is bounded to 120
seconds and each input POST to 10 seconds. No measured STT latency guarantee is
claimed for this candidate; Pipecat's default STT metadata is not a Giggy benchmark.

### Replay a recording through the live input protocol

The example needs your Giggy account, API key and paid Streaming credits. It needs
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
