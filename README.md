# Giggy TTS for Pipecat

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

## Maintenance

Compatibility updates are recorded in [CHANGELOG.md](CHANGELOG.md). Report issues
to the Giggy repository after publication. Source and dependencies are released
under their respective licenses; this integration uses the [ISC license](LICENSE).
