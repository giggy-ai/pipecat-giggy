# Giggy Voice Agent Sandbox â€” Pipecat

The cloud-only Giggy voice sandbox, running on native Pipecat 1.12.0 services, frames, context aggregation, interruptions, and FastAPI WebSocket transport. Giggy performs streaming STT and TTS; Pipecat's DeepSeek service generates replies. No local model or local/cloud toggle is included.

## Run

From the repository root, using Python 3.11+:

```sh
python -m venv .venv
# Activate .venv using your shell's activation command.
pip install -e ".[sandbox]"
python examples/voice-agent-sandbox/server.py
```

Open http://localhost:3003 in your browser. Create a [Giggy account](https://giggy.ai), obtain your own Giggy API key and DeepSeek API key, and enter them in the setup form. Angela Fowler is selected by default; starting a session triggers the agent's introduction automatically.

For deployment set `APP_ORIGIN` to the exact HTTPS origin, `HOST=0.0.0.0` and `PORT` to your hosting port. The loopback development default is `HOST=127.0.0.1`, `PORT=3003`, `APP_ORIGIN=http://localhost:3003`. Only these non-secret settings belong in an environment file. Public demos never ship operator credentials.

## Cloud contract and Pipecat flow

Microphone PCM16 mono 16 kHz is captured in the browser. The ingress processor translates browser speech boundaries to Pipecat speaking/VAD frames. `GiggySTTService` opens `POST https://giggy.ai/v1/audio/transcriptions` with `{ "mode": "streaming", "microphone": true }`. It omits `client_request_id`: the server announces the admitted UUID in `generation_created`, then `microphone_ready` supplies the same UUID, attempt and capability token. Ordered PCM uploads go to `/api/generations/{generation_uuid}/stt-input`, and an explicit final frame completes capture. Uploads are not retried or resumed.

Only successful durable STT completion produces a final Pipecat transcription. Native context aggregators invoke `DeepSeekLLMService`, and `GiggyHttpTTSService` streams 24 kHz PCM through the native WebSocket output transport. Stop and barge-in cancel the pipeline's active output and outstanding STT capture. The browser and its visual assets match the [standalone sandbox](https://github.com/giggy-ai/giggy-voice-agent).

## Credentials and scope

Keys are validated against production APIs and held only in bounded server memory for 30 minutes. The browser receives an opaque HttpOnly SameSite cookie. Disconnect deletes the account session and clears keys. Production hosting requires HTTPS. Use a trusted single-instance host: this example has in-memory sessions and is not a shared multi-worker session store.

The sandbox uses the existing Giggy production service. It does not change Giggy workers, database schemas, credits, or deployment. The underlying published adapter is now `0.3.1`, including WebSocket STT and TTS; install sandbox extras from this source checkout. No PyPI or upstream Pipecat release is made by this change. Physical microphone and deployed browser qualification remain release checks.

## Check

```sh
pip install -e ".[sandbox,test]"
python -m unittest discover -s examples/voice-agent-sandbox -p test_sandbox.py -v
python -m pytest tests -q
```

Tests run the real native Pipecat pipeline with deterministic provider stand-ins, covering automatic introduction, streamed audio, typed and microphone turns, credential isolation, origin restrictions, and disconnect. Provider production checks must use private environment credentials outside the repository.

## Current STT transport

The browser supplies turn boundaries. The server therefore uses `GiggyExternalSTTService(endpointing="external")` from the current adapter rather than adding a second provider VAD. Audio uses the authenticated `wss://api.giggy.ai/v1/audio/transcriptions` socket with start/readiness, binary 16 kHz PCM and stop controls; no per-frame HTTPS requests or automatic replay. The adapter validates final input and fenced transcript identity, then verifies the saved result. Consumers must drain the lifecycle and surface later verification errors.

Install sandbox extras from this PR source, not the published wheel, because the sandbox itself remains unreleased. Offline provider stand-ins do not qualify browser microphone capture, hosted deployment, echo cancellation or production latency; this PR remains draft until those checks are complete.
