# Changelog

## 0.2.0.dev0 — Unreleased local candidate

- Add `GiggySTTService` using the existing paid live PCM transcription API.
- Send ordered 500 ms PCM packets during VAD-delimited capture and flush the final tail.
- Emit replaceable interim snapshots and one canonical final transcript per successful utterance.
- Validate readiness identity, attempt, transcript sequence and durable audio duration.
- Bound capture, pending turns and response sizes; close SSE/sender tasks on failure or cancellation.
- Preserve STT capture during bot interruptions; reject unsupported audio/settings without retries.
- Add a billable WAV replay example through the live protocol, with no LLM or TTS dependencies.
- Keep Pipecat pinned to 1.12.0. Live microphone and combined STT/TTS qualification remain pending.
- Verify the unchanged live wire format against current V17 admission/input handlers, including optional request IDs and strict frame errors.
- Preserve one second of real pre-VAD audio so delayed detection does not cut quiet opening words; verified against production recorded speech.

Not published to PyPI or submitted to Pipecat.

## 0.1.0 — 2026-10-08

- HTTP synthesis through Giggy's OpenAI-compatible speech endpoint.
- Progressive mono PCM16 at 24 kHz, with aligned samples and Pipecat context IDs.
- Runtime voice and speed settings, TTS usage and first-audio metrics.
- Cancellation propagates and closes the active HTTP response; requests are never retried.
- Single-file synthesis example and explicit Pipecat 1.12.0 compatibility.

Initial public release maintained by Giggy.
