# Changelog

## 0.3.0.dev0 — Unreleased

- Use the tested V21 low-latency Pipecat bridge for the public `GiggySTTService`: readiness before capture, 20 ms first / 100 ms ongoing PCM, independent 100 ms Silero endpoints and softer continuation detection.
- Keep native upstream VAD/metrics unchanged; validate the final input ACK before publishing a fenced final and drain durable transcript verification afterward.
- Preserve short real captures, bounded next-turn reservations and ordered final delivery. No padding, transport fallback or automatic paid-request replay.
- Expose caller-owned boundaries and clean-source EOF through `GiggyExternalSTTService`; correct the finite-recording example to use that helper.
- Reject caller-session default credentials and recheck before signed artifact reads; reject boolean/non-numeric TTS speeds.
- Require the Silero extra, provide optional test dependencies, and correct language, transport, lifecycle and unreleased-version documentation.
- Remain pinned to Pipecat 1.12.0. This candidate is local and not published; the running production benchmark retains its separate frozen adapter snapshot.

## 0.2.0 — 2026-10-12

- Add `GiggySTTService` using the existing paid live PCM transcription API.
- Send ordered 500 ms PCM packets during VAD-delimited capture and flush the final tail.
- Emit replaceable interim snapshots and one canonical final transcript per successful utterance.
- Validate readiness identity, attempt, transcript sequence and durable audio duration.
- Bound capture, pending turns and response sizes; close SSE/sender tasks on failure or cancellation.
- Preserve STT capture during bot interruptions; reject unsupported audio/settings without retries.
- Add a billable WAV replay example through the live protocol, with no LLM or TTS dependencies.
- Keep Pipecat pinned to 1.12.0. Production recorded-input STT/TTS conversation and cancellation/refund checks pass; physical microphone hardware is not qualified by the demo.
- Verify the unchanged live wire format against current V17 admission/input handlers, including optional request IDs and strict frame errors.
- Preserve one second of real pre-VAD audio so delayed detection does not cut quiet opening words; verified against production recorded speech.

Includes a two-voice demo with actual live/final STT text and VAD-driven playback interruption.

## 0.1.0 — 2026-10-08

- HTTP synthesis through Giggy's OpenAI-compatible speech endpoint.
- Progressive mono PCM16 at 24 kHz, with aligned samples and Pipecat context IDs.
- Runtime voice and speed settings, TTS usage and first-audio metrics.
- Cancellation propagates and closes the active HTTP response; requests are never retried.
- Single-file synthesis example and explicit Pipecat 1.12.0 compatibility.

Initial public release maintained by Giggy.
