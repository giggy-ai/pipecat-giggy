"""Replay one WAV at capture cadence through Giggy live STT and Pipecat.

This is a billable API example, not microphone or live-call qualification.
"""

import argparse
import asyncio
import os
import wave

import aiohttp
from pipecat.frames.frames import (
    EndFrame,
    InputAudioRawFrame,
    InterimTranscriptionFrame,
    TranscriptionFrame,
    VADUserStartedSpeakingFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.workers.runner import WorkerRunner

from pipecat_giggy import GiggySTTService


class TranscriptOutput(FrameProcessor):
    """Display replaceable snapshots and retain only the canonical final text."""

    def __init__(self):
        super().__init__()
        self.final_text = None

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, InterimTranscriptionFrame):
                print(f"Interim snapshot (replace): {frame.text}")
            elif isinstance(frame, TranscriptionFrame):
                self.final_text = frame.text
                print(f"Canonical final: {frame.text}")
        await self.push_frame(frame, direction)


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", required=True, help="PCM16 mono 16 kHz WAV, 0.5–30 seconds"
    )
    args = parser.parse_args()
    if os.environ.get("GIGGY_ALLOW_BILLABLE_EXAMPLE") != "1":
        raise RuntimeError(
            "Set GIGGY_ALLOW_BILLABLE_EXAMPLE=1 to authorize one transcription request."
        )
    with wave.open(args.input, "rb") as source:
        if (
            source.getnchannels() != 1
            or source.getsampwidth() != 2
            or source.getframerate() != 16000
            or source.getcomptype() != "NONE"
        ):
            raise ValueError("Input must be uncompressed mono PCM16 at 16000 Hz.")
        if not 8000 <= source.getnframes() <= 480000:
            raise ValueError("Input must contain 0.5–30 seconds of audio.")
        audio = source.readframes(source.getnframes())
        if len(audio) != source.getnframes() * 2:
            raise ValueError("Input WAV is truncated.")
    output = TranscriptOutput()
    errors = []
    async with aiohttp.ClientSession() as session:
        stt = GiggySTTService(
            api_key=os.environ["GIGGY_API_KEY"], aiohttp_session=session
        )
        worker = PipelineWorker(
            Pipeline([stt, output]),
            params=PipelineParams(audio_in_sample_rate=16000),
            enable_rtvi=False,
        )

        @worker.event_handler("on_pipeline_started")
        async def on_started(worker, frame):
            # Explicit boundaries for this known recording. A live application
            # supplies these frames from its own upstream VAD processor.
            await worker.queue_frame(VADUserStartedSpeakingFrame())
            for offset in range(0, len(audio), 640):
                chunk = audio[offset : offset + 640]
                await worker.queue_frame(InputAudioRawFrame(chunk, 16000, 1))
                await asyncio.sleep(len(chunk) / 32000)
            await worker.queue_frames([VADUserStoppedSpeakingFrame(), EndFrame()])

        @worker.event_handler("on_pipeline_error")
        async def on_error(worker, frame):
            errors.append(frame.error)

        runner = WorkerRunner(handle_sigint=False)
        await runner.add_workers(worker)
        await runner.run()
    if errors:
        raise RuntimeError(errors[0])
    if output.final_text is None:
        raise RuntimeError("No durable final transcript was received.")


if __name__ == "__main__":
    asyncio.run(main())
