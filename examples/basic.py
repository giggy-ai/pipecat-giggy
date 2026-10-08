"""Run one billable Giggy synthesis through Pipecat and save its PCM as WAV."""

import argparse
import asyncio
import os
import wave

import aiohttp
from pipecat.frames.frames import EndFrame, TTSAudioRawFrame, TTSSpeakFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.workers.runner import WorkerRunner
from pipecat_giggy import GiggyHttpTTSService


class PCMOutput(FrameProcessor):
    """Collect the output of this single-utterance example without changing frames."""

    def __init__(self):
        super().__init__()
        self.audio = bytearray()

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if direction == FrameDirection.DOWNSTREAM and isinstance(frame, TTSAudioRawFrame):
            if frame.sample_rate != 24000 or frame.num_channels != 1:
                raise RuntimeError("Expected mono PCM at 24 kHz.")
            self.audio.extend(frame.audio)
        await self.push_frame(frame, direction)


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--text", default="A voice that knows when to listen.")
    parser.add_argument("--output", default="speech.wav")
    args = parser.parse_args()
    if os.environ.get("GIGGY_ALLOW_BILLABLE_EXAMPLE") != "1":
        raise RuntimeError("Set GIGGY_ALLOW_BILLABLE_EXAMPLE=1 to authorize one synthesis request.")
    if not args.text.strip():
        raise ValueError("Speech text must not be empty.")
    errors = []
    output = PCMOutput()
    async with aiohttp.ClientSession() as session:
        tts = GiggyHttpTTSService(
            api_key=os.environ["GIGGY_API_KEY"],
            voice_id=os.environ["GIGGY_VOICE_ID"],
            aiohttp_session=session,
        )
        worker = PipelineWorker(
            Pipeline([tts, output]),
            params=PipelineParams(audio_out_sample_rate=24000),
            enable_rtvi=False,
        )

        @worker.event_handler("on_pipeline_started")
        async def on_started(worker, frame):
            await worker.queue_frames([TTSSpeakFrame(args.text), EndFrame()])

        @worker.event_handler("on_pipeline_error")
        async def on_error(worker, frame):
            errors.append(frame.error)

        runner = WorkerRunner(handle_sigint=False)
        await runner.add_workers(worker)
        await runner.run()
    if errors:
        raise RuntimeError(errors[0])
    if not output.audio:
        raise RuntimeError("The synthesis completed without audio.")
    with wave.open(args.output, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(24000)
        wav.writeframes(output.audio)
    print(f"Saved {len(output.audio)} PCM bytes to {args.output}")


if __name__ == "__main__":
    asyncio.run(main())
