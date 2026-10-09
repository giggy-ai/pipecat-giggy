"""Browser microphone voice agent: OpenAI STT/LLM and Giggy streaming TTS.

Uses Pipecat's SmallWebRTC development runner. No inference worker is started.
"""

import os

import aiohttp
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker, ProcessorUnusablePolicy
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import create_transport
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.services.openai.stt import OpenAISTTService
from pipecat.transports.base_transport import TransportParams
from pipecat.workers.runner import WorkerRunner
from pipecat_giggy import GiggyHttpTTSService


async def bot(runner_args: RunnerArguments):
    """Run one microphone session with real speech-driven interruption."""
    if os.environ.get("GIGGY_ALLOW_BILLABLE_EXAMPLE") != "1":
        raise RuntimeError("Set GIGGY_ALLOW_BILLABLE_EXAMPLE=1 to allow paid API calls.")
    for name in ("GIGGY_API_KEY", "GIGGY_VOICE_ID", "OPENAI_API_KEY"):
        if not os.environ.get(name, "").strip():
            raise RuntimeError(f"Set {name} before starting the example.")

    transport = await create_transport(
        runner_args,
        {"webrtc": lambda: TransportParams(audio_in_enabled=True, audio_out_enabled=True)},
    )
    stt = OpenAISTTService(
        api_key=os.environ["OPENAI_API_KEY"],
        settings=OpenAISTTService.Settings(model="gpt-4o-transcribe"),
    )
    llm = OpenAILLMService(
        api_key=os.environ["OPENAI_API_KEY"],
        settings=OpenAILLMService.Settings(
            model="gpt-4o-mini",
            system_instruction=(
                "You are a friendly voice assistant. Answer in one or two short sentences. "
                "Use plain spoken text without markdown or exclamation marks. "
                "When the user interrupts, answer their latest question directly."
            ),
        ),
    )
    context = LLMContext()
    user, assistant = LLMContextAggregatorPair(
        context, user_params=LLMUserAggregatorParams(vad_analyzer=SileroVADAnalyzer())
    )

    async with aiohttp.ClientSession() as session:
        tts = GiggyHttpTTSService(
            api_key=os.environ["GIGGY_API_KEY"],
            voice_id=os.environ["GIGGY_VOICE_ID"],
            aiohttp_session=session,
        )
        worker = PipelineWorker(
            Pipeline([transport.input(), stt, user, llm, tts, transport.output(), assistant]),
            params=PipelineParams(
                audio_out_sample_rate=24000,
                enable_metrics=True,
                enable_usage_metrics=True,
            ),
            idle_timeout_secs=runner_args.pipeline_idle_timeout_secs,
            processor_unusable_policy=ProcessorUnusablePolicy.END,
        )
        runner = WorkerRunner(handle_sigint=runner_args.handle_sigint)

        @transport.event_handler("on_client_connected")
        async def on_connected(transport, client):
            context.add_message({"role": "developer", "content": "Say hello and ask what I would like to explore."})
            await worker.queue_frames([LLMRunFrame()])

        @transport.event_handler("on_client_disconnected")
        async def on_disconnected(transport, client):
            await runner.cancel()

        await runner.add_workers(worker)
        await runner.run()


if __name__ == "__main__":
    from pipecat.runner.run import main

    main()
