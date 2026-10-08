"""HTTP TTS service for the canonical Giggy Streaming speech endpoint."""

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field

import aiohttp
from pipecat.frames.frames import (
    ErrorFrame,
    Frame,
    TTSAudioRawFrame,
)
from pipecat.services.settings import TTSSettings
from pipecat.services.tts_service import TTSService
from pipecat.utils.tracing.service_decorators import traced_tts
from pipecat.utils.types import NOT_GIVEN, NotGiven

GIGGY_SPEECH_URL = 'https://giggy.ai/v1/audio/speech'


@dataclass
class GiggyTTSSettings(TTSSettings):
    """Runtime voice/model settings and Giggy speech speed (0.25â€“4)."""
    speed: float | None | NotGiven = field(
        default_factory=lambda: NOT_GIVEN
    )


class GiggyHttpTTSService(TTSService):
    """Complete-text Giggy synthesis with progressive PCM and no automatic retries."""
    Settings = GiggyTTSSettings
    _settings: GiggyTTSSettings

    def __init__(
        self,
        *,
        api_key: str,
        voice_id: str,
        aiohttp_session: aiohttp.ClientSession,
        sample_rate: int = 24000,
        speed: float = 1.0,
        **kwargs,
    ):
        """Create the service using a caller-owned aiohttp session.

        Args:
            api_key: Giggy API key.
            voice_id: Giggy voice UUID.
            aiohttp_session: HTTP session owned and closed by the application.
            sample_rate: PCM output rate: 24000 Hz (mono Streaming contract).
            speed: Speech speed from 0.25 to 4 inclusive.
            **kwargs: Additional Pipecat TTSService options.
        """
        if not api_key:
            raise ValueError(
                'api_key is required.'
            )

        if not voice_id:
            raise ValueError(
                'voice_id is required.'
            )

        if sample_rate != 24000:
            raise ValueError(
                'sample_rate must be 24000 for Giggy Streaming.'
            )

        if not 0.25 <= speed <= 4:
            raise ValueError(
                'speed must be between 0.25 and 4.'
            )

        settings = self.Settings(
            model='giggyspeech',
            voice=voice_id,
            language=None,
            speed=speed,
        )

        kwargs.setdefault("stop_frame_timeout_s", 90)
        super().__init__(
            sample_rate=sample_rate,
            push_start_frame=True,
            push_stop_frames=True,
            # HTTP completion closes the context; its idle deadline must match
            # the stream read timeout rather than Pipecat's three-second default.
            settings=settings,
            **kwargs,
        )

        self._api_key = api_key
        self._session = aiohttp_session

    def can_generate_metrics(self) -> bool:
        """Enable Pipecat's TTS usage and first-audio metrics."""
        return True

    @traced_tts
    async def run_tts(
        self,
        text: str,
        context_id: str,
    ) -> AsyncGenerator[Frame | None, None]:
        """Send complete text once and yield aligned mono PCM or a safe ErrorFrame.

        Cancellation propagates and closes the response. A failed request may
        already have incurred paid Streaming admission, so it is never retried.
        """
        voice_id = self._settings.voice
        speed = self._settings.speed

        if self.sample_rate != 24000:
            yield ErrorFrame(error='Giggy Streaming requires a 24000 Hz pipeline output rate.')
            return

        if self._settings.model != 'giggyspeech':
            yield ErrorFrame(error='Giggy TTS requires the giggyspeech model.')
            return

        if not voice_id:
            yield ErrorFrame(
                error='Giggy voice ID is not configured.'
            )
            return

        if not text.strip():
            yield ErrorFrame(error="Giggy synthesis text must not be empty.")
            return

        if not isinstance(speed, (int, float)) or not 0.25 <= speed <= 4:
            yield ErrorFrame(error="Giggy speed must be between 0.25 and 4.")
            return

        payload = {
            'model': 'giggyspeech',
            'input': text,
            'voice': voice_id,
            'response_format': 'pcm',
            'sample_rate': self.sample_rate,
            'speed': speed,
        }

        headers = {
            'Authorization': (
                'Bearer ' + self._api_key
            ),
            'Content-Type': 'application/json',
            'Accept': 'application/octet-stream',
        }

        try:
            async with self._session.post(
                GIGGY_SPEECH_URL,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=90),
                allow_redirects=False,
            ) as response:
                if response.status != 200:
                    yield ErrorFrame(
                        error=f"Giggy TTS returned HTTP {response.status}."
                    )
                    return

                content_type = response.headers.get(
                    'content-type',
                    '',
                ).split(";")[0].strip().lower()

                if content_type not in {"application/octet-stream", "audio/pcm"}:
                    yield ErrorFrame(
                        error="Giggy TTS returned an unexpected audio format."
                    )
                    return

                await self.start_tts_usage_metrics(text)

                pending = b""
                total_bytes = 0

                async for chunk in response.content.iter_chunked(
                    self.chunk_size
                ):
                    if not chunk:
                        continue

                    data = pending + chunk
                    usable = len(data) & ~1
                    if usable:
                        await self.stop_ttfb_metrics()
                        yield TTSAudioRawFrame(
                            data[:usable], self.sample_rate, 1, context_id=context_id
                        )
                        total_bytes += usable
                    pending = data[usable:]

                if pending:
                    yield ErrorFrame(error="Giggy TTS returned an incomplete PCM sample.")
                elif not total_bytes:
                    yield ErrorFrame(error="Giggy TTS returned no audio.")

        except asyncio.TimeoutError:
            yield ErrorFrame(error="Giggy TTS request timed out.")
        except aiohttp.ClientError:
            yield ErrorFrame(error="Giggy TTS network request failed.")

        finally:
            await self.stop_ttfb_metrics()
