"""Giggy's community-maintained speech integrations for Pipecat."""

from .tts import GiggyHttpTTSService, GiggyTTSSettings
from .stt import (
    GiggyV21PipecatSTTService as GiggySTTService,
    GiggySTTService as GiggyExternalSTTService,
)

__all__ = ["GiggyHttpTTSService", "GiggyTTSSettings", "GiggySTTService", "GiggyExternalSTTService"]
