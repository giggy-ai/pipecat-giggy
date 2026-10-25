"""Exercise the actual Pipecat pipeline with deterministic cloud stand-ins."""
import sys
import unittest
from pathlib import Path
from datetime import UTC, datetime
sys.path.insert(0, str(Path(__file__).parent))
from fastapi.testclient import TestClient
from pipecat.frames.frames import *
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection
from pipecat.services.tts_service import TTSService
from pipecat.services.settings import TTSSettings
from server import create_app, run_conversation
from sandbox_config import VOICES

class STT(FrameProcessor):
    def __init__(self):
        super().__init__(); self.turn = ""
    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if isinstance(frame, InputAudioRawFrame):
            self.turn = frame.user_id
        elif isinstance(frame, VADUserStoppedSpeakingFrame):
            await self.push_frame(TranscriptionFrame("Hello microphone", self.turn, datetime.now(UTC).isoformat()))
        else:
            await self.push_frame(frame, direction)

class LLM(FrameProcessor):
    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if isinstance(frame, LLMContextFrame):
            await self.push_frame(LLMFullResponseStartFrame())
            await self.push_frame(LLMTextFrame("Hello from Pipecat."))
            await self.push_frame(LLMFullResponseEndFrame())
        else:
            await self.push_frame(frame, direction)

class TTS(TTSService):
    def __init__(self):
        super().__init__(sample_rate=24000, push_start_frame=True, push_stop_frames=True,
                         settings=TTSSettings(model="fixture",voice=VOICES[7]["id"]))
    async def run_tts(self, text, context_id):
        yield TTSAudioRawFrame(bytes(9600),24000,1,context_id=context_id)

async def valid(*keys): return True
async def conversation(ws, account):
    await run_conversation(ws,account,services=(STT(),LLM(),TTS()))

class SandboxTests(unittest.TestCase):
    def test_deepseek_request_disables_reasoning_for_voice(self):
        from server import DeepSeekLLMService
        service = DeepSeekLLMService(api_key="fixture", settings=DeepSeekLLMService.Settings(
            model="deepseek-flash", max_tokens=160, extra={"extra_body":{"thinking":{"type":"disabled"}}}))
        params=service.build_chat_completion_params({"messages":[{"role":"user","content":"Hi"}]})
        self.assertEqual(params["extra_body"]["thinking"]["type"],"disabled")
        self.assertEqual(params["model"],"deepseek-flash")

    def test_auth_origins_and_private_files(self):
        with TestClient(create_app(validate_keys=valid, conversation=conversation)) as client:
            self.assertEqual(client.get("/api/session").status_code,401)
            self.assertEqual(client.get("/.env").status_code,404)
            self.assertEqual(client.post("/api/session",json={}).status_code,403)
            reply=client.post("/api/session",headers={"origin":"http://localhost:3003"},json={"giggyKey":"fixture-giggy-key","deepseekKey":"fixture-deepseek-key"})
            self.assertEqual(reply.status_code,200)
            self.assertNotIn("fixture-giggy-key",reply.text)
            self.assertIn("HttpOnly",reply.headers["set-cookie"])
            self.assertEqual(client.delete("/api/session",headers={"origin":"http://localhost:3003"}).status_code,200)
            self.assertEqual(client.get("/api/session").status_code,401)
    def test_native_intro_typed_and_microphone_pipeline(self):
        import base64
        with TestClient(create_app(validate_keys=valid,conversation=conversation)) as client:
            client.post("/api/session",headers={"origin":"http://localhost:3003"},json={"giggyKey":"fixture-giggy-key","deepseekKey":"fixture-deepseek-key"})
            with client.websocket_connect("/api/voice-agent/realtime",headers={"origin":"http://localhost:3003"}) as ws:
                ws.send_json({"type":"voice_agent_session_start","voice":{"voice_library_uuid":VOICES[7]["id"]}})
                def until_done():
                    events=[]
                    for _ in range(100):
                        item=ws.receive_json();events.append(item)
                        if item["type"]=="voice_agent_error": self.fail(item["message"])
                        if item["type"]=="voice_agent_turn_done": return events
                    self.fail("Turn did not complete")
                events=until_done()
                self.assertTrue(any(e["type"]=="voice_agent_intro" for e in events))
                self.assertTrue(any(e["type"]=="voice_agent_audio_chunk" for e in events))
                ws.send_json({"type":"voice_agent_user_text","turn_id":"typed","text":"Hi"})
                events=until_done()
                self.assertTrue(any(e["type"]=="voice_agent_assistant_text_delta" for e in events))
                self.assertFalse(any(e["type"]=="voice_agent_transcript_final" for e in events))
                ws.send_json({"type":"voice_agent_audio_start","turn_id":"mic"})
                ws.send_json({"type":"voice_agent_audio_chunk","turn_id":"mic","sequence":0,"sample_rate":16000,"chunk_base64":base64.b64encode(bytes(16000)).decode()})
                ws.send_json({"type":"voice_agent_audio_end","turn_id":"mic"})
                events=until_done()
                self.assertEqual(next(e for e in events if e["type"]=="voice_agent_transcript_final")["text"],"Hello microphone")
                self.assertEqual(events[-1]["turn_id"],"mic")
                ws.send_json({"type":"voice_agent_session_end"})

if __name__=="__main__": unittest.main()
