"""Clean source EOF finalizes; silence, source failure and cancellation do not."""
import asyncio
import unittest
from pipecat_giggy.stt import GiggySTTService
from test_external_stt import FiniteHTTPFixture, assert_exact_final_audio, wait_for_condition

class SourceEOFTests(unittest.IsolatedAsyncioTestCase):
    def service(self, fixture):
        svc = GiggySTTService(api_key="unit", aiohttp_session=fixture.session, endpointing="external")
        svc._origin = fixture.origin
        svc._user_id = "source-eof"
        svc.create_task = lambda coro, **kw: asyncio.create_task(coro)
        async def discard(*args, **kwargs): pass
        svc.push_frame = svc.push_error = discard
        self.addAsyncCleanup(svc._abort)
        return svc

    async def test_clean_source_eof_flushes_original_tail_and_returns_fast_final(self):
        pcm = b"\x12\x34" * 20800
        verification = asyncio.Event()
        async with FiniteHTTPFixture(pcm, verification_gate=verification) as fixture:
            svc = self.service(fixture)
            async def audio():
                self.assertTrue(svc._capture.ready.is_set())
                for start in range(0, len(pcm), 2400):
                    yield pcm[start:start + 2400]
            try:
                result = await asyncio.wait_for(svc.transcribe_stream(audio()), 2)
                self.assertEqual(result, fixture.text)
                assert_exact_final_audio(fixture.frames, pcm)
                self.assertFalse(verification.is_set())
                self.assertEqual(len(fixture.admissions), 1)
            finally:
                verification.set()
                await asyncio.gather(*tuple(svc._turn_tasks), return_exceptions=True)

    async def test_source_pause_does_not_finalize_but_eof_sends_one_empty_marker(self):
        pcm = b"\x01\x00" * 8000
        release = asyncio.Event()
        async with FiniteHTTPFixture(pcm) as fixture:
            svc = self.service(fixture)
            async def audio():
                yield pcm
                await release.wait()
            task = asyncio.create_task(svc.transcribe_stream(audio()))
            try:
                await wait_for_condition(lambda: len(fixture.frames) == 1)
                self.assertFalse(fixture.frames[0]["complete"])
                self.assertFalse(task.done())
                release.set()
                self.assertEqual(await asyncio.wait_for(task, 2), fixture.text)
                assert_exact_final_audio(fixture.frames, pcm)
                self.assertEqual(fixture.frames[-1]["pcm_base64"], "")
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                await asyncio.gather(*tuple(svc._turn_tasks), return_exceptions=True)

    async def test_source_exception_aborts_without_final_marker_or_retry(self):
        pcm = b"\x01\x00" * 8000
        async with FiniteHTTPFixture(pcm) as fixture:
            svc = self.service(fixture)
            async def audio():
                yield pcm
                await wait_for_condition(lambda: len(fixture.frames) == 1)
                raise OSError("capture failed")
            with self.assertRaisesRegex(OSError, "capture failed"):
                await svc.transcribe_stream(audio())
            self.assertFalse(any(frame["complete"] for frame in fixture.frames))
            self.assertFalse(svc._turn_tasks)
            self.assertIsNone(svc._capture)
            self.assertEqual(len(fixture.admissions), 1)

    async def test_cancellation_aborts_without_turning_truncation_into_final(self):
        pcm = b"\x01\x00" * 8000
        async with FiniteHTTPFixture(pcm) as fixture:
            svc = self.service(fixture)
            async def audio():
                yield pcm
                await asyncio.Event().wait()
            task = asyncio.create_task(svc.transcribe_stream(audio()))
            await wait_for_condition(lambda: len(fixture.frames) == 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
            self.assertFalse(any(frame["complete"] for frame in fixture.frames))
            self.assertFalse(svc._turn_tasks)
            self.assertIsNone(svc._capture)

    async def test_short_or_empty_eof_preserves_minimum_and_releases_reservation(self):
        for pcm in [b"", b"\x01\x00" * 100]:
            with self.subTest(bytes=len(pcm)):
                async with FiniteHTTPFixture(pcm) as fixture:
                    svc = self.service(fixture)
                    async def audio():
                        if pcm: yield pcm
                    with self.assertRaisesRegex(ValueError, "0.5 seconds"):
                        await svc.transcribe_stream(audio())
                    self.assertFalse(svc._turn_tasks)
                    self.assertIsNone(svc._capture)
                    self.assertFalse(any(frame["complete"] for frame in fixture.frames))

    async def test_rejected_concurrent_source_does_not_cancel_existing_turn(self):
        async with FiniteHTTPFixture(b"\x01\x00" * 8000) as fixture:
            svc = self.service(fixture)
            await svc.start_utterance()
            turn = svc._capture
            async def audio(): yield b"\x01\x00" * 8000
            with self.assertRaisesRegex(RuntimeError, "active"):
                await svc.transcribe_stream(audio())
            self.assertIs(svc._capture, turn)
            self.assertTrue(svc._turn_tasks)
            self.assertEqual(len(fixture.admissions), 1)
