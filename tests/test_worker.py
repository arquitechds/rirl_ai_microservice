import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import worker
from extraction import FIELDS

class FakeHeartbeat:
    def __init__(self):
        self.assert_owned = MagicMock()
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass

class WorkerTests(unittest.TestCase):
    def args(self):
        return SimpleNamespace(lease_seconds=300,max_text_bytes=2000000,max_attempts=3,retry_seconds=300)

    def job(self):
        return dict(job_id='job',s3_uri='file.pdf',target_id='row',claim_token='token',attempts=1)

    def test_success_saves_full_count(self):
        import json
        client=MagicMock()
        client.chat.completions.create.return_value.choices=[SimpleNamespace(
            finish_reason='stop',message=SimpleNamespace(content=json.dumps(dict.fromkeys(FIELDS,'NA'))))]
        with patch('worker.Heartbeat', return_value=FakeHeartbeat()), patch('worker.download_text',return_value='á 😀\n '), \
             patch('worker.db.complete') as save, patch('worker.log'):
            worker.process(self.job(),client,MagicMock(),self.args())
        self.assertEqual(save.call_args.args[2],5)
        self.assertEqual(client.chat.completions.create.call_args.kwargs['model'],'openai/gpt-oss-20b')

    def test_bad_response_retries_without_complete(self):
        client=MagicMock()
        client.chat.completions.create.return_value.choices=[SimpleNamespace(
            finish_reason='stop',message=SimpleNamespace(content='[]'))]
        with patch('worker.Heartbeat', return_value=FakeHeartbeat()), patch('worker.download_text',return_value='text'), \
             patch('worker.db.complete') as save, patch('worker.db.fail') as fail, patch('worker.log'):
            worker.process(self.job(),client,MagicMock(),self.args())
        save.assert_not_called()
        self.assertEqual(fail.call_args.args[2:],(4,3,300))

    def test_lease_loss_prevents_api_and_write(self):
        client=MagicMock()
        with patch('worker.Heartbeat', return_value=FakeHeartbeat()) as heartbeat, patch('worker.download_text',return_value='text'), \
             patch('worker.db.complete') as save, patch('worker.db.fail') as fail, patch('worker.log'):
            heartbeat.return_value.assert_owned.side_effect=worker.db.LeaseLost()
            worker.process(self.job(),client,MagicMock(),self.args())
        client.chat.completions.create.assert_not_called()
        save.assert_not_called()
        fail.assert_not_called()
