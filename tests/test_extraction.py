import json
import unittest
from extraction import FIELDS, parse_response, decode_text, count_characters

class ExtractionTests(unittest.TestCase):
    def test_reason_required(self):
        result=dict.fromkeys(FIELDS,'NA'); result['uniondues2_ai']='No'
        with self.assertRaises(ValueError): parse_response(json.dumps(result))
        result['uniondues_no_reason_ai']='El texto menciona cuotas pero no especifica el monto.'
        self.assertEqual(parse_response(json.dumps(result)),result)

    def test_full_count(self):
        text=decode_text(json.dumps("{'text': 'á 😀\\n '}").encode(),'a.json.json')
        self.assertEqual(count_characters(text),5)
