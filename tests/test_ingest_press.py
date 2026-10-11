import ast
import base64
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import runpy
import sys
import types
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / 'ingest_press.py'
tree = ast.parse(SCRIPT.read_text())
ns = {'io': io, 'base64': base64}
exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef)
    and n.name != 'parse_with_gemini'], type_ignores=[]), str(SCRIPT), 'exec'), ns)
sanitize = ns['sanitize_for_postgres']

def payload():
    return {'messageId': 'test-id', 'receivedAt': '2026-10-11T11:51:00Z',
            'from': 'sender@example.test', 'subject': '한글 제목 🚀',
            'bodyPlain': '정상\r\n본문\t🙂', 'attachments': []}

def execute(data, duplicate=False):
    posts, gemini_inputs = [], []
    class Client:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def post(self, url, headers, json):
            posts.append((url, headers, json))
            result = ([] if duplicate else [{'id': 'raw-test-id'}]) if url.endswith('press_raw') else []
            return types.SimpleNamespace(status_code=201, text='', json=lambda: result)
    class Models:
        def generate_content(self, **kwargs):
            gemini_inputs.append(kwargs)
            return types.SimpleNamespace(text=json.dumps({'category': 'finance', 'summary_ko': '요약\x00끝'}))
    httpx = types.ModuleType('httpx'); httpx.Client = Client
    google = types.ModuleType('google'); google.genai = types.SimpleNamespace(Client=lambda **kw: types.SimpleNamespace(models=Models()))
    env = {'SUPABASE_URL': 'https://example.test', 'SUPABASE_SERVICE_KEY': 'fake',
           'GEMINI_API_KEY': 'fake', 'PAYLOAD': json.dumps(data)}
    with patch.dict(sys.modules, {'httpx': httpx, 'google': google}), patch.dict(os.environ, env), contextlib.redirect_stdout(io.StringIO()):
        try: runpy.run_path(str(SCRIPT), run_name='__main__')
        except SystemExit as e:
            if e.code not in (0, None): raise
    return posts, gemini_inputs

class RegressionTests(unittest.TestCase):
    def test_normal_unicode_whitespace_and_literal_escape_preserved(self):
        text = '한글\r\n\t🙂\f e\u0301 \\u0000'
        self.assertEqual(sanitize(text), text)

    def test_nested_values_preserved_and_source_not_mutated(self):
        data = {'body': '앞\x00뒤', 'attachments': [{'extractedText': 'A\x00B'}], 'n': 3, 'ok': True, 'null': None}
        before = copy.deepcopy(data)
        out = sanitize(data)
        self.assertEqual(data, before)
        self.assertEqual(out, {'body': '앞뒤', 'attachments': [{'extractedText': 'AB'}], 'n': 3, 'ok': True, 'null': None})
        self.assertEqual(sanitize(out), out)

    def test_identity_and_timestamp_nul_rejected(self):
        for key in ('message_id', 'received_at'):
            with self.assertRaises(ValueError): sanitize({key: 'a\x00b'}, 'raw')

    def test_body_and_metadata_sanitized_only_at_database_boundary(self):
        p = payload(); p['bodyPlain'] += '\x00끝'; p['subject'] += '\x00'; p['from'] += '\x00'
        posts, inputs = execute(p)
        row = posts[0][2]
        self.assertEqual(row['body_plain'], p['bodyPlain'].replace('\x00', ''))
        self.assertEqual(row['subject'], p['subject'].replace('\x00', ''))
        self.assertEqual(row['sender'], p['from'].replace('\x00', ''))
        self.assertIn(p['bodyPlain'], inputs[0]['contents'])
        self.assertEqual(inputs[0]['model'], 'gemini-2.5-flash-lite')
        self.assertEqual(posts[1][2]['summary_ko'], '요약끝')
        self.assertEqual(posts[1][2]['category'], 'finance')

    def test_normal_collection_unchanged(self):
        p = payload(); posts, inputs = execute(p)
        self.assertEqual(posts[0][2]['body_plain'], p['bodyPlain'])
        self.assertEqual(len(posts), 2)
        self.assertEqual(len(inputs), 1)

    def test_pdf_extraction_nul_and_existing_limit(self):
        p = payload(); p['attachments'] = [{'mimeType': 'application/pdf', 'filename': '보도\x00.pdf', 'dataBase64': base64.b64encode(b'pdf').decode()}]
        def extract(data, out, **kw): out.write('PDF 한글\x00본문\n' + 'x' * 21000)
        with patch('pdfminer.high_level.extract_text_to_fp', side_effect=extract): posts, inputs = execute(p)
        row = posts[0][2]['raw_attachments'][0]
        self.assertEqual(row['filename'], '보도.pdf')
        self.assertEqual(row['extractedText'], ('PDF 한글\x00본문\n' + 'x' * 21000)[:20000].replace('\x00', ''))
        self.assertIn('PDF 한글\x00본문', inputs[0]['contents'])

    def test_docx_extraction_nul(self):
        p = payload(); p['attachments'] = [{'mimeType': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'filename': '자료.docx', 'dataBase64': base64.b64encode(b'docx').decode()}]
        doc = types.SimpleNamespace(paragraphs=[types.SimpleNamespace(text=t) for t in ['한글\x00본문', '', '둘째 🙂']])
        with patch('docx.Document', return_value=doc): posts, inputs = execute(p)
        self.assertEqual(posts[0][2]['raw_attachments'][0]['extractedText'], '한글본문\n둘째 🙂')
        self.assertIn('한글\x00본문', inputs[0]['contents'])

    def test_real_docx_normal_text(self):
        import docx
        doc = docx.Document(); doc.add_paragraph('한글 보도자료 🙂'); doc.add_paragraph('둘째 줄')
        buffer = io.BytesIO(); doc.save(buffer)
        text = ns['extract_docx'](buffer.getvalue())
        self.assertEqual(sanitize(text), '한글 보도자료 🙂\n둘째 줄')

    def test_plain_attachment_nul(self):
        p = payload(); p['attachments'] = [{'mimeType': 'text/plain', 'filename': '자료.txt', 'dataBase64': base64.b64encode('한글\x00🙂'.encode()).decode()}]
        posts, _ = execute(p)
        self.assertEqual(posts[0][2]['raw_attachments'][0]['extractedText'], '한글🙂')

    def test_duplicate_skips_gemini_and_structured(self):
        posts, inputs = execute(payload(), duplicate=True)
        self.assertEqual(len(posts), 1)
        self.assertEqual(inputs, [])
        self.assertIn('resolution=ignore-duplicates', posts[0][1]['Prefer'])

if __name__ == '__main__': unittest.main()
