# Copyright (c) 2010-2013 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import gzip
import unittest
import mock
import six
import tempfile
from hashlib import md5, sha1

from swiftclient import utils as u


class TestConfigTrueValue(unittest.TestCase):

    def test_TRUE_VALUES(self):
        for v in u.TRUE_VALUES:
            self.assertEqual(v, v.lower())

    @mock.patch.object(u, 'TRUE_VALUES', 'hello world'.split())
    def test_config_true_value(self):
        for val in 'hello world HELLO WORLD'.split():
            self.assertIs(u.config_true_value(val), True)
        self.assertIs(u.config_true_value(True), True)
        self.assertIs(u.config_true_value('foo'), False)
        self.assertIs(u.config_true_value(False), False)


class TestPrtBytes(unittest.TestCase):

    def test_zero_bytes(self):
        bytes_ = 0
        raw = '0'
        human = '0'
        self.assertEqual(raw, u.prt_bytes(bytes_, False).lstrip())
        self.assertEqual(human, u.prt_bytes(bytes_, True).lstrip())

    def test_one_byte(self):
        bytes_ = 1
        raw = '1'
        human = '1'
        self.assertEqual(raw, u.prt_bytes(bytes_, False).lstrip())
        self.assertEqual(human, u.prt_bytes(bytes_, True).lstrip())

    def test_less_than_one_k(self):
        bytes_ = (2 ** 10) - 1
        raw = '1023'
        human = '1023'
        self.assertEqual(raw, u.prt_bytes(bytes_, False).lstrip())
        self.assertEqual(human, u.prt_bytes(bytes_, True).lstrip())

    def test_one_k(self):
        bytes_ = 2 ** 10
        raw = '1024'
        human = '1.0K'
        self.assertEqual(raw, u.prt_bytes(bytes_, False).lstrip())
        self.assertEqual(human, u.prt_bytes(bytes_, True).lstrip())

    def test_a_decimal_k(self):
        bytes_ = (3 * 2 ** 10) + 512
        raw = '3584'
        human = '3.5K'
        self.assertEqual(raw, u.prt_bytes(bytes_, False).lstrip())
        self.assertEqual(human, u.prt_bytes(bytes_, True).lstrip())

    def test_a_bit_less_than_one_meg(self):
        bytes_ = (2 ** 20) - (2 ** 10)
        raw = '1047552'
        human = '1023K'
        self.assertEqual(raw, u.prt_bytes(bytes_, False).lstrip())
        self.assertEqual(human, u.prt_bytes(bytes_, True).lstrip())

    def test_just_a_hair_less_than_one_meg(self):
        bytes_ = (2 ** 20) - (2 ** 10) + 1
        raw = '1047553'
        human = '1.0M'
        self.assertEqual(raw, u.prt_bytes(bytes_, False).lstrip())
        self.assertEqual(human, u.prt_bytes(bytes_, True).lstrip())

    def test_one_meg(self):
        bytes_ = 2 ** 20
        raw = '1048576'
        human = '1.0M'
        self.assertEqual(raw, u.prt_bytes(bytes_, False).lstrip())
        self.assertEqual(human, u.prt_bytes(bytes_, True).lstrip())

    def test_ten_meg(self):
        bytes_ = 10 * 2 ** 20
        human = '10M'
        self.assertEqual(human, u.prt_bytes(bytes_, True).lstrip())

    def test_bit_less_than_ten_meg(self):
        bytes_ = (10 * 2 ** 20) - (100 * 2 ** 10)
        human = '9.9M'
        self.assertEqual(human, u.prt_bytes(bytes_, True).lstrip())

    def test_just_a_hair_less_than_ten_meg(self):
        bytes_ = (10 * 2 ** 20) - 1
        human = '10.0M'
        self.assertEqual(human, u.prt_bytes(bytes_, True).lstrip())

    def test_a_yotta(self):
        bytes_ = 42 * 2 ** 80
        self.assertEqual('42Y', u.prt_bytes(bytes_, True).lstrip())

    def test_overflow(self):
        bytes_ = 2 ** 90
        self.assertEqual('1024Y', u.prt_bytes(bytes_, True).lstrip())


class TestTempURL(unittest.TestCase):
    url = '/v1/AUTH_account/c/o'
    seconds = 3600
    key = 'correcthorsebatterystaple'
    method = 'GET'
    expected_url = url + ('?temp_url_sig=temp_url_signature'
                          '&temp_url_expires=1400003600')
    expected_body = '\n'.join([
        method,
        '1400003600',
        url,
    ]).encode('utf-8')

    @mock.patch('hmac.HMAC')
    @mock.patch('time.time', return_value=1400000000)
    def test_generate_temp_url(self, time_mock, hmac_mock):
        hmac_mock().hexdigest.return_value = 'temp_url_signature'
        url = u.generate_temp_url(self.url, self.seconds,
                                  self.key, self.method)
        key = self.key
        if not isinstance(key, six.binary_type):
            key = key.encode('utf-8')
        self.assertEqual(url, self.expected_url)
        self.assertEqual(hmac_mock.mock_calls, [
            mock.call(),
            mock.call(key, self.expected_body, sha1),
            mock.call().hexdigest(),
        ])
        self.assertIsInstance(url, type(self.url))

    def test_generate_temp_url_invalid_path(self):
        with self.assertRaises(ValueError) as exc_manager:
            u.generate_temp_url(b'/v1/a/c/\xff', self.seconds, self.key,
                                self.method)
        self.assertEqual(exc_manager.exception.args[0],
                         'path must be representable as UTF-8')

    @mock.patch('hmac.HMAC.hexdigest', return_value="temp_url_signature")
    def test_generate_absolute_expiry_temp_url(self, hmac_mock):
        if isinstance(self.expected_url, six.binary_type):
            expected_url = self.expected_url.replace(
                b'1400003600', b'2146636800')
        else:
            expected_url = self.expected_url.replace(
                u'1400003600', u'2146636800')
        url = u.generate_temp_url(self.url, 2146636800, self.key, self.method,
                                  absolute=True)
        self.assertEqual(url, expected_url)

    def test_generate_temp_url_bad_seconds(self):
        with self.assertRaises(TypeError) as exc_manager:
            u.generate_temp_url(self.url, 'not_an_int', self.key, self.method)
        self.assertEqual(exc_manager.exception.args[0],
                         'seconds must be an integer')

        with self.assertRaises(ValueError) as exc_manager:
            u.generate_temp_url(self.url, -1, self.key, self.method)
        self.assertEqual(exc_manager.exception.args[0],
                         'seconds must be a positive integer')


class TestTempURLUnicodePathAndKey(TestTempURL):
    url = u'/v1/\u00e4/c/\u00f3'
    key = u'k\u00e9y'
    expected_url = (u'%s?temp_url_sig=temp_url_signature'
                    u'&temp_url_expires=1400003600') % url
    expected_body = u'\n'.join([
        u'GET',
        u'1400003600',
        url,
    ]).encode('utf-8')


class TestTempURLUnicodePathBytesKey(TestTempURL):
    url = u'/v1/\u00e4/c/\u00f3'
    key = u'k\u00e9y'.encode('utf-8')
    expected_url = (u'%s?temp_url_sig=temp_url_signature'
                    u'&temp_url_expires=1400003600') % url
    expected_body = '\n'.join([
        u'GET',
        u'1400003600',
        url,
    ]).encode('utf-8')


class TestTempURLBytesPathUnicodeKey(TestTempURL):
    url = u'/v1/\u00e4/c/\u00f3'.encode('utf-8')
    key = u'k\u00e9y'
    expected_url = url + (b'?temp_url_sig=temp_url_signature'
                          b'&temp_url_expires=1400003600')
    expected_body = b'\n'.join([
        b'GET',
        b'1400003600',
        url,
    ])


class TestTempURLBytesPathAndKey(TestTempURL):
    url = u'/v1/\u00e4/c/\u00f3'.encode('utf-8')
    key = u'k\u00e9y'.encode('utf-8')
    expected_url = url + (b'?temp_url_sig=temp_url_signature'
                          b'&temp_url_expires=1400003600')
    expected_body = b'\n'.join([
        b'GET',
        b'1400003600',
        url,
    ])


class TestTempURLBytesPathAndNonUtf8Key(TestTempURL):
    url = u'/v1/\u00e4/c/\u00f3'.encode('utf-8')
    key = b'k\xffy'
    expected_url = url + (b'?temp_url_sig=temp_url_signature'
                          b'&temp_url_expires=1400003600')
    expected_body = b'\n'.join([
        b'GET',
        b'1400003600',
        url,
    ])


class TestReadableToIterable(unittest.TestCase):

    def test_iter(self):
        chunk_size = 4
        write_data = tuple(x.encode() for x in ('a', 'b', 'c', 'd'))
        actual_md5sum = md5()

        with tempfile.TemporaryFile() as f:
            for x in write_data:
                f.write(x * chunk_size)
                actual_md5sum.update(x * chunk_size)
            f.seek(0)
            data = u.ReadableToIterable(f, chunk_size, True)

            for i, data_chunk in enumerate(data):
                self.assertEqual(chunk_size, len(data_chunk))
                self.assertEqual(data_chunk, write_data[i] * chunk_size)

            self.assertEqual(actual_md5sum.hexdigest(), data.get_md5sum())

    def test_md5_creation(self):
        # Check creation with a real and noop md5 class
        data = u.ReadableToIterable(None, None, md5=True)
        self.assertEqual(md5().hexdigest(), data.get_md5sum())
        self.assertIs(type(data.md5sum), type(md5()))

        data = u.ReadableToIterable(None, None, md5=False)
        self.assertEqual('', data.get_md5sum())
        self.assertIs(type(data.md5sum), u.NoopMD5)

    def test_unicode(self):
        # Check no errors are raised if unicode data is feed in.
        unicode_data = u'abc'
        actual_md5sum = md5(unicode_data.encode()).hexdigest()
        chunk_size = 2

        with tempfile.TemporaryFile(mode='w+') as f:
            f.write(unicode_data)
            f.seek(0)
            data = u.ReadableToIterable(f, chunk_size, True)

            x = next(data)
            self.assertEqual(2, len(x))
            self.assertEqual(unicode_data[:2], x)

            x = next(data)
            self.assertEqual(1, len(x))
            self.assertEqual(unicode_data[2:], x)

            self.assertEqual(actual_md5sum, data.get_md5sum())


class TestLengthWrapper(unittest.TestCase):

    def test_stringio(self):
        contents = six.StringIO(u'a' * 50 + u'b' * 50)
        contents.seek(22)
        data = u.LengthWrapper(contents, 42, True)
        s = u'a' * 28 + u'b' * 14
        read_data = u''.join(iter(data.read, ''))

        self.assertEqual(42, len(data))
        self.assertEqual(42, len(read_data))
        self.assertEqual(s, read_data)
        self.assertEqual(md5(s.encode()).hexdigest(), data.get_md5sum())

        data.reset()
        self.assertEqual(md5().hexdigest(), data.get_md5sum())

        read_data = u''.join(iter(data.read, ''))
        self.assertEqual(42, len(read_data))
        self.assertEqual(s, read_data)
        self.assertEqual(md5(s.encode()).hexdigest(), data.get_md5sum())

    def test_bytesio(self):
        contents = six.BytesIO(b'a' * 50 + b'b' * 50)
        contents.seek(22)
        data = u.LengthWrapper(contents, 42, True)
        s = b'a' * 28 + b'b' * 14
        read_data = b''.join(iter(data.read, ''))

        self.assertEqual(42, len(data))
        self.assertEqual(42, len(read_data))
        self.assertEqual(s, read_data)
        self.assertEqual(md5(s).hexdigest(), data.get_md5sum())

    def test_tempfile(self):
        with tempfile.NamedTemporaryFile(mode='wb') as f:
            f.write(b'a' * 100)
            f.flush()
            contents = open(f.name, 'rb')
            data = u.LengthWrapper(contents, 42, True)
            s = b'a' * 42
            read_data = b''.join(iter(data.read, ''))

            self.assertEqual(42, len(data))
            self.assertEqual(42, len(read_data))
            self.assertEqual(s, read_data)
            self.assertEqual(md5(s).hexdigest(), data.get_md5sum())

    def test_segmented_file(self):
        with tempfile.NamedTemporaryFile(mode='wb') as f:
            segment_length = 1024
            segments = ('a', 'b', 'c', 'd')
            for c in segments:
                f.write((c * segment_length).encode())
            f.flush()
            for i, c in enumerate(segments):
                contents = open(f.name, 'rb')
                contents.seek(i * segment_length)
                data = u.LengthWrapper(contents, segment_length, True)
                read_data = b''.join(iter(data.read, ''))
                s = (c * segment_length).encode()

                self.assertEqual(segment_length, len(data))
                self.assertEqual(segment_length, len(read_data))
                self.assertEqual(s, read_data)
                self.assertEqual(md5(s).hexdigest(), data.get_md5sum())

                data.reset()
                self.assertEqual(md5().hexdigest(), data.get_md5sum())
                read_data = b''.join(iter(data.read, ''))
                self.assertEqual(segment_length, len(data))
                self.assertEqual(segment_length, len(read_data))
                self.assertEqual(s, read_data)
                self.assertEqual(md5(s).hexdigest(), data.get_md5sum())


class TestGroupers(unittest.TestCase):
    def test_n_at_a_time(self):
        result = list(u.n_at_a_time(range(100), 9))
        self.assertEqual([9] * 11 + [1], list(map(len, result)))

        result = list(u.n_at_a_time(range(100), 10))
        self.assertEqual([10] * 10, list(map(len, result)))

        result = list(u.n_at_a_time(range(100), 11))
        self.assertEqual([11] * 9 + [1], list(map(len, result)))

        result = list(u.n_at_a_time(range(100), 12))
        self.assertEqual([12] * 8 + [4], list(map(len, result)))

    def test_n_groups(self):
        result = list(u.n_groups(range(100), 9))
        self.assertEqual([12] * 8 + [4], list(map(len, result)))

        result = list(u.n_groups(range(100), 10))
        self.assertEqual([10] * 10, list(map(len, result)))

        result = list(u.n_groups(range(100), 11))
        self.assertEqual([10] * 10, list(map(len, result)))

        result = list(u.n_groups(range(100), 12))
        self.assertEqual([9] * 11 + [1], list(map(len, result)))


class TestApiResponeParser(unittest.TestCase):

    def test_utf8_default(self):
        result = u.parse_api_response(
            {}, u'{"test": "\u2603"}'.encode('utf8'))
        self.assertEqual({'test': u'\u2603'}, result)

        result = u.parse_api_response(
            {}, u'{"test": "\\u2603"}'.encode('utf8'))
        self.assertEqual({'test': u'\u2603'}, result)

    def test_bad_json(self):
        self.assertRaises(ValueError, u.parse_api_response,
                          {}, b'{"foo": "bar}')

    def test_bad_utf8(self):
        self.assertRaises(UnicodeDecodeError, u.parse_api_response,
                          {}, b'{"foo": "b\xffr"}')

    def test_latin_1(self):
        result = u.parse_api_response(
            {'content-type': 'application/json; charset=iso8859-1'},
            b'{"t\xe9st": "\xff"}')
        self.assertEqual({u't\xe9st': u'\xff'}, result)

    def test_gzipped_utf8(self):
        buf = six.BytesIO()
        gz = gzip.GzipFile(fileobj=buf, mode='w')
        gz.write(u'{"test": "\u2603"}'.encode('utf8'))
        gz.close()
        result = u.parse_api_response(
            {'content-encoding': 'gzip'},
            buf.getvalue())
        self.assertEqual({'test': u'\u2603'}, result)
