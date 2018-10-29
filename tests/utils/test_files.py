import os
import unittest
import json
import datetime

import boto3
from moto import mock_s3

import rastervision as rv
from rastervision.utils.files import (
    file_to_str, str_to_file, download_if_needed, upload_or_copy,
    load_json_config, ProtobufParseException, make_dir, get_local_path,
    file_exists, sync_from_dir, sync_to_dir, list_paths)
from rastervision.filesystem import (NotReadableError, NotWritableError, GCSFileSystem)
from rastervision.filesystem.filesystem import FileSystem
from rastervision.protos.task_pb2 import TaskConfig as TaskConfigMsg
from rastervision.rv_config import RVConfig
from google.api_core.exceptions import NotFound


LOREM = """ Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do
        eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut
        enim ad minim veniam, quis nostrud exercitation ullamco
        laboris nisi ut aliquip ex ea commodo consequat. Duis aute
        irure dolor in reprehenderit in voluptate velit esse cillum
        dolore eu fugiat nulla pariatur. Excepteur sint occaecat
        cupidatat non proident, sunt in culpa qui officia deserunt
        mollit anim id est laborum.  """


class TestMakeDir(unittest.TestCase):
    def setUp(self):
        self.lorem = LOREM

        # Mock S3 bucket
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        self.s3 = boto3.client('s3')
        self.bucket_name = 'mock_bucket'
        self.s3.create_bucket(Bucket=self.bucket_name)

        # Temporary directory
        self.temp_dir = RVConfig.get_tmp_dir()

    def tearDown(self):
        self.temp_dir.cleanup()
        self.mock_s3.stop()

    def test_default_args(self):
        dir = os.path.join(self.temp_dir.name, 'hello')
        make_dir(dir)
        self.assertTrue(os.path.isdir(dir))

    def test_file_exists_local_true(self):
        path = os.path.join(self.temp_dir.name, 'lorem', 'ipsum.txt')
        directory = os.path.dirname(path)
        make_dir(directory, check_empty=False)

        with open(path, 'w+') as file:
            file.write(self.lorem)

        self.assertTrue(file_exists(path))

    def test_file_exists_local_false(self):
        path = os.path.join(self.temp_dir.name, 'hello', 'hello.txt')
        directory = os.path.dirname(path)
        make_dir(directory, check_empty=False)

        self.assertFalse(file_exists(path))

    def test_file_exists_s3_true(self):
        path = os.path.join(self.temp_dir.name, 'lorem', 'ipsum.txt')
        directory = os.path.dirname(path)
        make_dir(directory, check_empty=False)

        with open(path, 'w+') as file:
            file.write(self.lorem)

        s3_path = 's3://{}/lorem.txt'.format(self.bucket_name)
        upload_or_copy(path, s3_path)
        self.assertTrue(file_exists(s3_path))

    def test_file_exists_s3_false(self):
        s3_path = 's3://{}/hello.txt'.format(self.bucket_name)
        self.assertFalse(file_exists(s3_path))

    def test_file_exists_gcs_false(self):
        gcs_path = 'gs://{}/hello.txt'.format(self.bucket_name)
        self.assertFalse(file_exists(gcs_path))

    def test_check_empty(self):
        path = os.path.join(self.temp_dir.name, 'hello', 'hello.txt')
        dir = os.path.dirname(path)
        str_to_file('hello', path)

        make_dir(dir, check_empty=False)
        with self.assertRaises(Exception):
            make_dir(dir, check_empty=True)

    def test_force_empty(self):
        path = os.path.join(self.temp_dir.name, 'hello', 'hello.txt')
        dir = os.path.dirname(path)
        str_to_file('hello', path)

        make_dir(dir, force_empty=False)
        self.assertTrue(os.path.isfile(path))
        make_dir(dir, force_empty=True)
        is_empty = len(os.listdir(dir)) == 0
        self.assertTrue(is_empty)

    def test_use_dirname(self):
        path = os.path.join(self.temp_dir.name, 'hello', 'hello.txt')
        dir = os.path.dirname(path)
        make_dir(path, use_dirname=True)
        self.assertTrue(os.path.isdir(dir))


class TestGetLocalPath(unittest.TestCase):
    def test_local(self):
        download_dir = '/download_dir'
        uri = '/my/file.txt'
        path = get_local_path(uri, download_dir)
        self.assertEqual(path, uri)

    def test_s3(self):
        download_dir = '/download_dir'
        uri = 's3://bucket/my/file.txt'
        path = get_local_path(uri, download_dir)
        self.assertEqual(path, '/download_dir/s3/bucket/my/file.txt')

    def test_http(self):
        download_dir = '/download_dir'
        uri = 'http://bucket/my/file.txt'
        path = get_local_path(uri, download_dir)
        self.assertEqual(path, '/download_dir/http/bucket/my/file.txt')

    def test_gcs(self):
        download_dir = '/download_dir'
        uri = 'gs://bucket/my/file.txt'
        path = get_local_path(uri, download_dir)
        self.assertEqual(path, '/download_dir/gs/bucket/my/file.txt')



class TestFileToStr(unittest.TestCase):
    """Test file_to_str and str_to_file."""

    def setUp(self):
        # Setup mock S3 bucket.
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        self.s3 = boto3.client('s3')
        self.bucket_name = 'mock_bucket'
        self.s3.create_bucket(Bucket=self.bucket_name)



        self.content_str = 'hello'
        self.file_name = 'hello.txt'
        self.s3_path = 's3://{}/{}'.format(self.bucket_name, self.file_name)

        self.temp_dir = RVConfig.get_tmp_dir()
        self.local_path = os.path.join(self.temp_dir.name, self.file_name)

    def tearDown(self):
        self.temp_dir.cleanup()
        self.mock_s3.stop()

    def test_file_to_str_gcs(self):
        """"""
        wrong_path = 'gs://wrongpath/x.txt'

        with self.assertRaises(NotWritableError):
            str_to_file(self.content_str, wrong_path)

        str_to_file(self.content_str, 'gs://d81fa190-ef22-413e-8ea9-22bd5416f4df/hello.txt')
        content_str = file_to_str(self.s3_path)
        self.assertEqual(self.content_str, content_str)

        with self.assertRaises(NotReadableError):
            file_to_str(wrong_path)


    def test_file_to_str_local(self):
        str_to_file(self.content_str, self.local_path)
        content_str = file_to_str(self.local_path)
        self.assertEqual(self.content_str, content_str)

        wrong_path = '/wrongpath/x.txt'
        with self.assertRaises(NotReadableError):
            file_to_str(wrong_path)

    def test_file_to_str_s3(self):
        wrong_path = 's3://wrongpath/x.txt'

        with self.assertRaises(NotWritableError):
            str_to_file(self.content_str, wrong_path)

        str_to_file(self.content_str, self.s3_path)
        content_str = file_to_str(self.s3_path)
        self.assertEqual(self.content_str, content_str)

        with self.assertRaises(NotReadableError):
            file_to_str(wrong_path)


class TestDownloadIfNeeded(unittest.TestCase):
    """Test download_if_needed and upload_or_copy and str_to_file."""

    def setUp(self):
        # Setup mock S3 bucket.
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        self.s3 = boto3.client('s3')
        self.bucket_name = 'mock_bucket'
        self.s3.create_bucket(Bucket=self.bucket_name)

        self.content_str = 'hello'
        self.file_name = 'hello.txt'
        self.s3_path = 's3://{}/{}'.format(self.bucket_name, self.file_name)

        self.temp_dir = RVConfig.get_tmp_dir()
        self.local_path = os.path.join(self.temp_dir.name, self.file_name)

    def tearDown(self):
        self.temp_dir.cleanup()
        self.mock_s3.stop()

    def test_download_if_needed_local(self):
        with self.assertRaises(NotReadableError):
            download_if_needed(self.local_path, self.temp_dir.name)

        str_to_file(self.content_str, self.local_path)
        upload_or_copy(self.local_path, self.local_path)
        local_path = download_if_needed(self.local_path, self.temp_dir.name)
        self.assertEqual(local_path, self.local_path)

    def test_download_if_needed_s3(self):
        with self.assertRaises(NotReadableError):
            download_if_needed(self.s3_path, self.temp_dir.name)

        str_to_file(self.content_str, self.local_path)
        upload_or_copy(self.local_path, self.s3_path)
        local_path = download_if_needed(self.s3_path, self.temp_dir.name)
        content_str = file_to_str(local_path)
        self.assertEqual(self.content_str, content_str)

        wrong_path = 's3://wrongpath/x.txt'
        with self.assertRaises(NotWritableError):
            upload_or_copy(local_path, wrong_path)


class TestLoadJsonConfig(unittest.TestCase):
    def setUp(self):
        self.file_name = 'config.json'
        self.temp_dir = RVConfig.get_tmp_dir()
        self.file_path = os.path.join(self.temp_dir.name, self.file_name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def write_config_file(self, config):
        file_contents = json.dumps(config)
        with open(self.file_path, 'w') as myfile:
            myfile.write(file_contents)

    def test_valid(self):
        config = {
            'taskType': 'CHIP_CLASSIFICATION',
            'chipClassificationConfig': {
                'classItems': [{
                    'id': 1,
                    'name': 'car'
                }]
            }
        }
        self.write_config_file(config)
        task = load_json_config(self.file_path, TaskConfigMsg())
        self.assertEqual(task.task_type, rv.CHIP_CLASSIFICATION)
        self.assertEqual(task.chip_classification_config.class_items[0].id, 1)
        self.assertEqual(task.chip_classification_config.class_items[0].name,
                         'car')
        self.assertEqual(len(task.chip_classification_config.class_items), 1)

    def test_bogus_field(self):
        config = {
            'taskType': 'CHIP_CLASSIFICATION',
            'chipClassificationConfig': {
                'classItems': [{
                    'id': 1,
                    'name': 'car'
                }]
            },
            'bogus_field': 0
        }

        self.write_config_file(config)
        with self.assertRaises(ProtobufParseException):
            load_json_config(self.file_path, TaskConfigMsg())

    def test_bogus_value(self):
        config = {'task': 'bogus_value'}
        self.write_config_file(config)
        with self.assertRaises(ProtobufParseException):
            load_json_config(self.file_path, TaskConfigMsg())

    def test_invalid_json(self):
        invalid_json_str = '''
            {
                "taskType": "CHIP_CLASSIFICATION
            }
        '''
        with open(self.file_path, 'w') as myfile:
            myfile.write(invalid_json_str)

        with self.assertRaises(ProtobufParseException):
            load_json_config(self.file_path, TaskConfigMsg())


class TestS3Misc(unittest.TestCase):
    def setUp(self):
        self.lorem = LOREM

        # Mock S3 bucket
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        self.s3 = boto3.client('s3')
        self.bucket_name = 'mock_bucket'
        self.s3.create_bucket(Bucket=self.bucket_name)

        # Temporary directory
        self.temp_dir = RVConfig.get_tmp_dir()

    def tearDown(self):
        self.temp_dir.cleanup()
        self.mock_s3.stop()

    def test_last_modified_s3(self):
        path = os.path.join(self.temp_dir.name, 'lorem', 'ipsum1.txt')
        s3_path = 's3://{}/lorem1.txt'.format(self.bucket_name)
        directory = os.path.dirname(path)
        make_dir(directory, check_empty=False)

        fs = FileSystem.get_file_system(s3_path, 'r')

        with open(path, 'w+') as file:
            file.write(self.lorem)
        upload_or_copy(path, s3_path)
        stamp = fs.last_modified(s3_path)

        self.assertTrue(isinstance(stamp, datetime.datetime))

    def test_list_paths_s3(self):
        path = os.path.join(self.temp_dir.name, 'lorem', 'ipsum.txt')
        s3_path = 's3://{}/xxx/lorem.txt'.format(self.bucket_name)
        s3_directory = 's3://{}/xxx/'.format(self.bucket_name)
        directory = os.path.dirname(path)
        make_dir(directory, check_empty=False)

        with open(path, 'w+') as file:
            file.write(self.lorem)
        upload_or_copy(path, s3_path)

        list_paths(s3_directory)
        self.assertEqual(len(list_paths(s3_directory)), 1)


class TestLocalMisc(unittest.TestCase):
    def setUp(self):
        self.lorem = LOREM
        self.temp_dir = RVConfig.get_tmp_dir()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_bytes_local(self):
        path = os.path.join(self.temp_dir.name, 'lorem', 'ipsum.txt')
        directory = os.path.dirname(path)
        make_dir(directory, check_empty=False)

        expected = bytes([0x00, 0x01, 0x02])
        fs = FileSystem.get_file_system(path, 'r')

        fs.write_bytes(path, expected)
        actual = fs.read_bytes(path)

        self.assertEqual(actual, expected)

    def test_bytes_local_false(self):
        path = os.path.join(self.temp_dir.name, 'xxx')
        fs = FileSystem.get_file_system(path, 'r')
        self.assertRaises(NotReadableError, lambda: fs.read_bytes(path))

    def test_sync_from_dir_local(self):
        path = os.path.join(self.temp_dir.name, 'lorem', 'ipsum.txt')
        src = os.path.dirname(path)
        dst = os.path.join(self.temp_dir.name, 'xxx')
        make_dir(src, check_empty=False)
        make_dir(dst, check_empty=False)

        fs = FileSystem.get_file_system(path, 'r')
        fs.write_bytes(path, bytes([0x00, 0x01]))
        sync_from_dir(src, dst, delete=True)

        self.assertEqual(len(list_paths(dst)), 1)

    def test_sync_from_dir_noop_local(self):
        path = os.path.join(self.temp_dir.name, 'lorem', 'ipsum.txt')
        src = os.path.join(self.temp_dir.name, 'lorem')
        make_dir(src, check_empty=False)

        fs = FileSystem.get_file_system(src, 'r')
        fs.write_bytes(path, bytes([0x00, 0x01]))
        sync_from_dir(src, src, delete=True)

        self.assertEqual(len(list_paths(src)), 1)

    def test_sync_to_dir_local(self):
        path = os.path.join(self.temp_dir.name, 'lorem', 'ipsum.txt')
        src = os.path.dirname(path)
        dst = os.path.join(self.temp_dir.name, 'xxx')
        make_dir(src, check_empty=False)
        make_dir(dst, check_empty=False)

        fs = FileSystem.get_file_system(path, 'r')
        fs.write_bytes(path, bytes([0x00, 0x01]))
        sync_to_dir(src, dst, delete=True)

        self.assertEqual(len(list_paths(dst)), 1)

    def test_copy_to_local(self):
        path1 = os.path.join(self.temp_dir.name, 'lorem', 'ipsum.txt')
        path2 = os.path.join(self.temp_dir.name, 'yyy', 'ipsum.txt')
        dir1 = os.path.dirname(path1)
        dir2 = os.path.dirname(path2)
        make_dir(dir1, check_empty=False)
        make_dir(dir2, check_empty=False)

        with open(path1, 'w+') as file:
            file.write(self.lorem)

        upload_or_copy(path1, path2)
        self.assertEqual(len(list_paths(dir2)), 1)

    def test_last_modified(self):
        path = os.path.join(self.temp_dir.name, 'lorem', 'ipsum1.txt')
        directory = os.path.dirname(path)
        make_dir(directory, check_empty=False)

        fs = FileSystem.get_file_system(path, 'r')

        with open(path, 'w+') as file:
            file.write(self.lorem)
        stamp = fs.last_modified(path)

        self.assertTrue(isinstance(stamp, datetime.datetime))

class TestGCSMisc(unittest.TestCase):

    def test_matches_uri(self):
        fs = FileSystem.get_file_system('gs://tooadfasdf/asdfasdf')
        self.assertEqual(fs, GCSFileSystem)

    def test_bucket_dont_exist(self):

        with self.assertRaises(NotFound):
            read_str('gs://this-is-not-a-real-bucket/maybe-a-real-key')


    def setUp(self):
        self.lorem = LOREM
        self.temp_dir = RVConfig.get_tmp_dir()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_sync_to_gcs(self):
        local_path = os.path.join(self.temp_dir.name, 'lorem.txt')
        with open(local_path, 'w') as f:
            f.write(self.lorem)

        local_two = os.path.join(self.temp_dir.name, 'hello.txt')
        with open(local_two, 'w') as f:
            f.write("hello world")

        bucket = 'gs://d81fa190-ef22-413e-8ea9-22bd5416f4df'
        fs = FileSystem.get_file_system(bucket)
        print(fs)
        fs.sync_from_dir(self.temp_dir.name, bucket)
        fs.sync_to_dir(bucket, self.temp_dir.name)

    def test_file_exists_gcs_true(self):
        """"""
        gcs_path = 'gs://d81fa190-ef22-413e-8ea9-22bd5416f4df/analytics-ready/pansharpened/DIM_PHR1A_PMS_201810111620245_ORT_3373346101-001.tif'
        self.assertTrue(file_exists(gcs_path))

    def test_file_exists_gcs_false(self):
        """"""
        gcs_path = 'gs://f22-413e-8ea9-22bd5416f4df/analytics-ready/pansharpened/DIM_PHR1A_PMS_201810111620245_ORT_3373346101-001.tif'
        self.assertFalse(file_exists(gcs_path))

    def test_last_modified_gcs(self):
        gcs_path = 'gs://d81fa190-ef22-413e-8ea9-22bd5416f4df/analytics-ready/pansharpened/DIM_PHR1A_PMS_201810111620245_ORT_3373346101-001.tif'
        fs = FileSystem.get_file_system(gcs_path, 'r')
        print(fs)
        mod_date = fs.last_modified(gcs_path)
        print(mod_date)
        self.assertEqual(fs.last_modified(gcs_path), mod_date)

    def test_list_paths_gcs(self):
        gcs_path = 'gs://d81fa190-ef22-413e-8ea9-22bd5416f4df/'
        paths = list_paths(gcs_path)
        print('---', paths)
        self.assertTrue(paths)

class TestHttpMisc(unittest.TestCase):
    def setUp(self):
        self.lorem = LOREM
        self.temp_dir = RVConfig.get_tmp_dir()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_file_exists_http_true(self):
        http_path = ('https://raw.githubusercontent.com/tensorflow/models/'
                     '17fa52864bfc7a7444a8b921d8a8eb1669e14ebd/README.md')
        self.assertTrue(file_exists(http_path))

    def test_file_exists_http_false(self):
        http_path = ('https://raw.githubusercontent.com/tensorflow/models/'
                     '17fa52864bfc7a7444a8b921d8a8eb1669e14ebd/XXX')
        self.assertFalse(file_exists(http_path))

    def test_write_str_http(self):
        self.assertRaises(NotWritableError,
                          lambda: str_to_file('xxx', 'http://localhost/'))

    def test_sync_to_http(self):
        src = self.temp_dir.name
        dst = 'http://localhost/'
        self.assertRaises(NotWritableError, lambda: sync_to_dir(src, dst))

    def test_sync_from_http(self):
        src = 'http://localhost/'
        dst = self.temp_dir.name
        self.assertRaises(NotReadableError, lambda: sync_from_dir(src, dst))

    def test_copy_to_http(self):
        path = os.path.join(self.temp_dir.name, 'lorem', 'ipsum.txt')
        dst = 'http://localhost/'
        directory = os.path.dirname(path)
        make_dir(directory, check_empty=False)

        with open(path, 'w+') as file:
            file.write(self.lorem)

        self.assertRaises(NotWritableError, lambda: upload_or_copy(path, dst))
        os.remove(path)

    def test_copy_from_http(self):
        http_path = ('https://raw.githubusercontent.com/tensorflow/models/'
                     '17fa52864bfc7a7444a8b921d8a8eb1669e14ebd/README.md')
        expected = os.path.join(
            self.temp_dir.name, 'http', 'raw.githubusercontent.com',
            'tensorflow/models',
            '17fa52864bfc7a7444a8b921d8a8eb1669e14ebd/README.md')
        download_if_needed(http_path, self.temp_dir.name)

        self.assertTrue(file_exists(expected))
        os.remove(expected)

    def test_last_modified_http(self):
        uri = 'http://localhost/'
        fs = FileSystem.get_file_system(uri, 'r')
        self.assertEqual(fs.last_modified(uri), None)

    def test_write_bytes_http(self):
        uri = 'http://localhost/'
        fs = FileSystem.get_file_system(uri, 'r')
        self.assertRaises(NotWritableError,
                          lambda: fs.write_bytes(uri, bytes([0x00, 0x01])))


if __name__ == '__main__':
    unittest.main()
