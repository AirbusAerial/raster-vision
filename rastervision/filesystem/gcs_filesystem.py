
import os
import shutil
import urllib
import urllib.request
from datetime import datetime
import json
from rastervision.filesystem import (FileSystem, NotReadableError,
                                     NotWritableError)
from urllib.parse import urlparse
import subprocess
from google.api_core.exceptions import NotFound

class GCSFileSystem(FileSystem):

    # QUESTION: why not just have this be a variable of the class

    # NOTE: this from when i was thinking of using apache-libcloud
    # @staticmethod
    # def get_session():
    #     # Lazily load boto
    #     from libcloud.storage.types import Provider
    #     from libcloud.storage.providers import get_driver
    #     cls = get_driver(Provider.GOOGLE_STORAGE)
    #     credential_json_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    #     with open(credential_json_path) as f:
    #         creds = json.load(f)
    #     key = creds['client_email']
    #     secret = creds['private_key']
    #     sess = cls(key, secret)
    #     return sess

    @staticmethod
    def get_session():
        """"""
        from google.cloud import storage
        client = storage.Client()
        return client

    @staticmethod
    def get_bucket(bucket_name):
        """"""
        client = GCSFileSystem.get_session()
        return client.get_bucket(bucket_name)

    @staticmethod
    def matches_uri(uri: str, mode: str) -> bool:
        parsed_uri = urlparse(uri)
        return parsed_uri.scheme == 'gs'

    @staticmethod
    def file_exists(uri: str) -> bool:
        parsed_uri = urlparse(uri)
        client = GCSFileSystem.get_session()
        try:
            bucket = client.get_bucket(parsed_uri.netloc)
        except NotFound:
            return False
        return bool(bucket.get_blob(parsed_uri.path[1:]))

    @staticmethod
    def read_str(uri: str) -> str:
        return GCSFileSystem.read_bytes(uri).decode('utf-8')

    @staticmethod
    def read_bytes(uri: str) -> bytes:
        parsed_uri = urlparse(uri)
        client = GCSFileSystem.get_session()
        try:
            bucket = client.get_bucket(parsed_uri.netloc)
        except NotFound:
            return ''
        blob = bucket.get_blob(parsed_uri.path[1:])
        return blob.download_as_string()

    @staticmethod
    def write_str(uri: str, data: str) -> None:
        data = bytes(data, encoding='utf-8')
        GCSFileSystem.write_bytes(uri, data)

    @staticmethod
    def write_bytes(uri: str, data: bytes) -> None:
        parsed_uri = urlparse(uri)
        client = GCSFileSystem.get_session()
        bucket = client.get_bucket(parsed_uri.netloc)
        blob = bucket.blob(parsed_uri.path[1:])
        blob.upload_from_string(data)

    # NOTE: i wrote these as a fallback is gsutil is not installed
    @staticmethod
    def sync_to_dir_without_gsutil(src_dir_uri: str, dest_dir_uri: str,
                    delete: bool = False) -> None:
        if delete:
            shutil.rmtree(dest_dir_uri)

        parsed_uri = urlparse(src_dir_uri)
        client = GCSFileSystem.get_session()
        bucket = client.get_bucket(parsed_uri.netloc)
        objects = bucket.list_blobs(parsed_uri.path[1:])
        for object in objects:
            dst = os.path.join(dest_dir_uri, object.name)
            dst_folder = os.path.split(dst)
            os.makedirs(dst_folder)
            object.download_to_filename(dst)


    @staticmethod
    def sync_to_dir(src_dir_uri: str, dest_dir_uri: str,
                    delete: bool = False) -> None:
        command = ['gsutil', '-m', 'rsync', src_dir_uri, dest_dir_uri]
        if delete:
            command.append('-d')
        subprocess.run(command)


    @staticmethod
    def sync_from_dir(src_dir_uri: str, dest_dir_uri: str,
                    delete: bool = False) -> None:
        command = ['gsutil', '-m', 'rsync', src_dir_uri, dest_dir_uri]
        if delete:
            command.append('-d')

        subprocess.run(command)

    # NOTE: i wrote these as a fallback is gsutil is not installed
    @staticmethod
    def sync_from_dir_without_gsutil(src_dir_uri: str,
                      dest_dir_uri: str,
                      delete: bool = False) -> None:
        """"""
        parsed_uri = urlparse(dest_dir_uri)
        client = GCSFileSystem.get_session()
        bucket = client.get_bucket(parsed_uri.netloc)
        if delete:
            objects = bucket.list_blobs(parsed_uri.path[1:])
            for object in objects:
                object.delete()

        length_root = len(src_dir_uri)
        for a, b, c in os.walk(src_dir_uri):
            for x in c:
                src = os.path.join(a, c)
                dst_key = src[length_root:]
                dst = bucket.blob(dst_key)
                dst.upload_from_filename(src)

    @staticmethod
    def copy_to(src_path: str, dst_uri: str) -> None:
        parsed_uri = urlparse(dst_uri)
        client = GCSFileSystem.get_session()
        bucket = client.get_bucket(parsed_uri.netloc)
        blob = bucket.blob(parsed_uri.path[1:])
        blob.upload_from_filename(src_path)

    @staticmethod
    def copy_from(uri: str, path: str) -> None:
        parsed_uri = urlparse(uri)
        client = GCSFileSystem.get_session()
        bucket = client.get_bucket(parsed_uri.netloc)
        blob = bucket.get_blob(parsed_uri.path[1:])
        print(blob)
        assert blob, "{uri} does not exist".format(uri=uri)
        blob.download_to_filename(path)

    @staticmethod
    def local_path(uri: str, download_dir: str) -> None:
        parsed_uri = urlparse(uri)
        path = os.path.join(download_dir, 'gs', parsed_uri.netloc, parsed_uri.path[1:])
        return path

    @staticmethod
    def last_modified(uri: str) -> datetime:
        parsed_uri = urlparse(uri)
        client = GCSFileSystem.get_session()
        bucket = client.get_bucket(parsed_uri.netloc)
        blob = bucket.get_blob(parsed_uri.path[1:])
        return blob.updated

    @staticmethod
    def list_paths(uri, ext=None):
        parsed_uri = urlparse(uri)
        client = GCSFileSystem.get_session()
        bucket = client.get_bucket(parsed_uri.netloc)
        items = bucket.list_blobs(prefix=parsed_uri.path[1:])
        for item in items:
            if ext and os.path.splitext(item)[-1] != ext:
                continue
            return os.path.join('gs://', parsed_uri.netloc, item.name)

def register_plugin(plugin_registry):
    plugin_registry.register_filesystem(GCSFileSystem)
