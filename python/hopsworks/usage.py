import functools
import logging
import traceback
import requests
import platform
import json
import os
import time
import uuid
import hashlib
import random

from os.path import expanduser, join
from datetime import datetime
from hopsworks.core import variable_api
from hsfs.core import variable_api as hsfs_variable_api

class EnvironmentAttribute:

    def __init__(self):
        self._platform = None
        self._hsml_version = None
        self._hsfs_version = None
        self._hopsworks_version = None
        self._python_version = None
        self._user_id = None
        self._backend_version = None
        self._timezone = None

    def _get_lib_version(self, lib_name):
        try:
            lib = __import__(lib_name)
            return lib.__version__
        except ImportError:
            return ""

    def get_hsml_version(self):
        if self._hsml_version is None:
            self._hsml_version = self._get_lib_version("hsml")
        return self._hsml_version

    def get_hsfs_version(self):
        if self._hsfs_version is None:
            self._hsfs_version = self._get_lib_version("hsfs")
        return self._hsfs_version

    def get_hopsworks_version(self):
        if self._hopsworks_version is None:
            self._hopsworks_version = self._get_lib_version("hopsworks")
        return self._hopsworks_version

    def get_python_version(self):
        if not self._python_version:
            self._python_version = platform.python_version()
        return self._python_version

    def get_user_id(self):
        if not self._user_id:
            hopsworks_dir = _create_hopsworks_dir_if_not_exist()
            user_id_file = join(hopsworks_dir, _USER_ID_FILE)
            if os.path.exists(user_id_file):
                with open(user_id_file, "r") as fr:
                    self._user_id = fr.read().rstrip()
            else:
                with open(user_id_file, "w") as fw:
                    self._user_id = _generate_user_id()
                    fw.write(self._user_id)
        return self._user_id

    def get_platform(self):
        if not self._platform:
            self._platform = platform.platform()
        return self._platform

    def get_backend_host_name(self):
        return _backend_hostname

    def get_backend_version(self):
        if self._backend_version is None:
            try:
                self._backend_version = variable_api.VariableApi().get_version("hopsworks")
                return self._backend_version
            except Exception:
                pass
            try:
                self._backend_version = hsfs_variable_api.VariableApi().get_version("hopsworks")
                return self._backend_version
            except Exception:
                # Do not set empty because users can be temporarily offline.
                pass
        return self._backend_version

    def get_timezone(self):
        if self._timezone is None:
            self._timezone = datetime.now().astimezone().tzinfo
        return self._timezone


class MethodCounter:
    def __init__(self):
        self.method_counts = {}
        random.seed(42)

    def add(self, m):
        s = self._get_method_name(m)
        self.method_counts[s] = self.method_counts.get(s, 0) + 1

    def get_count(self, m):
        s = self._get_method_name(m)
        return self.method_counts.get(s, 0)

    def should_sample(self, m):
        cnt = self.get_count(m)
        if cnt < 100:
            return True
        elif cnt < 1000:
            return random.random() < 0.1
        elif cnt < 10000:
            return random.random() < 0.01
        else:
            return random.random() < 0.001

    def _get_method_name(self, m):
        return m.__module__ + m.__name__


_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)
_env_attr = EnvironmentAttribute()
_method_counter = MethodCounter()
_backend_hostname = None
HOPSWORKS_DIR = join(expanduser("~"), ".hopsworks")
_USER_ID_FILE = "user_id"
_is_enabled = (
    os.getenv("ENABLE_HOPSWORKS_USAGE", default="true").lower() == "true"
)


def init_usage(hostname):
    global _backend_hostname, _env_attr, _method_counter
    _backend_hostname = hostname
    _env_attr = EnvironmentAttribute()
    _method_counter = MethodCounter()


def _hash_string(input_string):
    if input_string:
        hash_object = hashlib.md5()
        hash_object.update(input_string.encode('utf-8'))
        hashed_string = hash_object.hexdigest()
        return hashed_string
    else:
        ""


def _create_hopsworks_dir_if_not_exist():
    if not os.path.exists(HOPSWORKS_DIR):
        os.makedirs(HOPSWORKS_DIR)
    return HOPSWORKS_DIR


def _generate_user_id():
    random_uuid = uuid.uuid4()
    # Convert the UUID to a hexadecimal string and remove hyphens
    uuid_hex = random_uuid.hex.replace('-', '')
    short_uuid = uuid_hex[:16]
    return short_uuid


def method_logger(func):
    if not _is_enabled:
        return func

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        exception = None
        try:
            # Call the original method
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            exception = str(e)
            raise e
        finally:
            try:
                end_time = time.time()
                execution_time = end_time - start_time
                _method_counter.add(func)
                # Send log to REST API server
                log_data = {
                    # env
                    "user_id": _env_attr.get_user_id(),
                    "datetime": datetime.now(_env_attr.get_timezone()).strftime(
                        '%Y-%m-%d %H:%M:%S %Z'
                    ),
                    "backend_hostname": _hash_string(
                        _env_attr.get_backend_host_name()
                    ),
                    "backend_version": _env_attr.get_backend_version(),
                    "platform": _env_attr.get_platform(),
                    "python_version": _env_attr.get_python_version(),
                    "hsml_version": _env_attr.get_hsml_version(),
                    "hsfs_version": _env_attr.get_hsfs_version(),
                    "hopsworks_version": _env_attr.get_hopsworks_version(),
                    # method
                    "method_name": func.__name__,
                    "module_name": func.__module__,
                    "arguments": f"args: {args}; kwargs: {kwargs}",
                    "execution_time": int(execution_time * 1000),
                    "num_call": _method_counter.get_count(func),
                    # error
                    "last_error": exception if exception else None,
                    "stack_trace": traceback.format_exc() if exception else None,
                }
                if exception or _method_counter.should_sample(func):
                    send_log_to_api(log_data)
            except Exception as e:
                # pass
                raise e

    return wrapper


def send_log_to_api(log_data):
    api_url = "https://a2816e8b28.execute-api.us-east-2.amazonaws.com/hopsworks/usage"
    headers = {'Content-type': 'application/json'}
    data = json.dumps({
            "Data": log_data})
    _logger.debug(f"data: {data}")
    response = requests.post(
        api_url,
        data=data,
        headers=headers
    )
    _logger.debug(str(response.content))