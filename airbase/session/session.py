from __future__ import absolute_import

import json
import sys

try:
    from requests import codes
    from requests import Session as _Session
    from requests.adapters import HTTPAdapter
    from requests.exceptions import ConnectionError, Timeout

    SUCCESS_CODES = (
        codes.ok,
        codes.created,
        codes.accepted,
        codes.partial_content,
    )

except AttributeError:
    raise AttributeError(
        "Stack frames are disabled, please enable stack frames.\
        If in pyRevit, place the following at the top of your file: \
        '__fullframeengine__ = True' and reload pyRevit."
    )

try:
    from System.Net import (
        SecurityProtocolType,
        ServicePointManager,
        WebRequest,
    )
    from System.IO import File, StreamReader
    from System.Text.Encoding import UTF8

    # from System.Threading import Tasks

    ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12

    SUCCESS_CODES = ("OK", "Created", "Accepted", "Partial Content")
except ImportError:
    pass

from ..utils import Logger  # noqa: E402

logger = Logger.start(__name__)


class Request(object):
    def __init__(self, request, stream=False, message=""):
        self.response = request
        self.stream = stream
        self.message = message

    @property
    def data(self):
        if not getattr(self, "_data", None):
            if sys.implementation.name == "ironpython":
                pass
            else:  # if sys.implementation.name == "cpython"
                # if response is a json object
                try:
                    self._data = self.response.json()
                # else if raw data
                except json.decoder.JSONDecodeError:
                    self._data = self.response.content
        return self._data

    @property
    def success(self):
        if not getattr(self, "_success", None):
            if sys.implementation.name == "ironpython":
                pass
            else:  # if sys.implementation.name == "cpython"
                self._success = self.response.status_code in SUCCESS_CODES

        if not self._success:
            self._log_error()

        return self._success

    def _log_error(self):
        if sys.implementation.name == "ironpython":
            error_msg = ""
        else:  # if sys.implementation.name == "cpython"
            try:
                response_error = (
                    json.loads(self.response.text).get("error")
                    or json.loads(self.response.text).get("errors")
                    or self.response.json().get("message")
                )
            except json.decoder.JSONDecodeError:
                response_error = self.response.text
            if response_error:
                if isinstance(response_error, list):
                    error_msg = ", ".join(
                        [error["detail"] for error in response_error]
                    )
                elif isinstance(response_error, dict):
                    error_msg = response_error.get(
                        "message"
                    ) or response_error.get("type")
                else:
                    error_msg = str(response_error)
            else:
                error_msg = self.response.status_code

        logger.warning(
            "Failed to {} - ERROR: {}".format(self.message, error_msg)
        )


class Session(object):
    def __init__(self, timeout=2, max_retries=3, base_url=None):
        """
        Kwargs:
            timeout (``int``, default=2): maximum time for one request in minutes.
            max_retries (``int``, default=3): maximum number of retries.
            base_url (``str``, optional): Base URL for this Session
        """  # noqa:E501
        try:
            self.session = _Session()
            if base_url:
                adapter = HTTPAdapter(max_retries=max_retries)
                self.session.mount(base_url, adapter)
            self.timeout = int(timeout * 60)  # in secs
            self.success_codes = (codes.ok, codes.created, codes.accepted)
        except Exception:
            self.session = None
            self.timeout = int(timeout * 60 * 1000)  # in ms
            self.success_codes = ("OK", "Created", "Accepted")

    @staticmethod
    def _add_url_params(url, params):
        """
        Appends an encoded dict as url parameters to the call API url
        Args:
            url (``str``): uri for API call.
            params (``dict``): dictionary of request uri parameters.
        Returns:
            url (``str``): url with params
        """
        url_params = ""
        count = 0
        for key, value in params.items():
            if count == 0:
                url_params += "?"
            else:
                url_params += "&"
            url_params += key + "="
            url_params += str(params[key])
            count += 1
        return url + url_params

    @staticmethod
    def _url_encode(data):
        """
        Encodes a dict into a url encoded string.
        Args:
            data (``dict``): source data
        Returns:
            urlencode (``str``): url encoded string
        """
        urlencode = ""
        count = len(data)
        for key, value in data.items():
            urlencode += key + "=" + str(value)
            if count != 1:
                urlencode += "&"
            count -= 1
        return urlencode

    def _request_cpython(self, *args, **kwargs):
        method, url = args
        headers = kwargs.get("headers")
        params = kwargs.get("params")
        json_data = kwargs.get("json_data")
        byte_data = kwargs.get("byte_data")
        urlencode = kwargs.get("urlencode")
        filepath = kwargs.get("filepath")
        stream = kwargs.get("stream")

        try:
            if headers:
                self.session.headers = headers

            # get file contents as bytes
            if filepath:
                with open(filepath, "rb") as fp:
                    data = fp.read()
            # else raw bytes
            elif byte_data:
                data = byte_data
            # else urlencode
            elif urlencode:
                data = urlencode
            else:
                data = None

            return self.session.request(
                method.lower(),
                url,
                params=params,
                json=json_data,
                data=data,
                timeout=self.timeout,
                stream=stream,
            )

        except (ConnectionError, Timeout) as e:
            raise e

    def _request_ironython(self, *args, **kwargs):
        method, url = args
        headers = kwargs.get("headers")
        params = kwargs.get("params")
        json_data = kwargs.get("json_data")
        byte_data = kwargs.get("byte_data")
        urlencode = kwargs.get("urlencode")
        filepath = kwargs.get("filepath")

        try:
            # prepare params
            if params:
                url = self._add_url_params(url, params)

            web_request = WebRequest.Create(url)
            web_request.Method = method.upper()
            web_request.Timeout = self.timeout

            # prepare headers
            if headers:
                for key, value in headers.items():
                    if key == "Content-Type":
                        web_request.ContentType = value
                    elif key == "Content-Length":
                        web_request.ContentLength = value
                    else:
                        web_request.Headers.Add(key, value)

            byte_arrays = []
            if json_data:
                byte_arrays.append(
                    UTF8.GetBytes(json.dumps(json_data, ensure_ascii=False))
                )
            if filepath:
                byte_arrays.append(File.ReadAllBytes(filepath))
            if byte_data:
                pass
                # TODO - Add byte input for System.Net
            if urlencode:
                byte_arrays.append(UTF8.GetBytes(self._url_encode(urlencode)))

            for byte_array in byte_arrays:
                web_request.ContentLength = byte_array.Length
                with web_request.GetRequestStream() as req_stream:
                    req_stream.Write(byte_array, 0, byte_array.Length)
            try:
                with web_request.GetResponse() as response:
                    success = response.StatusDescription in SUCCESS_CODES

                    with response.GetResponseStream() as response_stream:
                        with StreamReader(response_stream) as stream_reader:
                            data = json.loads(stream_reader.ReadToEnd())
            except SystemError:
                return None, None
            finally:
                web_request.Abort()

        except Exception as e:
            raise e

        return data, success

    def request(
        self,
        method,
        url,
        headers=None,
        params=None,
        json_data=None,
        byte_data=None,
        urlencode=None,
        filepath=None,
        stream=False,
        message="",
    ):
        """
        Request wrapper for cpython and ironpython.
        Args:
            method (``str``): api method.
            url (``str``): uri for API call.
        Kwargs:
            headers (``dict``, optional): dictionary of request headers.
            params (``dict``, optional): dictionary of request uri parameters.
            json_data (``json``, optional): request body if Content-Type is json.
            urlencode (``dict``, optional): request body if Content-Type is urlencoded.
            filepath (``str``, optional): filepath of object to upload.
            stream (``bool``, default=False) whether to sream content of not
            message (``str``, optional): filepath of object to upload.

        Returns:
            data (``json``): Body of response.
            success (``bool``): True if response returned a accepted, created or ok status code.
        """  # noqa:E501
        if sys.implementation.name == "ironpython":
            return self._request_ironpython(
                method,
                url,
                headers=headers,
                params=params,
                json_data=json_data,
                byte_data=byte_data,
                urlencode=urlencode,
                filepath=filepath,
                stream=stream,
            )
        else:  # if sys.implementation.name == "cpython"
            response = Request(
                self._request_cpython(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json_data=json_data,
                    byte_data=byte_data,
                    urlencode=urlencode,
                    filepath=filepath,
                    stream=stream,
                ),
                message=message,
            )
            return response.data, response.success


if __name__ == "__main__":
    pass
