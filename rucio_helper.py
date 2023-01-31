import os
import six
import requests
import six
import json
from requests.exceptions import HTTPError
import subprocess
import re


def get_account_rules(client, account, dbs_regex):
    sample_names = []
    transfered_samples = []
    request_details = client.list_account_rules(account=account)
    for i, request in enumerate(request_details):
        # check is state is OK, and if the sample name matches the regex
        if request["state"] == "OK" and re.search(dbs_regex, request["name"]):
            transfered_samples.append(request)
            if request["name"] in sample_names:
                print("Duplicate sample name found: ", request["name"])
            sample_names.append(request["name"])
        else:
            print(request["name"], request["state"])
    print(f"Successfully transfered samples: {len(transfered_samples)} / {6*(18)}")
    return transfered_samples


def get_block_files(client, block, scope):
    files = client.list_files(scope=scope, name=block)
    return list(files)


def get_rucio_blocks(client, scope, account, samplename):
    # in rucio, different naming is used:
    # containers -> datasets
    # datasets -> blocks
    # get the full list of available blocks
    results = client.list_dids(
        scope=scope, filters={"account": account}, did_type="dataset", long=True
    )
    # now filter the list so only matching blocks are returned
    results = [block for block in results if re.search(samplename, block["name"])]
    slim_results = []
    for block in list(results):
        files = get_block_files(client, block["name"], scope)
        slim_results.append({"blockname": block["name"], "files": files})
    return slim_results


# Collection of functions taken from
# https://gitlab.etp.kit.edu/GridKa/CMS-VoBoxTools/-/tree/rucio/software/toolKIT


def setup_rucio_account(cert=None):
    if "RUCIO_ACCOUNT" not in os.environ:
        if not cert:
            cert = os.environ["X509_USER_PROXY"]
        dn = subprocess.check_output(["grid-proxy-info", "-identity"]).strip()
        dn = dn.decode("utf-8")

        url = "http://cms-cric.cern.ch/api/accounts/user/query/?json&preset=people"
        result = readJSON(url, params=None, cert=cert, method="GET")
        for person in result["result"]:
            if person[4] == dn:
                os.environ["RUCIO_ACCOUNT"] = person[0]
                break

    # warn if account not found
    if "RUCIO_ACCOUNT" not in os.environ:
        print(
            "Warning! Could not identify correct RUCIO_ACCOUNT: no person mapped "
            "to certificate DN ({dn}). Requests to Rucio API will likely fail.".format(
                dn=dn
            )
        )
    else:
        print("Using rucio account %s" % os.environ["RUCIO_ACCOUNT"])


def expath(fn):
    return os.path.realpath(os.path.expanduser(fn))


def user_agent(value):
    user_agent.value = value


user_agent.value = "toolKIT/0.1"


def removeUnicode(obj):
    # no need for special unicode handling in Python 3
    if six.PY3:
        return obj
    if type(obj) in (list, tuple, set):
        (obj, oldType) = (list(obj), type(obj))
        for i, v in enumerate(obj):
            obj[i] = removeUnicode(v)
        obj = oldType(obj)
    elif isinstance(obj, dict):
        result = {}
        for k, v in six.iteritems(obj):
            result[removeUnicode(k)] = removeUnicode(v)
        return result
    # use `six.text_type` instead of `unicode` to avoid
    # triggering PY2/3 compatibility checkers
    elif isinstance(obj, six.text_type):
        return obj.encode("utf-8")
    return obj


def readURL(url, params=None, headers={}, cert=None, method="GET"):
    headers.setdefault("User-Agent", user_agent.value)

    rest_client = RESTClient(cert=cert, default_headers=headers)

    print("Starting http query: %r %r" % (url, params))
    print("Connecting with header: %r" % headers)

    try:
        if method in ("POST", "PUT"):
            return getattr(rest_client, method.lower())(url=url, data=params)
        else:
            return getattr(rest_client, method.lower())(url=url, params=params)
    except:
        print("Unable to open", url, "with arguments", params, "and header", headers)
        raise


def parseJSON(data):
    # from .vendor import simplejson as json
    return removeUnicode(json.loads(data))


def readJSON(url, params=None, headers={}, cert=None, method="GET"):
    return parseJSON(readURL(url, params, headers, cert, method))


class RESTClient(object):
    def __init__(
        self, cert=None, default_headers=None, result_process_func=lambda x: x
    ):
        self._session = RequestSession(cert=cert, default_headers=default_headers)
        self._result_process_func = result_process_func

    def _request(self, request_method, url, api, headers, **kwargs):
        if api:
            url = "%s/%s" % (url, api)
        return self._result_process_func(
            self._session.request(request_method, url, headers=headers, **kwargs)
        )

    def get(self, url, api=None, headers=None, params=None):
        return self._request("GET", url, api=api, headers=headers, params=params)

    def post(self, url, api=None, headers=None, data=None):
        return self._request("POST", url, api=api, headers=headers, data=data)

    def put(self, url, api=None, headers=None, data=None):
        return self._request("PUT", url, api=api, headers=headers, data=data)

    def delete(self, url, api=None, headers=None, params=None):
        return self._request("DELETE", url, api=api, headers=headers, params=params)


def remove_unicode(obj):
    # no need for special unicode handling in Python 3
    if six.PY3:
        return obj
    if type(obj) in (list, tuple, set):
        (obj, oldType) = (list(obj), type(obj))
        for i, v in enumerate(obj):
            obj[i] = remove_unicode(v)
        obj = oldType(obj)
    elif isinstance(obj, dict):
        result = {}
        for k, v in six.iteritems(obj):
            result[remove_unicode(k)] = remove_unicode(v)
        return result
    # use `six.text_type` instead of `unicode` to avoid
    # triggering PY2/3 compatibility checkers
    elif isinstance(obj, six.text_type):
        return obj.encode("utf-8")
    return obj


class JSONRESTClient(RESTClient):
    def __init__(self, cert=None, default_headers=None, result_process_func=None):
        default_headers = default_headers or {}
        default_headers.update(
            {"Content-Type": "application/json", "Accept": "application/json"}
        )
        if not result_process_func:
            result_process_func = lambda result: remove_unicode(json.loads(result))
        super(JSONRESTClient, self).__init__(
            cert=cert,
            default_headers=default_headers,
            result_process_func=result_process_func,
        )


class RequestSession(object):
    _requests_client_session = None

    def __init__(self, cert=None, default_headers=None):
        self._cert, self._default_headers = cert, default_headers or {}

        # disable ssl ca verification errors
        from requests.packages.urllib3.exceptions import InsecureRequestWarning

        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

        if not self._requests_client_session:
            self._create_session()

    @classmethod
    def _create_session(cls):
        cls._requests_client_session = requests.Session()

    def _raise_for_status(self, response):
        """
        checks for status not ok and raises corresponding http error
        """
        try:
            response.raise_for_status()
        except HTTPError as http_error:
            if six.PY2:
                raise HTTPError(
                    "Server response: %s\nInfos: %s"
                    % (str(http_error).encode("utf-8"), response.text.encode("utf-8"))
                )
            elif six.PY3:
                raise HTTPError(
                    "Server response: %s\nInfos: %s" % (str(http_error), response.text)
                )
            else:
                assert False

    def request(self, request_method, url, headers, **kwargs):
        headers = (headers or {}).update(self._default_headers)
        request_func = getattr(self._requests_client_session, request_method.lower())
        response = request_func(
            url=url, verify=False, cert=self._cert, headers=headers, **kwargs
        )
        self._raise_for_status(response)
        # str() to avoid codec problems
        return str(response.text)
