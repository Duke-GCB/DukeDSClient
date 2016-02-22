"""DataServiceApi - communicates with to Duke Data Service REST API."""
import json
import requests


class DataServiceApi(object):
    """
    Sends json messages and receives responses back from Duke Data Service api.
    See https://github.com/Duke-Translational-Bioinformatics/duke-data-service.
    Should be eventually replaced by https://github.com/Duke-Translational-Bioinformatics/duke-data-service-pythonClient.
    """
    def __init__(self, auth, url, http=requests):
        self.auth = auth
        self.base_url = url
        self.bytes_per_chunk = 5242880
        self.http = http

    def _url_parts(self, url_suffix, url_data):
        url = self.base_url + url_suffix
        data_str = json.dumps(url_data)
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self.auth
        }
        return url, data_str, headers

    def _post(self, url_suffix, post_data):
        (url, data_str, headers) = self._url_parts(url_suffix, post_data)
        resp = self.http.post(url, data_str, headers=headers)
        return self._check_err(resp, url_suffix, post_data)

    def _put(self, url_suffix, put_data):
        (url, data_str, headers) = self._url_parts(url_suffix, put_data)
        resp = self.http.put(url, data_str, headers=headers)
        return self._check_err(resp, url_suffix, put_data)

    def _get(self, url_suffix, get_data):
        (url, data_str, headers) = self._url_parts(url_suffix, get_data)
        resp = self.http.get(url, headers=headers)
        return self._check_err(resp, url_suffix, get_data)

    def _check_err(self, resp, url_suffix, data):
        if resp.status_code != 200 and resp.status_code != 201:
            raise DataServiceError(resp, url_suffix, data)
        return resp

    def create_project(self, project_name, desc):
        data = {
            "name": project_name,
            "description": desc
        }
        return self._post("/projects", data)

    def get_projects(self):
        return self._get("/projects", {})

    def create_folder(self, folder_name, parent_kind_str, parent_uuid):
        data = {
            'name': folder_name,
            'parent': {
                'kind': parent_kind_str,
                'id': parent_uuid
            }
        }
        return self._post("/folders", data)

    def get_project_children(self, project_id, name_contains):
        return self._get_children('projects', project_id, name_contains)

    def get_folder_children(self, folder_id, name_contains):
        return self._get_children('folders', folder_id, name_contains)

    def _get_children(self, parent_name, parent_id, name_contains):
        data = {
            'name_contains': name_contains
        }
        return self._get("/" + parent_name + "/" + parent_id + "/children", data)

    def create_upload(self, project_id, filename, content_type, size,
            hash_value, hash_alg):
        data = {
            "name": filename,
            "content_type": content_type,
            "size": size,
            "hash": {
                "value": hash_value,
                "algorithm": hash_alg
            }
        }
        return self._post("/projects/" + project_id + "/uploads", data)

    def create_upload_url(self, upload_id, number, size, hash_value, hash_alg):
        data = {
            "number": number,
            "size": size,
            "hash": {
                "value": hash_value,
                "algorithm": hash_alg
            }
        }
        return self._put("/uploads/" + upload_id + "/chunks", data)

    def complete_upload(self, upload_id):
        return self._put("/uploads/" + upload_id + "/complete", {})

    def create_file(self, parent_kind, parent_id, upload_id):
        data = {
            "parent": {
                "kind": parent_kind,
                "id": parent_id
            },
            "upload": {
                "id": upload_id
            }
        }
        return self._post("/files/", data)

    def send_external(self, http_verb, host, url, http_headers, chunk):
        if http_verb == 'PUT':
            return requests.put(host + url, data=chunk, headers=http_headers)
        elif http_verb == 'POST':
            return requests.post(host + url, data=chunk, headers=http_headers)
        else:
            raise ValueError("Unsupported http_verb:" + http_verb)


class DataServiceError(Exception):
    def __init__(self, response, url_suffix, request_data):
        if response.status_code == 500:
            resp_json = {'reason':'Internal Server Error', 'suggestion':'Contact DDS support.'}
        else:
            resp_json = response.json()
        Exception.__init__(self,'Error {} on {} Reason:{} Suggestion:{}'.format(
            response.status_code, url_suffix, resp_json.get('reason',''), resp_json.get('suggestion','')
        ))
        self.response = resp_json
        self.url_suffix = url_suffix
        self.request_data = request_data