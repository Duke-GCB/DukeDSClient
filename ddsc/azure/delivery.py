import requests
import requests.exceptions
import json
from ddsc.exceptions import DDSUserException
from ddsc.core.d4s2 import UNAUTHORIZED_MESSAGE

MISSING_DELIVERY_TOKEN_MSG = """
ERROR: Missing credential to deliver projects.
Please add 'delivery_token' to ~/.ddsclient config file.
"""


class DataDelivery(object):
    def __init__(self, config, api):
        self.api = api
        self.delivery_api = DeliveryApi(config)

    def deliver(self, project_path, to_netid, user_message, share_user_ids, resend):
        if not share_user_ids:
            share_user_ids = []
        payload = {
            "source_project": {
                "path": project_path,
                "container_url": self.api.get_container_url()
            },
            "from_netid": self.api.current_user_netid,
            "to_netid": to_netid,
            "user_message": user_message,
            "share_user_ids": share_user_ids
        }
        self._validate_payload(payload)
        try:
            if resend:
                delivery_id = self.delivery_api.find_incomplete_delivery(payload)
                self.delivery_api.send_delivery(delivery_id)
            else:
                if self.delivery_api.find_incomplete_delivery(payload, raise_on_not_found=False):
                    raise DDSUserException("Data Delivery Error: An active delivery for this project already exists.")
                delivery_id = self.delivery_api.create_delivery(payload)
                self.delivery_api.send_delivery(delivery_id)
            print(f"\nDelivery email message sent to {to_netid}\n")
        except requests.exceptions.ConnectionError as e:
            raise DDSUserException("Error: Failed to connect to the Data Delivery server.\n\n" + str(e))

    def _validate_payload(self, payload):
        self._ensure_user_exists(payload["to_netid"])
        if payload["to_netid"] == self.api.current_user_netid:
            raise DDSUserException("ERROR: You cannot deliver to yourself.")
        for share_user_id in payload["share_user_ids"]:
            self._ensure_user_exists(share_user_id)

    def _ensure_user_exists(self, netid):
        # Raises exception if netid isn't a valid user
        self.api.users.ensure_user_exists(netid)


class DeliveryApi(object):
    def __init__(self, config):
        if not config.delivery_token:
            raise DDSUserException(MISSING_DELIVERY_TOKEN_MSG)
        # Switch to v3 for azure endpoints (Once DDS API is deprecated switch default to v3)
        self.deliveries_url = config.azure_delivery_url + "/az-deliveries/"
        self.json_headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Token ' + config.delivery_token
        }

    def create_delivery(self, payload):
        data = json.dumps(payload)
        resp = requests.post(self.deliveries_url, headers=self.json_headers, data=data)
        self._check_response(resp)
        return resp.json()["id"]

    def find_incomplete_delivery(self, payload, raise_on_not_found=True):
        deliveries = self._find_incomplete_deliveries(payload)
        if not deliveries:
            if raise_on_not_found:
                raise DDSUserException("ERROR: No incomplete delivery found.")
            else:
                return None
        if len(deliveries) > 1:
            raise DDSUserException("ERROR: Multiple incomplete deliveries found.")
        return deliveries[0]["id"]

    def _find_incomplete_deliveries(self, payload):
        deliveries = []
        params = {
            'from_netid': payload["from_netid"],
            'to_netid': payload["to_netid"],
            'source_project__path': payload["source_project"]["path"],
            'source_project__container_url': payload["source_project"]["container_url"]
        }
        resp = requests.get(self.deliveries_url, headers=self.json_headers, params=params)
        self._check_response(resp)
        for delivery in resp.json():
            if not delivery["complete"]:
                deliveries.append(delivery)
        return deliveries

    def send_delivery(self, delivery_id):
        resp = requests.post(f"{self.deliveries_url}{delivery_id}/send/", headers=self.json_headers)
        self._check_response(resp)

    @staticmethod
    def _check_response(response):
        """
        Raises error if the response isn't successful.
        :param response: requests.Response response to be checked
        """
        if response.status_code == 401:
            raise DDSUserException(UNAUTHORIZED_MESSAGE)
        if not 200 <= response.status_code < 300:
            msg_fmt = "Request to {} failed with {}:\n{}"
            raise DDSUserException(msg_fmt.format(response.url, response.status_code, response.text))
