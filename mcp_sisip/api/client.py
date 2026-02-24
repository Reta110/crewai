import requests
from typing import Any, Dict, Optional
from mcp_sisip.config import get_config

class APIClient:
    def __init__(self):
        config = get_config()["api"]
        self.base_url = config["base_url"].rstrip("/")
        self.token = config["token"]
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def _handle_response(self, response: requests.Response) -> Dict[str, Any]:
        """Handles response and errors uniformly."""
        try:
            data = response.json()
        except Exception:
            data = {"message": response.text}

        if not response.ok:
            error_msg = data.get("message", "Unknown API Error")
            if "error" in data:
                error_msg = data["error"]
            raise Exception(f"API Error ({response.status_code}): {error_msg}")
        return data

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = requests.get(url, headers=self.headers, params=params)
        return self._handle_response(response)

    def post(self, endpoint: str, data: Dict) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = requests.post(url, headers=self.headers, json=data)
        return self._handle_response(response)

    def put(self, endpoint: str, data: Dict) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = requests.put(url, headers=self.headers, json=data)
        return self._handle_response(response)

    def delete(self, endpoint: str) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = requests.delete(url, headers=self.headers)
        return self._handle_response(response)
