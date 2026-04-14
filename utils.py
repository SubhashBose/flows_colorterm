###########  FLOWSql API client Python code  block to copy and paste ###########
import requests

class FLOWSql:
    def __init__(self, api_key: str,
                    url: str = "https://flows.phys.au.dk/api/sqlquery.php",
                    timeout: int = 30):
        """
        Initialize FLOWSql API client.

        Parameters
        ----------
        api_key : str
            API token for FLOWSql.
        url : str
            FLOWSql endpoint URL.
        timeout : int
            Request timeout in seconds.
        """
        self.api_key = api_key
        self.url = url
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}"
        })

    def query(self, sql: str, admin: bool = False) -> dict:
        """
        Execute an SQL query.

        Returns
        -------
        dict
            Select queries JSON format:
                {
                    "type": "select",
                    "columns": [...],
                    "rows": [...],
                    "count": int
                }

            Write queries (DELETE, UPDATE etc.) JSON format:
                {
                    "type": "write",
                    "affected_rows": int
                }
        """

        try:
            response = self.session.post(
                self.url,
                data={"query": sql, "admin": admin},
                timeout=self.timeout
            )
            response.raise_for_status()
        except requests.RequestException as e:
            raise FLOWSqlError(f"HTTP error: {e}") from e

        try:
            data = response.json()
        except ValueError:
            raise FLOWSqlError(f"Invalid JSON response: {response.text}")

        if not data.get("success"):
            raise FLOWSqlError(data.get("error", "Unknown API error"))

        if data["type"] == "select":
            return {
                "type": "select",
                "columns": data.get("columns", []),
                "rows": data.get("rows", []),
                "count": data.get("count", 0)
            }

        if data["type"] == "write":
            return {
                "type": "write",
                "affected_rows": data.get("affected_rows", 0)
            }

        raise FLOWSqlError(f"Unknown response format: {data}")


    def close(self):
        """Close HTTP session."""
        self.session.close()

class FLOWSqlError(Exception):
    """Raised when the SQL API returns an error."""
    pass

################# END FLOWSql API client Python code block  ###########


class IncrementalStruct:
    def __init__(self, mergelist=False):
        self.mergelist = mergelist
        self.result = {}

    @staticmethod
    def _deep_merge(dest, src, mergelist):
        for key, value in src.items():
            if key in dest:
                # If both are dicts → recurse
                if isinstance(dest[key], dict) and isinstance(value, dict):
                    IncrementalStruct._deep_merge(dest[key], value, mergelist)

                # If both are lists → extend (optional behavior)
                elif mergelist and isinstance(dest[key], list) and isinstance(value, list):
                    dest[key].extend(value)

                # Otherwise → overwrite
                else:
                    dest[key] = value
            else:
                dest[key] = value
        return dest

    def merge(self, data):
        self.result=self._deep_merge(self.result,data,self.mergelist)
        return self

    def loadJsons(self, file):
        import json
        with open(file) as f:
            for line in f:
                self.merge(json.loads(line))
        return self

    def __call__(self):
        return self.result