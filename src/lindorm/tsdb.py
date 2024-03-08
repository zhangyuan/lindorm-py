import requests
import json
from requests.auth import HTTPBasicAuth

class Client:
    def __init__(self, endpoint: str, username: str, password: str, database_name: str, chunk_size = 1000) -> None:
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.database_name = database_name
        self.chunk_size = chunk_size
    
    def query(self, sql):
        basic = HTTPBasicAuth(self.username, self.password)
        r = requests.post(self.endpoint, sql, auth=basic, params={
            "database": self.database_name,
            "chunked": "true",
            "chunk_size": self.chunk_size
        })
        
        if r.status_code != 200:
            raise Exception(r.content)
        
        rows = []
        columns = None
        data_types = None
        
        for line in r.iter_lines():
            json_line = json.loads(line)
            if not columns:
                columns = json_line['columns']
            if not data_types:
                data_types = json_line['metadata']
            rows.extend(json_line['rows'])
            
        return {
            "columns": columns,
            "rows": rows,
            "data_types": data_types
        }
