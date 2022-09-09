from typing import Dict

from api.adapter.dynamodb_adapter import DynamoDBAdapter


class JobService:
    def __init__(self, db_adapter=DynamoDBAdapter()):
        self.db_adapter = db_adapter

    def get_all_jobs(self) -> list[Dict]:
        return self.db_adapter.get_jobs()
