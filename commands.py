from pydantic import BaseModel


class NumOfRecordsCommand(BaseModel):
    records: int
