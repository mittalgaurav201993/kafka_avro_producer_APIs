from pydantic import BaseModel
from typing import Optional


class RecordBasicModel(BaseModel):
    ssn: Optional[str]
    name: Optional[str]
    dob: str


class RecordModel(BaseModel):
    ssn: str
    dob: str
    name: str
    citizenship_level: Optional[str]
    address: Optional[str]
    pancard: Optional[str]


class Order(BaseModel):
    id: int
    customer_id: int
    product_id: int
    product_qty: int
    created_ms: int
    validity: bool


class Product(BaseModel):
    id: int
    name: str
    price: int


class Customer(BaseModel):
    id: int
    name: str


class Revenue(BaseModel):
    cust_id: int
    cust_name: str
    order_amount_total: int
