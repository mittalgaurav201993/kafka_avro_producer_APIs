record_value_v1 = """
{
    "namespace": "com.thecodinginterface.avrodomainevents",
    "name": "RecordModel",
    "type": "record",
    "fields": [
        {"name": "ssn",
         "type": ["null", "string"],
         "default": null},
        {"name": "name",
         "type": ["null", "string"],
         "default": null},
        {"name": "dob",
         "type": "string"}
         ]
}"""

record_value_vn = """
{
    "namespace": "com.thecodinginterface.avrodomainevents",
    "name": "RecordModel",
    "type": "record",
    "fields": [
        {"name": "ssn",
         "type": "string"},
        {"name": "name",
         "type": "string"},
        {"name": "dob",
         "type": "string"},
        {"name": "citizenship_level",
         "type": ["null", "string"],
         "default": null},
        {"name": "pancard",
         "type": ["null", "string"],
         "default": null},
        {"name": "address",
         "type": ["null", "string"],
         "default": null}
         ]
}"""

orders_schema = """
{
"namespace": "com.thecodinginterface.avrodomainevents",
"type": "record",
"name": "order_format",
"fields": [
        {"name": "id",
         "type": "int"},
        {"name": "customer_id",
         "type": "int"},
        {"name": "product_id",
         "type": "int"},
        {"name": "product_qty",
         "type": "int"},
        {"name": "created_ms",
         "type": "long",
         "logicalType": "timestamp-millis"},
        {"name": "validity",
         "type": "boolean",
         "default": false}
         ]
}"""

product_schema = """
{
"namespace": "com.thecodinginterface.avrodomainevents",
"type": "record",
"name": "product_details_format",
"fields": [
        {"name": "id",
         "type": "int"},
        {"name": "name",
         "type": "string"},
        {"name": "price",
         "type": "int"}
         ]
}"""

customer_schema = """
{
"namespace": "com.thecodinginterface.avrodomainevents",
"type": "record",
"name": "customer_details_format",
"fields": [
        {"name": "id",
         "type": "int"},
        {"name": "name",
         "type": "string"}
         ]
}"""

revenue_schema = """
{
"namespace": "com.thecodinginterface.avrodomainevents",
"type": "record",
"name": "revenue_format",
"fields": [
        {"name": "cust_id",
         "type": "int"},
        {"name": "cust_name",
         "type": "string"},
        {"name": "order_amount_total",
         "type": "int"}
         ]
}"""