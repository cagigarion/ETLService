import _pulsar
import json
from pulsar.schema import AvroSchema, JsonSchema, Record
from fastavro.schema import parse_schema

def convert_json_to_pulsar_schema(**schema_input):
    schema_input_type = schema_input["type"]
    schema_input_data = schema_input["data"]
    schema_definition = parse_schema(schema_input_data)
    dataSchema = None
    match schema_input_type:
        case "JSON":
            dataSchema = JsonSchema()
        case _:
            dataSchema = AvroSchema(None, schema_definition=schema_definition)  
    return dataSchema