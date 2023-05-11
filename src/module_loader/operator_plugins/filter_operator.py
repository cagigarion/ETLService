from plugin import Plugin
import logging
from jsonpath_ng.ext import parse
from datetime import datetime, timedelta
from main import get_run_next_time, set_run_next_time
import pandas as pd


class MatchPlugin(Plugin):
    namespace = "operator"
    name = "filter"
    querys: list = []
    invoked: bool = False
    null_value_key = ""
    not_null_value_key = ""

    def load(self):
        self.invoked = True

    def show_operator(self):
        return self.name

    def build_operator(self, where):
        field = where["field"]
        operator = where["operator"]
        if operator == "<" or operator == "<=" or operator == ">" or operator == ">=":
            value = where["value"]
        elif type(where["value"]) == type(""):
            value = f'"{where["value"]}"'
        else:
            value = where["value"]

        operator_query = ""
        if operator == "between":
            operator_query = f'{field}>={value["from"]}&{field}<={value["to"]}'
        else:
            operator_query = f'{field}{operator}{value}'

        return operator_query

    def build_logical_operator(self, logical_operators):
        array_query = []
        for logical_operator in logical_operators:
            where = logical_operator["where"]
            operator = where["operator"]
            if operator == "every":
                array_query.append(
                    dict(operator_name=operator, field=where["field"], operator_value=where["value"]))
            elif operator == "equals_field" or operator == "not_equals_field" or operator == "greater_field" or operator == "less_field":
                array_query.append(
                    dict(operator_name=operator,
                         operator_value={"field": where["field"],
                                         "operator_field": where["value"]}))
            elif operator == "is_null" or operator == "is_not_null":
                array_query.append(
                    dict(operator_name=operator,
                         operator_value=where["field"]))
            elif operator == "in" or operator == "not_in":
                at = logical_operator["at"]
                operator_name = "==" if operator == "in" else "!="
                field = where["field"]
                operator_querys = []
                for value in where["value"]:
                    if type(value) == type(""):
                        operator_querys.append(
                            f'{field}{operator_name}"{value}"')
                    else:
                        operator_querys.append(
                            f'{field}{operator_name}{value}')
                if operator == "in":
                    array_query.append(
                        dict(operator_name="jsonpath_ng",
                             operator_value='|'.join([f'({at}[?({operator_query})])'for operator_query in operator_querys])))
                else:
                    query_and = '&'.join(
                        operator_query for operator_query in operator_querys)
                    array_query.append(
                        dict(operator_name="jsonpath_ng",
                             operator_value=f'{at}[?({query_and})]'))
            else:
                operator_query = f'{logical_operator["at"]}[?({ self.build_operator(where)})]'
                array_query.append(
                    dict(operator_name="jsonpath_ng", operator_value=operator_query))

        return array_query

    def build_query(self, op_specs):
        for op_spec in op_specs:
            if op_spec.get("and") is not None:
                self.querys.append(
                    {"and": self.build_logical_operator(op_spec.get("and"))})
            elif op_spec.get("or") is not None:
                self.querys.append(
                    {"or": self.build_logical_operator(op_spec.get("or"))})

        return self

    def processing_operator(self, operator, input_data):
        operator_name = operator["operator_name"]
        operator_value = operator["operator_value"]
        if operator_name == "jsonpath_ng":
            return len(parse(operator_value).find([input_data])) > 0
        elif operator_name == "equals_field":
            return input_data.get(operator_value["field"]) == input_data.get(operator_value["operator_field"])
        elif operator_name == "not_equals_field":
            return input_data.get(operator_value["field"]) != input_data.get(operator_value["operator_field"])
        elif operator_name == "greater_field":
            return float(input_data.get(operator_value["field"])) > float(input_data.get(operator_value["operator_field"]))
        elif operator_name == "less_field":
            return float(input_data.get(operator_value["field"])) < float(input_data.get(operator_value["operator_field"]))
        elif operator_name == "is_null":
            return input_data.get(operator_value) is None
        elif operator_name == "is_not_null":
            return input_data.get(operator_value) is not None
        elif operator_name == "every":
            return self.every_expression(operator, input_data)
        else:
            return False

    def get_interval_data(self, interval_data):        
        logging.info("get data interval")
        next_time = get_run_next_time()
        if next_time is None:
            return
        time_value = float(interval_data.get("time_value"))
        type_name = interval_data.get("type")
        if type_name == "seconds":
            return next_time + timedelta(seconds=time_value)
        elif type_name == "minutes":
            return next_time + timedelta(minutes=time_value)
        elif type_name == "hours":
            return next_time + timedelta(hours=time_value)

    def every_expression(self, operator, input_data):
        field_name = operator["field"]
        run_next_time = self.get_interval_data({"time_value": operator["operator_value"], "type":"seconds"})
        logging.info("run_next_time value:" + str(run_next_time))
        current_time = pd.to_datetime(input_data[field_name])

        if run_next_time is not None and run_next_time <= current_time:
            set_run_next_time(run_next_time)
            return True
        else:
            if run_next_time is None:
                set_run_next_time(current_time)
            return False

    def processing_data(self, input_data):
        for query in self.querys:
            logging.debug(f'The data which want to parser = {input_data}')
            name = ""
            if query.get("and"):
                name = "and"
            elif query.get("or"):
                name = "or"
            else:
                break
            spec = query.get(name)
            or_valid = False
            for operator in spec:
                valid = self.processing_operator(operator, input_data)
                if name == "and" and not valid:
                    return {}
                if name == "or" and valid:
                    or_valid = True
                    break
            if not or_valid and name == "or":
                return {}
        logging.debug(f'The data after filter = {input_data}')
        return input_data
