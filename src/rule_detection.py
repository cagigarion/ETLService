from datetime import timedelta
import pandas as pd


def simple_rule_detection(value: str):
    operators = [
        {"operator_name": "greater_equal", "value": ">="},
        {"operator_name": "greater_equal", "value": ">="},
        {"operator_name": "less_equal", "value": "<="},
        {"operator_name": "less", "value": "<"},
        {"operator_name": "greater", "value": ">"},
        {"operator_name": "div", "value": "/"},
        {"operator_name": "add", "value": "+"},
        {"operator_name": "sub", "value": "-"},
        {"operator_name": "mul", "value": "*"}
    ]
    for operator in operators:
        if value[0:len(operator["operator_name"])] == operator["operator_name"]:
            string_args = value[len(operator["operator_name"])+1:len(value)-1]
            return eval(string_args.replace(",", operator["value"]))

    return value


def rule_detection(value: str):
    if value[0:len("python_exe=")] == "python_exe=":
        return eval(value[len("python_exe="):value])
    elif value[0:len("or")] == "or":
        string_args = value[len("or")+1:len(value)-1]
        args = string_args.split(";")
        for arg in args:
            conditions = rule_detection(arg)
            if conditions:
                return True
        return False
    elif value[0:len("and")] == "and":
        string_args = value[len("and")+1:len(value)-1]
        args = string_args.split(";")
        for arg in args:
            conditions = rule_detection(arg)
            if not conditions:
                return False
        return True
    else:
        return simple_rule_detection(value)


def datetime_rule_detection(value: str):
    operators = [
        {"operator_name": "greater_equal", "value": ">="},
        {"operator_name": "greater_equal", "value": ">="},
        {"operator_name": "less_equal", "value": "<="},
        {"operator_name": "less", "value": "<"},
        {"operator_name": "greater", "value": ">"},
        {"operator_name": "div", "value": "/"},
        {"operator_name": "add", "value": "+"},
        {"operator_name": "sub", "value": "-"},
        {"operator_name": "mul", "value": "*"}
    ]
    for operator in operators:
        if value[0:len(operator["operator_name"])] == operator["operator_name"]:
            string_args = value[len(operator["operator_name"])+1:len(value)-1]
            return eval(string_args.replace(",", operator["value"]))

    return value

def datetime_rule_detection(value: str):
    if value[0:len("add_minutes")] == "add_minutes":
        arg = value[len("add_minutes")+1:len(value)-1]
        args = arg.split(",")
        return add_minutes(pd.to_datetime(args[0]), int(args[1]))
    elif value[0:len("sub_minutes")] == "sub_minutes":
        arg = value[len("sub_minutes")+1:len(value)-1]
        args = arg.split(",")
        return sub_minutes(pd.to_datetime(args[0]), int(args[1]))
    elif value[0:len("add_seconds")] == "add_seconds":
        arg = value[len("add_seconds")+1:len(value)-1]
        args = arg.split(",")
        return add_seconds(pd.to_datetime(args[0]), int(args[1]))
    elif value[0:len("sub_seconds")] == "sub_seconds":
        arg = value[len("sub_seconds")+1:len(value)-1]
        args = arg.split(",")
        return sub_seconds(pd.to_datetime(args[0]), int(args[1]))
        
    return value

    
def add_minutes(value, minutes: int):
    return pd.to_datetime(value) + timedelta(minutes=minutes)


def sub_minutes(value, minutes: int):
    return pd.to_datetime(value) - timedelta(minutes=minutes)


def add_seconds(value, seconds: int):
    return pd.to_datetime(value) + timedelta(seconds=seconds)


def sub_seconds(value, seconds: int):
    return pd.to_datetime(value) - timedelta(seconds=seconds)

