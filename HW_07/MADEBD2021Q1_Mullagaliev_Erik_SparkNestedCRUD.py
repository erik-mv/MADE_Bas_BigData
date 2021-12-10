"""
HW7 of Big Data course
"""


import json

from pyspark.sql.functions import col, struct


def create_inner(schema, update_dict, name):
    """
    Create selected dict from df schema and update values with updated dict
    :param schema: schema of df to select
    :param update_dict: dict with columns to update
    :param name: full name to current path in schema
    :return: dict with stuctured key to select
    """
    result = {}
    for field in schema["fields"]:

        if name:
            full_name = f"{name}.{field['name']}"
        else:
            full_name = field["name"]

        if isinstance(field["type"], dict):

            result[field["name"]] = create_inner(field["type"], update_dict, full_name)

        else:
            if full_name in update_dict:
                result[field["name"]] = update_dict[full_name]
                update_dict.pop(full_name)
            else:
                result[field["name"]] = col(full_name)
    return result


def parse_external(update_dict, selected_dict):
    """
    parse values in updated dictionary and add to selected_dict
    :param update_dict: dict with columns to update
    :param selected_dict: structured by keys dict
    :return: updated structured by keys dict
    """
    for key, value in update_dict.items():
        if key.count(".") == 0:
            selected_dict[key] = value
        else:
            inner_keys = key.split(".")
            inner_key = inner_keys[0]
            if inner_key not in selected_dict:
                selected_dict[inner_key] = {}
            inner_dict = selected_dict[inner_key]
            for inner_key in inner_keys[1:-1]:
                if inner_key not in inner_dict:
                    inner_dict[inner_key] = {}
                inner_dict = inner_dict[inner_key]
            inner_dict[inner_keys[-1]] = value
    return selected_dict


def struct_dict(parsed):
    """
    convert dictionary to selected types
    :param parsed: dictionary with columns to select with values
    :return: list of cols to select from spark data frame
    """
    result = []
    for key, value in parsed.items():
        if isinstance(value, dict):
            result.append(struct(*struct_dict(value)).alias(key))
        else:
            result.append(value.alias(key))
    return result


def update_df(df, columns_dict):
    """
    Updates existing columns or creates new in dataframe df using
    columns from columns_dict.
    :param df: input dataframe
    :type df: pyspark.sql.Dataframe
    :param columns_dict: Key-value dictionary of columns which need to
      be updated. Key is a column name in
      the format of path.to.col
    :type columns_dict: Dict[str, pyspark.sql.Column]
    :return: dataframe with updated columns
    :rtype pyspark.sql.DataFrame
    """
    schema = json.loads(df.schema.json())
    result_dict = create_inner(schema, columns_dict, "")
    result_dict = parse_external(columns_dict, result_dict)
    new_columns = struct_dict(result_dict)
    return df.select(*new_columns)
