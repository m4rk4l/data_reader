import logging

logger = logging.getLogger(__name__)


def read(spark_inst, location, _format, options=[]):
    """Using a spark instance, read from a location to get a dataframe.
    
    use the `format` to specify the format of the data stored in `location`
    in case you need to pass additional options to the reader, you can use
    the options paramether to pass a list of tuples containing key value pairs
    i.e. `options=[("mergeSchema", "True")]`
    """
    reader = spark_inst.read.format(_format)
    assert not options or (options and type(options[0]) is tuple), "options must be a list of tuples, the tuple must contain 2 values."
    [reader.option(k, v) for k, v in options]
    df = reader.load(location)
    return df


def read_with_type_validation(spark_inst, location, _format, schema, options=[]):
    """read with type validation.
    
    read a dataframe and validate its schema. RaiseValueError if either the columns or the types
    don't match the given schema.
    """
    df = read(spark_inst, location, _format, options=options)
    assert schema and type(schema[0]) is tuple, "Must provide a schema (a list of tuples)"
    expected_columns = sorted([c.lower() for c, _ in schema])
    expected_types = sorted([t for _, t in schema])
    actual_columns = sorted([c.lower() for c, _ in df.dtypes])
    actual_types = sorted([t for _, t in df.dtypes])
    logger.debug(f"expected columns: {expected_columns}")
    logger.debug(f"actual columns: {actual_columns}")
    logger.debug(f"expected data types: {expected_types}")
    logger.debug(f"actual data types: {actual_types}")
    has_same_columns = expected_columns == actual_columns
    has_same_types = expected_types == actual_types
    if not has_same_columns:
        raise ValueError(f"Did not get the columns as specified in the schema, got: {actual_columns}")
    if not has_same_types:
        raise ValueError(f"Did not get the same data types for the columns, got: {df.dtypes}")
    return df


def read_with_non_nullable_columns(spark_inst, location, _format, non_nullable_columns, options=[]):
    """read a location and validate nullable columns."""
    df = read(spark_inst, location, _format, options=options)
    failed_columns = []
    for col in non_nullable_columns:
        non_nullable_df = df.filter(getattr(df, col).isNull())
        if non_nullable_df.count():
            failed_columns.append(col)
    if failed_columns:
        raise ValueError(f"The following columns contain null values: {failed_columns}")
    return df


def read_with_regular_expression_columns(spark_inst, location, _format, reg_expr, options=[]):
    """"""
    raise NotImplementedError(":)")


def read_with_unique_column_values(spark_inst, location, _format, unique_value_columns, options=[]):
    df = read(spark_inst, location, _format, options=options)
    duplicates = df.groupBy(*unique_value_columns).count().filter("count > 1")
    if duplicates.count():
        raise ValueError(f"Data has duplicates for columns: {unique_value_columns}")
    return df
