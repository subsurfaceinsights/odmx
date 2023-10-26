"""
ODMX REST API
"""
import json
import inspect
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union
from datetime import datetime, date, time
from argparse import ArgumentParser
from psycopg.errors import InvalidParameterValue
import uvicorn
from odmx.support.config import Config
from  odmx.support import db
import odmx.data_model as odmx
import urllib.parse
import tempfile
import traceback

Scope = Dict[str, Any]
Receive = Callable[[], Awaitable[Dict[str, Any]]]
Send = Callable[[Dict[str, Any]], Awaitable[None]]


async def start_body(send: Send, status: int,
                     content_type: Optional[str]=None, headers=None) -> None:
    """
    Send the HTTP response headers and start the body.
    """
    if headers is None:
        headers = {}
    if 'content-type' not in headers:
        headers['content-type'] = content_type

    headers_encoded = []
    for key, value in headers.items():
        value = str(value)
        headers_encoded.append((key.encode('utf-8'), value.encode('utf-8')))
    await send({
        'type': 'http.response.start',
        'status': status,
        'headers': headers_encoded
    })


async def send_body_part(send: Send, body: str) -> None:
    """
    Send a part of the HTTP response body.
    """
    await send({
        'type': 'http.response.body',
        'body': bytes(body, 'utf-8'),
        'more_body': True
    })


async def end_body(send: Send, body: str) -> None:
    """
    Send the last part of the HTTP response body.
    """
    await send({
        'type': 'http.response.body',
        'body': bytes(body, 'utf-8'),
        'more_body': False
    })

async def send_body(send: Send,
                    status: int,
                    body: str,
                    content_type: str) -> None:
    """
    Send a complete HTTP response body, adds a content-length header.
    """
    body_bytes = bytes(body, 'utf-8')
    headers = {
        'content-length': len(body_bytes),
        'content-type': content_type
    }
    await start_body(send, status, headers=headers)
    await end_body(send, body)

async def start_db_send_json_obj_list(send: Send) -> None:
    """ Send headers and start body for json object list"""
    await start_body(send, 200, 'application/json')
    await send_body_part(send, '[')

async def start_db_send_json_table_list(send: Send, cols: list,
                                        limit: Optional[int],
                                        offset: Optional[int]) -> None:
    """ Send headers and start body for json table list"""
    await start_body(send, 200, 'application/json')
    limit_section = ''
    if limit is not None:
        limit_section = f'"limit":{limit},'
    offset_section = ''
    if offset is not None:
        offset_section = f'"offset":{offset},'
    limit_offset_section = limit_section + offset_section
    await send_body_part(send,
                         '{"headers":' + json.dumps(cols) + \
                        f',{limit_offset_section}"rows":[')


async def start_db_send_csv(send: Send, cols: list) -> None:
    """ Send headers and start body for csv"""
    await start_body(send, 200, 'text/csv')
    await send_body_part(send, ','.join(cols) + '\n')


async def end_db_send_json_obj_list(send: Send) -> None:
    """ End body for json object list"""
    await end_body(send, ']')


async def end_db_send_json_table_list(send: Send) -> None:
    """ End body for json table list"""
    await end_body(send, ']}')


async def end_db_send_csv(send: Send) -> None:
    """ End body for csv"""
    await end_body(send, '')


async def send_db_model_json_obj(
        model_obj,
        send: Send,
        cols=None,
        singular=False) -> None:
    """ Send model json object"""
    d = model_obj.to_json_dict()
    if cols is not None:
        d = {k: v for k, v in d.items() if k in cols}
    d = json.dumps(d)
    if singular:
        await start_body(send, 200, 'application/json')
        await end_body(send, d)
    else:
        await send_body_part(send, d)


async def send_db_model_json_list(
        model_obj,
        send: Send,
        cols=None) -> None:
    """ Send model json list"""
    d = model_obj.to_json_dict()
    if cols is not None:
        d = {k: v for k, v in d.items() if k in cols}
    d = list(d.values())
    d = json.dumps(d)
    await send_body_part(send, d)


def process_csv_row(d: list):
    """
    Produces a CSV row from a list, escaping values as needed.
    """
    def process(v) -> str:
        if v is None:
            v = ''
        v = str(v)
        escape = False
        if '"' in v:
            v = v.replace('"', '"""')
            escape = True
        if ',' in v:
            escape = True
        if '\n' in v:
            v.replace('\n', '\\n')
        if escape:
            v = '"' + v + '"'
        return v
    d2 = [process(v) for v in d]
    d3 = ','.join(d2)
    return d3


async def send_db_model_csv(
        model_obj,
        send: Send,
        cols=None) -> None:
    """ Send model csv"""
    d = model_obj.to_json_dict()
    d = {k: v for k, v in d.items() if k in cols}
    d = list(d.values())
    d = process_csv_row(d)
    await send_body_part(send, d)


async def send_db_models_json_obj_list(model_objs, send: Send, cols=None):
    """ Send model json object list"""
    print('send_db_models_json_obj_list')
    first = True
    for model_obj in model_objs:
        if first:
            if cols is None:
                cols = list(model_obj.to_json_dict().keys())
            await start_db_send_json_obj_list(send)
            first = False
        else:
            await send_body_part(send, ',')
        await send_db_model_json_obj(model_obj, send, cols)
    if first:
        await start_db_send_json_obj_list(send)
    await end_db_send_json_obj_list(send)

async def send_db_models_single_col_list(model_objs, send: Send, col):
    """ Send single column model list"""
    first = True
    for model_obj in model_objs:
        json_obj = model_obj.to_json_dict()
        if first:
            if col not in json_obj:
                await bad_request(send, f'Column {col} not found')
                return
            await start_db_send_json_obj_list(send)
            first = False
        else:
            await send_body_part(send, ',')
        await send_body_part(send, json.dumps(json_obj[col]))
    if first:
        await start_db_send_json_obj_list(send)
    await end_db_send_json_obj_list(send)


async def send_db_models_single_col_list_distinct(model_objs, send: Send, col):
    """ Send single column model list with only distinct values"""
    distinct = set()
    for model_obj in model_objs:
        json_obj = model_obj.to_json_dict()
        distinct.add(json_obj[col])
    await send_json(send, list(distinct))

async def send_db_models_json_table_list(model_objs, send: Send,
                                         cols: Optional[list],
                                         limit: Optional[int],
                                         offset: Optional[int]):
    """ Send json table model list"""
    first = True
    for model_obj in model_objs:
        if first:
            if cols is None:
                cols = list(model_obj.to_json_dict().keys())
            await start_db_send_json_table_list(send, cols, limit, offset)
            first = False
        else:
            await send_body_part(send, ',')
        await send_db_model_json_list(model_obj, send, cols)
    if first:
        if cols is None:
            cols = []
        await start_db_send_json_table_list(send, cols, limit, offset)
    await end_db_send_json_table_list(send)


async def send_db_models_csv(model_objs, send: Send, cols=None):
    """ Send csv models"""
    first = True
    for model_obj in model_objs:
        if first:
            if cols is None:
                cols = list(model_obj.to_json_dict().keys())
            await start_db_send_csv(send, cols)
            first = False
        else:
            await send_body_part(send, '\n')
        await send_db_model_csv(model_obj, send, cols)
    if first:
        if cols is None:
            cols = []
        await start_db_send_csv(send, cols)
    await end_db_send_csv(send)


async def bad_request(send: Send, msg: str):
    """ bad request"""
    await send_body(send, 400, msg, 'text/plain')

async def forbidden(send: Send, msg: str):
    """ forbidden"""
    await send_body(send, 403, msg, 'text/plain')

async def not_found(send: Send, msg: str):
    """not found"""
    await send_body(send, 404, msg, 'text/plain')

async def send_json(send: Send, obj):
    """ send json"""
    await send_body(send, 200, json.dumps(obj), 'application/json')

def parse_qs(query_string: bytes) -> Dict[str, Union[str, List[str]]]:
    """ parse query """
    query_string_str = query_string.decode('utf-8')
    query_vars = {}
    for var in query_string_str.split('&'):
        if '=' in var:
            k, v = var.split('=')
            # Decode the value
            v = v.replace('+', ' ')
            v = urllib.parse.unquote(v)

            if k in query_vars:
                query_vars[k].append(v)
            else:
                query_vars[k] = [v]
        else:
            if var:
                if var in query_vars:
                    query_vars[var].append('')
                else:
                    query_vars[var] = ['']
    # Pull out singletons
    for k, v in query_vars.items():
        if len(v) == 1:
            query_vars[k] = v[0]
    return query_vars

special_handlers = {}

def handle_path(path):
    """ Create a decorator to add a function as a special path handler """
    def decorator(f):
        special_handlers[path] = f
        return f
    return decorator

@handle_path('datastream_data')
async def handle_datastreams(path_elements,
                             scope: Scope,
                             receive: Receive,
                             send: Send) -> None:
    """ Handle Datastreams"""
    method = scope['method']
    if method == 'GET':
        assert path_elements[0] == 'datastream_data'
        if len(path_elements) != 2:
            await bad_request(send, 'Expected datastream_data/<datastream_id>')
            return
        datastream_id = int(path_elements[1])
        headers = get_headers(scope)
        con = get_connection(headers)
        query_string = scope['query_string']
        query_vars = parse_qs(query_string)
        datastream = \
            odmx.read_sampling_feature_timeseries_datastreams_one_or_none(
                con, datastream_id=datastream_id)
        if 'full_precision' in query_vars and query_vars['full_precision'] != '0':
            con.execute("SET extra_float_digits = 3")
        else:
            con.execute("SET extra_float_digits = -5")
        if datastream is None:
            await not_found(send, f'No datastream with id {datastream_id}')
            return
        #db.set_current_schema(con, datastream.datastream_database)
        db.set_current_schema(con, 'datastreams')
        try:
            async def get_date(key, is_datetime=False):
                """ get date"""
                if key in query_vars:
                    key_str = query_vars[key]
                    if not isinstance(key_str, str):
                        raise ValueError(f'{key} must be a single date')
                    if is_datetime:
                        return datetime.fromisoformat(key_str)
                    else:
                        return date.fromisoformat(key_str)
                return None
            start_date = await get_date('start_date')
            end_date = await get_date('end_date')
            start_datetime = await get_date('start_datetime', True)
            end_datetime = await get_date('end_datetime', True)
        except ValueError as e:
            traceback.print_exception(e)
            await bad_request(send, str(e))
            return
        if start_date and start_datetime:
            await bad_request(send, ('Cannot specify both start_date and '
                                     'start_datetime'))
            return
        if end_date and end_datetime:
            await bad_request(send, ('Cannot specify both end_date and '
                                     'end_datetime'))
            return
        if 'qa_flag' in query_vars:
            qa_flag = query_vars['qa_flag']
            if len(qa_flag) != 1:
                await bad_request(send, 'qa_flag must be a single character')
                return
        else:
            qa_flag = 'z'
        start_op = '>='
        end_op = '<='
        if 'open_interval' in query_vars:
            open_interval = query_vars['open_interval']
            if open_interval not in ['none', 'start', 'end', 'both']:
                await bad_request(send, ('closed_interval must be start, '
                                         'end or both'))
                return
            if open_interval == 'start':
                start_op = '>'
            elif open_interval == 'end':
                end_op = '<'
            elif open_interval == 'both':
                start_op = '>'
                end_op = '<'
        if 'qa_flag_mode' in query_vars:
            qa_flag_mode = query_vars['qa_flag_mode']
            if qa_flag_mode not in ['greater_or_eq', 'less_or_eq', 'equal']:
                await bad_request(send, ('qa_flag_mode must be greater_or_eq, '
                                         'less_or_eq or equal'))
                return
        else:
            qa_flag_mode = 'greater_or_eq'
        downsample_interval = query_vars.get('downsample_interval', None)
        downsample_method = query_vars.get('downsample_method', 'mean')
        timezone = query_vars.get('tz', 'UTC')

        if start_date:
            start_datetime = datetime.combine(start_date, datetime.min.time())
        if end_date:
            end_datetime = datetime.combine(end_date, datetime.max.time())
        if not start_datetime:
            start_datetime = datetime.min
        if not end_datetime:
            end_datetime = datetime.max
        quoted_table = db.quote_id(con, datastream.datastream_tablename)
        qa_flag_mode = {
            'greater_or_eq': '>=',
            'less_or_eq': '<=',
            'equal': '='
        }[qa_flag_mode]
        sql_clause = f"""
            SELECT * FROM (
                SELECT
                    (TIMESTAMP 'epoch' + utc_time * INTERVAL '1 second')
                        AT TIME ZONE 'UTC'
                            AT TIME ZONE %s AS datetime_local,
                    data_value,
                    qa_flag
                FROM {quoted_table}
            ) AS data
            WHERE
                datetime_local {start_op} %s AND
                datetime_local {end_op} %s AND
                qa_flag {qa_flag_mode} %s
        """
        if downsample_interval:
            valid_downsample_intervals = ['week', 'year', 'month', 'day',
                                          'hour', 'minute', 'second']
            if downsample_interval not in valid_downsample_intervals:
                await bad_request(send,
                                  ('downsample_interval must be one of '
                                   f'{", ".join(valid_downsample_intervals)}'))
                return
            downsample_functions = {
                'mean': 'AVG',
                'sum': 'SUM',
                'count': 'COUNT',
                'stddev': 'STDDEV',
                'variance': 'VARIANCE',
            }
            if not isinstance(downsample_method, str):
                await bad_request(send, 'downsample_method must be a string')
                return
            downsample_function = downsample_functions.get(downsample_method)
            if downsample_function:
                sql_clause = f"""
                    SELECT
                        DATE_TRUNC('{downsample_interval}', datetime_local)
                         AS truncated_datetime_local,
                        {downsample_function}(data_value) AS data_value
                    FROM ( {sql_clause} ) AS downsampled
                    GROUP BY truncated_datetime_local
                    ORDER BY truncated_datetime_local
                """
            else:
                maxdata = f"""
                    maxdata AS (
                        SELECT
                            truncated_datetime_local,
                            datetime_local as max_datetime_local,
                            data_value as max_data_value
                        FROM (
                            SELECT
                                DISTINCT ON (truncated_datetime_local)
                                DATE_TRUNC('{downsample_interval}',
                                            datetime_local)
                                 AS truncated_datetime_local,
                                data_value,
                                datetime_local
                            FROM data2 AS downsampled
                            ORDER BY
                                truncated_datetime_local,
                                data_value DESC,
                                datetime_local
                        ) subquery
                    )"""
                mindata = f"""
                    mindata AS (
                        SELECT
                            truncated_datetime_local,
                            datetime_local as min_datetime_local,
                            data_value as min_data_value
                        FROM (
                            SELECT
                                DISTINCT ON (truncated_datetime_local)
                                DATE_TRUNC('{downsample_interval}',
                                           datetime_local)
                                 AS truncated_datetime_local,
                                data_value,
                                datetime_local
                            FROM data2 AS downsampled
                            ORDER BY
                                truncated_datetime_local,
                                data_value ASC,
                                datetime_local
                        ) subquery
                    )"""
                if downsample_method == 'min':
                    sql_clause = f"""
                        WITH data2 as (
                            {sql_clause}
                        ),
                        {mindata}
                        SELECT * FROM mindata
                    """
                elif downsample_method == 'max':
                    sql_clause = f"""
                        WITH data2 as (
                            {sql_clause}
                        ),
                        {maxdata}
                        SELECT * FROM maxdata
                    """
                elif downsample_method == 'min_max':
                    sql_clause = f"""
                        WITH data2 as (
                            {sql_clause}
                        ),
                        {maxdata},
                        {mindata}
                        SELECT
                            mindata.truncated_datetime_local,
                            mindata.min_datetime_local,
                            mindata.min_data_value,
                            maxdata.max_datetime_local,
                            maxdata.max_data_value
                        FROM maxdata
                        INNER JOIN mindata
                        ON maxdata.truncated_datetime_local =
                         mindata.truncated_datetime_local
                        ORDER BY maxdata.truncated_datetime_local
                    """
                else:
                    await bad_request(send, ('Unsupported downsample '
                                             f'method {downsample_method}'))
                    return

        sql_args = [timezone, start_datetime, end_datetime, qa_flag]
        data = con.execute(sql_clause, sql_args) # pyright: ignore[reportGeneralTypeIssues]

        query_format = query_vars.get('format', 'json')
        if query_format == 'json':
            await start_body(send, 200, 'application/json')
            await send_body_part(send, '[')
            first = True
            for data_row in data:
                if first:
                    first = False
                else:
                    await send_body_part(send, ',')
                await send_body_part(send, data_row.to_json_list())
            await end_body(send, ']')
        elif query_format == 'csv':
            await start_body(send, 200, 'text/csv')
            assert data.description
            columns = [d[0] for d in data.description]
            await send_body_part(send, ','.join(columns) + '\n')
            for data_row in data:
                csv_row = process_csv_row(data_row.to_list())
                await send_body_part(send, csv_row + "\n")
            await end_body(send, '')
        else:
            await bad_request(send, f'Unsupported format {query_format}')
    else:
        await bad_request(send, f'Unsupported method {method}')





async def handle_odmx_request(scope: Scope,
                              receive: Receive,
                              send: Send) -> None:
    """ Handle ODMX request"""
    call = scope['call']
    print(call)
    path_elements = call.split('/')[1:]
    entity = path_elements[0]
    if entity in special_handlers:
        await special_handlers[entity](path_elements, scope, receive, send)
        return
    table_class = odmx.get_table_class(entity)
    if table_class is None:
        await not_found(send, f'No entity "{entity}"')
        return
    headers = get_headers(scope)
    con = get_connection(headers)
    method = scope['method']
    if method in ('POST', 'PUT', 'PATCH', 'DELETE'):
        if not config.enable_writes:
            await forbidden(send, 'Writes are disabled')
            return
    if method == 'GET':
        query_string = scope['query_string']
        query_vars = parse_qs(query_string)
        if '_format' in query_vars:
            query_format = query_vars['_format']
            del query_vars['_format']
        else:
            query_format = None
        if '_cols' in query_vars:
            query_vars_str = query_vars['_cols']
            if not isinstance(query_vars_str, str):
                await bad_request(send, '_cols must be a single string')
                return
            cols = query_vars_str.split(',')
            del query_vars['_cols']
        else:
            cols = None
        fuzzy = False
        if '_fuzzy' in query_vars:
            fuzzy = True
            del query_vars['_fuzzy']
        if '_limit' in query_vars:
            limit_str = query_vars['_limit']
            if not isinstance(limit_str, str):
                await bad_request(send, '_limit must be a single integer')
                return
            limit = int(limit_str)
            del query_vars['_limit']
        else:
            limit = None
        if '_offset' in query_vars:
            offset_str = query_vars['_offset']
            if not isinstance(offset_str, str):
                await bad_request(send, '_offset must be a single integer')
                return
            try:
                offset = int(offset_str)
            except ValueError:
                await bad_request(send, f'Invalid offset "{offset_str}"')
                return
            del query_vars['_offset']
        else:
            offset = None
        if '_distinct' in query_vars:
            distinct = True
            if cols is None or len(cols) != 1:
                await bad_request(send, 'Distinct requires exactly one column')
                return
            del query_vars['_distinct']
            if not query_format:
                query_format = 'json_flat_list'
            elif query_format != 'json_flat_list':
                await bad_request(send, ('Distinct only supported for '
                                         'format=json_flat_list'))
                return
        else:
            distinct = False
        # Do type conversions based on kwargs of read method
        params = inspect.signature(table_class.read).parameters
        types = {}
        for k, v in params.items():
            if v.annotation is not inspect.Parameter.empty:
                types[k] = v.annotation
        new_query_vars = {}
        for k, v in query_vars.items():
            if k not in params:
                await bad_request(send, f'Unknown query parameter "{k}"')
                return
            if isinstance(v, list):
                await bad_request(send, f'Multiple values for "{k}"')
                return
            if k in types:
                t = types[k]
                if t == Optional[int]:
                    v = int(v)
                elif t == Optional[float]:
                    v = float(v)
                elif t == Optional[bool]:
                    v = bool(v)
                elif t == Optional[str]:
                    v = str(v)
                elif t == Optional[datetime]:
                    v = datetime.fromisoformat(v)
                elif t == Optional[date]:
                    v = date.fromisoformat(v)
                elif t == Optional[time]:
                    v = time.fromisoformat(v)
                new_query_vars[k] = v
        query_vars = new_query_vars

        if len(path_elements) == 1:
            try:
                if not fuzzy:
                    model_objs = table_class.read(con, **query_vars,
                                                  _limit=limit, _offset=offset)
                else:
                    model_objs = table_class.read_fuzzy(con, **query_vars,
                                                        _limit=limit,
                                                        _offset=offset)
            except IOError as e:
                await bad_request(send, str(e))
                return
            if query_format is None:
                if cols is None or len(cols) > 1:
                    query_format = 'json_obj_list'
                else:
                    query_format = 'json_flat_list'
            if query_format == 'json_flat_list':
                if cols is None or len(cols) != 1:
                    await bad_request(send, ('Single list requires exactly '
                                             'one column'))
                    return
                if distinct:
                    await send_db_models_single_col_list_distinct(model_objs,
                                                                   send,
                                                                   cols[0])
                else:
                    await send_db_models_single_col_list(model_objs,
                                                         send,
                                                         cols[0])
            elif query_format == 'json_obj_list':
                await send_db_models_json_obj_list(model_objs, send, cols)
            elif query_format == 'json_table':
                await send_db_models_json_table_list(model_objs, send, cols,
                                                     limit, offset)
            elif query_format == 'csv':
                await send_db_models_csv(model_objs, send, cols)
            else:
                await bad_request(send, f'Unknown format "{query_format}"')
        elif len(path_elements) == 2:
            ids = path_elements[1].split(',')
            try:
                ids = [int(table_id) for table_id in ids]
            except ValueError:
                await bad_request(send, f'Invalid id(s) specified: {ids}')
                return
            if len(ids) == 1:
                try:
                    model_obj = table_class.read_by_id(con, ids[0])
                    await send_db_model_json_obj(model_obj,
                                                 send,
                                                 cols,
                                                 singular=True)
                except db.NoResultsFound:
                    await not_found(send, f'No {entity} with id {ids[0]}')
                    return
            else:
                kwargs = {table_class.PRIMARY_KEY: ids}
                model_objs = table_class.read_any(con, **kwargs)
                await send_db_models_json_obj_list(model_objs, send, cols)
        else:
            await bad_request(send, "Trailling path elements")
    elif method == 'POST':
        if len(path_elements) == 1:
            try:
                json_data = await receive()
                data = json.loads(json_data['body'])
                print(data)
                table_id = table_class.write(con, **data)
                await send_json(send, table_id)
            except IOError as e:
                await bad_request(send, str(e))
                return
        else:
            await bad_request(send, "Unexpected pathing for POST")
    elif method == 'PUT':
        if len(path_elements) == 2:
            try:
                put_id = int(path_elements[1])
                model_obj = table_class.read_by_id(con, put_id)
                if not model_obj:
                    await not_found(send, (f'No entity "{entity}" with '
                                           f'id {put_id}'))
                    return
                json_data = await receive()
                data = json.loads(json_data['body'])
                table_id = int(path_elements[1])
                data[table_class.PRIMARY_KEY] = table_id
                table_id = table_class.update(con, **data)
                await send_json(send, table_id)
            except IOError as e:
                await bad_request(send, str(e))
                return
        else:
            await bad_request(send, "Unexpected pathing for PUT, need ID")
    elif method == 'DELETE':
        if len(path_elements) == 2:
            try:
                table_id = int(path_elements[1])
                table_class.delete(con, table_id)
                await send_json(send, table_id)
            except IOError as e:
                await bad_request(send, str(e))
                return
        else:
            await bad_request(send, "Unexpected pathing for DELETE, need ID")
    else:
        await bad_request(send, f'Unknown method "{method}"')
        return


def get_connection(headers):
    """ Get Connection """
    project_header = config.project_name_header.lower()
    if project_header and project_header in headers:
        project_name = headers[project_header]
    else:
        project_name = config.project_name
    con = db.connect(config, db_name=f'odmx_{project_name}')
    db.set_current_schema(con, 'odmx')
    return con

def get_headers(scope) -> Dict[str, str]:
    """ Get headers"""
    headers = {}
    for k, v in scope['headers']:
        headers[k.decode('utf-8').lower()] = v.decode('utf-8')
    return headers

async def app(scope: Scope, receive: Receive, send: Send) -> None:
    """ application"""
    assert scope['type'] == 'http'
    prefix = '/api/odmx/v3/'
    # prefix_len = len(prefix)
    if scope['path'].startswith(prefix):
        scope['call'] = scope['path'][12:]
        try:
            await handle_odmx_request(scope, receive, send)
        except InvalidParameterValue as e:
            await bad_request(send, str(e))
    else:
        await not_found(send, f'No handler for {scope["path"]}')


def setup_config():
    config = Config()
    config.add_config_param('project_name',
                            help='Project Name to serve requests for by default')
    config.add_config_param('project_name_header',
                            help='Header to use for project name in the case that'
                                 'you are serving multiple projects from the same '
                                 'server',
                            default='X-Odmx-Project-Name')
    config.add_config_param('enable_writes',
                            help='Enable write operations',
                            validator='boolean',
                            default=True)
    config.add_config_param('host',
                            help='Host to listen on',
                            default='127.0.0.1')
    config.add_config_param('port',
                            help='Port to listen on',
                            validator='integer',
                            default='8000')
    config.add_config_param('workers',
                            help='Number of workers',
                            validator='integer',
                            default='1')
    config.add_config_param('debug',
                            help='Debug',
                            validator='boolean',
                            default=True)
    db.add_db_parameters_to_config(config)
    return config

if __name__ == '__main__':
    config = setup_config()
    parser = ArgumentParser()
    parser.add_argument('--config', help='Config file to use')
    config.add_args_to_argparser(parser)
    args = parser.parse_args()
    if args.config is not None:
        config.add_yaml_file(args.config, True, False)
    config.validate_config(args)
    tmp_file_path = None
    if not config.debug:
        # Get a tmp file name
        tmp_file_path = tempfile.NamedTemporaryFile(delete=False).name
        # We need to pass the config to the subprocesses
        config_env = config.to_env_file(tmp_file_path)

    uvicorn.run("odmx.rest_api:app" if not config.debug else app,
                host=config.host,
                port=int(config.port),
                workers=int(config.workers),
                env_file=tmp_file_path)
else:
    config = setup_config()
    config.validate_config()
