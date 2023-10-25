"""
ODMX REST API Client
"""
import requests
from beartype import beartype
from beartype.typing import Optional, Type, Generator
from datetime import date, datetime
import argparse
import json
from odmx.support.config import Config
import odmx.data_model as odmx

class ApiException(Exception):
    """
    An exception that is raised when the API returns an error.
    """
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message

    def __str__(self):
        return "API Error " + str(self.status_code) + ": " + self.message

class ApiClient:
    """
    A simple REST API client for the ODMX API.
    """
    def __init__(self, url):
        """
        Create a new client instance.
        - url: The URL of the server to connect to, for example:
            http://localhost:8000/api/odmx/v3/
        """
        if not url.endswith('/'):
            url += '/'
        self.url = url
        self.session = requests.Session()


    def add_request_header(self, key, value):
        """
        Useful if you are guarding your API with a token or something.
        """
        self.session.headers[key] = value

    def request(self,
             endpoint: str,
             limit: Optional[int] = None,
             offset: Optional[int] = None,
             fuzzy: bool = False,
             format: Optional[str] = None,
             params: Optional[dict] = None) -> requests.Response:
        params = {}
        if limit:
            params['_limit'] = limit
        if offset:
            params['_offset'] = offset
        if fuzzy:
            params['_fuzzy'] = '1' if fuzzy else '0'
        if format:
            params['_format'] = format
        response =  self.session.get(self.url + endpoint, params=params)
        if response.status_code != 200:
            raise ApiException(response.status_code, response.text)
        return response

    # TODO generate these methods from the data model
    def get(self,
            table_class: Type[odmx.Base],
            fuzzy: bool = False,
            items_per_request: Optional[int] = 256,
            **kwargs) -> Generator[odmx.Base, None, None]:
        """
        Perform a GET request against an endpoint
        """
        params = kwargs
        table_name = table_class.TABLE_NAME
        assert table_name
        offset = 0
        if items_per_request is None:
            limit = 65536
        else:
            limit = items_per_request
        response = self.request(
                table_name, fuzzy=fuzzy, format='json_table',
                offset=offset, limit=limit, **params)
        while True:
            next_response = self.request(
                    table_name, fuzzy=fuzzy, format='json_table',
                    offset=offset + limit, limit=limit,
                    **params)
            # Now we need to convert the response into a list of objects
            # and yield them
            data = response.json()
            columns = data['headers']
            rows = data['rows']
            if len(rows) == 0:
                break
            for row in rows:
                result = {}
                for i, value in enumerate(row):
                    result[columns[i]] = value
                assert table_class.create_from_json_dict
                obj = table_class.create_from_json_dict(result)
                yield obj
            offset += limit
            response = next_response


    @beartype
    def datastream(self,
                   datastream_id: int,
                   full_precision: bool =False,
                   start_date: Optional[date]=None,
                   end_date: Optional[date]=None,
                   start_datetime: Optional[datetime]=None,
                   end_datetime: Optional[datetime]=None,
                   qa_flag: str ='z',
                   qa_flag_mode: str ='greater_or_eq',
                   downsample_interval: Optional[str] = None,
                   downsample_method: Optional[str] = 'mean',
                   format: str='json',
                   open_interval: Optional[str] = None,
                   tz: Optional[str] = None) -> str:
        """
        Get a datastream from the server.
        - datastream_id: The id of the datastream to get.
        - full_precision: If True, return the full precision data, otherwise
            return with possibly rounded values.
        - start_date: The start date of the data to retrieve.
        - end_date: The end date of the data to retrieve.
        - start_datetime: The start datetime of the data to retrieve. Must not
            be used with start_date.
        - end_datetime: The end datetime of the data to retrieve. Must not be
            used with end_date.
        - qa_flag: The QA flag to filter on.
        - qa_flag_mode: The QA flag mode to use. Can be 'greater_or_eq' or
            'less_or_eq', 'equal'
        - downsample_interval: The interval to downsample the data to, must be
            'week', 'day', 'hour', 'minute', 'second'
        - downsample_method: The method to use for downsampling, must be
            'mean', 'sum', 'count', 'stddev', 'variance', 'min', 'max'
            'min_max'
        - format: The format to return the data in, must be 'json' or 'csv'.
        - tz: The timezone to use for the data, must be interpretable by
                pytz.timezone()
        - open_interval: If specified, whether the start and end dates are
            open or closed. Closed is the default, which means that the
            start and end dates are included in the data. The specified
            options are 'none', 'start', 'end', 'both'. The default is 'none'
        """
        params = {}
        if full_precision:
            params['full_precision'] = '1'
        if start_date:
            params['start_date'] = start_date.isoformat()
        if end_date:
            params['end_date'] = end_date.isoformat()
        if start_datetime:
            params['start_datetime'] = start_datetime.isoformat()
        if end_datetime:
            params['end_datetime'] = end_datetime.isoformat()
        if qa_flag:
            params['qa_flag'] = qa_flag
        if qa_flag_mode:
            params['qa_flag_mode'] = qa_flag_mode
        if downsample_interval:
            params['downsample_interval'] = downsample_interval
        if downsample_method:
            params['downsample_method'] = downsample_method
        if format:
            params['format'] = format
        if tz:
            params['tz'] = tz
        if open_interval:
            params['open_interval'] = open_interval
        response = self.session.get(self.url + 'datastream_data/' + str(datastream_id), params=params)
        if response.status_code != 200:
            raise ApiException(response.status_code, response.text)
        if format == 'json':
            return response.json()
        if format == 'csv':
            return response.text
        raise Exception("Unknown format: " + format)

def test():
    config = Config()
    config.add_config_param('remote_url',
                            help='The URL of the remote server to connect to',
                            default='http://localhost:8000/api/odmx/v3/')
    argparser = argparse.ArgumentParser()
    config.add_args_to_argparser(argparser)
    args = argparser.parse_args()
    config.validate_config(args)
    api = ApiClient(config.remote_url)
    for obj in api.get(odmx.SamplingFeatureTimeseriesDatastreams):
        print(obj)


if __name__ == "__main__":
    test()
