import collections
import concurrent.futures
import csv
import io
import logging

# Module for abstracting the SOAP interface.
import zeep

# A namedtuple is useful for keeping the field names in order.
Record = collections.namedtuple('Record', 'NU_ENTIDAD RG_TRANS RG_COL RG_ROW RG_VALUE CYYYYMM TRANS_FILETYPE')

# It is also useful for storing argument values that the user provides interactively.
UserValues = collections.namedtuple('UserValues', 'year month format output_file')

month_range = range(1, 13, 1)
year_range = range(2007, 2019, 1)
format_types = ['csv', 'txt']
quit_values = ['q', 'quit']


def interactive_menu():
    quit_prompt = 'or enter [Q]uit to quit the application.'
    year = None
    month = None
    fmt = None
    output_file = None

    while year not in year_range:
        print('Enter a year between {} and {} (inclusive)'.format(min(year_range), max(year_range)))
        print(quit_prompt)
        year = input().strip().lower()
        if year in quit_values:
            return None
        else:
            try:
                year = int(year)
                if year not in year_range:
                    raise ValueError
            except ValueError:
                print('That is not a valid year.')

    while month not in month_range and month != '':
        print('Enter a month between {} and {} (inclusive), press Enter to fetch data '
              'for the whole year,'.format(min(month_range), max(year_range)))
        print(quit_prompt)
        month = input().strip().lower()
        if month in quit_values:
            return None
        elif month == '':
            break
        else:
            try:
                month = int(month)
                if month not in month_range:
                    raise ValueError
            except ValueError:
                print('That is not a valid month.')

    while fmt not in format_types:
        print('Enter one of the following formats {}'.format(format_types))
        print(quit_prompt)
        fmt = input().strip().lower()
        if fmt in quit_values:
            return None

    while not output_file:
        print('Enter the path to save the data, press Enter to output to stdout,')
        print(quit_prompt)
        path = input().strip()
        if path in quit_values:
            return None
        else:
            try:
                output_file = open(path, 'w', encoding='utf-8')
            except OSError as e:
                print('{} is not writable because: {}'.format(path, e))

    return UserValues(year=year, month=month, format=fmt, output_file=output_file)


class StatsData:
    def __init__(self, wsdl):
        self.log = logging.getLogger('estadisticas')
        self.wsdl = wsdl

    def _soap_query(self, wsdl, year, month):
        """The actual SOAP call."""
        client = zeep.Client(wsdl)
        results = client.service.DatosLey103Mes(str(year), str(month).zfill(2))

        # The real results are a few layers down. The first _value_1 is a wrapper,
        # and the second _value_1 is a list of dictionaries that have a single key
        # ('DatosWsspMes'). The value of that key is the actual result we want.
        # That value, however, is not a real mapping (some zeep object), so "cast"
        # it to a dictionary first and then make a tuple out of it.
        #
        # I'm guessing 'NU_ENTIDAD' is the record ID, so sort by that field so the
        # user gets nicely ordered results.
        real_results = [Record(**{field: x['DatosWsspMes'][field] for field in dir(x['DatosWsspMes'])})
                        for x in results._value_1._value_1]
        sorted_results = sorted(real_results, key=lambda x: x.NU_ENTIDAD)
        return sorted_results

    def get_data(self, year, month=None):
        """Thread manager for each query.

        Spawn at least one thread, regardless of how much data is requested.
        If the user wants a whole year, spawn twelve threads (one per month)
        for better performance."""
        if month:
            months_to_fetch = [month]
        else:
            months_to_fetch = range(1, 13, 1)
        self.log.debug('Data to fetch: {}-{}'.format(year, month or 'all'))

        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(months_to_fetch)) as executor:
            threads = {}
            for mth in months_to_fetch:
                threads[executor.submit(self._soap_query, self.wsdl, year, mth)] = mth
                self.log.debug('Spawned thread for {}-{}'.format(year, mth))

            for thread in concurrent.futures.as_completed(threads):
                mth = threads[thread]
                try:
                    results[mth] = thread.result()
                    self.log.debug('Downloaded data for {}-{}'.format(year, mth))
                except Exception as exc:
                    self.log.warning('Could not fetch data for {}-{} because: {}'.format(year, mth, exc))

        # Now sort again, this time by date.
        sorted_results = []
        for mth in months_to_fetch:
            sorted_results += results[mth]
        return sorted_results


class DataFormatter:
    """Formatter class.

    'txt' format is space-delimited with a width of 14 by default. (Some of those
    decimals are pretty long.) 'csv' format is comma-delimited. The column width
    is ignored in the CSV format."""
    def __init__(self, fmt, column_width=14):
        self.format = fmt
        self.column_width = column_width

    def _delimit(self, values):
        delimited_string = ''
        if self.format == 'txt':
            for val in values:
                delimited_string += '{value: <{col_width}}'.format(value=val, col_width=self.column_width)
        return delimited_string

    def format_header(self, record):
        return self._delimit(record._fields)

    def format_row(self, record):
        return self._delimit(record)
