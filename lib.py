####################################################################
# Coding exercise for a position at the Instituto de Estadisticas  #
# Pedro G. Acevedo                                                 #
####################################################################

import collections
import concurrent.futures
import logging
import os
import sys

# Module for abstracting the SOAP interface.
import zeep

# A namedtuple keeps the field names in order when printing to CSV.
# We also use it to store arguments that the user provides interactively
# with fields that match the command-line argument names.
_response_field_names = ['NU_ENTIDAD', 'RG_TRANS', 'RG_COL', 'RG_ROW', 'RG_VALUE', 'CYYYYMM', 'TRANS_FILETYPE']
Record = collections.namedtuple('Record', _response_field_names)
UserValues = collections.namedtuple('UserValues', 'year month format output_file')

month_range = range(1, 13, 1)
year_range = range(2007, 2019, 1)
format_types = ['csv', 'txt']
quit_values = ['q', 'quit']
quit_prompt = 'or enter [Q]uit to quit the application:'


def validate_path(path):
    """Check that the output file the user selected is writable and return a handle to it.

    We can't use argparse's FileType to validate the path because it doesn't
    take the newline argument that we need in order to suppress the extra
    blank lines in the CSV output. So we need to manually add the newline
    to each line in the space-delimited output."""
    return open(os.path.abspath(path), 'w', encoding='utf-8', newline='')


def interactive_menu():
    """Prompt the user for values."""
    year = None
    month = None
    fmt = None
    output_file = None

    while year not in year_range:
        print()
        print('Enter a year between {} and {} (inclusive)'.format(min(year_range), max(year_range)),
              quit_prompt)
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
        print()
        print('Enter a month between {} and {} (inclusive), leave it blank to fetch '
              'data for the whole year,'.format(min(month_range), max(month_range)),
              quit_prompt)
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
        print()
        print('Enter one of the following formats {}'.format(format_types), quit_prompt)
        fmt = input().strip().lower()
        if fmt in quit_values:
            return None
        elif fmt not in format_types:
            print('That is not a valid format.')

    while not output_file:
        print()
        print('Enter the path to save the data, leave it blank to output to stdout,',
              quit_prompt)
        path = input().strip()
        if path in quit_values:
            return None
        elif path == '':
            output_file = sys.stdout
        else:
            try:
                output_file = validate_path(path)
            except OSError as e:
                print('{} is not writable because: {}'.format(path, e))

    print()
    return UserValues(year=year, month=month, format=fmt, output_file=output_file)


def prompt_for_another_query():
    response = None
    print('Query complete.')
    while response not in quit_values and response != '':
        print('Press Enter to run another query', quit_prompt)
        response = input().strip().lower()
        if response in quit_values:
            return False
        elif response == '':
            return True
        else:
            continue


def format_space_delimited(values, column_width=14):
    delimited_string = ''
    for val in values:
        delimited_string += '{value: <{col_width}}'.format(value=val, col_width=column_width)
    return delimited_string + '\n'


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
        real_results = [Record(**{field: x['DatosWsspMes'][field] for field in _response_field_names})
                        for x in results._value_1._value_1]
        return real_results

    def get_data(self, year, month=None):
        """Thread manager for each query.

        Spawn at least one thread, regardless of how much data is requested.
        If the user wants a whole year, spawn twelve threads (one per month)
        for better performance: >60 minutes one thread for the whole year
        calling DatosLey103() vs. ~14 minutes calling 12 instances of
        DatosLey103Mes()."""
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

        # Sort the results by month.
        sorted_results = []
        for mth in months_to_fetch:
            sorted_results += results[mth]
        return sorted_results
