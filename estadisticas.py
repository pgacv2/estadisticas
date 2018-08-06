#!/usr/bin/python3

####################################################################
# Coding exercise for a position at the Instituto de Estadisticas  #
# Pedro G. Acevedo                                                 #
####################################################################

# El ejercicio consiste en la creacion de una aplicacion sencilla que se conecte
# a un web service y descargue la informacion en un archivo de texto o tabla.
# El web service a utilizar es http://67.203.240.172/L103WS.asmx. Los campos
# de informacion que este web service presenta son:
#
# NU_ENTIDAD RG_TRANS RG_COL RG_ROW RG_VALUE CYYYYMM TRANS_FILETYPE
#
# La aplicacion debera consumir el web service y presentar los datos que residen
# en este, y permitir su descarga en formato de texto plano (.txt) o CSV.

import argparse
import csv
import logging
import sys

import lib


# Read args from the command line, or prompt the user if they are not provided.
parser = argparse.ArgumentParser(description='Tool for downloading data from the Instituto de Estadisticas '
                                 'web service')
parser.add_argument('--year', type=int, choices=lib.year_range,
                    help='If this argument is specified, the tool will run non-interactively with any other '
                         'arguments supplied at the command line (or their defaults, if any of them are not '
                         'specified). If this argument is not specified, the tool will run interactively '
                         'and prompt the user for each argument.')
parser.add_argument('--month', type=int, choices=lib.month_range,
                    help='Which month (in the specified --year) to fetch data for.')
parser.add_argument('--format', type=str.lower, choices=lib.format_types, default='txt',
                    help='Output the data in either CSV or space-delimited format.')
parser.add_argument('--output-file', type=lib.validate_path,
                    help='The file in which the data will be saved. If omitted, the data will be displayed '
                         'onscreen. (Not recommended due to the high volume of data.)')
parser.add_argument('--verbose', action='store_true', help='Verbose debugging log messages.')
args = parser.parse_args()

# Set up logging.
log = logging.getLogger('estadisticas')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
log.addHandler(handler)
if args.verbose:
    log.setLevel(logging.DEBUG)
    handler.setLevel(logging.DEBUG)
else:
    log.setLevel(logging.WARNING)
    handler.setLevel(logging.WARNING)

interactive_mode = False
if not args.year:
    interactive_mode = True
log.debug('Interactive mode: {}'.format(interactive_mode))

# Start the event loop.
query = lib.StatsData('http://67.203.240.172/L103WS.asmx?WSDL')
while True:
    if interactive_mode:
        args = lib.interactive_menu()
        if not args:
            break
    elif not args.output_file:
        args.output_file = sys.stdout

    results = query.get_data(args.year, args.month)

    # In either format, first write the column headers, which we get from
    # the namedtuple structure. Then write everything else.
    log.debug('Starting output')
    if args.format == 'txt':
        args.output_file.write(lib.format_space_delimited(results[0]._fields))
        for record in results:
            args.output_file.write(lib.format_space_delimited(record))
    else:
        writer = csv.writer(args.output_file)
        writer.writerow(results[0]._fields)
        writer.writerows(results)

    log.debug('Closing file handle to {}'.format(args.output_file.name))
    if args.output_file != sys.stdout:
        args.output_file.close()

    if interactive_mode and lib.prompt_for_another_query():
        continue
    else:
        break
