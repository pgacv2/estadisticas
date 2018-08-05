#!/usr/bin/python3
# Coding exercise for a position at the Instituto de Estadisticas
# Pedro G. Acevedo
# 2018-08-03
#
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
import logging
import sys

import lib

month_range = range(1, 13, 1)
year_range = range(2007, 2019, 1)
format_types = ['csv', 'txt']
yes_values = ['Y', 'y']
no_values = ['N', 'n']
exit_values = ['E', 'e']

# Read args from the command line, or prompt the user if they are not provided.
parser = argparse.ArgumentParser()
parser.add_argument('-v', '--verbose', action='store_true')
parser.add_argument('--format', choices=format_types, default='txt')
parser.add_argument('--month', type=int, choices=month_range)
parser.add_argument('--year', type=int, choices=year_range)
parser.add_argument('--output-file', type=argparse.FileType('w', encoding='utf-8'))

args = parser.parse_args()
if args.year:
    year = args.year
    month = args.month
elif args.month:
    parser.error('A month must be accompanied by a year')
else:
    pass

# Set up logging.
log = logging.getLogger('estadisticas')
handler = logging.StreamHandler(sys.stdout)
log.addHandler(handler)
if args.verbose:
    log.setLevel(logging.DEBUG)
    handler.setLevel(logging.DEBUG)
else:
    log.setLevel(logging.WARNING)
    handler.setLevel(logging.WARNING)

Query = lib.StatsData('http://67.203.240.172/L103WS.asmx?WSDL')
data = Query.get_data(args.year, args.month)
formatter = lib.DataFormatter('txt')

print(formatter.format_header(data[0]))
for x in data:
    print(formatter.format_row(x))
