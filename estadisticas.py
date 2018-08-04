# Coding exercise for a position at the Instituto de Estadisticas
# Pedro G. Acevedo
# 2018-08-03
#
# El ejercicio consiste en la creacion de una aplicacion sencilla que se conecte
# a un web service y descargue la informacion en un archivo de texto o tabla.
# El web service a utilizar es http://67.203.240.172/L103WS.asmx. Los campos
# de informacion que este web service presenta son
#
# NU_ENTIDAD RG_TRANS RG_COL RG_ROW RG_VALUE CYYYYMM TRANS_FILETYPE
#
# La aplicacion debera consumir el web service y presentar los datos que residen
# en este, y permitir su descarga en formato de texto plano (.txt) o CSV.

import argparse
import logging
import sys

# Module for abstracting the SOAP interface.
import zeep

parser = argparse.ArgumentParser()
parser.add_argument('-v', '--verbose', action='store_true', help='Verbose')
args = parser.parse_args()

log = logging.getLogger('stats')
handler = logging.StreamHandler(sys.stdout)
log.addHandler(handler)
if args.verbose:
    log.setLevel(logging.DEBUG)
    handler.setLevel(logging.DEBUG)

client = zeep.Client('http://67.203.240.172/L103WS.asmx?WSDL')
x = client.service.DatosLey103Mes('10', '2010')
y = client.service.DatosLey103('2010')
print(dir(x))
print(dir(y))
