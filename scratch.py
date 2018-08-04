import requests
import pprint

url = 'http://67.203.240.172'


def headers(version, month=False):
    if version == 1.1:
        return {'Content-Type': 'text/xml; charset=utf-8',
                'SOAPAction': 'http://microsoft.com/webservices/DatosLey103{}'.format('Mes' if month else '')}
    elif version == 1.2:
        return {'Content-Type': 'application/soap+xml; charset=utf-8'}
    else:
        raise ValueError('Bad SOAP version')


def data(version, year, month=None):
    is_mes = 'Mes' if month else ''
    month = '<mes>{}</mes>'.format(str(month).zfill(2)) if month else ''
    if version == 1.1:
        return '''<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <DatosLey103{is_mes} xmlns="http://microsoft.com/webservices/">
              <anio>year</anio>{month}
            </DatosLey103{is_mes}>
          </soap:Body>
        </soap:Envelope>'''.format(is_mes=is_mes, year=year, month=month)
    elif version == 1.2:
        return '''<?xml version="1.0" encoding="utf-8"?>
        <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
          <soap12:Body>
            <DatosLey103{is_mes} xmlns="http://microsoft.com/webservices/">
              <anio>{year}</anio>{month}
            </DatosLey103{is_mes}>
          </soap12:Body>
        </soap12:Envelope>'''.format(is_mes=is_mes, year=year, month=month)
    else:
        raise ValueError('Bad SOAP version')


r = requests.post(url, headers=headers(1.1), data=data(1.1, 2017))
# print = pprint.pprint
print(r.text)
