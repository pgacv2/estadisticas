import concurrent.futures
import logging

# Module for abstracting the SOAP interface.
import zeep


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
        # I'm guessing 'NU_ENTIDAD' is the record ID, so sort by that field so the
        # user gets nicely ordered results.
        real_results = [x['DatosWsspMes'] for x in results._value_1._value_1]
        sorted_results = sorted(real_results, key=lambda x: x['NU_ENTIDAD'])
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
            threads = {executor.submit(self._soap_query, self.wsdl, year, mth): mth for mth in months_to_fetch}
            for thread in concurrent.futures.as_completed(threads):
                mth = threads[thread]
                try:
                    results[mth] = thread.result()
                except Exception as exc:
                    self.log.warning('Could not fetch data for {year}-{month} because: {exc}'.format(year=year,
                                                                                                     month=mth,
                                                                                                     exc=exc))

        # Now sort again, this time by date.
        sorted_results = []
        for mth in months_to_fetch:
            sorted_results += results[mth]
        return sorted_results
