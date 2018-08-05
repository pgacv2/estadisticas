import concurrent.futures
import zeep

class StatsData:
    def __init__(self, wsdl):
        self.wsdl = wsdl

    def _soap_query(self, wsdl, year, month):
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
        # Spawn at least one thread, regardless of how much data is requested.
        # If the user wants a whole year, spawn twelve threads (one per month)
        # for better performance.
        if month:
            months_to_fetch = [month]
        else:
            months_to_fetch = range(1, 13, 1)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(months_to_fetch)) as executor:
            # Start the load operations and mark each future with its URL
            threads = {executor.submit(self._soap_query, self.wsdl, year, mth): url for mth in months_to_fetch}
            for future in concurrent.futures.as_completed(threads):
                url = threads[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (url, exc))
                else:
                    print('%r page is %d bytes' % (url, len(data)))