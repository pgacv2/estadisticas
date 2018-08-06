import io
import os
import unittest
import unittest.mock as mock

import lib


class TestEstadisticas(unittest.TestCase):
    def setUp(self):
        self.patches = []
        mock_data = mock.MagicMock()
        mock_data._value_1._value_1 = [{'DatosWsspMes': {'NU_ENTIDAD': 1,
                                                         'RG_TRANS': 'trans 1',
                                                         'RG_COL': 'col 1',
                                                         'RG_ROW': 'row 1',
                                                         'RG_VALUE': 'value 1',
                                                         'CYYYYMM': 'date 1',
                                                         'TRANS_FILETYPE': 'filetype 1'}},
                                       {'DatosWsspMes': {'NU_ENTIDAD': 2,
                                                         'RG_TRANS': 'trans 2',
                                                         'RG_COL': 'col 2',
                                                         'RG_ROW': 'row 2',
                                                         'RG_VALUE': 'value 2',
                                                         'CYYYYMM': 'date 2',
                                                         'TRANS_FILETYPE': 'filetype 2'}}]
        mock_zeep = mock.MagicMock()
        mock_zeep.Client.return_value = mock_client = mock.MagicMock()
        mock_client.service.DatosLey103Mes.return_value = mock_data

        self.patches.append(mock.patch('lib.zeep', mock_zeep))

        for p in self.patches:
            p.start()

    def test_get_data(self):
        query_object = lib.StatsData('dummy wsdl')
        # The mock data has two records per month, so one month of results
        # should return two records, and 12 months should return 24.
        results = query_object.get_data(1234)
        self.assertEqual(24, len(results))
        self.assertEqual('value 2', results[1].RG_VALUE)
        results = query_object.get_data(1234, 5)
        self.assertEqual(2, len(results))

    def test_validate_path_fails_on_nonexistent_path(self):
        with self.assertRaises(OSError):
            lib.validate_path(os.path.join(os.path.dirname(__file__), 'some-nonexistent-path', 'file.txt'))

    def test_format_space_delimited(self):
        self.assertEqual(lib.format_space_delimited([1, 2, 'three'], 5),
                         '1    2    three\n')

    def tearDown(self):
        for p in self.patches:
            p.stop()


if __name__ == '__main__':
    unittest.main()
