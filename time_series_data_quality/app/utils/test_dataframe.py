import pandas as pd

class TestDf:

    def get_test_df(self):
    
        df = pd.DataFrame(
                    {
                    'timedate': [
                                 '2020-01-01', '2020-01-02', '2020-01-03',
                                 '2020-01-04', '2020-01-05', '2020-01-06',
                                 '2020-01-07', '2020-01-08', '2020-01-09',
                                 '2020-01-10', '2020-01-11', '2020-01-12', 
                                 '2020-01-13', '2020-01-14', '2020-01-15',
                                 '2020-01-16', '2020-01-17', '2020-01-18', 
                                 '2020-01-19', '2020-01-01', '2020-01-02',
                                 '2020-01-03', '2020-01-04', '2020-01-05',
                                 '2020-01-06', '2020-01-07', '2020-01-08',
                                 '2020-01-09', '2020-01-10', '2020-01-11', 
                                 '2020-01-12', '2020-01-13', '2020-01-14', 
                                 '2020-01-15', '2020-01-16', '2020-01-17',
                                 '2020-01-18', '2020-01-19'
                                 ],
                    'entity': [
                               'tabla_1', 'tabla_1', 'tabla_1', 'tabla_1',
                               'tabla_1', 'tabla_1', 'tabla_1', 'tabla_1',
                               'tabla_1', 'tabla_1', 'tabla_1', 'tabla_1',
                               'tabla_1', 'tabla_1', 'tabla_1', 'tabla_1', 
                               'tabla_1', 'tabla_1', 'tabla_1', 'tabla_1',
                               'tabla_1', 'tabla_1', 'tabla_1', 'tabla_1',
                               'tabla_1', 'tabla_1', 'tabla_1', 'tabla_1',
                               'tabla_1', 'tabla_1', 'tabla_1', 'tabla_1',
                               'tabla_1', 'tabla_1', 'tabla_1', 'tabla_1', 
                               'tabla_1', 'tabla_1'
                               ],
                    'entity_var': [
                                   'var_1', 'var_1', 'var_1', 'var_1',
                                   'var_1', 'var_1', 'var_1', 'var_1',
                                   'var_1', 'var_1', 'var_1', 'var_1',
                                   'var_1', 'var_1', 'var_1', 'var_1',
                                   'var_1', 'var_1', 'var_1', 'var_2',
                                   'var_2', 'var_2', 'var_2', 'var_2',
                                   'var_2', 'var_2', 'var_2', 'var_2',
                                   'var_2', 'var_2', 'var_2', 'var_2',
                                   'var_2', 'var_2', 'var_2', 'var_2',
                                   'var_2', 'var_2' 
                                   ],
                    #Rule 1 validation
                    #'mtr_var': [5, 6, 3, 6, 2, 7, 8, 2, 9, 12, 7, 8, 2, 9, 12, 2, 9, 12, 100]

                    #Rule 2 validation
                    #'mtr_var': [5, 6, 3, 6, 2, 7, 8, 2, 9, 12, 7, 8, 2, 9, 12, 2, 9, 90, 100]
                    
                    #Rule 3 validation
                    #'mtr_var': [5, 6, 3, 6, 2, 7, 8, 2, 9, 12, 7, 8, 2, 9, 100, 10, 106, 104, 103]

                    #Rule 4 validation
                    'mtr_var': [5, 6, 3, 6, 2, 7, 8, 2, 9, 12, 100, 104, 106, 104, 103, 105, 105, 107, 103,
                                5, 6, 3, 6, 2, 7, 8, 2, 9, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12]             
                    }
                ).sort_values(by=['timedate', 'entity', 'entity_var'],ascending=False)
        return df