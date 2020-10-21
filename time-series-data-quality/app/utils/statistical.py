import pandas as pd
import numpy as np
from infraestructure.conf import getConf
from utils.read_params import ReadParams
from utils.etl.extraction import Extraction
from utils.etl.load import Load
from utils.etl.send_information import SendInformation

class Statistical:

    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def get_mean_cal(self, df_eval):
        mean_var = df_eval.groupby(["entity", "entity_var"])['mtr_var'].mean()
        df_eval = df_eval.set_index(["entity", "entity_var"])
        df_eval["mean_var"] = mean_var
        df_eval = df_eval.reset_index()

        return df_eval

    def get_std_dev_cal(self, df_eval):
        std_dev_var = df_eval.groupby(["entity", "entity_var"])['mtr_var'].std()
        df_eval = df_eval.set_index(["entity", "entity_var"])
        df_eval["std_dev_var"] = std_dev_var
        df_eval = df_eval.reset_index()

        return df_eval

    def get_eval_w_rules(self, df_eval):
        # Rule 1
        df_eval['eval_rule_1'] = np.\
            where(np.abs(df_eval.mtr_var - df_eval.mean_var) > 3.0 * \
                df_eval.std_dev_var, 1, 0)

        # Rule 2
        df_eval['eval_rule_2'] = np.\
            where(np.abs(df_eval.mtr_var - df_eval.mean_var) > 2.0 * \
                df_eval.std_dev_var, 1, 0)

        # Rule 3
        df_eval['eval_rule_3'] = np.\
            where(np.abs(df_eval.mtr_var - df_eval.mean_var) > \
                df_eval.std_dev_var, 1, 0)

        # Rule 4
        df_eval['eval_rule_4'] = np.\
            where((df_eval.mtr_var - df_eval.mean_var) >= 0, 1, -1)

        return df_eval

    def get_w_rule_1(self, df_eval):
        """
            Input:
            value: current value sorted by date of numbers

            Description: One point is more than 3 standard deviations
            from the mean.
            Problem: One sample (two shown in this case) is grossly
            out of control.
            Solves: identifying outliers
        """
        # pylint: disable=R1719
        w_rule_1_var = pd.DataFrame(df_eval.groupby(["entity", "entity_var"])\
            ['eval_rule_1'].agg(lambda x: x.iloc[0]))

        w_rule_1_var['w_rule_1'] = w_rule_1_var['eval_rule_1'].\
            apply(lambda x: True if x > 0 else False)

        w_rule_1_var = w_rule_1_var.reset_index()

        return w_rule_1_var

    def get_w_rule_2(self, df_eval):
        """
            Input:
            value: current value sorted by date of numbers

            Description: One point is more than 3 standard deviations
            from the mean.
            Problem: One sample (two shown in this case) is grossly
            out of control.
            Solves: identifying outliers
            # pylint: disable-all
            # pylint: disable=W0108
        """
        # pylint: disable=W0108, R1719
        w_rule_2_var = pd.DataFrame(df_eval.groupby(["entity", "entity_var"])\
            ['eval_rule_2'].agg(lambda x: x.iloc[0:3].tolist()))

        w_rule_2_var['w_rule_2_val'] = w_rule_2_var['eval_rule_2'].\
            apply(lambda x: sum(x))

        w_rule_2_var['w_rule_2'] = w_rule_2_var['w_rule_2_val'].\
            apply(lambda x: True if x >= 2 else False)

        w_rule_2_var = w_rule_2_var.reset_index()

        return w_rule_2_var

    def get_w_rule_3(self, df_eval):
        """
            Input:
            value: current value sorted by date of numbers

            Description: One point is more than 3 standard deviations
            from the mean.
            Problem: One sample (two shown in this case) is grossly
            out of control.
            Solves: identifying outliers
        """
        # pylint: disable=W0108, R1719
        w_rule_3_var = pd.DataFrame(df_eval.groupby(["entity", "entity_var"])\
            ['eval_rule_3'].agg(lambda x: x.iloc[0:5].tolist()))

        w_rule_3_var['w_rule_3_val'] = w_rule_3_var['eval_rule_3'].\
            apply(lambda x: sum(x))

        w_rule_3_var['w_rule_3'] = w_rule_3_var['w_rule_3_val'].\
            apply(lambda x: True if x >= 4 else False)

        w_rule_3_var = w_rule_3_var.reset_index()

        return w_rule_3_var

    def get_w_rule_4(self, df_eval):
        """
            Input:
            value: current value sorted by date of numbers

            Description: One point is more than 3 standard deviations
            from the mean.
            Problem: One sample (two shown in this case) is grossly
            out of control.
            Solves: identifying outliers
        """
        # pylint: disable=W0108, R1719
        w_rule_4_var = pd.DataFrame(df_eval.groupby(["entity", "entity_var"])\
            ['eval_rule_4'].agg(lambda x: x.iloc[0:9].tolist()))

        w_rule_4_var['w_rule_4_val'] = w_rule_4_var['eval_rule_4'].\
            apply(lambda x: sum(x))

        w_rule_4_var['w_rule_4'] = w_rule_4_var['w_rule_4_val'].\
            apply(lambda x: False if x < 9 & x > -9 else True)

        w_rule_4_var = w_rule_4_var.reset_index()

        return w_rule_4_var

    def generate(self):
        load = Load()
        # Instance input data object
        df_eval_obj = Extraction(self.conf, self.params)
        # df_eval = df_eval_obj.get_data_pulse_ts_ad_phone_number_called()

        df_eval = pd.concat([
            df_eval_obj.get_data_pulse_ts_ad_phone_number_called(),
            df_eval_obj.get_data_dw_ts_ad_phone_number_called()
        ], ignore_index=True).sort_values(by=['fecha'], ascending=True)


        # Add base statistics measures to time series metrics
        df_stadistic_measures = self.get_mean_cal(df_eval)
        df_stadistic_measures = self.get_std_dev_cal(df_stadistic_measures)

        # Evaluate 4 principals rules of Western Electric
        df_eval_w_rules = self.get_eval_w_rules(df_stadistic_measures)
        df_result_eval = df_eval_w_rules
        w_rule_1_var = self.get_w_rule_1(df_result_eval)
        w_rule_2_var = self.get_w_rule_2(df_result_eval)
        w_rule_3_var = self.get_w_rule_3(df_result_eval)
        w_rule_4_var = self.get_w_rule_4(df_result_eval)

        # Generate output eval
        df_result_eval = pd.merge(w_rule_1_var,
                                  w_rule_2_var,
                                  on=['entity', 'entity_var'])
        df_result_eval = pd.merge(df_result_eval,
                                  w_rule_3_var,
                                  on=['entity', 'entity_var'])
        df_result_eval = pd.merge(df_result_eval,
                                  w_rule_4_var,
                                  on=['entity', 'entity_var'])
        df_result_eval['timedate'] = self.params.date_to
        df_result_eval[['w_rule_1_rank',
                        'w_rule_2_rank',
                        'w_rule_3_rank',
                        'w_rule_4_rank']] = \
                            df_result_eval[['w_rule_1',
                                            'w_rule_2',
                                            'w_rule_3',
                                            'w_rule_4']].astype(int)
        df_result_eval["eval_rank"] = \
            df_result_eval[['w_rule_1_rank',
                            'w_rule_2_rank',
                            'w_rule_3_rank',
                            'w_rule_4_rank']].astype(int).sum(axis=1)
        df_result_eval = df_result_eval[['timedate',
                                         'entity',
                                         'entity_var',
                                         'w_rule_1',
                                         'w_rule_2',
                                         'w_rule_3',
                                         'w_rule_4',
                                         'eval_rank']
                                        ]

        load.write_data_dwh_output_eval(self.conf,
                                        self.params,
                                        df_result_eval)
        df_to_save = df_eval.rename(
            columns={'fecha': 'timedate'})
        df_to_save['timedate'] = df_to_save['timedate'].apply(
            lambda x: x.strftime('%Y-%m-%d')
        )
        df_to_save = df_to_save[
            df_to_save.timedate == self.params.get_date_from()]
        load.write_data_timelines_in_dwh(self.conf,
                                         self.params,
                                         df_to_save)

        SendInformation(self.conf, self.params)\
            .send_information(df_result_eval[[
                'entity',
                'entity_var',
                'w_rule_1',
                'w_rule_2',
                'w_rule_3',
                'w_rule_4'
            ]], df_eval_obj.get_data_last_day())
        return True
