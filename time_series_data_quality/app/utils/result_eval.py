import pandas as pd
from utils.statistical import Statistical

STATICS_OBJ = Statistical()

class ResultEval:

    def get_df_result_eval(self, df_eval, process_date):

        df_stadistic_measures = STATICS_OBJ.get_mean_cal(df_eval)

        df_stadistic_measures = STATICS_OBJ.get_std_dev_cal(df_stadistic_measures)

        df_eval_w_rules = STATICS_OBJ.get_eval_w_rules(df_stadistic_measures)

        df_result_eval = df_eval_w_rules

        w_rule_1_var = STATICS_OBJ.get_w_rule_1(df_result_eval)

        w_rule_2_var = STATICS_OBJ.get_w_rule_2(df_result_eval)

        w_rule_3_var = STATICS_OBJ.get_w_rule_3(df_result_eval)

        w_rule_4_var = STATICS_OBJ.get_w_rule_4(df_result_eval)

        df_result_eval = pd.merge(w_rule_1_var, w_rule_2_var, on=['entity', 'entity_var'])

        df_result_eval = pd.merge(df_result_eval, w_rule_3_var, on=['entity', 'entity_var'])

        df_result_eval = pd.merge(df_result_eval, w_rule_4_var, on=['entity', 'entity_var'])

        df_result_eval['timedate'] = process_date

        df_result_eval[['w_rule_1_rank', 'w_rule_2_rank', 'w_rule_3_rank', 'w_rule_4_rank']] =\
        df_result_eval[['w_rule_1', 'w_rule_2', 'w_rule_3', 'w_rule_4']].astype(int)

        df_result_eval["eval_rank"] = df_result_eval[['w_rule_1_rank', 'w_rule_2_rank',
                                                      'w_rule_3_rank', 'w_rule_4_rank']].\
                                                          astype(int).sum(axis=1)

        df_result_eval = df_result_eval[['timedate', 'entity', 'entity_var', 'w_rule_1',
                                         'w_rule_2', 'w_rule_3', 'w_rule_4', 'eval_rank']]
        return df_result_eval
