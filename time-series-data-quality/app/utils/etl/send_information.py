import pandas as pd
import numpy as np
import logging
from infraestructure.conf import getConf
from infraestructure.email import Email
from utils.read_params import ReadParams
from utils.etl.extraction import Extraction
from utils.etl.load import Load

class SendInformation:
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf
        self.log = logging.getLogger('EmailSender')

    def prepare_rules(self, row):
        if row['w_rule_1']:
            row['w_rule_2'] = False
            row['w_rule_3'] = False
            row['w_rule_4'] = False
        if row['w_rule_2'] or ['w_rule_3']:
            row['w_rule_4'] = False
        return row

    def rule_to_string(self, row):
        rule = row['rule']
        if rule == 'w_rule_1':
            row['message'] = \
                'Valores lejanos a la media en linea de tiempo (Regla 1)'
        elif rule in ('w_rule_2', 'w_rule_3'):
            row['message'] = \
                'Anomalias en la linea de tiempo (Regla 2 y 3)'
        elif rule == 'w_rule_4':
            row['message'] = \
                'Tendencia a cambio en la linea de tiempo (Regla 4)'
        return row

    def separe_timelines_by_urgency(self, df_today, df_yest):
        rl_ur_1 = df_today[df_today['rule'] == 'w_rule_1']
        rl_2_3 = df_today[(df_today.rule == 'w_rule_2') |
                          (df_today.rule == 'w_rule_3')]
        rl_2_3 = rl_2_3.merge(df_yest, how='left', on=['entity', 'entity_var'])

        rl_ur_2_3 = rl_2_3[rl_2_3.value == 1.0]
        df_urg = pd.concat([rl_ur_1, rl_ur_2_3],
                           ignore_index=True).drop(columns=['rule', 'value'])
        df_urg = df_urg.drop_duplicates(ignore_index=True)

        rl_no_ur_4 = df_today[df_today['rule'] == 'w_rule_4']
        rl_no_ur_2_3 = rl_2_3[rl_2_3.value != 1.0]
        df_no_urg = pd.concat([rl_no_ur_2_3, rl_no_ur_4],
                              ignore_index=True).drop(columns=['rule', 'value'])
        df_no_urg = df_no_urg.drop_duplicates(ignore_index=True)
        return df_urg, df_no_urg

    def send_information(self, df, df_last):
        self.log.info('Starting ETL to send info')

        df_melt = df.apply(self.prepare_rules, axis=1)
        df_melt = pd.melt(df_melt, id_vars=["entity", "entity_var"],
                          var_name="rule", value_name="val")

        df_melt = df_melt[df_melt.val]
        df_melt = df_melt.drop(columns=['val'])\
            .apply(self.rule_to_string, axis=1)

        df_yest = df_last[
            np.logical_or(df_last.w_rule_2, df_last.w_rule_3)
        ].drop(columns=['w_rule_1', 'w_rule_4'])
        df_yest = pd.melt(df_yest,
                          id_vars=["entity", "entity_var"],
                          var_name="rule_2", value_name="val")

        df_yest = df_yest[df_yest.val][["entity", "entity_var"]]
        df_yest = df_yest.drop_duplicates(ignore_index=True)
        df_yest['value'] = 1
        df_urg, df_no_urg = self.separe_timelines_by_urgency(df_melt, df_yest)
        
        self.log.info('Sending Email')
        Email(
            self.params,
            self.conf,
            subject="Data Quality",
            body="""
            Estimad@s,
                Se adjunta resultado revision de calidad de datos.
            Saludos,
            Data Team
            -----
            Este correo ha sido generado de forma automática.
            """,
            email_to=['ricardo.alvarez@adevinta.com']
        ).send_email_with_excel(
            file_name='data_quality_review.xlsx',
            excel_sheets=[
                ('Alta Prioridad', df_urg),
                ('Baja Prioridad', df_no_urg)
            ]
        )
        self.log.info('Email Sended')

