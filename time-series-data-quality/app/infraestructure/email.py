# pylint: disable=no-member
# utf-8
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.encoders import encode_base64
import pandas as pd
from infraestructure.conf import getConf
from utils.read_params import ReadParams

class Email:
    """
    Class that allow send email
    """
    def __init__(self,
                 params: ReadParams,
                 conf: getConf,
                 subject='',
                 body='',
                 email_from=None,
                 email_to=None):
        self.email_from = params.email_from \
            if email_from is None else email_from
        self.email_to = params.email_to \
            if email_to is None else email_to
        self.subject = subject
        self.body = body
        self.smtp_server = conf.SMPTConf.host
        self.logger = logging.getLogger('email')

    def send_email(self, msg):
        """
        Method [ send_email ] create a SMTP instance with host ip in
        [ smpt_server ] instance variable and send email in msg argument
        Param [ msg ] is a MIME object ready to send
        """
        server = smtplib.SMTP(self.smtp_server)
        server.sendmail(self.email_from,
                        self.email_to,
                        msg.as_string())

    def send_email_with_csv(self, data_to_send):
        """
        Method [send_email_with_csv] send a email with one or multiples
        csv files attached to recipients.
        Param [ data_to send ] is a array of tuple composed by
                (name_file_send, dataframe_to_csv)
            Param [name_file_send] is the name of file,
                must contain the extension ".csv"
            Param [dataframe_to_csv] is a pandas dataframe with data that
                will be saved as a csv and sent
        """
        self.logger.info('Preparing email')
        msg = MIMEMultipart('mixed')
        msg['Subject'] = self.subject
        msg['From'] = self.email_from
        msg['To'] = ", ".join(self.email_to)
        msg.attach(MIMEText(self.body, 'plain'))
        for file in data_to_send:
            self.logger.info('Creating files')
            file[1].to_csv(file[0], sep=";", index=False)
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(file[0], "rb").read())
            encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment', filename=file[0])
            msg.attach(part)
            self.logger.info('File {} charged'.format(file[0]))
        self.send_email(msg)

    def send_email_with_excel(self, file_name, excel_sheets):
        """
        Method [send_email_with_excel] send a email with one
        excel files attached with multiples sheets to recipients.
        Param [file_name] is the name of file,
                must contain the extension ".xlsx"
        Param [ excel_sheets send ] is a array of tuple composed by
                (name_sheet, dataframe)
            Param [name_sheet] is the name of sheet
            Param [dataframe] is a pandas dataframe that will be
                saved in sheet
        """
        self.logger.info('Preparing email')
        msg = MIMEMultipart('mixed')
        msg['Subject'] = self.subject
        msg['From'] = self.email_from
        msg['To'] = ", ".join(self.email_to)
        msg.attach(MIMEText(self.body, 'plain'))
        # pylint: disable=E0110
        writer = pd.ExcelWriter(file_name,
                                engine='xlsxwriter')
        for sheet in excel_sheets:
            sheet[1].to_excel(writer,
                              sheet_name=sheet[0],
                              index=False)
        writer.save()
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(file_name, "rb").read())
        encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment', filename=file_name)
        msg.attach(part)
        self.logger.info('File {} charged'.format(file_name))
        self.send_email(msg)

    def send_email_with_file(self, files):
        """
        Method [send_email_w_file] send a email with one or multiples
        files attached to recipients.
        Param [ files ] is a array of name of files to send
        """
        self.logger.info('Preparing email')
        msg = MIMEMultipart('mixed')
        msg['Subject'] = self.subject
        msg['From'] = self.email_from
        msg['To'] = ", ".join(self.email_to)
        msg.attach(MIMEText(self.body, 'plain'))
        for file in files:
            self.logger.info('Charging files')
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(file, "rb").read())
            encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment', filename=file)
            msg.attach(part)
            self.logger.info('File {} charged'.format(file))
        self.send_email(msg)
