from infraestructure.conf import getConf
from utils.read_params import ReadParams


class Query:
    """
    Class that store all querys
    """
    def __init__(self, config: getConf, params: ReadParams):
        self.config = config
        self.params = params

    def delete_data_rundeck(self) -> str:
        """
        Method that returns str with delete table statement
        """
        command = """
                delete from """ + self.config.db.table + """ where
                date_init::date between
                '""" + self.params.get_date_from() + """'::date and
                '""" + self.params.get_date_to() + """'::date """
        return command
