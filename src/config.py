from os import path

from utils.read_config import ConfigRead

config_read_obj = ConfigRead(path.dirname(__file__), 'config/config.ini')
