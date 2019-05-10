from ConfigParser import ConfigParser
from os import path
from set_working_directory import *


class ConfigRead:
    def __init__(self, config_file_path, config_file_name):
        self.config = ConfigParser()
        self.config.read(path.join(config_file_path, config_file_name))

    def get_config_section(self, section_name):
        if not section_name:
            raise Exception("Section name is required")

        if section_name not in self.config.sections():
            raise Exception("Invalid section name")

        return dict(self.config.items(section_name))

    def get_property(self, section_name, property_name):
        section_dict = self.get_config_section(section_name)

        if not property_name:
            raise Exception("Property name is required")

        if property_name not in section_dict.keys():
            raise Exception("Invalid property name")

        return section_dict[property_name]


if '__name__' == '__main__':
    print dummy
