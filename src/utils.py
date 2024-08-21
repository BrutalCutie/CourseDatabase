import psycopg2
from configparser import ConfigParser


def config(config_file='database.ini', section='postgresql'):
    parser_ = ConfigParser()
    parser_.read(config_file)

    params = {}

    if parser_.has_section(section):
        for key, value in parser_.items(section):
            params[key] = value

    else:
        raise TypeError('Cant find a file')

    return params
