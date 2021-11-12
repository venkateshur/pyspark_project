import configparser


def load_app_config(config_path, conf_type=None):
    conf = configparser.ConfigParser()
    return conf.read(config_path)


config = load_app_config("C:\Users\Venky\PycharmProjects\pyspark_project\conf\app_conf.cfg")

dev_source = config['DEV']['source_system']
dev_target = config['DEV']['target_system']

prod_source = config['PROD']['source_system']
prod_target = config['PROD']['target_system']


