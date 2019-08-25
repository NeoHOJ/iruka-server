import os


UID = os.getuid()
TMP_PATH = os.getenv('IRUKA_TMP_PATH', '/run/user/{uid}'.format(uid=UID))


class Config(object):
    def __init__(self):
        self.host = '[::]'
        self.port = 50051
        self.use_https = False

    def load_from_dict(self, config_dict):
        self.__dict__.update(config_dict)
