# Exception
#       |__Error
#          |__TooManyClients

class Error(Exception):
    pass

class TooManyClients(Error):
    pass
