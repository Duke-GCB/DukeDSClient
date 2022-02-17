from ddsc.exceptions import DDSUserException


class DDSAzureSetupException(DDSUserException):
    pass


class DDSAzCopyException(DDSUserException):
    pass


class AzureUserNotFoundException(DDSUserException):
    pass
