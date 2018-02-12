class BaseError(Exception):
    """Base class for all exceptions"""
    pass


class TraderError(BaseError):
    """"Raised when a Trader class replies with an error"""
    pass


class NotImplemented(TraderError):
    """Raised if the endpoint is not offered/not yet implemented by the Trader Class"""
    pass
