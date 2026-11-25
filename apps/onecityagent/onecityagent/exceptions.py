class ToolError(Exception):
    """Raised when a tool encounters an error."""

    def __init__(self, message):
        self.message = message


class OneCityAgentError(Exception):
    """Base exception for all OneCityAgent errors"""


class TokenLimitExceeded(OneCityAgentError):
    """Exception raised when the token limit is exceeded"""