"""OneCity package. Runtime configuration is loaded only by explicit entrypoints."""

class _LegacyConfig:
    def __getattr__(self, name):
        from onecityagent.config import Config
        return getattr(Config(), name)

config = _LegacyConfig()
