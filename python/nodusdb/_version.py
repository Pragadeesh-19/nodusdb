from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("nodusdb")
except PackageNotFoundError:
    __version__ = "0+unknown"
