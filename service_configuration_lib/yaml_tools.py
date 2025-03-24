import yaml

try:
    from yaml import CSafeLoader as Loader
    from yaml import CSafeDumper as Dumper
except ImportError:  # pragma: no cover
    from yaml import SafeLoader as Loader  # type: ignore
    from yaml import SafeDumper as Dumper  # type: ignore


def dump(*args, **kwargs):
    kwargs['Dumper'] = Dumper
    return yaml.dump(*args, **kwargs)


def dump_all(*args, **kwargs):
    kwargs['Dumper'] = Dumper
    return yaml.dump_all(*args, **kwargs)


def load(*args, **kwargs):
    kwargs['Loader'] = Loader
    return yaml.load(*args, **kwargs)


def load_all(*args, **kwargs):
    kwargs['Loader'] = Loader
    return yaml.load_all(*args, **kwargs)


safe_dump = dump
safe_dump_all = dump_all
safe_load = load
safe_load_all = load_all
