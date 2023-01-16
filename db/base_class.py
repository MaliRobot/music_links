import typing as t
from sqlalchemy.orm import registry

class_registry: t.Dict = {}

mapper_registry = registry()
Base = mapper_registry.generate_base()
