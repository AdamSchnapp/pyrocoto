#!/usr/bin/env python
from abc import ABC, abstractmethod


class Validator(ABC):
    def __set_name__(self, owner, name):
        self.private_name = f'_{name}'

    def __get__(self, obj, objtype=None):
        return getattr(obj, self.private_name)

    def get_name(self):
        ''' can be used by subclass with super().get_name() to discover
            the private name without _ ; helpfull for raising informative errors'''
        return self.private_name.strip('_')

    def __set__(self, obj, value):
        v = self.validate(value)
        if v is not None:
            value = v
        setattr(obj, self.private_name, value)
        if not hasattr(obj, '_validated'):
            obj._validated = []
        if self.private_name not in obj._validated:
            obj._validated.append(self.private_name)

    @abstractmethod
    def validate(self, value):
        ''' validate method can accept (null return), augment (return augmented)
        or raise an error'''
        pass
