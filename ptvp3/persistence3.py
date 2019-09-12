"""Persistence protocol v3
uses pickle to store dict
"""


import pickle as _pickle


class Persistence3:
    def __init__(self,  filename: str):
        self.o = object()
        self.filename = filename
        self.load()
        with open(filename + '_pre-dump.pers3.bin', 'wb') as _f:
            _pickle.dump(self.o, _f)
        self.save()

    def load(self):
        try:
            with open(self.filename + '.pers3.bin', 'rb') as _f:
                self.o = _pickle.load(_f)
        except FileNotFoundError:
            pass
        except EOFError:
            pass

    def save(self):
        with open(self.filename + '.pers3.bin', 'wb') as _f:
            _pickle.dump(self.o, _f)

    def restore(self, do_replace=False):
        with open(self.filename + '_pre-dump.pers3.bin', 'rb') as _f:
            _o = _pickle.load(_f)
            if do_replace:
                self.o = _o
            return _o
