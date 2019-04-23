"""Persistence protocol v3
union of dicts
"""


from typing import List, Iterator, Hashable, Iterable
from itertools import cycle as _cycle


class DynamicCluster:
    """
Unites dicts as one virtual dict
    """
    def __init__(self, dicts: Iterable[dict]):
        self.dicts = dicts
        self.get_iter = self.get_next

    def __getitem__(self, item: Hashable):
        for _d in self.dicts:
            if item in _d:
                return _d[item]
        raise KeyError(item)

    def __setitem__(self, key: Hashable, value):
        for _d in self.dicts:
            if key in _d:
                _d[key] = value
        next(self.get_iter)[key] = value

    def __contains__(self, item: Hashable):
        for _d in self.dicts:
            if item in _d:
                return True
        return False

    @property
    def get_next(self) -> Iterator:
        return _cycle(self.dicts)

    def __iter__(self):
        for _d in self.dicts:
            yield from _d

    def __delitem__(self, key):
        for _d in self.dicts:
            if key in _d:
                del _d[key]

    def get(self, k, d=None):
        """
reimplementation of get of dict
        :param k:
        :param d:
        :return:
        """
        for _d in self.dicts:
            _d.get(k, d)

    def setdefault(self, k, d=None):
        """
reimplementation of setdefault of dict
        :param k:
        :param d:
        :return:
        """
        if k not in self:
            self[k] = d
        return self[k]


class LimitedCluster(DynamicCluster):
    """
adds some algorithms for limited-sized (list) iterator for dicts
    """
    def __init__(self, dicts: List[dict]):
        super().__init__(dicts)
        self.dicts = dicts

    def items(self):
        _d_out = {}
        for _d in self.dicts:
            for key in _d:
                _d_out[key] = _d[key]
        return _d_out.items()

    def clean(self):
        _d_self = dict(self.items())
        for _d in self.dicts:
            _d.clear()
        for key in _d_self:
            self[key] = _d_self[key]
        return _d_self

    def clear(self):
        for _d in self.dicts:
            _d.clear()

    def sub_cluster(self, name: str):
        _dicts: List[dict] = []
        for _i in range(len(self.dicts)):
            _d = self.dicts[_i]
            full_name = '__sub_cluster_{}__'.format(_i) + name
            _d.setdefault(full_name, {})
            if type(_d[full_name]) != dict:
                _d[full_name] = {}
            _dicts.append(_d[full_name])
        return LimitedCluster(_dicts)
