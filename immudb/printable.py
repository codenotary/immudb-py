class printable:
    def __repr__(self) -> str:
        ret = "class {}\n".format(type(self).__name__)
        for k in self.__dict__:
            v = self.__dict__[k]
            if type(v) == bytes:
                ret += "\t{}: {}\n".format(k, list(v))
            elif hasattr(v, '__getitem__') and type(v[0]) == bytes:
                ret += "\t{}:\n".format(k)
                for t in v:
                    ret += "\t- {}\n".format(list(t))
            else:
                ret += "\t{}: {}\n".format(k, v)
        return ret
