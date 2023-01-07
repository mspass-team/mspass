import pymongo

_UJOIN = u"%s.%s"


class Collection(pymongo.database.Collection):
    """
    A modified Mongo collection class.

    The native collection class of Mongo does not have __setstate__ and
    __getstate__ defined, which prevents it from being serialized. We add
    the two methods here as an addition so that the collection can be passed
    around by the scheduler and worker.
    """

    def __init__(self, *args, **kwargs):
        super(Collection, self).__init__(*args, **kwargs)

    def __getstate__(self):
        ret = self.__dict__.copy()
        return ret

    def __setstate__(self, data):
        self.__dict__.update(data)

    def __getitem__(self, name):
        return Collection(
            self.__database,
            _UJOIN % (self.__name, name),
            False,
            self.codec_options,
            self.read_preference,
            self.write_concern,
            self.read_concern,
        )

    def with_options(
        self,
        codec_options=None,
        read_preference=None,
        write_concern=None,
        read_concern=None,
    ):
        return Collection(
            self.__database,
            self.__name,
            False,
            codec_options or self.codec_options,
            read_preference or self.read_preference,
            write_concern or self.write_concern,
            read_concern or self.read_concern,
        )
