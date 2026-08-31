"""Python interfaces for MsPASS."""


def _register_dask_serializers():
    from mspasspy.util.dask_serialization import register_dask_serialization

    register_dask_serialization()


try:
    from distributed.protocol.serialize import dask_deserialize, dask_serialize
except ImportError:
    # Importing MsPASS must remain safe in installations without Dask.
    pass
else:
    # The callback imports the native seismic module only when Dask first sees
    # an MsPASS object.  The hook is installed independently in every driver
    # and worker process that imports mspasspy.
    dask_serialize.register_lazy("mspasspy", _register_dask_serializers)
    dask_deserialize.register_lazy("mspasspy", _register_dask_serializers)
