"""Dask-specific serialization for native MsPASS ensemble objects.

The native pickle representation remains the persistence format for ordinary
``pickle`` calls.  These handlers only replace Dask's transport representation
so protocol-5 sample buffers remain separate frames.
"""

import pickle

from distributed.protocol import deserialize, register_serialization, serialize

from mspasspy.ccore.seismic import SeismogramEnsemble, TimeSeriesEnsemble

_WIRE_FORMAT_VERSION = 1
_SUPPORTED_ENSEMBLE_TYPES = (TimeSeriesEnsemble, SeismogramEnsemble)


def _serialize_ensemble(ensemble, context=None):
    # Dask dispatches subclasses through a registered base class.  Falling
    # back preserves the subclass's native pickle contract instead of silently
    # reconstructing it as a base ensemble.
    if type(ensemble) not in _SUPPORTED_ENSEMBLE_TYPES:
        raise NotImplementedError

    shell = type(ensemble)(ensemble._get_ensemble_md(), 0)
    shell.elog = ensemble.elog
    state = (
        pickle.dumps(shell, protocol=pickle.HIGHEST_PROTOCOL),
        ensemble.live,
        list(ensemble.member),
    )
    state_header, frames = serialize(
        state,
        serializers=("pickle",),
        on_error="raise",
        context=context,
        iterate_collection=False,
    )
    return {
        "version": _WIRE_FORMAT_VERSION,
        "state-header": state_header,
    }, frames


def _deserialize_ensemble(header, frames):
    version = header.get("version")
    if version != _WIRE_FORMAT_VERSION:
        raise ValueError(
            "Unsupported MsPASS Dask ensemble wire-format version="
            f"{version!r}; expected {_WIRE_FORMAT_VERSION}"
        )

    shell_payload, is_live, members = deserialize(header["state-header"], frames)
    shell = pickle.loads(shell_payload)
    if type(shell) not in _SUPPORTED_ENSEMBLE_TYPES:
        raise TypeError(f"Unsupported MsPASS ensemble type={type(shell)}")

    result = type(shell)(shell._get_ensemble_md(), len(members))
    result.elog = shell.elog

    # The target vector is pre-reserved.  Clear each temporary Python-owned
    # member immediately after its one required copy into the C++ ensemble.
    for index, member in enumerate(members):
        result.member.append(member)
        members[index] = None
    member = None

    if is_live:
        result.set_live()
    else:
        result.kill()
    return result


def register_dask_serialization():
    """Register frame-based Dask transport for exact native ensembles."""

    for ensemble_type in _SUPPORTED_ENSEMBLE_TYPES:
        register_serialization(
            ensemble_type,
            _serialize_ensemble,
            _deserialize_ensemble,
        )
