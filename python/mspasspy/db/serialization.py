"""Versioned, non-executable database representations for complex objects."""

import copy
import io
import pickle
from datetime import datetime

import numpy as np
from bson import ObjectId
from obspy import Inventory, read_inventory
from obspy.core.inventory import Channel, Network, Station

from mspasspy.ccore.seismic import DoubleVector, PowerSpectrum
from mspasspy.ccore.utility import (
    AtomicType,
    ErrorLogger,
    ErrorSeverity,
    LogData,
    Metadata,
    MsPASSError,
    NodeData,
    ProcessingHistory,
    ProcessingStatus,
    dmatrix,
)

TYPE_KEY = "_mspass_serialization_type"
VERSION_KEY = "_mspass_serialization_version"
VERSION = 1


def _invalid(message):
    return MsPASSError(message, ErrorSeverity.Invalid)


def _header(type_name):
    return {TYPE_KEY: type_name, VERSION_KEY: VERSION}


def _require_document(value, type_name):
    if not isinstance(value, dict):
        raise _invalid(
            f"Expected versioned {type_name} document; legacy pickle payloads "
            "must be migrated explicitly"
        )
    if value.get(TYPE_KEY) != type_name or value.get(VERSION_KEY) != VERSION:
        raise _invalid(f"Unsupported {type_name} representation")


def _severity_name(value):
    return value.name


def _severity_from_name(value):
    try:
        return getattr(ErrorSeverity, value)
    except (AttributeError, TypeError) as exc:
        raise _invalid(f"Invalid ErrorSeverity value={value!r}") from exc


def _enum_from_name(enum_type, value, label):
    try:
        return getattr(enum_type, value)
    except (AttributeError, TypeError) as exc:
        raise _invalid(f"Invalid {label} value={value!r}") from exc


def encode_error_logger(logger):
    return {
        "job_id": logger.get_job_id(),
        "messages": [
            {
                "job_id": item.job_id,
                "process_id": item.p_id,
                "algorithm": item.algorithm,
                "message": item.message,
                "severity": _severity_name(item.badness),
            }
            for item in logger.get_error_log()
        ],
    }


def decode_error_logger(document):
    if not isinstance(document, dict):
        raise _invalid("ErrorLogger representation is not a document")
    try:
        messages = [
            LogData(
                {
                    "job_id": item["job_id"],
                    "p_id": item["process_id"],
                    "algorithm": item["algorithm"],
                    "message": item["message"],
                    "badness": _severity_from_name(item["severity"]),
                }
            )
            for item in document["messages"]
        ]
        return ErrorLogger(document["job_id"], messages)
    except MsPASSError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise _invalid("Malformed ErrorLogger representation") from exc


def _encode_node(node):
    return {
        "status": node.status.name,
        "uuid": node.uuid,
        "type": node.type.name,
        "stage": node.stage,
        "algorithm": node.algorithm,
        "algorithm_id": node.algid,
    }


def _decode_node(document):
    if not isinstance(document, dict):
        raise _invalid("ProcessingHistory node is not a document")
    try:
        node = NodeData()
        node.status = _enum_from_name(
            ProcessingStatus, document["status"], "ProcessingStatus"
        )
        node.uuid = document["uuid"]
        node.type = _enum_from_name(AtomicType, document["type"], "AtomicType")
        node.stage = document["stage"]
        node.algorithm = document["algorithm"]
        node.algid = document["algorithm_id"]
        return node
    except MsPASSError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise _invalid("Malformed ProcessingHistory node") from exc


def encode_processing_history(history):
    if not isinstance(history, ProcessingHistory):
        raise TypeError("history must be a ProcessingHistory")
    edges = []
    for child, parents in history.get_nodes().items():
        for parent in parents:
            edges.append({"child": child, "parent": _encode_node(parent)})
    document = _header("ProcessingHistory")
    document.update(
        {
            "job_name": history.jobname(),
            "job_id": history.jobid(),
            "current": _encode_node(history.current_nodedata()),
            "edges": edges,
            "error_log": encode_error_logger(history.elog),
        }
    )
    return document


def decode_processing_history(document):
    _require_document(document, "ProcessingHistory")
    try:
        edges = [
            (edge["child"], _decode_node(edge["parent"])) for edge in document["edges"]
        ]
        return ProcessingHistory(
            document["job_name"],
            document["job_id"],
            edges,
            _decode_node(document["current"]),
            decode_error_logger(document["error_log"]),
        )
    except MsPASSError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise _invalid("Malformed ProcessingHistory representation") from exc


def inventory_subset(inventory, network, station, channels):
    """Return a one-station Inventory while preserving Inventory metadata."""
    station_copy = copy.deepcopy(station)
    station_copy.channels = [copy.deepcopy(channel) for channel in channels]
    network_copy = copy.deepcopy(network)
    network_copy.stations = [station_copy]
    return Inventory(
        networks=[network_copy],
        source=inventory.source,
        sender=inventory.sender,
        module=inventory.module,
        module_uri=inventory.module_uri,
        created=inventory.created,
    )


def encode_inventory(inventory):
    if isinstance(inventory, Network):
        inventory = Inventory(networks=[copy.deepcopy(inventory)], source="MsPASS")
    if not isinstance(inventory, Inventory):
        raise TypeError("inventory must be an ObsPy Inventory or Network")
    stream = io.BytesIO()
    inventory.write(stream, format="STATIONXML", validate=True)
    document = _header("Inventory")
    document["stationxml"] = stream.getvalue().decode("utf-8")
    return document


def decode_inventory(document):
    return _decode_stationxml(document, "Inventory")


def encode_response(channel, document):
    """Encode one channel response with enough StationXML context to restore it."""
    if not isinstance(channel, Channel):
        raise TypeError("channel must be an ObsPy Channel")
    stream = io.BytesIO()
    channel_inventory(channel, document).write(
        stream, format="STATIONXML", validate=True
    )
    result = _header("Response")
    result["stationxml"] = stream.getvalue().decode("utf-8")
    return result


def _decode_stationxml(document, type_name):
    _require_document(document, type_name)
    try:
        payload = document["stationxml"]
        if not isinstance(payload, str):
            raise TypeError("stationxml is not a string")
        return read_inventory(io.BytesIO(payload.encode("utf-8")), format="STATIONXML")
    except MsPASSError:
        raise
    except Exception as exc:
        raise _invalid(f"Malformed {type_name} representation") from exc


def merge_inventories(inventories):
    """Merge one-station codec records back into a complete Inventory."""
    inventories = list(inventories)
    if not inventories:
        return Inventory()
    first = inventories[0]
    result = Inventory(
        networks=[],
        source=first.source,
        sender=first.sender,
        module=first.module,
        module_uri=first.module_uri,
        created=first.created,
    )
    networks = {}
    stations = {}
    for inventory in inventories:
        for network in inventory.networks:
            network_key = (
                network.code,
                str(network.start_date),
                str(network.end_date),
            )
            target_network = networks.get(network_key)
            if target_network is None:
                target_network = copy.deepcopy(network)
                target_network.stations = []
                networks[network_key] = target_network
                result.networks.append(target_network)
            for station in network.stations:
                key = (
                    network_key,
                    station.code,
                    str(station.start_date),
                    str(station.end_date),
                )
                target_station = stations.get(key)
                if target_station is None:
                    target_station = copy.deepcopy(station)
                    target_station.channels = []
                    stations[key] = target_station
                    target_network.stations.append(target_station)
                existing = {
                    (
                        channel.code,
                        channel.location_code,
                        str(channel.start_date),
                        str(channel.end_date),
                    )
                    for channel in target_station.channels
                }
                for channel in station.channels:
                    key = (
                        channel.code,
                        channel.location_code,
                        str(channel.start_date),
                        str(channel.end_date),
                    )
                    if key not in existing:
                        target_station.channels.append(copy.deepcopy(channel))
                        existing.add(key)
    return result


def channel_inventory(channel, document):
    if not isinstance(channel, Channel):
        raise TypeError("channel must be an ObsPy Channel")
    station = Station(
        code=document.get("sta", "UNKNOWN"),
        latitude=channel.latitude,
        longitude=channel.longitude,
        elevation=channel.elevation,
        channels=[copy.deepcopy(channel)],
    )
    network = Network(code=document.get("net", "XX"), stations=[station])
    return Inventory(networks=[network], source="MsPASS")


def decode_channel(document):
    inventory = _decode_stationxml(document, "Response")
    try:
        return inventory.networks[0].stations[0].channels[0]
    except (IndexError, AttributeError) as exc:
        raise _invalid("Inventory representation contains no channel") from exc


def decode_response(document):
    return decode_channel(document).response


def _encode_metadata_value(value):
    if value is None or isinstance(
        value, (bool, int, float, str, bytes, ObjectId, datetime)
    ):
        return value
    if isinstance(value, np.generic):
        return _encode_metadata_value(value.item())
    if isinstance(value, DoubleVector):
        return {
            TYPE_KEY: "DoubleVector",
            VERSION_KEY: VERSION,
            "values": list(value),
        }
    if isinstance(value, dmatrix):
        return {
            TYPE_KEY: "dmatrix",
            VERSION_KEY: VERSION,
            "values": np.asarray(value).tolist(),
        }
    if isinstance(value, np.ndarray):
        return {
            TYPE_KEY: "ndarray",
            VERSION_KEY: VERSION,
            "dtype": str(value.dtype),
            "shape": list(value.shape),
            "values": value.reshape(-1).tolist(),
        }
    if isinstance(value, tuple):
        return {
            TYPE_KEY: "tuple",
            VERSION_KEY: VERSION,
            "values": [_encode_metadata_value(item) for item in value],
        }
    if isinstance(value, list):
        return [_encode_metadata_value(item) for item in value]
    if isinstance(value, dict):
        return {
            TYPE_KEY: "dict",
            VERSION_KEY: VERSION,
            "items": [
                {
                    "key": _encode_metadata_value(key),
                    "value": _encode_metadata_value(item),
                }
                for key, item in value.items()
            ],
        }
    raise _invalid(f"Unsupported Metadata value type={type(value)}")


def _decode_metadata_value(value):
    if isinstance(value, list):
        return [_decode_metadata_value(item) for item in value]
    if not isinstance(value, dict):
        return value
    value_type = value.get(TYPE_KEY)
    if value_type is None:
        return {key: _decode_metadata_value(item) for key, item in value.items()}
    if value.get(VERSION_KEY) != VERSION:
        raise _invalid("Unsupported Metadata value representation")
    if value_type == "tuple":
        return tuple(_decode_metadata_value(item) for item in value["values"])
    if value_type == "dict":
        return {
            _decode_metadata_value(item["key"]): _decode_metadata_value(item["value"])
            for item in value["items"]
        }
    if value_type == "DoubleVector":
        return DoubleVector(value["values"])
    if value_type == "ndarray":
        result = np.asarray(value["values"], dtype=value["dtype"])
        return result.reshape(value["shape"])
    if value_type == "dmatrix":
        return dmatrix(np.asarray(value["values"], dtype=float))
    raise _invalid(f"Unsupported Metadata value type={value_type}")


def encode_power_spectrum(spectrum):
    if not isinstance(spectrum, PowerSpectrum):
        raise TypeError("spectrum must be a PowerSpectrum")
    document = _header("PowerSpectrum")
    document.update(
        {
            "metadata": _encode_metadata_value(dict(spectrum)),
            "spectrum": list(spectrum.spectrum),
            "df": spectrum.df(),
            "f0": spectrum.f0(),
            "spectrum_type": spectrum.spectrum_type,
            "parent_dt": spectrum.dt(),
            "parent_npts": spectrum.timeseries_npts(),
            "live": spectrum.live(),
            "error_log": encode_error_logger(spectrum.elog),
        }
    )
    return document


def decode_power_spectrum(document):
    _require_document(document, "PowerSpectrum")
    try:
        result = PowerSpectrum(
            Metadata(_decode_metadata_value(document["metadata"])),
            DoubleVector(document["spectrum"]),
            document["df"],
            document["spectrum_type"],
            document["f0"],
            document["parent_dt"],
            document["parent_npts"],
        )
        if not document["live"]:
            result.kill()
        result.elog = decode_error_logger(document["error_log"])
        return result
    except MsPASSError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise _invalid("Malformed PowerSpectrum representation") from exc


def _legacy_payload(document):
    fields = (
        "processing_history",
        "serialized_inventory",
        "serialized_channel_data",
        "serialized_data",
    )
    present = [field for field in fields if field in document]
    if len(present) != 1:
        raise _invalid("Legacy document must contain exactly one supported payload")
    return present[0], document[present[0]]


def _has_new_representation(document):
    expected_types = {
        "processing_history": "ProcessingHistory",
        "serialized_inventory": "Inventory",
        "serialized_channel_data": "Response",
        "serialized_data": "PowerSpectrum",
    }
    return any(
        isinstance(document.get(field), dict)
        and document[field].get(TYPE_KEY) == type_name
        and document[field].get(VERSION_KEY) == VERSION
        for field, type_name in expected_types.items()
    )


def _encode_legacy_object(field, value, document):
    if field == "processing_history" and isinstance(value, ProcessingHistory):
        return encode_processing_history(value)
    if field == "serialized_inventory" and isinstance(value, (Inventory, Network)):
        return encode_inventory(value)
    if field == "serialized_channel_data" and isinstance(value, Channel):
        return encode_response(value, document)
    if field == "serialized_data" and isinstance(value, PowerSpectrum):
        return encode_power_spectrum(value)
    raise _invalid(f"Legacy payload field={field} contains an unexpected object type")


def trusted_legacy_pickle(database, collection, query=None):
    """Migrate caller-declared trusted pickle records in ascending ``_id`` order.

    Calling this function explicitly is the trust declaration.  It executes
    pickle deserialization and must never be used on untrusted records.
    """
    target = database[collection]
    migrated = 0
    for original in target.find(query or {}).sort("_id", 1):
        try:
            if _has_new_representation(original):
                continue
            field, payload = _legacy_payload(original)
            expected_type = {
                "processing_history": "ProcessingHistory",
                "serialized_inventory": "Inventory",
                "serialized_channel_data": "Response",
                "serialized_data": "PowerSpectrum",
            }[field]
            if isinstance(payload, dict):
                _require_document(payload, expected_type)
            if not isinstance(payload, (bytes, bytearray)):
                raise _invalid(
                    f"Legacy payload field={field} must contain pickle bytes"
                )
            value = pickle.loads(payload)
            replacement = copy.deepcopy(original)
            replacement[field] = _encode_legacy_object(field, value, original)
            result = target.replace_one({"_id": original["_id"]}, replacement)
            if result.matched_count != 1:
                raise RuntimeError("legacy record disappeared before replacement")
            migrated += 1
        except MsPASSError:
            raise
        except Exception as exc:
            raise _invalid(
                f"trusted_legacy_pickle failed at record _id={original.get('_id')}"
            ) from exc
    return migrated
