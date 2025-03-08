"""
Tools to define the schema of Metadata.
"""

import os

import yaml
import schema
import bson.objectid

import mspasspy.ccore.seismic
from mspasspy.ccore.utility import MsPASSError


class SchemaBase:
    def __init__(self, schema_file=None):
        self._attr_dict = {}
        if schema_file is None:
            if "MSPASS_HOME" in os.environ:
                schema_file = (
                    os.path.abspath(os.environ["MSPASS_HOME"])
                    + "/data/yaml/mspass.yaml"
                )
            else:
                schema_file = os.path.abspath(
                    os.path.dirname(__file__) + "/../data/yaml/mspass.yaml"
                )
        elif not os.path.isfile(schema_file):
            if "MSPASS_HOME" in os.environ:
                schema_file = os.path.join(
                    os.path.abspath(os.environ["MSPASS_HOME"]), "data/yaml", schema_file
                )
            else:
                schema_file = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "../data/yaml", schema_file)
                )
        try:
            with open(schema_file, "r") as stream:
                schema_dic = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            raise MsPASSError(
                "Cannot parse schema definition file: " + schema_file, "Fatal"
            ) from e
        except EnvironmentError as e:
            raise MsPASSError(
                "Cannot open schema definition file: " + schema_file, "Fatal"
            ) from e

        try:
            _check_format(schema_dic)
        except schema.SchemaError as e:
            raise MsPASSError("The schema definition is not valid", "Fatal") from e
        self._raw = schema_dic

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError as ae:
            raise MsPASSError(
                "The schema of " + key + " is not defined", "Invalid"
            ) from ae

    def __delitem__(self, key):
        try:
            return delattr(self, key)
        except AttributeError as ae:
            raise MsPASSError(
                "The schema of " + key + " is not defined", "Invalid"
            ) from ae

    def __contains__(self, key):
        return key in self._attr_dict


class SchemaDefinitionBase:
    _main_dic = {}
    _alias_dic = {}
    _required_keys = []
    _xref_keys = []

    def add(self, name, attr):
        """
        Add a new entry to the definitions. Note that because the internal
        container is `dict` if attribute for name is already present it
        will be silently replaced.

        :param name: The name of the attribute to be added
        :type name: str
        :param attr: A dictionary that defines the property of the added attribute.
            Note that the type must be defined.
        :type attr: dict
        :raises mspasspy.ccore.utility.MsPASSError: if type is not defined in attr
        """
        self._main_dic[name] = attr
        if "aliases" in attr:
            for als in attr["aliases"]:
                self._alias_dic[als] = name
        if attr["constraint"] == "required":
            self._required_keys.append(name)
        elif attr["constraint"] == "xref_key":
            self._xref_keys.append(name)

    def add_alias(self, key, aliasname):
        """
        Add an alias for key

        :param key: key to be added
        :type key: str
        :param aliasname: aliasname to be added
        :type aliasname: str
        """
        self._main_dic[key]["aliases"].append(aliasname)
        self._alias_dic[aliasname] = key

    def remove_alias(self, alias):
        """
        Remove an alias. Will silently ignore if alias is not found

        :param alias: alias to be removed
        :type alias: str
        """
        if alias in self._alias_dic:
            key = self._alias_dic[alias]
            self._main_dic[key]["aliases"].remove(alias)
            del self._alias_dic[alias]

    def aliases(self, key):
        """
        Get a list of aliases for a given key.

        :param key: The unique key that has aliases defined.
        :type key: str
        :return: A list of aliases associated to the key.
        :rtype: list
        """
        return (
            None
            if "aliases" not in self._main_dic[key]
            else self._main_dic[key]["aliases"]
        )

    def apply_aliases(self, md, alias):
        """
        Apply a set of aliases to a data object.

        This method will change the unique keys of a data object into aliases.
        The alias argument can either be a path to a valid yaml file of
        key:alias pairs or a dict. If the "key" is an alias itself, it will
        be converted to its corresponding unique name before being used to
        change to the alias. It will also add the applied alias to the schema's
        internal alias container such that the same schema object can be used
        to convert the alias back.

        :param md: Data object to be altered. Normally a :class:`mspasspy.ccore.seismic.Seismogram`
            or :class:`mspasspy.ccore.seismic.TimeSeries` but can be a raw :class:`mspasspy.ccore.utility.Metadata`.
        :type md: :class:`mspasspy.ccore.utility.Metadata`
        :param alias: a yaml file or a dict that have pairs of key:alias
        :type alias: dict/str

        """
        alias_dic = alias
        if isinstance(alias, str) and os.path.isfile(alias):
            try:
                with open(alias, "r") as stream:
                    alias_dic = yaml.safe_load(stream)
            except yaml.YAMLError as e:
                raise MsPASSError(
                    "Cannot parse alias definition file: " + alias, "Fatal"
                ) from e
            except EnvironmentError as e:
                raise MsPASSError(
                    "Cannot open alias definition file: " + alias, "Fatal"
                ) from e
        if isinstance(alias_dic, dict):
            for k, a in alias_dic.items():
                unique_k = self.unique_name(k)
                self.add_alias(unique_k, a)
                md.change_key(unique_k, a)
        else:
            raise MsPASSError(
                "The alias argument of type {} is not recognized, it should be either a {} path or a {}".format(
                    type(alias), str, dict
                ),
                "Fatal",
            )

    def clear_aliases(self, md):
        """
        Restore any aliases to unique names.

        Aliases are needed to support legacy packages, but can cause downstream problem
        if left intact. This method clears any aliases and sets them to the unique_name
        defined by this object. Note that if the unique_name is already defined, it will
        silently remove the alias only.

        :param md: Data object to be altered. Normally a :class:`mspasspy.ccore.seismic.Seismogram`
            or :class:`mspasspy.ccore.seismic.TimeSeries` but can be a raw :class:`mspasspy.ccore.utility.Metadata`.
        :type md: :class:`mspasspy.ccore.utility.Metadata`
        """
        for key in md.keys():
            if self.is_alias(key):
                if self.unique_name(key) not in md:
                    md.change_key(key, self.unique_name(key))
                else:
                    del md[key]

    def concept(self, key):
        """
        Return a description of the concept this attribute defines.

        :param key: The name that defines the attribute of interest
        :type key: str
        :return: A string with a terse description of the concept this attribute defines
        :rtype: str
        :raises mspasspy.ccore.utility.MsPASSError: if concept is not defined
        """
        if "concept" not in self._main_dic[key]:
            raise MsPASSError("concept is not defined for " + key, "Complaint")
        return self._main_dic[key]["concept"]

    def has_alias(self, key):
        """
        Test if a key has registered aliases

        Sometimes it is helpful to have alias keys to define a common concept.
        For instance, if an attribute is loaded from a relational db one might
        want to use alias names of the form table.attribute as an alias to
        attribute. has_alias should be called first to establish if a name has
        an alias. To get a list of aliases call the aliases method.

        :param key: key to be tested
        :type key: str
        :return: `True` if the key has aliases, else or if key is not defined return `False`
        :rtype: bool
        """
        return key in self._main_dic and "aliases" in self._main_dic[key]

    def is_alias(self, key):
        """
        Test if a key is a registered alias

        This asks the inverse question to has_alias. That is, it yields true of the key is
        registered as a valid alias. It returns false if the key is not defined at all. Note
        it will yield false if the key is a registered unique name and not an alias.

        :param key: key to be tested
        :type key: str
        :return: `True` if the key is a alias, else return `False`
        :rtype: bool
        """
        return key in self._alias_dic

    def is_defined(self, key):
        """
        Test if a key is defined either as a unique key or an alias

        :param key: key to be tested
        :type key: str
        :return: `True` if the key is defined
        :rtype: bool
        """
        return key in self._main_dic or key in self._alias_dic

    def keys(self):
        """
        Get a list of all the unique keys defined.

        :return: list of all the unique keys defined
        :rtype: list
        """
        return self._main_dic.keys()

    def type(self, key):
        """
        Return the type of an attribute. If not recognized, it returns None.

        :param key: The name that defines the attribute of interest
        :type key: str
        :return: type of the attribute associated with ``key``
        :rtype: :class:`type`
        """
        tp = self._main_dic[key]["type"].strip().casefold()
        if tp in ["int", "integer"]:
            return int
        if tp in ["double", "float"]:
            return float
        if tp in ["str", "string"]:
            return str
        if tp in ["bool", "boolean"]:
            return bool
        if tp in ["dict"]:
            return dict
        if tp in ["list"]:
            return list
        if tp in ["objectid"]:
            return bson.objectid.ObjectId
        if tp in ["bytes", "byte", "object"]:
            return bytes
        return None

    def unique_name(self, aliasname):
        """
        Get definitive name for an alias.

        This method is used to ask the opposite question as aliases. The aliases method
        returns all acceptable alternatives to a definitive name defined as the key to
        get said list. This method asks what definitive key should be used to fetch an
        attribute. Note that if the input is already the unique name, it will return itself.

        :param aliasname: the name of the alias for which we want the definitive key
        :type aliasname: str
        :return: the name of the definitive key
        :rtype: str
        :raises mspasspy.ccore.utility.MsPASSError: if aliasname is not defined
        """
        if aliasname in self._main_dic:
            return aliasname
        if aliasname in self._alias_dic:
            return self._alias_dic[aliasname]
        raise MsPASSError(aliasname + " is not defined", "Invalid")

    def required_keys(self):
        """
        Return all the required keys in the current collection as a list

        :return: type of data associated with the collection
        :rtype: a :class:`list` of :class:`str`
        """
        return self._required_keys

    def xref_keys(self):
        """
        Return all the xref keys in the current collection as a list

        :return: type of data associated with the collection
        :rtype: a :class:`list` of :class:`str`
        """
        return self._xref_keys

    def constraint(self, key):
        """
        Return a description of the constraint this attribute defines.

        :param key: The name that defines the attribute of interest
        :type key: str
        :return: A string with a terse description of the constraint this attribute defines
        :rtype: str
        """
        return self._main_dic[key]["constraint"]

    def is_required(self, key):
        """
        Test if the constraint of the key is required to the schema

        :param key: key to be tested
        :type key: str
        :return: `True` if the constraint of the key is required
        :rtype: bool
        """
        return self._main_dic[key]["constraint"] == "required"

    def is_xref_key(self, key):
        """
        Test if the constraint of the key is xref_key to the schema

        :param key: key to be tested
        :type key: str
        :return: `True` if the constraint of the key is xref_key
        :rtype: bool
        """
        return self._main_dic[key]["constraint"] == "xref_key"

    def is_normal(self, key):
        """
        Test if the constraint of the key is normal to the schema

        :param key: key to be tested
        :type key: str
        :return: `True` if the constraint of the key is normal
        :rtype: bool
        """
        return self._main_dic[key]["constraint"] == "normal"

    def is_optional(self, key):
        """
        Test if the constraint of the key is optional to the schema

        :param key: key to be tested
        :type key: str
        :return: `True` if the constraint of the key is optional
        :rtype: bool
        """
        return self._main_dic[key]["constraint"] == "optional"


class DatabaseSchema(SchemaBase):
    def __init__(self, schema_file=None):
        super().__init__(schema_file)
        self._default_dic = {}
        for collection in self._raw["Database"]:
            schemadef = DBSchemaDefinition(self._raw["Database"], collection)
            setattr(self, collection, schemadef)
            self._attr_dict[collection] = schemadef
            if "default" in self._raw["Database"][collection]:
                self._default_dic[self._raw["Database"][collection]["default"]] = (
                    collection
                )

    def __setitem__(self, key, value):
        if not isinstance(value, DBSchemaDefinition):
            raise MsPASSError("value is not a DBSchemaDefinition", "Invalid")
        setattr(self, key, value)
        self._attr_dict[key] = value
        self._default_dic[key] = key

    def default(self, name):
        """
        Return the schema definition of a default collection.

        This method is used when multiple collections of the same concept is defined.
        For example, the wf_TimeSeries and wf_Seismogram are both collections that
        are used for data objects (characterized by their common wf prefix). The
        Database API needs a default wf collection to operate on when no collection
        name is explicitly given. In this case, this default_name method can be used.
        Note that if requested name has no default collection defined and it is a
        defined collection, it will treat that collection itself as the default.

        :param name: The requested default collection
        :type name: str
        :return: the schema definition of the default collection
        :rtype: :class:`mspasspy.db.schema.DBSchemaDefinition`
        :raises mspasspy.ccore.utility.MsPASSError: if the name has no default defined
        """
        if name in self._default_dic:
            return getattr(self, self._default_dic[name])
        if name in self._raw["Database"]:
            return getattr(self, name)
        raise MsPASSError(name + " has no default defined", "Invalid")

    def default_name(self, name):
        """
        Return the name of a default collection.

        This method is behaves similar to the default method, but it only returns the
        name as a string instead.

        :param name: The requested default collection
        :type name: str
        :return: the name of the default collection
        :rtype: str
        :raises mspasspy.ccore.utility.MsPASSError: if the name has no default defined
        """
        if name in self._default_dic:
            return self._default_dic[name]
        if name in self._raw["Database"]:
            return name
        raise MsPASSError(name + " has no default defined", "Invalid")

    def set_default(self, collection: str, default: str = None):
        """
        Set a collection as the default.

        This method is used to change the default collections (e.g., switching between
        wf_TimeSeries and wf_Seismogram). If ``default`` is not given, it will try to
        infer one from ``collection`` at the first occurrence of "_"
        (e.g., wf_TimeSeries will become wf).

        :param collection: The name of the targetting collection
        :type collection: str
        :param default: the default name to be set to
        :type default: str, optional
        """
        if not hasattr(self, collection):
            raise MsPASSError(collection + " is not a defined collection", "Invalid")
        if default is None:
            self._default_dic[collection.split("_", 1)[0]] = collection
        else:
            self._default_dic[default] = collection

    def unset_default(self, default: str):
        """
        Unset a default.

        This method does nothing if ``default`` is not defined.

        :param default: the default name to be unset
        :type default: str
        """
        if default in self._default_dic:
            self._default_dic.pop(default)


class DBSchemaDefinition(SchemaDefinitionBase):
    def __init__(self, schema_dic, collection_str):
        self._collection_str = collection_str
        self._main_dic = {}
        self._alias_dic = {}
        self._data_type = None
        self._required_keys = []
        self._xref_keys = []
        self._main_dic.update(schema_dic[collection_str]["schema"])
        for key, attr in self._main_dic.items():
            if "reference" in attr:
                k = key
                if k == attr["reference"] + "_id":
                    k = "_id"
                refer_dic = schema_dic[attr["reference"]]
                while "base" in refer_dic:
                    refer_dic = schema_dic[refer_dic["base"]]
                    if k in refer_dic["schema"]:
                        break
                foreign_attr = refer_dic["schema"][k]
                # The order of below operation matters. The behavior is that we only
                # extend attr with items from foreign_attr that are not defined in attr.
                # This garantees that the foreign_attr won't overwrite attr's exisiting keys.
                compiled_attr = dict(list(foreign_attr.items()) + list(attr.items()))
                self._main_dic[key] = compiled_attr

            if "aliases" in attr:
                self._alias_dic.update(
                    {item: key for item in attr["aliases"] if item != key}
                )

            if "constraint" in attr:
                if attr["constraint"] == "required":
                    self._required_keys.append(key)
                elif attr["constraint"] == "xref_key":
                    self._xref_keys.append(key)

        if "data_type" in schema_dic[collection_str]:
            self._data_type = schema_dic[collection_str]["data_type"]

    def add(self, name, attr):
        """
        Add a new entry to the definitions. Note that because the internal
        container is `dict` if attribute for name is already present it
        will be silently replaced.

        :param name: The name of the attribute to be added
        :type name: str
        :param attr: A dictionary that defines the property of the added attribute.
            Note that the type must be defined.
        :type attr: dict
        :raises mspasspy.ccore.utility.MsPASSError: if type is not defined in attr
        """
        schema_v = _get_schema_definition_schema(None, "database", False)
        attr = schema_v.validate(attr)
        SchemaDefinitionBase.add(self, name, attr)

    def reference(self, key):
        """
        Return the collection name that a key is referenced from.

        :param key: the name of the key
        :type key: str
        :return: the name of the collection
        :rtype: str
        :raises mspasspy.ccore.utility.MsPASSError: if the key is not defined
        """
        if key not in self._main_dic:
            raise MsPASSError(key + " is not defined", "Invalid")
        return (
            self._collection_str
            if "reference" not in self._main_dic[key]
            else self._main_dic[key]["reference"]
        )

    def data_type(self):
        """
        Return the data type that the collection is used to reference.
        If not recognized, it returns None.

        :return: type of data associated with the collection
        :rtype: :class:`type`
        """
        if self._data_type in ["TimeSeries", "timeseries"]:
            return mspasspy.ccore.seismic.TimeSeries
        if self._data_type in ["Seismogram", "seismogram"]:
            return mspasspy.ccore.seismic.Seismogram
        return None


class MetadataSchema(SchemaBase):
    def __init__(self, schema_file=None):
        super().__init__(schema_file)
        dbschema = DatabaseSchema(schema_file)
        for collection in self._raw["Metadata"]:
            schemadef = MDSchemaDefinition(self._raw["Metadata"], collection, dbschema)
            setattr(self, collection, schemadef)
            self._attr_dict[collection] = schemadef

    def __setitem__(self, key, value):
        if not isinstance(value, MDSchemaDefinition):
            raise MsPASSError("value is not a MDSchemaDefinition", "Invalid")
        setattr(self, key, value)
        self._attr_dict[key] = value


class MDSchemaDefinition(SchemaDefinitionBase):
    def __init__(self, schema_dic, collection_str, dbschema):
        self._main_dic = {}
        self._alias_dic = {}
        self._required_keys = []
        self._xref_keys = []
        self._main_dic.update(schema_dic[collection_str]["schema"])
        for key, attr in self._main_dic.items():
            if "collection" in attr:
                s_key = key
                col_name = attr["collection"]
                if key.startswith(col_name):
                    s_key = key.replace(col_name + "_", "")
                # get the default name in case one is used in the dbschema
                col_name = dbschema.default_name(col_name)
                foreign_attr = getattr(dbschema, col_name)._main_dic[s_key]
                # The order of below operation matters. The behavior is that we only
                # extend attr with items from foreign_attr that are not defined in attr.
                # This garantees that the foreign_attr won't overwrite attr's exisiting keys.
                compiled_attr = dict(list(foreign_attr.items()) + list(attr.items()))
                self._main_dic[key] = compiled_attr
            # have to use "self._main_dic[key]" instead of attr here because the dict is updated above
            if "aliases" in self._main_dic[key]:
                self._alias_dic.update(
                    {
                        item: key
                        for item in self._main_dic[key]["aliases"]
                        if item != key
                    }
                )
            if "constraint" in self._main_dic[key]:
                if self._main_dic[key]["constraint"] == "required":
                    self._required_keys.append(key)
                elif self._main_dic[key]["constraint"] == "xref_key":
                    self._xref_keys.append(key)

    def add(self, name, attr):
        """
        Add a new entry to the definitions. Note that because the internal
        container is `dict` if attribute for name is already present it
        will be silently replaced.

        :param name: The name of the attribute to be added
        :type name: str
        :param attr: A dictionary that defines the property of the added attribute.
            Note that the type must be defined.
        :type attr: dict
        :raises mspasspy.ccore.utility.MsPASSError: if type is not defined in attr
        """
        schema_v = _get_schema_definition_schema(None, "metadata", False)
        attr = schema_v.validate(attr)
        SchemaDefinitionBase.add(self, name, attr)

    def collection(self, key):
        """
        Return the collection name that a key belongs to

        :param key: the name of the key
        :type key: str
        :return: the name of the collection
        :rtype: str
        """
        return (
            None
            if "collection" not in self._main_dic[key]
            else self._main_dic[key]["collection"]
        )

    def set_collection(self, key, collection, dbschema=None):
        """
        Set the collection name that a key belongs to. It optionally takes
        a dbschema argument and will set the attribute of that key with the
        corresponding one defined in the dbschema.

        :param key: the name of the key
        :type key: str
        :param collection: the name of the collection
        :type collection: str
        :param dbschema: the database schema used to set the attributes of the key.
        :type dbschema: :class:`mspasspy.db.schema.DatabaseSchema`
        :raises mspasspy.ccore.utility.MsPASSError: if the key is not defined
        """
        if key not in self._main_dic:
            raise MsPASSError(key + " is not defined", "Invalid")
        if dbschema:
            readonly = self.readonly(key)
            aliases = self.aliases(key)

            s_key = key
            col_name = collection
            if key.startswith(col_name):
                s_key = key.replace(col_name + "_", "")
            col_name = dbschema.default_name(col_name)
            foreign_attr = getattr(dbschema, col_name)._main_dic[s_key]
            self._main_dic[key] = foreign_attr
            if not readonly:
                self.set_writeable(key)
            if aliases:
                if "aliases" not in self._main_dic[key]:
                    self._main_dic[key]["aliases"] = aliases
                else:
                    [
                        self._main_dic[key]["aliases"].append(x)
                        for x in aliases
                        if x not in self._main_dic[key]["aliases"]
                    ]
                    self._alias_dic.update(
                        {
                            item: key
                            for item in self._main_dic[key]["aliases"]
                            if item != key
                        }
                    )
        self._main_dic[key]["collection"] = collection

    def swap_collection(self, original_collection, new_collection, dbschema=None):
        """
        Swap the collection name of all the keys of a matching colletions.
        It optionally takes a dbschema argument and will set the attribute
        of that key with the corresponding one defined in the dbschema. It
        will silently do nothing if no matching collection is defined.

        :param original_collection: the name of the collection to be swapped
        :type original_collection: str
        :param new_collection: the name of the collection to be changed into
        :type new_collection: str
        :param dbschema: the database schema used to set the attributes of the key.
        :type dbschema: :class:`mspasspy.db.schema.DatabaseSchema`
        """
        for key, attr in self._main_dic.items():
            if "collection" in attr and attr["collection"] == original_collection:
                self.set_collection(key, new_collection, dbschema)

    def readonly(self, key):
        """
        Check if an attribute is marked readonly.

        :param key: key to be tested
        :type key: str
        :return: `True` if the key is readonly or its readonly attribute is not defined, else return `False`
        :rtype: bool
        :raises mspasspy.ccore.utility.MsPASSError: if the key is not defined
        """
        if key not in self._main_dic:
            raise MsPASSError(key + " is not defined", "Invalid")
        return (
            True
            if "readonly" not in self._main_dic[key]
            else self._main_dic[key]["readonly"]
        )

    def writeable(self, key):
        """
        Check if an attribute is writeable. Inverted logic from the readonly method.

        :param key: key to be tested
        :type key: str
        :return: `True` if the key is not readonly, else or its readonly attribute is not defined return `False`
        :rtype: bool
        :raises mspasspy.ccore.utility.MsPASSError: if the key is not defined
        """
        return not self.readonly(key)

    def set_readonly(self, key):
        """
        Lock an attribute to assure it will not be saved.

        Parameters can be defined readonly. That is a standard feature of this class,
        but is normally expected to be set on construction of the object.
        There are sometimes reason to lock out a parameter to keep it from being
        saved in output. This method allows this. On the other hand, use this feature
        only if you fully understand the downstream implications or you may experience
        unintended consequences.

        :param key: the key for the attribute with properties to be redefined
        :type key: str
        :raises mspasspy.ccore.utility.MsPASSError: if the key is not defined
        """
        if key not in self._main_dic:
            raise MsPASSError(key + " is not defined", "Invalid")
        self._main_dic[key]["readonly"] = True

    def set_writeable(self, key):
        """
        Force an attribute to be writeable.

        Normally some parameters are marked readonly on construction to avoid
        corrupting the database with inconsistent data defined with a common
        key. (e.g. sta) This method overrides such definitions for any key so
        marked. This method should be used with caution as it could have
        unintended side effects.

        :param key: the key for the attribute with properties to be redefined
        :type key: str
        :raises mspasspy.ccore.utility.MsPASSError: if the key is not defined
        """
        if key not in self._main_dic:
            raise MsPASSError(key + " is not defined", "Invalid")
        self._main_dic[key]["readonly"] = False


"""below is all about _check_format utility function """


def _is_basic_type(s):
    """
    Helper function used to check if the value of the type attribute is valid

    :param s: the str value in the type attribute
    :type s: str

    :return: `True` if the str value is valid, else return `False`
    :rtype: bool
    """
    s = s.strip().casefold()
    if s in [
        "objectid",
        "int",
        "integer",
        "float",
        "double",
        "bool",
        "boolean",
        "str",
        "string",
        "list",
        "dict",
        "bytes",
        "byte",
        "object",
    ]:
        return True
    return False


def _check_min_default_key(db_dict):
    """
    Helper function used to check if there are at least 3 defined default keys, which are 'wf', 'elog' and 'history_object'

    :param db_dict: the database schema dictionary
    :type db_dict: dict

    :return: True if contains, else raise a schema.SchemaError
    :rtype: bool
    """
    default_key_set = set()
    for collection in db_dict:
        default_key_set.add(collection)
        if "default" in db_dict[collection]:
            default_key_set.add(db_dict[collection]["default"])

    if (
        "wf" not in default_key_set
        or "elog" not in default_key_set
        or "history_object" not in default_key_set
    ):
        raise schema.SchemaError(
            "wf, elog and history_object must be all defined as default key in a collection or as a collection name itself"
        )
    return True


def _is_valid_schema(dic, schema_):
    """
    Helper function used to validate all collections in a schema dictionary
    :param dic: a schema dictionary
    :type dic: dict

    :param schema_: the defined schema used to validate a database collection
    :type schema_: schema.Schema

    :return: True if all collections pass the validation
    :rtype: bool
    """
    for collection in dic:
        schema_.validate(dic[collection])
    return True


def _is_valid_database_schema_definition(dic, collection_name_list):
    """
    Helper function used to validate all attributes in a database schema definition dictionary
    :param dic: a schema definition dictionary
    :type dic: dict

    :param collection_name_list: a list of collection name to be used for reference
    :type collection_name_list: list

    :return: True if all attributes pass the validation
    :rtype: bool
    """
    for attr in dic:
        if "reference" in dic[attr]:
            dic[attr] = _get_schema_definition_schema(
                collection_name_list, "database", True
            ).validate(dic[attr])
        else:
            dic[attr] = _get_schema_definition_schema(
                collection_name_list, "database", False
            ).validate(dic[attr])
    return True


def _is_valid_metadata_schema_definition(dic, collection_name_list):
    """
    Helper function used to validate all attributes in a metadata schema definition dictionary
    :param dic: a schema definition dictionary
    :type dic: dict

    :param collection_name_list: a list of collection name to be used for reference
    :type collection_name_list: list

    :return: True if all attributes pass the validation
    :rtype: bool
    """
    for attr in dic:
        if "collection" in dic[attr]:
            dic[attr] = _get_schema_definition_schema(
                collection_name_list, "metadata", True
            ).validate(dic[attr])
        else:
            dic[attr] = _get_schema_definition_schema(
                collection_name_list, "metadata", False
            ).validate(dic[attr])
    return True


def _get_schema_definition_schema(collection_name_list, name, ref):
    """
    Helper function to get the schema for schema definitions

    :param collection_name_list: a list of collection name to be used for reference
    :type collection_name_list: list

    :param name: the name of the schema, which can be "database" or "metadata"
    :type name: str

    :param ref: whether the schema definition uses a reference key
    :type ref: bool

    :return: the schema for valication
    :rtype: schema.Schema
    """
    type_key = schema.And(str, lambda s: _is_basic_type(s))
    type_required_schema = {"type": type_key}
    type_optional_schema = {schema.Optional("type"): type_key}

    constraint_key = schema.And(
        str, lambda s: s in ["required", "xref_key", "normal", "optional"]
    )
    constraint_schema = {schema.Optional("constraint"): constraint_key}
    constraint_default_schema = {
        schema.Optional("constraint", default="normal"): constraint_key
    }

    common_db_schema = {
        # if collection_name_list is empty, the reference check will always pass. This is used in the add method of SchemaDefinition only.
        schema.Optional("reference"): schema.And(
            str, lambda s: s in collection_name_list if collection_name_list else True
        ),
        schema.Optional("concept"): str,
        # aliases can only be string or a list of string anything else will be converted automatically here
        schema.Optional("aliases"): schema.Use(
            lambda s: (
                [s]
                if type(s) is str
                else ([str(i) for i in s] if type(s) is list else str(s))
            )
        ),
    }
    common_md_schema = {
        schema.Optional("collection"): schema.And(
            str, lambda s: s in collection_name_list if collection_name_list else True
        ),
        schema.Optional("concept"): str,
        # aliases can only be string or a list of string anything else will be converted automatically here
        schema.Optional("aliases"): schema.Use(
            lambda s: (
                [s]
                if type(s) is str
                else ([str(i) for i in s] if type(s) is list else str(s))
            )
        ),
        schema.Optional("readonly"): bool,
    }

    if name == "database":
        if ref:
            return schema.Schema(
                dict(
                    list(common_db_schema.items())
                    + list(type_optional_schema.items())
                    + list(constraint_schema.items())
                ),
                ignore_extra_keys=True,
            )
        else:
            return schema.Schema(
                dict(
                    list(common_db_schema.items())
                    + list(type_required_schema.items())
                    + list(constraint_default_schema.items())
                ),
                ignore_extra_keys=True,
            )
    else:  # name == "metadata"
        if ref:
            return schema.Schema(
                dict(
                    list(common_md_schema.items())
                    + list(type_optional_schema.items())
                    + list(constraint_schema.items())
                ),
                ignore_extra_keys=True,
            )
        else:
            return schema.Schema(
                dict(
                    list(common_md_schema.items())
                    + list(type_required_schema.items())
                    + list(constraint_default_schema.items())
                ),
                ignore_extra_keys=True,
            )


def _check_format(schema_dic):
    """
    check if a mspass.yaml file user provides is valid or not

    :param schema_dic: the dictionary that a yaml file is dumped
    :type schema_dic: dict
    :return: `True` if the schema is valid, else return `False`
    :rtype: bool
    """
    # Make sure Database and Metadata exist
    schema.Schema({"Database": dict, "Metadata": dict}).validate(schema_dic)
    collection_name_list = schema_dic["Database"].keys()

    database_collection_schema = schema.Schema(
        {
            schema.Optional("default"): str,
            schema.Optional("data_type"): str,
            schema.Optional("base"): schema.And(
                str, lambda s: s in collection_name_list
            ),
            "schema": schema.And(
                dict,
                lambda dic: _is_valid_database_schema_definition(
                    dic, collection_name_list
                ),
            ),
        },
        ignore_extra_keys=True,
    )

    metadata_collection_schema = schema.Schema(
        {
            "schema": schema.And(
                dict,
                lambda dic: _is_valid_metadata_schema_definition(
                    dic, collection_name_list
                ),
            )
        },
        ignore_extra_keys=True,
    )

    yaml_schema = schema.Schema(
        {
            "Database": schema.And(
                dict,
                schema.And(
                    lambda dic: _is_valid_schema(dic, database_collection_schema),
                    lambda dic: _check_min_default_key(dic),
                ),
            ),
            "Metadata": schema.And(
                dict, lambda dic: _is_valid_schema(dic, metadata_collection_schema)
            ),
        },
        ignore_extra_keys=True,
    )

    yaml_schema.validate(schema_dic)
