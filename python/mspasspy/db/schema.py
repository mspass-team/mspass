"""
Tools to define the schema of Metadata.
"""
import os

import yaml
import schema

from mspasspy.ccore.utility import MsPASSError

class SchemaBase:
    def __init__(self, schema_file=None):
        if schema_file is None and 'MSPASS_HOME' in os.environ:
            schema_file = os.path.abspath(os.environ['MSPASS_HOME']) + '/data/yaml/mspass.yaml'
        else:
            schema_file = os.path.abspath(os.path.dirname(__file__) + '/../data/yaml/mspass.yaml')
        try:
            with open(schema_file, 'r') as stream:
                try:
                    schema_dic = yaml.safe_load(stream)
                except yaml.YAMLError as e:
                    raise MsPASSError('Cannot parse schema definition file: ' + schema_file, 'Fatal') from e
        except EnvironmentError as e:
            raise MsPASSError('Cannot open schema definition file: ' + schema_file, 'Fatal') from e
        
        try:
            _check_format(schema_dic)
        except schema.SchemaError as e:
            raise MsPASSError('The schema definition is not valid', 'Fatal') from e
        self._raw = schema_dic

class SchemaDefinitionBase:
    _main_dic = {}
    _alias_dic = {}
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
        if 'type' not in attr:
            raise MsPASSError('type is not defined for the new attribute', 'Invalid')
        self._main_dic[name] = attr
        if 'aliases' in attr:
            for als in attr['aliases']:
                self._alias_dic[als] = name
    
    def add_alias(self, key, aliasname):
        """
        Add an alias for key

        :param key: key to be added
        :type key: str
        :param aliasname: aliasname to be added
        :type aliasname: str
        """
        self._main_dic[key]['aliases'].append(aliasname)
        self._alias_dic[aliasname] = key
    
    def aliases(self, key):
        """
        Get a list of aliases for a given key.

        :param key: The unique key that has aliases defined. 
        :type key: str
        :return: A list of aliases associated to the key.
        :rtype: list
        """
        return None if 'aliases' not in self._main_dic[key] else self._main_dic[key]['aliases']
    
    # TODO def apply_aliases

    def clear_aliases(self, md):
        """
        Restore any aliases to unique names.

        Aliases are needed to support legacy packages, but can cause downstream problem 
        if left intact. This method clears any aliases and sets them to the unique_name 
        defined by this object.

        :param md: Data object to be altered. Normally a class:`mspasspy.ccore.seismic.Seismogram` 
            or class:`mspasspy.ccore.seismic.TimeSeries` but can be a raw class:`mspasspy.ccore.utility.Metadata`.
        :type md: class:`mspasspy.ccore.utility.Metadata`
        """
        for key in md.keys():
            if self.is_alias(key):
                md.change_key(key, self.unique_name(key))

    def concept(self, key):
        """
        Return a description of the concept this attribute defines.

        :param key: The name that defines the attribute of interest
        :type key: str
        :return: A string with a terse description of the concept this attribute defines
        :rtype: str
        :raises mspasspy.ccore.utility.MsPASSError: if concept is not defined
        """
        if 'concept' not in self._main_dic[key]:
            raise MsPASSError('concept is not defined for ' + key, 'Complaint')
        return self._main_dic[key]['concept']

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
        return key in self._main_dic and 'aliases' in self._main_dic[key]

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

    def is_optional(self, key):
        """
        Test if a key is optional to the schema

        :param key: key to be tested
        :type key: str
        :return: `True` if the key is optional
        :rtype: bool
        """
        return False if 'optional' not in self._main_dic[key] else self._main_dic[key]['optional']

    def keys(self):
        """
        Get a list of all the unique keys defined.

        :return: `True` if the key is defined
        :rtype: list
        """
        return self._main_dic.keys()

    def type(self, key):
        """
        Return the type of an attribute.

        :param key: The name that defines the attribute of interest
        :type key: str
        :return: `True` if the key is defined
        :rtype: list
        """
        return self._main_dic[key]['type']

    def unique_name(self, aliasname):
        """
        Get definitive name for an alias.

        This method is used to ask the opposite question as aliases. The aliases method 
        returns all acceptable alternatives to a definitive name defined as the key to 
        get said list. This method asks what definitive key should be used to fetch an 
        attribute.

        :param aliasname: the name of the alias for which we want the definitive key
        :type aliasname: str
        :return: the name of the definitive key
        :rtype: str
        :raises mspasspy.ccore.utility.MsPASSError: if aliasname is not an alias or not defined
        """
        if aliasname not in self._alias_dic:
            raise MsPASSError(aliasname + ' is not an alias or not defined' 'Invalid')
        return self._alias_dic[aliasname]

class DatabaseSchema(SchemaBase):
    def __init__(self, schema_file=None):
        super().__init__(schema_file)
        for collection in self._raw['Database']:
            setattr(self, collection, DBSchemaDefinition(self._raw['Database'], collection))


class DBSchemaDefinition(SchemaDefinitionBase):
    def __init__(self, schema_dic, collection_str):
        self._collection_str = collection_str
        if 'base' in schema_dic[collection_str]:
            base_def = DBSchemaDefinition(schema_dic, schema_dic[collection_str]['base'])
            self._main_dic = base_def._main_dic
            self._alias_dic = base_def._alias_dic
        else:
            self._main_dic = {}
            self._alias_dic = {}
        self._main_dic.update(schema_dic[collection_str]['schema'])
        for key, attr in self._main_dic.items():
            if 'reference' in attr:
                k = key
                if k == attr['reference'] + '_id':
                    k = '_id'
                foreign_attr = schema_dic[attr['reference']]['schema'][k]
                # The order of below operation matters. The behavior is that we only 
                # extend attr with items from foreign_attr that are not defined in attr.
                # This garantees that the foreign_attr won't overwrite attr's exisiting keys.
                compiled_attr = dict(list(foreign_attr.items()) + list(attr.items()))
                self._main_dic[key] = compiled_attr

            if 'aliases' in attr:
                self._alias_dic.update({item:key for item in attr['aliases']})
    
    def reference(self, key):
        """
        Return the collection name that a key is referenced from

        :param key: the name of the key
        :type key: str
        :return: the name of the collection
        :rtype: str
        :raises mspasspy.ccore.utility.MsPASSError: if the key is not defined
        """
        if key not in self._main_dic:
            raise MsPASSError(key + ' is not defined' 'Invalid')
        return self._collection_str if 'reference' not in self._main_dic[key] else self._main_dic[key]['reference']

class MetadataSchema(SchemaBase):
    def __init__(self, schema_file=None):
        super().__init__(schema_file)
        dbschema = DatabaseSchema(schema_file)
        for collection in self._raw['Metadata']:
            setattr(self, collection, MDSchemaDefinition(self._raw['Metadata'], collection, dbschema))


class MDSchemaDefinition(SchemaDefinitionBase):
    def __init__(self, schema_dic, collection_str, dbschema):
        if 'base' in schema_dic[collection_str]:
            base_def = MDSchemaDefinition(schema_dic, schema_dic[collection_str]['base'], dbschema)
            self._main_dic = base_def._main_dic
            self._alias_dic = base_def._alias_dic
        else:
            self._main_dic = {}
            self._alias_dic = {}
        self._main_dic.update(schema_dic[collection_str]['schema'])
        for key, attr in self._main_dic.items():
            if 'collection' in attr:
                s_key = key
                col_name = attr['collection']
                if key.startswith(col_name):
                    s_key = key.replace(col_name + '_', '')
                foreign_attr = getattr(dbschema,col_name)._main_dic[s_key]
                # The order of below operation matters. The behavior is that we only 
                # extend attr with items from foreign_attr that are not defined in attr.
                # This garantees that the foreign_attr won't overwrite attr's exisiting keys.
                compiled_attr = dict(list(foreign_attr.items()) + list(attr.items()))
                self._main_dic[key] = compiled_attr

            if 'aliases' in attr:
                self._alias_dic.update({item:key for item in attr['aliases']})
                
    def collection(self, key):
        """
        Return the collection name that a key belongs to

        :param key: the name of the key
        :type key: str
        :return: the name of the collection
        :rtype: str
        """
        return None if 'collection' not in self._main_dic[key] else self._main_dic[key]['collection']

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
            raise MsPASSError(key + ' is not defined' 'Invalid')
        return True if 'readonly' not in self._main_dic[key] else self._main_dic[key]['readonly']
        
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
            raise MsPASSError(key + ' is not defined' 'Invalid')
        self._main_dic[key]['readonly'] = True
        
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
            raise MsPASSError(key + ' is not defined' 'Invalid')
        self._main_dic[key]['readonly'] = False

def _check_format(schema_dic):
    pass