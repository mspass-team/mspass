import os
import yaml
import collections
from mspasspy.ccore.utility import MsPASSError, AntelopePf
from mspasspy.util.converter import AntelopePf2dict


def str_to_parameters_dict(parameter_str):
    """
     Parse the parameter string defined by user into an ordered dict.
     The input str should be in the format like "a, b, c=d, e=f, ..."
    :param parameter_str: a parameter string defined by user
    :return: An OrderedDict of parameters and arguments.
    """
    parameters_dict = collections.OrderedDict()

    pairs = parameter_str.replace(" ", "").split(",")
    unkeyword_index = 0
    for pair in pairs:
        k_v = pair.split("=")
        #   unkeyworded para
        if len(k_v) == 1:
            key = "arg_{arg_index:d}".format(arg_index=unkeyword_index)
            value = k_v[0]
            unkeyword_index += 1
        if len(k_v) == 2:
            key = k_v[0]
            value = k_v[1]
        if len(k_v) > 2:
            raise MsPASSError(
                "Wrong parameter string format: " + parameter_str + " Fatal"
            )
        parameters_dict[key] = value

    return parameters_dict


def params_to_parameters_dict(*args, **kwargs):
    """
     Capture a function's parameters, return a dict that stores parameters and arguments.
     Filepath arguments will be parsed into python object, and then turned into a dict. Now we support pf files
     and yaml files.
    :param args: Non-keyworded arguments
    :param kwargs: Keyworded arguments
    :return: An OrderedDict of parameters and arguments.
    """
    parameters_dict = collections.OrderedDict()
    #   Iterate over non-keyworded args and store them in dict, each one is assigned a key "arg_1" "arg_2" ...
    i = 0
    for value in args:
        key = "arg_{index:d}".format(index=i)
        parameters_dict[key] = str(value)
        i += 1
    #   Iterate over keyworded args and store them in dict
    for key, value in kwargs.items():
        parameters_dict[key] = str(value)

    return parameters_dict


def parse_filepath_in_parameters(parameters_dict):
    """
     Parse the filepath parameters in a function's parameters dict,
     Filepath arguments will be parsed into python object, and then turned into a dict.
     Currently we support pf files and yaml files.
    :param parameters_dict: parameter dict of a function
    :return: An OrderedDict of parameters and arguments.
    """

    def check_and_parse_file(arg):
        if (
            isinstance(arg, os.PathLike)
            or isinstance(arg, str)
            or isinstance(arg, bytes)
        ) and os.path.isfile(arg):
            file_path = str(arg)
            if file_path.endswith(".pf"):
                pf = AntelopePf(file_path)
                #   Convert PF into an OrderedDict to coordinate with the GTree
                pf_value = AntelopePf2dict(pf)
                return pf_value
            elif file_path.endswith(".yaml"):
                with open(file_path, "r") as yaml_file:
                    yaml_value = yaml.safe_load(yaml_file)
                return yaml_value
            #   Currently only support pf and yaml
            else:
                raise MsPASSError("Cannot handle file: " + file_path + "Fatal")
        return arg

    for key, value in parameters_dict.items():
        parameters_dict[key] = check_and_parse_file(value)

    return parameters_dict


def parameter_to_GTree(*args, parameters_str=None, **kwargs):
    """
     A helper function to parse parameters and build a GTree accordingly. This function would
     be used in GlobalHistoryManager to help record the parameters.
    :param args: Non-keyworded arguments
    :param kwargs: Keyworded arguments
    :param parameter_str: a parameter string defined by user
    :return: An OrderedDict of parameters and arguments.
    """
    if parameters_str:
        #   preprocess parameters and parse files, store in parsed_args_list and parsed_kwargs_dict
        parameters_dict = str_to_parameters_dict(parameters_str)
    else:
        parameters_dict = params_to_parameters_dict(*args, **kwargs)
    parameters_dict = parse_filepath_in_parameters(parameters_dict)
    gTree = ParameterGTree(parameters_dict)
    return gTree


class ParameterGTree(collections.OrderedDict):
    """
    Base class for family of objects used to hold an abstraction of a
    set of control parameters for a data processing function.   The
    base class abstracts the concept of storing such data in an g-tree
    structure.  In the documentation here the tree should be pictures as
    upright in the biological tree analog. i.e. up mean higher levels in
    the tree and down means dropping to lower levels in the tree.

    This class is inherited from collections.OrderedDict, so users can get
    access to the data using common index operation, for example:
    gTree['phases']['travel_time_calculator']['taup']['model_name']
    In addition, user can operate adding, deleting, updating, creating in
    the same way as the operations on OrderedDict.

    Each node of the g-tree may have leaves and/or a set
    of branches.   Most simple algorithms need only one node with leaves
    made up of name-value pairs.  More complex algorithms often need
    the tree structure to describe more complicated data control structures.
    """

    def __init__(self, doc=None):
        """
        Construct from a MongoDB document, which with pymongo is equivalenced
        to a python dict.  Branches are defined by subdocuments.
        """
        if doc is not None and not isinstance(doc, collections.OrderedDict):
            raise MsPASSError("[Error] Doc must be an OrderedDict.")

        self.control = doc

        if self.control is not None:
            for key, val in self.control.items():
                if isinstance(val, collections.OrderedDict):
                    branch = ParameterGTree(val)
                    self[key] = branch
                else:
                    self[key] = val

    def __setitem__(self, key, value):
        if isinstance(value, collections.OrderedDict):
            #   First check if the input value is already a GTree.
            #   if so, we don't need to construct it.
            if isinstance(value, ParameterGTree):
                branch = value
            else:
                branch = ParameterGTree(value)

            if key in self.get_leaf_keys():
                raise MsPASSError(
                    "[Warning] There already exists a leaf in this GTree with key '{leaf_key}', Please check again.".format(
                        leaf_key=key
                    )
                )
            collections.OrderedDict.__setitem__(self, key, branch)

        else:
            if key in self.get_branch_keys():
                raise MsPASSError(
                    "[Warning] There already exists a branch in this GTree with key '{branch_key}', Please check again.".format(
                        branch_key=key
                    )
                )
            collections.OrderedDict.__setitem__(self, key, value)

    def __getitem__(self, key):
        if key not in self:
            raise MsPASSError(
                "[Warning] The key provided ({branch_key}) is not in this GTree, Please check again.".format(
                    branch_key=key
                )
            )
        return collections.OrderedDict.__getitem__(self, key)

    def update_control(self):
        """
        Update the control doc according to the children in this level.
        As the hierarchy data may change in deeper level, so we need to check
        every sub tree. It is implemented by recursively updating the control
        doc of a tree.
        """
        new_control = self.copy()

        branch_keys = self.get_branch_keys()
        for key in branch_keys:
            branch = self[key]
            new_control[key] = branch.asdict()

        self.control = new_control

    def asdict(self):
        """
        Return the dictionary representation of the ParameterGTree instance.
        This function will first update the internal dictionary control doc.
        Its return can be a build-in dict or a collections.OrderedDict,
        according to the input when building this GTree.

        Since ParameterGTree is inheritted from OrderedDict, an instance can be
        directed transfered into a dict/OrderedDict without calling this function.
        """
        self.update_control()
        return self.control

    def get_leaf_keys(self):
        """
        Return the keys for all key-value pairs that are leaves at the current
        level of the parameter tree.   For branches this method can be used
        to extract leaves at the current level. Return is a dict of key-value
        pairs that we are calling the leaves of the tree.
        """
        leaf_keys = list()
        for key, value in self.items():
            if not isinstance(value, ParameterGTree):
                leaf_keys.append(key)

        return leaf_keys

    def get_branch_keys(self):
        """
        Return the keys for all branches from this level.   Branches are keyed with a keyword
        string like leaves.  Branches can be extracted with prune or we can
        walk the tree with a set of methods defined below.
        """
        branch_keys = list()
        for key, value in self.items():
            if isinstance(value, ParameterGTree):
                branch_keys.append(key)

        return branch_keys

    def get_branch(self, key):
        """
        Extract the contents of a named branch.  Returns a copy of the tree
        with the associated key from the branch name upward.  The tree
        returned will have the root of the tree set as current.
        """
        if key not in self.get_branch_keys():
            raise MsPASSError("[Error] Wrong Key, Please check your input key again.")

        branch = self[key]
        return branch

    def get_leaf(self, key):
        """
        Returns a copy of the key-value pair defined by key.
        This function only search for the key in this layer, and won't return
        value stored in higher levels.
        To search in the entire tree, use "get".
        """
        if key not in self.get_leaf_keys():
            raise MsPASSError("[Error] Wrong Key, Please check your input key again.")

        leaf = self[key]
        return leaf

    def prune(self, key):
        """
        Remove a branch or leaf defined by key from self. Return a copy of the
        branch/leaf pruned in the process (like get_branch/get_leaf but self is altered)
        """
        if key not in self:
            raise MsPASSError("[Error] Wrong Key, Please check your input key again.")
        if key in self.get_leaf_keys():
            ret_val = self.get_leaf(key)
        if key in self.get_branch_keys():
            ret_val = self.get_branch(key)
        collections.OrderedDict.popitem(self, key)
        return ret_val

    def get(self, key, seperator="."):
        """
        Fetch a value defined by key.  For leaves at the root node the
        key can be a simple string.  For a leaf attached at a higher level node
        we specify a chain of one or more branch names with keys between
        the specified seperator.  Examples (all used default value of seperator):
        1.  If we had a leaf node with the key 'name' under the branch name
            'phases' we use the compound key 'phases.name'. Such a tag could, for
            example, contain 'P' for the seismic to define this set of parameters
            as related to the P phase.
        2.  Suppose the phases branch was linked to higher level branch with
            the key 'travel_time_calculator' that had a leaf parameter 'taup'
            that is itself a branch name with terminal leaf keys under it.
            A real life example might be 'model_name'.  We would refer to that
            leaf with the string 'phases.travel_time_calculator.taup.model_name'.
            That key might, for example, have he value 'iasp91' which could be
            passed to obspy's taup calculator.

        This method should also support key defined as a python list.   The list
        would need to be a set of keys that would define the path to climb the
        tree to fetch the desired leaf.  For this form the two examples above
        would be represented as follows:
        1.  ['phases','name']
        2.  ['phases','travel_time_calculator','taup','model_name']

        Users can also use the build-in index operation to access the children elements,
        which is more natural.
        for example:
        1.  ['phases']['name']
        2.  ['phases']['travel_time_calculator']['taup']['model_name']

        """
        key_list = key
        if isinstance(key, str):
            key_list = key.split(seperator)

        if len(key_list) == 0:
            raise MsPASSError("The key is empty, please check again.")

        root = self
        for i in range(len(key_list) - 1):
            root = root.get_branch(key_list[i])

        return root.get_leaf(key_list[-1])

    def put(self, key, value, separator="."):
        """
        putter with same behavior for compound keys defined for get method.
        A put would create a new branch it implies if that branch is not
        already present.

        Same as the setter function, users can also use index to put new
        data in GTree here. Please note that when put data using indexes,
        new branches won't be created automatically, and users should add the
        intermediate branches themselves.
        """
        key_list = key
        if isinstance(key, str):
            key_list = key.split(separator)

        if len(key_list) == 0:
            raise MsPASSError("The key is empty, please check again.")

        root = self
        for i in range(len(key_list) - 1):
            branch_level = key_list[i]
            if branch_level in root.get_leaf_keys():
                raise MsPASSError(
                    "[Error] Invalid compound Key, there is a leaf with the same name in level "
                    + branch_level
                    + ". Please check your input key again."
                )
            if branch_level not in root.get_branch_keys():
                root[branch_level] = ParameterGTree()
            root = root.get_branch(branch_level)

        leaf_key = key_list[-1]
        if leaf_key in root.get_branch_keys():
            raise MsPASSError(
                "[Error] Invalid compound Key, there is a branch with the same name in "
                + leaf_key
                + ". Please check your input key again."
            )
        root[leaf_key] = value

    '''
    This function here is not necessary, user can just use index to add new data,
    for example:
        root[branch_level] = ParameterGTree()

    def sprout(self, key, seperator="."):
        """
        Add an empty branch with tag key. Compound keys are as described in
        get method above.
        Similar to the putter method, but create a branch in the end.
        """
        key_list = key
        if isinstance(key, str):
            key_list = key.split(seperator)

        if len(key_list) == 0:
            raise MsPASSError("The key is empty, please check again.")

        root = self
        for i in range(len(key_list) - 1):
            branch_level = key_list[i]
            if branch_level in root.get_leaf_keys():
                raise MsPASSError(
                    "[Error] Invalid compound Key, there is a leaf with the same name in level "
                    + branch_level
                    + ". Please check your input key again."
                )
            if branch_level not in root.get_branch_keys():
                root[branch_level] = ParameterGTree()
            root = root.get_branch(branch_level)

        branch_key = key_list[-1]
        if branch_key in root.get_leaf_keys():
            raise MsPASSError(
                "[Error] Invalid compound Key, there is a leaf with the same name in "
                + branch_key
                + ". Please check your input key again."
            )
        root[branch_key] = ParameterGTree()
    '''
