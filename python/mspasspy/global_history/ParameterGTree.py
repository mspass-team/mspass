import collections
from mspasspy.ccore.utility import MsPASSError

class ParameterGTree:
    """
    Base class for family of objects used to hold an abstraction of a
    set of control parameters for a data processing function.   The
    base class abstracts the concept of storing such data in an g-tree
    structure.  In the documentation here the tree should be pictures as
    upright in the biological tree analog. i.e. up mean higher levels in
    the tree and down means dropping to lower levels in the tree.

    Each node of the g-tree may have leaves and/or a set
    of branches.   Most simple algorithms need only one node with leaves
    made up of name-value pairs.  More complex algorithms often need
    the tree structure to describe more complicated data control structures.
    """

    def __init__(self, doc = None):
        """
        Construct from a MongoDB document, which with pymongo is equivalenced
        to a python dict.  Branches are defined by subdocuments.
        """
        self.children = collections.OrderedDict()
        #self.parent = None
        #self.leaf_keys = None
        #self.branch_keys = None
        self.control = dict(doc)
        
        for key, val in self.control.items():
            if(isinstance(val, dict)):
                branch = ParameterGTree(val)
                self.children[key] = branch
            else:
                self.children[key] = val
    
    def get_leaf_keys(self):
        """
        Return the keys for all key-value pairs that are leaves at the current
        level of the parameter tree.   For branches this method can be used
        to extract leaves at the current level. Return is a dict of key-value
        pairs that we are calling the leaves of the tree.
        """
        #if(self.leaf_keys != None):
        #    return self.leaf_keys
        
        leaf_keys = list()
        for key, value in self.children.items():
            if not isinstance(value, ParameterGTree):
                leaf_keys.insert(value)
        #self.leaf_keys = leaf_keys

        return leaf_keys

    def get_branch_keys(self):
        """
        Return the keys for all branches from this level.   Branches are keyed with a keyword
        string like leaves.  Branches can be extracted with prune or we can
        walk the tree with a set of methods defined below.
        """

        #if(self.branch_keys != None):
        #    return self.leaf_keys
        
        branch_keys = list()
        for key, value in self.children.items():
            if isinstance(value, ParameterGTree):
                branch_keys.insert(value)
        #self.branch_keys = branch_keys
        
        return branch_keys

    def get_branch(self,key):
        """
        Extract the contents of a named branch.  Returns a copy of the tree
        with the associated key from the branch name upward.  The tree
        returned will have the root of the tree set as current.
        """
        if(key not in self.children):
            raise MsPASSError("[Error] Wrong Key, Please check your input key again.")
        
        branch = self.children[key]
        if not isinstance(branch, ParameterGTree):
            raise MsPASSError("[Error] The Value paired with this key is not a branch, Please check again.")
        
        return branch

    def get_leaf(self,key):
        """
        Returns a copy of the key-value pair defined by key.
        This function only search for the key in this layer, and won't return
        value stored in higher levels.
        To search in the entire tree, use "get".
        """
        if(key not in self.children):
            raise MsPASSError("[Error] Wrong Key, Please check your input key again.")
        
        leaf = self.children[key]
        if isinstance(leaf, ParameterGTree):
            raise MsPASSError("[Error] The Value paired with this key is not a leaf, Please check again.")
        
        return leaf

    def prune(self,key):
        """
        Remove a branch defined by key from self.  Return a copy of the
        branch pruned in the process (like get_branch but self is altered)
        """

        branch = self.get_branch(key)
        self.children.popitem(key)
        return branch

    def get(self,key,seperator='.'):
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
        """

        key_list = key
        if(isinstance(key, str)):
            key_list = key.split(seperator)
        
        if(len(key_list) == 0):
            raise MsPASSError("The key is empty, please check again.")

        if(len(key_list) == 1):
            val = self.get_leaf(key_list[0])
            return val
        else:
            higher_branch = self.get_branch(key_list[0])
            return higher_branch.get(key_list[1:])

        '''
        Non-Recursive Version:

        root = self
        for i in range(len(key_list) - 1):
            root = root.get_branch(key_list[i])

        return root.get_leaf(key_list[-1])
        
        '''
    def put(self,key,separator='.'):
        """
        putter with same behavior for compound keys defined for get method.
        A put should create a new branch it implies if that branch is not
        already present.
        """
        key_list = key
        if(isinstance(key, str)):
            key_list = key.split(separator)
        
        if(len(key_list) == 1):
            val = self.get_leaf(key_list[0])
            return val
        else:
            branch = self.get_branch(key_list[0])
            return self.get(key_list[1:])


    def sprout(self,key,seperator='.'):
        """
        Add an empty branch with tag key. Compound keys are as described in
        get method above.
        In this method, the key path is assumed to exist.
        Return the root of the new branch.
        """

        key_list = key
        if(isinstance(key, str)):
            key_list = key.split(seperator)
        
        if(len(key_list) == 0):
            raise MsPASSError("The key is empty, please check again.")

        #   First check the branch in the next layer, if not exits, create it
        created = False
        higher_branch_str = key_list[0]
        if(higher_branch_str not in self.children):    
            new_branch = ParameterGTree()
            self.children[higher_branch_str] = new_branch
            created = True

        higher_branch = self.children[higher_branch_str]
        if(len(key_list) == 1):
            if(created):
                return higher_branch
            else:
                raise MsPASSError("Warning, the key already exists. Please check again.")
        else:
            return higher_branch.sprout(key_list[1:])
