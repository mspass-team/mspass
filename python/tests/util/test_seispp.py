from mspasspy.db import Database
from mspasspy.db import Client
from mspasspy.util.seispp import index_data

def setup_function(function):
    client = Client('localhost')
    function.db = Database(client, 'dbtest')
    function.data = "python/tests/data/sample"

# FIXME: index_data will read the whole file to figure out the offset, 
# which is inefficient. i.e. junk=np.fromfile(fh,dtype=dtyp,count=ns3c)
def backup_test_index_data():
    index_data(test_index_data.data, test_index_data.db)
    wfcol = test_index_data.db.wf
    assert wfcol.count_documents({}) == 3
    assert wfcol.count_documents({'sta': 'ARV'}) == 1
    assert wfcol.count_documents({'sta': 'BC3'}) == 1
    assert wfcol.count_documents({'sta': 'BCC'}) == 1
    assert wfcol.find({'sta': 'BCC'})[0]['foff'] == 1344048