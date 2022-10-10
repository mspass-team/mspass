from mspasspy import hasDask, hasSpark

if hasDask or hasSpark:
    from mspasspy.db.database import *
else:
    from mspasspy.db.database import Database
from mspasspy.db.client import *
