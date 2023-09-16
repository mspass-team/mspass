.. _schema_choices

What database schema should I use?
=======================================

This is another question with endmembers.  At one end is a research workflow
that is highly experimental and subject to large changes.   At the other end
is a production workflow you are constructing to run on a large dataset
and a cluster with many processors.

For the former (research code), the answer is usually
none.  The baggage of a schema mostly gets in the way.
The best default for that situation is the one called `mspass-lite`.
It differs from the global default, `mspass`, in that it imposes no
read-only restriction that can cause confusing errors in some situations.
For this situation if you aim to use the database with the name tag
`mydb` use the following as the top box of your jupyter notebook file
defining your workflow:

.. code-block:: python

  from mspasspy.db.client import DBClient
  dbclient = DBClient()
  db = dbclient.get_database("mydb",schema="mspass_lite.yaml")

For a more tuned application, you should first consider a related
question:  Does my workflow need schema enforcement?   If the answer is no,
just use the `mspass-lite` schema as illustrated above.
In most case, however, it is our experience that for large datasets
schema constraints can provide an important way to avoid mysterious errors.
For the case with a yes answer there are two options.   For most
application the standard `mspass` schema should be sufficient.   It provides
base functionality for source and receiver metadata.   It also defines
the `wf_miniseed` collection that is essential when the processing
sequence uses miniseed data as the parent data.  If that has issues
for you look at the `data/yaml` directory for the mspass source code
tree for alternatives you may find there.   If none of those work for you
it is relatively easy to extend any of the existing schema files.
See related sections in this User's Manual for guidance.
