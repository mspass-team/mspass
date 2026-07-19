.. _schema_choices:

What database schema should I use?
=======================================

This choice has two endmembers.  At one end is a research workflow
that is highly experimental and subject to large changes.  At the other end
is a production workflow designed to run on a large dataset and a cluster
with many processors.

Even an experimental workflow loads a schema when its MsPASS
:py:class:`Database<mspasspy.db.database.Database>` handle is constructed.
The best default for that situation is the compact ``mspass_lite.yaml``
schema.  Compared with the default ``mspass.yaml``, it defines fewer
collections and metadata keys, keeps the SEED identifiers directly in the
waveform documents, and minimizes read-only metadata.  The MongoDB ``_id``
key remains read-only in both schemas.

For example, to use a database named ``mydb``, put the following near the
top of the Jupyter notebook that defines your workflow:

.. code-block:: python

  from mspasspy.db.client import DBClient

  dbclient = DBClient()
  db = dbclient.get_database("mydb", schema="mspass_lite.yaml")

Schema selection and schema enforcement are related but distinct.  The YAML
file defines recognized collections, metadata keys, types, aliases,
cross-references, required fields, and read-only fields.  Database methods
that accept a ``mode`` argument control how strictly they apply relevant
schema checks.  See :ref:`CRUD operations <CRUD_operations>` for the behavior
of ``promiscuous``, ``cautious``, and ``pedantic`` modes.

For most production workflows, start with ``mspass.yaml``.  It defines the
normalized ``site``, ``channel``, and ``source`` collections, as well as the
``wf_miniseed`` collection used to index raw miniSEED data.  The complete
collection and metadata-key tables for that schema are in the
:ref:`MsPASS schema reference <mspass_schema>`.

The other shipped alternatives in :file:`data/yaml` are
``mspass_fdsn.yaml`` and ``mspass_s3.yaml``.  They adapt the
``wf_miniseed`` metadata for the FDSN and Amazon S3 access paths,
respectively.  If no shipped schema fits, copy the closest YAML file and
update its ``Database`` and ``Metadata`` sections consistently, then pass
that filename or path through the ``schema`` argument.  See the APIs for
:py:class:`DatabaseSchema<mspasspy.db.schema.DatabaseSchema>` and
:py:class:`MetadataSchema<mspasspy.db.schema.MetadataSchema>` for the schema
objects created from that file.
