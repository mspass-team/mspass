.. _mongodb_6_to_8_migration:

MongoDB 6.0 to 8.0 migration
================================

MsPASS container images install MongoDB ``8.0.29`` exactly.  MongoDB does not
support skipping a major release when upgrading persisted data.  A 6.0 data
directory must therefore pass through 7.0 before an 8.0 binary opens it.

Back up production data and rehearse the migration on disposable data first.
The repository supplies a standalone rehearsal harness that performs and
verifies this sequence:

#. MongoDB 6.0.26 with FCV 6.0, followed by a read/write check and clean stop.
#. Start MongoDB 7.0.29 at FCV 6.0, set FCV 7.0, cleanly restart the same
   7.0.29 binary, and repeat the check.
#. Start MongoDB 8.0.29 at FCV 7.0, set FCV 8.0, cleanly restart the same
   8.0.29 binary, and repeat the check.

Run it with a new, explicitly disposable directory:

.. code-block:: bash

   export MSPASS_MONGO_UPGRADE_ROOT="$(mktemp -d)/mspass-mongo-upgrade"
   scripts/mongodb_upgrade_6_to_8.sh

The harness refuses an existing directory and broad paths such as ``/`` or a
home directory.  It writes ``last_completed_stage`` after each verified stage.
If any command fails, ``set -e`` stops the harness before the next binary or
FCV is started and leaves the data directory available for inspection.
**Do not point this rehearsal harness at production data.**

CI also exercises the stop boundary explicitly.  The following command exits
after the 6.0 verification and must leave ``last_completed_stage`` equal to
``6.0``:

.. code-block:: bash

   MSPASS_MONGO_UPGRADE_FAIL_AFTER=6.0 \
     MSPASS_MONGO_UPGRADE_ROOT="$(mktemp -d)/mspass-mongo-upgrade" \
     scripts/mongodb_upgrade_6_to_8.sh

For a production migration, take and validate a restorable backup, stop all
MsPASS writers, and follow the same major-version/FCV sequence using the
deployment's supported backup, rolling-upgrade, monitoring, and rollback
procedures.  Never start the next major binary until the current binary has
restarted at its target FCV, its read/write validation has succeeded, and the
current stage has stopped cleanly.
