[200~CHANGELOG
=========

This project uses `semantic versioning <http://semver.org/>`_.
This change log uses principles from `keep a changelog <http://keepachangelog.com/>`_.

0.2.2
------------

Added
^^^^^
* Data underlying a workflow definition is now shared; This makes it easier
  to build a single definition from python modules.
* The _shared argument was added to the workflow init for making workflows that are not
  based on the shared data. This was added for testing purposes and is not considered part of the API.
* Expressing the cycledef for a task can be done via the CycleDefinition objects that
  are returned by workflow.define_cycle() or by the group name string.
* workflow.define_cycle() will no longer raise an error if adding a cycle with the same
  group name so long as the definitions are identical.

0.2.1
------------

Fixed
^^^^^
* Fixed double memory tag bug
* Fixed metatask name attribute bug


0.2.0 First Beta intended for users
------------

Added
^^^^^
* API for creating rocoto XML workflow definition files
* Brief documentation and some simple examples


Changed
^^^^^^^


Deprecated
^^^^^^^^^^


Removed
^^^^^^^


Fixed
^^^^^


Security
^^^^^^^^



