Database [handlers] (2021-03-01)
*  [sql_generic_v3]
  * Initial commit of generic SQL handler. 

Database [bridge-adapters] (2024-02-20)
  * [kinetic-bridgehub-adapter-database] v1.0.4
    * remove mysql dependency due to vulnerability.  Updated the distribution repository to pull from the latest kinetic internal repository.

Database [bridge-adapters] (2024-11-05)
  * [kinetic-bridgehub-adapter-database] v1.0.6
    * Rebuild the bridgehub adapter from commit 0f13d05, and cherry-pick commits from commit 156a1f9.
      When we refactored the main and master branches for this repository, it seems that we did not properly manage the branches.
      DO NOT USE 1.0.5.