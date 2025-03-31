sql_generic_v5 (2025-03-31) - Travis Wiese
    * Added sqljdbc_auth.dll (12.10) to sql_generic_v5/vendor/auth folder - This needs to be moved to java.library.path location in order to use integratedSecurity
    * Adjusted connection logic to support integratedSecurity and trustServerCertificate
    * Updated info and node files to support integratedSecurity and trustServerCertificate
sql_generic_v4 (2022-09-29)
    * Updated to remove datastore references
    * Added info value for kapp_slug
    * Renamed info value space_form to kapp_slug_form
sql_generic_v3 (2021-03-01)
    * Initial version.  See README for details.
