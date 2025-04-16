# Sql Generic V5

NOTE: To use IntegratedSecurity you must copy the sqljdbc_auth.dll from "vendor/auth" into a java.library.path location (ex: java_install_path/bin or tomcat_install_path/bin)

The generic sql handler uses a predefined query to execute against a SQL database.  The predefined queries are kept in a Kapp Form submission and retrieved when the handler runs.  The Kapp Slug and Kapp Form's slug is defined as a info value for the handler.  
In order for the handler to retrieve the correct query the Kapp Form needs a unique index to search on.  The handler expects the Kapp From to have a field with the name **Template Name** and the form should use that field as a unique index.

This handler works with Platform v5.2+

## Info values
[database_server]

	JDBC connection that specifies the database location.
[database_port]

	The port the database is on
[integrated_security]

	true/false - Enables integrated security on connection - NOTE sqljdbc_auth.dll required and username is ignored
[trust_server_cert_ssl_require]

	SQL Connection
	true/false - Encrypt but bypass walking the certificate chain to validate trust.
	-----
	ORACLE Connection

	-----
	POSTGRESQL Connection
	disable
	allow
	prefer
	require
	verify-ca
	verify-full
[database_username]

	The username used to connect to the database
[database_password]

	The password for the user used to connect to the database
[kinetic_api_location]

	API Location (https://acme.kinops.io/app/api/v1).
[kinetic_api_username]

	The username for impersonated user.
[kinetic_api_password]

	The impersonated user's password.
[kapp_slug]

	The kapp slug where the form that holds the query templates is located.
[kapp_form_slug]

	The form slug for the form where query templates are saved.
[enable_debug_logging]

	Enable debug logging if the value is set to 'Yes'.

## Parameters

[Error Handling]

	Determine what to return if an error is encountered.
[Database]

	Database to Access for example sqlserver, oracle or postgresql.
[Database Name]

	Database Name
[SQL Action]

	SQL Action Type. Use fetch for a SELECT statement or run for an UPDATE query.
[Template Name]

	The identifier of the predefined query.
[Query Values]

	Values that will be parameterized in the query.  Send in as a JSON object where the key is the parameter defined in the predefined query and the value is what is should be parameterized to.
## Results

[Result]  

    For SELECT statements the result will be a JSON string containing the records matching the WHERE clause.
	For action statements this value will = Successful if there are no errors running the command

[Handler Error Message]  

    If any errors were encountered and the Error Handling is set to Error Message the error will be returned

### example query

 `Update super_heros SET real_name={{name}} WHERE hero_name={{secret_id}}`

 The above query will update the **real_name** field with the value for the _name_ parameter.  The update is to the **super_heros** table where the **hero_name** equals the value for the *secret_id* parameter.

 An example of the Query Values for the above query:
 ```javascript
{
  name: Bruce Wayne,
  secret_id: Batman
}
 ```

### Notes:
 * There is a template kapp form included with the handler.  Add kapp_form_definition.json to the appropriate kapp to have a form that is configured to work with this handler.
