{
  'info' => {
    'database_server' => '',
	  'database_port' => '',
    'database_username' => '',
    'database_password' => '',
    'kinetic_api_location' => '',
    'kinetic_api_username' => '',
    'kinetic_api_password' => '',
    'kapp_slug' => 'datastore',
    'kapp_form_slug' => 'sql-query-template',
    'enable_debug_logging' => 'Yes'
  },
  'parameters' => {
    'jdbc_database' => 'postgresql',
	  'dbname' => 'my_postgres',
    'action'  => 'fetch',
    'template_name' => 'Update Hero',
    'query_values' => '{"secret_id":"superman","name":"clark kent"}',

    #'query'  => "UPDATE MKTest.dbo.test SET [First Name] = 'asdf' WHERE [Last Name] = 'Klein'",
	  #'action'  => 'run',

	  'error_handling' => 'Raise Error'
  }
}
