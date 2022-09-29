require 'java'

handler_path = File.expand_path(File.dirname(__FILE__))

##### REQUIRE REST CLINET #####

# Load the ruby Mime Types library unless it has already been loaded.  This 
# prevents multiple handlers using the same library from causing problems.
if not defined?(MIME)
  library_path = File.join(handler_path, "vendor/mime-types-1.19/lib/")
  $:.unshift library_path
  require "mime/types"
end

# Validate the the loaded Mime Types library is the library that is expected for
# this handler to execute properly.
if not defined?(MIME::Types::VERSION)
  raise "The Mime class does not define the expected VERSION constant."
elsif MIME::Types::VERSION != "1.19"
  raise "Incompatible library version #{MIME::Types::VERSION} for Mime Types.  Expecting version 1.19."
end


# Load the ruby rest-client library unless it has already been loaded.  This 
# prevents multiple handlers using the same library from causing problems.
if not defined?(RestClient)
  library_path = File.join(handler_path, "vendor/rest-client-1.6.7/lib")
  $:.unshift library_path
  require "rest-client"
end

# Validate the the loaded rest-client library is the library that is expected for
# this handler to execute properly.
if not defined?(RestClient.version)
  raise "The RestClient class does not define the expected VERSION constant."
elsif RestClient.version.to_s != "1.6.7"
  raise "Incompatible library version #{RestClient.version} for rest-client.  Expecting version 1.6.7."
end

##### REQUIRE THE DATABASE LIBRARY AND DRIVERS #####

if not defined?(Sequel)
  # Calculate the location of our library and add it to the Ruby load path
  library_path = File.join(handler_path, "vendor/sequel-5.24.0/lib")
  $:.unshift library_path
  # Require the library
  require "sequel"
end

if not defined?(Sequel::VERSION)
  raise "The Sequel class does not define the expected VERSION constant."
elsif Sequel::VERSION.to_s != "5.24.0"
  raise "Incompatible library version #{Sequel::VERSION} for Sequel.  Expecting version 5.24.0."
end

begin
  org.postgresql.Driver
rescue NameError
  require File.join(handler_path, "vendor", "postgresql-9.4-1206-jdbc41.jar")
  import org.postgresql.Driver
end

begin
  com.microsoft.sqlserver.jdbc.SQLServerDriver
rescue NameError
  require File.join(handler_path, "vendor", "sqljdbc42.jar")
  import com.microsoft.sqlserver.jdbc.SQLServerDriver
end

begin
  oracle.jdbc.driver.OracleDriver
rescue NameError
  require File.join(handler_path, "vendor", "ojdbc8.jar")
  java_import 'oracle.jdbc.OracleDriver'
end

#begin
#  com.mysql.cj.jdbc.Driver
#rescue NameError
#  handler_path = File.expand_path(File.dirname(__FILE__))
#  require File.join(handler_path, "vendor", "mysql-connector-java-8.0.18.jar")
#  import com.mysql.cj.jdbc.Driver
#end

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException