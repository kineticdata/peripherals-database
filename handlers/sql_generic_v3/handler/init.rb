# Ensure that the JRuby java library is loaded
require 'java'
require File.expand_path(File.join(File.dirname(__FILE__), 'dependencies'))

#this is to test the server and port number
require 'socket'

class SqlGenericV3
  # Initializes the handler and set the following instance variables:
  # * @input_document - A REXML::Document object that represents the input Xml.
  # * @configuration  - A Hash of handler configuration information.
  # * @info_values    - A Hash of info value names to values.
  # * @parameters     - A Hash of parameter names to parameter values.
  #
  # This is a required method that is automatically called by the Kinetic Task
  # Engine.
  #
  # ==== Parameters
  # * +input+ - The String of Xml that was built by evaluating the node.xml
  #   handler template.
  def initialize(input)
    # It may make sense to parameterize this value.
    # For some returned queries the volume of data generated an error message when
    # this is set to the default value of 10240.  Future releases of Kinetic Platform
    # may adjust this setting.  Here we're checking if it is currently less than an
    # arbitrarily higher value.  This setting is globally applicable within Task.
    if REXML::Security.entity_expansion_text_limit < 163840
      puts "Setting 'Entity Expansion Limit' to 163840" if @enable_debug_logging
      REXML::Security.entity_expansion_text_limit=(163840)
    end

    # Set the input document attribute
    @input_document = REXML::Document.new(input)

    # Retrieve all of the handler info values and store them in a hash attribute
    # named @info_values.
    @info_values = {}
    REXML::XPath.match(@input_document, '/handler/infos/info').each do |node|
      @info_values[node.attribute('name').value] = node.text
    end

    # Retrieve all of the handler configuration and store them in a hash attribute
    # named @configuration.
    @configuration = {}
    REXML::XPath.match(@input_document, '/handler/configuration/config').each do |node|
      @configuration[node.attribute('name').value] = node.text
    end

	# Retrieve all of the handler parameters and store them in a hash variable named @parameters.
    @parameters = {}
    REXML::XPath.each(@input_document, "/handler/parameters/parameter") do |item|
      @parameters[item.attributes["name"]] = item.text.to_s.strip
    end

    @enable_debug_logging = @info_values['enable_debug_logging'] == 'Yes'
    @accept = :json
    @content_type = :json

  end

  # Executes the specified SQL query.
  #
  # This is a required method that is automatically called by the Kinetic Task
  # Engine.
  #
  # ==== Returns
  # An Xml formatted String representing the return variable results.
  def execute
  template_name = @parameters["template_name"]
  error_handling = @parameters['error_handling']
  space_form = @info_values["space_form"]
  db_server = @info_values["database_server"]
  db_port = @info_values["database_port"]
  db_name = @parameters["dbname"]
  db_username = @info_values['database_username']
  db_password = @info_values["database_password"]
  error_message = nil
  

    #checking to see if the server and port are open
    #this was put in to test java vm access to my local sql server
    begin
      kinetic_api_route = "#{@info_values["kinetic_api_location"]}/datastore/forms/" +
        "#{space_form}/submissions?index=values[Template Name]" +
        "&q=values[Template Name]=\"#{template_name}\"&include=values"
      puts "Calling Kinetic Platform at: #{kinetic_api_route}" if @enable_debug_logging
      
      response = RestClient::Request.execute \
        method: 'GET', \
        url: kinetic_api_route, \
        user: @info_values["kinetic_api_username"], \
        password: @info_values["kinetic_api_password"], \
        headers: {:content_type => @content_type, :accept => @accept}
      response_code = response.code

      # The query should be indexed to a unique field.
      submission = JSON.parse(response)['submissions'][0]
      if (submission.nil?) 
        raise "No submissions found with a Template Name of '#{template_name}'"
      end

      query_template = submission["values"]["SQL Query"]
      if (query_template.nil? || query_template.strip.empty?) 
        raise "The query was nil or empty.  Check that the '#{space_form}' form has the SQL Query field."
      end

      socket = TCPSocket.new(db_server, db_port)
      puts "Port #{db_port} at server '#{db_server}' is open." if @enable_debug_logging
      begin
        if @parameters["jdbc_database"].downcase == "sqlserver"
            @db = Sequel.connect("jdbc:#{@parameters["jdbc_database"]}://#{db_server}:#{db_port};database=#{db_name};user=#{db_username};password=#{db_password}")
            @db.extension :identifier_mangling
            @db.identifier_input_method = nil
            @db.identifier_output_method = nil
            @max_db_identifier_size = 128
        elsif @parameters["jdbc_database"].downcase == "oracle"
          Sequel.database_timezone = :utc
            #Sequel.application_timezone = :utc
            @db = Sequel.connect("jdbc:#{@parameters["jdbc_database"]}:thin:#{db_username}/#{db_password}@#{db_server}:#{db_port}:#{db_name}")
            @db.extension :identifier_mangling
            @db.identifier_input_method = nil
            @db.identifier_output_method = nil
            @max_db_identifier_size = 30
        elsif @parameters["jdbc_database"].downcase == "postgresql"
            @max_db_identifier_size = 64
            @db = Sequel.connect("jdbc:#{@parameters["jdbc_database"]}://#{db_server}:#{db_port}/#{db_name}?user=#{db_username}&password=#{db_password}")
        elsif @parameters["jdbc_database"].downcase == "mysql"
          # TODO: what goes here
        end
        
        query_values = !@parameters["query_values"].empty? && JSON.parse(@parameters["query_values"])
        if @parameters['action'] == "fetch"
          replacement_values = Array.new
          query_template = query_template.gsub(/{{(.*?)}}/) { |match|
            replacement_values.push(query_values[match[2..-3]])
            '?'
          }
          puts "The SQL query executed: #{query_template}" if @enable_debug_logging

          record_set = @db.fetch(query_template, *replacement_values).all
          json_result = JSON(record_set)
        else
          query_template = query_template.gsub(/{{(.*?)}}/) { |match|
            @db.literal(query_values[match[2..-3]])
          }
          puts "The SQL query executed: #{query_template}" if @enable_debug_logging

          record_set = @db.run(query_template)
          json_result = "Successful"
        end

      rescue Exception => e
        if error_handling == "Raise Error"
          raise e
        else
          error_message = e
        end
      ensure
        if @db
          puts "Closing the JDBC connections that were opened" if @enable_debug_logging
          @db.disconnect
		    end
      end
      
    rescue RestClient::Exception => e
      if error_handling == "Raise Error"
        raise e
      else
          error_message = e
      end
    rescue Exception => e
      if error_handling == "Raise Error"
        raise e
      else
          error_message = e
      end
    rescue Errno::ECONNREFUSED, Errno::ETIMEDOUT
      puts "Port #{db_port} at server '#{@info_values['server']}' is CLOSED or not accessible." if @enable_debug_logging
      if error_handling == "Raise Error"
        raise "Port #{db_port} at server '#{@info_values['server']}' is CLOSED or is not accessible."
      else
        error_message = "Port #{db_port} at server '#{@info_values['server']}' is CLOSED or is not accessible."
      end
    ensure
	    if socket
          puts "Closing the socket connection." if @enable_debug_logging
          socket.close
	    end
    end
  	# Return the results as a JSON string
  	<<-RESULTS
    <results>
      <result name="Result">#{escape(json_result)}</result>
      <result name="Handler Error Message">#{escape(error_message)}</result>
    </results>
    RESULTS
  end

  ##############################################################################
  # General handler utility functions
  ##############################################################################

  # This is a template method that is used to escape results values (returned in
  # execute) that would cause the XML to be invalid.  This method is not
  # necessary if values do not contain character that have special meaning in
  # XML (&, ", <, and >), however it is a good practice to use it for all return
  # variable results in case the value could include one of those characters in
  # the future.  This method can be copied and reused between handlers.
  def escape(string)
    # Globally replace characters based on the ESCAPE_CHARACTERS constant
    string.to_s.gsub(/[&"><]/) { |special| ESCAPE_CHARACTERS[special] } if string
  end
  # This is a ruby constant that is used by the escape method
  ESCAPE_CHARACTERS = {'&'=>'&amp;', '>'=>'&gt;', '<'=>'&lt;', '"' => '&quot;'}
  def get_info_value(document, name)
    # Retrieve the XML node representing the desired info value
    info_element = REXML::XPath.first(document, "/handler/infos/info[@name='#{name}']")
    # If the desired element is nil, return nil; otherwise return the text value of the element
    info_element.nil? ? nil : info_element.text
  end
end
