package com.kineticdata.bridgehub.adapter.sql;

import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.BridgeUtils;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.DisposableAdapter;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.commons.v1.config.ConfigurableProperty;
import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;
/**
 * Generic SQL Bridge class that leverages JDBC to expose access to a SQL
 * database.  The SqlBridge defines four bridge properties:
 *  - Adapter Class
 *  - Connection String
 *  - Username
 *  - Password
 *
 * The Adapter Class represents the Java Class to use as a JDBC Driver.  For
 * example:
 *  - com.ibm.db2.jcc.DB2Driver
 *  - com.microsoft.sqlserver.jdbc.SQLServerDriver
 *  - oracle.jdbc.OracleDriver
 *
 * The Connection String represents the complete JDBC connection string.  For
 * example:
 *  - jdbc:db2://SERVER:PORT/DATABASE
 *  - jdbc:sqlserver://SERVER:PORT;databaseName=DATABASE
 *  - jdbc:oracle:thin:@//SERVER:PORT/SERVICE
 *
 * The Username and Password represent the credentials of the user that the SQL
 * queries should be made on behalf of.
 *
 *
 * This class can be extended in order to provide more specific property
 * definitions.  For example:
 *
 * public class FuBridge extends com.kineticdata.bridge.sql.SqlBridge {
 *     // Specify the adapter class and ensure it is loaded
 *     public static final String ADAPTER_CLASS = "xxx.sample.FuDriver";
 *     // Specify the name of the Bridge properties
 *     public static final String PROPERTY_USERNAME = "Username";
 *     public static final String PROPERTY_PASSWORD = "Password";
 *     public static final String PROPERTY_SERVER = "Server";
 *     public static final String PROPERTY_PORT = "Port";
 *     public static final String PROPERTY_BAR = "Bar";
 *     /**
 *       * Specify the name, type, and default values for the configurable
 *       * properties.  When using the Kinetic Bridge Bootstrap, these will be
 *       * displayed as configurable values within the AdminConsole.
 *       *\/
 *     public static final ConfigurableProperty[] CONFIGURABLE_PROPERTIES = new ConfigurableProperty[] {
 *         new ConfigurableProperty(PROPERTY_USERNAME, ""),
 *         new ConfigurableProperty(PROPERTY_PASSWORD, "", ConfigurableProperty.SENSITIVE),
 *         new ConfigurableProperty(PROPERTY_SERVER, "127.0.0.1"),
 *         new ConfigurableProperty(PROPERTY_PORT, "1521"),
 *         new ConfigurableProperty(PROPERTY_BAR, "")
 *     };
 *
 *     public FuSqlBridge(Map<String,String> configuration) {
 *         super(
 *             ADAPTER_CLASS,
 *              "jdbc:fu://"+
 *                  configuration.get(PROPERTY_SERVER)+":"+
 *                  configuration.get(PROPERTY_PORT)+"/"+
 *                  configuration.get(PROPERTY_BAR),
 *              configuration.get(PROPERTY_USERNAME),
 *              configuration.get(PROPERTY_PASSWORD)
 *          );
 *     }
 * }
 */
public class SqlAdapter implements BridgeAdapter,DisposableAdapter {
    /*----------------------------------------------------------------------------------------------
     * PROPERTIES
     *--------------------------------------------------------------------------------------------*/
    
    /** Defines the adapter display name. */
    public static final String NAME = "SQL Bridge";

    /** Defines the logger */
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SqlAdapter.class);

    /** Adapter version constant. */
    public static String VERSION;
    /** Load the properties version from the version.properties file. */
    static {
        try {
            java.util.Properties properties = new java.util.Properties();
            properties.load(SqlAdapter.class.getResourceAsStream("/"+SqlAdapter.class.getName()+".version"));
            VERSION = properties.getProperty("version");
        } catch (IOException e) {
            logger.warn("Unable to load "+SqlAdapter.class.getName()+" version properties.", e);
            VERSION = "Unknown";
        }
    }

    /** Defines the collection of property names for the adapter. */
    public static class Properties {
        public static final String USERNAME = "Username";
        public static final String PASSWORD = "Password";
        public static final String ADAPTER_CLASS = "Adapter Class";
        public static final String CONNECTION_STRING = "Connection String";
    }

    private final ConfigurablePropertyMap properties = new ConfigurablePropertyMap(
            new ConfigurableProperty(Properties.USERNAME).setIsRequired(true),
            new ConfigurableProperty(Properties.PASSWORD).setIsRequired(true).setIsSensitive(true),
            new ConfigurableProperty(Properties.ADAPTER_CLASS).setIsRequired(true),
            new ConfigurableProperty(Properties.CONNECTION_STRING).setIsRequired(true)
    );

    /** Defines the variables to be used in the adapter **/
    /**
     * String representing the Java Class to use as a JDBC Driver.
     */
    private String adapterClass;
    /**
     * Complete JDBC connection string.
     */
    private String connectionString;
    /**
     * The login name for the user that the JDBC connection is being made on
     * behalf of.
     */
    private String username;
    /**
     * The password of the user that the JDBC connection is being made on
     * behalf of.
     */
    private String password;

    private static final List<Integer> BLOB_TYPES = Arrays.asList(
        Types.BLOB, Types.CLOB, Types.NCLOB
    );

    /*---------------------------------------------------------------------------------------------
     * SETUP METHODS
     *-------------------------------------------------------------------------------------------*/
    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public String getVersion() {
       return  VERSION;
    }
    
    @Override
    public ConfigurablePropertyMap getProperties() {
        return properties;
    }
    
    @Override
    public void setProperties(Map<String,String> parameters) {
        properties.setValues(parameters);
    }

    @Override
    public void initialize() throws BridgeError {
        initalize (
                properties.getValue(Properties.ADAPTER_CLASS),
                properties.getValue(Properties.CONNECTION_STRING),
                properties.getValue(Properties.USERNAME),
                properties.getValue(Properties.PASSWORD)
        );        
    }
    
    // Internal initalize method used by extending classes
    public void initalize(String adapterClass, String connectionString, String username, String password) throws BridgeError {
        this.adapterClass = adapterClass;
        this.connectionString = connectionString;
        this.username = username;
        this.password = password;
        
        // Validate that we can load the adapter class
        try {
            // Register the class
            logger.info("Registering: " +adapterClass);
            Class.forName(adapterClass);
            // Verify the connection
            DriverManager.getConnection(connectionString, username, password);
        } catch (Exception e) {
            throw new BridgeError("Unable to intialize the "+adapterClass+" adapter class.", e);
        }
    }

    /*---------------------------------------------------------------------------------------------
     * IMPLEMENTATION METHODS
     *-------------------------------------------------------------------------------------------*/

    @Override
    public Count count(BridgeRequest request) throws BridgeError {
        // Try to retrieve the count
        Integer count = null;

        PreparedStatement statement = null;
        ResultSet resultSet = null;
        Connection connection = null;

        // Try to execute the query
        try {
            // Build a connection
            connection = DriverManager.getConnection(connectionString, username, password);

            // Build up the SQL WHERE clause
            SqlQualification qualification = SqlQualificationParser.parse(request.getQuery());

            // Build up the query statement
            StringBuilder statementString = new StringBuilder();
            statementString.append("SELECT COUNT(*)");
            statementString.append(" FROM ").append(request.getStructure());
            if (StringUtils.isNotBlank(request.getQuery())){
                statementString.append(" WHERE ").append(qualification.getParameterizedString());
            }

            // Prepare the statement
            logger.debug("Preparing Query");
            logger.debug("  "+statementString);
            statement = connection.prepareStatement(statementString.toString());
            for (SqlQualificationParameter parameter : qualification.getParameters()) {
                // Retrieve the parameter value
                String parameterValue = request.getParameter(parameter.getName());
                // If there is a reference to a parameter that was not passed
                if (parameterValue == null) {
                    throw new BridgeError("Unable to parse qualification, "+
                        "the '"+parameter.getName()+"' parameter was "+
                        "referenced but not provided.");
                }
                
                // Log each parameter value in the query
                logger.trace("  "+ Integer.toString(parameter.getIndex()+1) + " (" +parameter.getName()+") : "+parameterValue);
                
                // Set the value for the parameter in the SQL statement.
                statement.setObject(parameter.getIndex(), parameterValue);
            }

            // Execute the Query
            resultSet = statement.executeQuery();
            while(resultSet.next()){
                count = new Integer(resultSet.getInt(1));
            }
        } catch (SQLException e) {
            throw new BridgeError("Unable to execute count request.", e);
        } finally {
            closeResource(resultSet);
            closeResource(statement);
            closeResource(connection);
        }
        
        return new Count(count);
    }

    @Override
    public Record retrieve(BridgeRequest request) throws BridgeError {
        // Initialize the record
        Record record = null;

        PreparedStatement statement = null;
        ResultSet resultSet = null;
        Connection connection = null;

        // Try to execute the query
        try {
            // Build a connection
            connection = DriverManager.getConnection(connectionString, username, password);

            // Build the list of columns to retrieve from the field string
            String columns = request.getFieldString();
            // If the field string was not provided, default it to all columns
            if (StringUtils.isBlank(columns)) {columns = "*";}

            // Build up the SQL WHERE clause
            SqlQualification qualification = SqlQualificationParser.parse(request.getQuery());

            // Build up the query statement
            StringBuilder statementString = new StringBuilder();
            statementString.append("SELECT ").append(columns);
            statementString.append(" FROM ").append(request.getStructure());
            statementString.append(" WHERE ").append(qualification.getParameterizedString());

            // Use the metadata order if it is available
            if (StringUtils.isNotBlank(request.getMetadata("order"))) {
                List<String> orderFields = new ArrayList<String>();
                for (Map.Entry<String,String> entry : BridgeUtils.parseOrder(request.getMetadata("order")).entrySet()) {
                    orderFields.add(entry.getKey() + " " + entry.getValue());
                }
                statementString.append(" ORDER BY ").append(StringUtils.join(orderFields,","));
            }
            // Use the order of the fields requested (unless all fields are requested)
            else if(!"*".equals(columns)) {
                statementString.append(" ORDER BY ").append(columns);
            }
        
            // Prepare the statement
            logger.debug("Preparing Query");
            logger.debug("  "+statementString);
            statement = connection.prepareStatement(statementString.toString());
            for (SqlQualificationParameter parameter : qualification.getParameters()) {
                // Retrieve the parameter value
                String parameterValue = request.getParameter(parameter.getName());
                // If there is a reference to a parameter that was not passed
                if (parameterValue == null) {
                    throw new BridgeError("Unable to parse qualification, "+
                        "the '"+parameter.getName()+"' parameter was "+
                        "referenced but not provided.");
                }
                
                // Log each parameter value in the query
                logger.trace("  "+ Integer.toString(parameter.getIndex()) + " (" +parameter.getName()+") : "+parameterValue);
                
                // Set the value for the parameter in the SQL statement.
                statement.setObject(parameter.getIndex(), parameterValue);
            }

            // Execute the Query
            resultSet = statement.executeQuery();
            // Retrieve the metadata
            ResultSetMetaData metadata = resultSet.getMetaData();

            // For each row
            List<String> fields = request.getFields();
            while(resultSet.next()) {
                // We only want one record, so if a second is return we will throw an Exception
                if (record != null) {
                    connection.close(); 
                    throw new BridgeError("Multiple results matched an expected single match query of "+request.getStructure()+":"+request.getQuery());
                } else {
                    record = buildRecord(resultSet, metadata, fields);
                }
            }
        } catch (SQLException e) {
            throw new BridgeError("Unable to execute retrieve request.", e);
        } finally {
            closeResource(resultSet);
            closeResource(statement);
            closeResource(connection);
        }

        if (record == null) { record = new Record(); }
        return record;
    }

    @Override
    public RecordList search(BridgeRequest request) throws BridgeError {
        // Initialize the record list
        List<Record> records = new ArrayList<Record>();
        // Initialize the metadata
        Map<String,String> metadata = new LinkedHashMap();

        PreparedStatement statement = null;
        ResultSet resultSet = null;
        Connection connection = null;

        // Try to execute the query
        try {
            // Retrieve the metadata
            Long pageSize = getNumericalMetadata(request.getMetadata("pageSize"));
            Long pageNumber = getNumericalMetadata(request.getMetadata("pageNumber"));
            Long offset = getNumericalMetadata(request.getMetadata("offset"));

            // Validate the requests
            if (pageNumber != null && pageSize == null) {
                throw new BridgeError("Illegal search, the pageNumber metadata value was passed without specifying a pageSize.");
            }
            else if(pageNumber != null && offset != null && offset != (pageNumber-1)*pageSize) {
                throw new BridgeError("Illegal search, the offset does not match the specified pageSize and pageNumber.");
            }

            // Build a connection
            connection = DriverManager.getConnection(connectionString, username, password);
            
            // Default the values
            if (pageNumber == null) {pageNumber = 1L;}
            if (pageSize == null) {pageSize = 0L;}
            if (offset == null) {offset = (pageNumber-1)*pageSize;}
            logger.trace("Searching for "+pageSize+" records starting at "+offset+".");

            // Prepare the statement
            statement = buildPaginatedStatement(connection, request, offset, pageSize);

            // Execute the Query
            resultSet = statement.executeQuery();
            // Retrieve the metadata
            ResultSetMetaData resultSetMetadata = resultSet.getMetaData();
            // Retrieve the fields
            List<String> fields = request.getFields();

            // For each row
            while(resultSet.next()) {
                records.add(buildRecord(resultSet, resultSetMetadata, fields));
            }

            int count = count(request).getValue();
            // Build the metadata
            metadata.put("pageSize", pageSize.toString());
            metadata.put("pageNumber", String.valueOf((pageSize == 0) ? 1 : (int)Math.ceil(offset/pageSize)+1));
            metadata.put("offset", offset.toString());
            metadata.put("count", String.valueOf(count));
            metadata.put("size", String.valueOf(records.size()));

        } catch (SQLException e) {
            throw new BridgeError("Unable to execute search request.", e);
        } finally {
            closeResource(resultSet);
            closeResource(statement);
            closeResource(connection);
        }

        return new RecordList(request.getFields(),records,metadata);
        
    }

    protected PreparedStatement buildPaginatedStatement(
        Connection connection,
        BridgeRequest request,
        Long offset,
        Long pageSize
    ) throws BridgeError, SQLException {
        // Build the list of columns to retrieve from the field string
        String columns = request.getFieldString();
        // If the field string was not provided, default it to all columns
        if (StringUtils.isBlank(columns)) {columns = "*";}

        // Build up the SQL WHERE clause
        SqlQualification qualification = SqlQualificationParser.parse(request.getQuery());
        // Build the SQL ORDER BY clause (validating that only the requested
        // fields are used in the column list and that there is no attempt at
        // injection).
        String order = null;
        if (StringUtils.isNotBlank(request.getMetadata("order"))) {
            order = SqlQualificationParser.buildOrderByClause(request.getFields(), request.getMetadata("order"));
        } else if(!"*".equals(columns)) {
            order = SqlQualificationParser.buildOrderByClause(request.getFields(), columns);
        }

        // Build the statement string
        StringBuilder statementString = new StringBuilder();
        statementString.append("SELECT ").append(columns);
        statementString.append(" FROM ").append(request.getStructure());
        statementString.append(" WHERE ").append(qualification.getParameterizedString());
        // If there is an order that should be used
        if (order != null) {
            statementString.append(" ORDER BY ").append(order);
        }
        
        if (pageSize > 0) {
            statementString.append(" LIMIT ").append(pageSize);
            if (offset >= 0) {
                statementString.append(" OFFSET ").append(offset);
            }
        } else if (offset > 0) {
            statementString.append(" OFFSET ").append(offset);
        }

        // Prepare the statement
        logger.debug("Preparing Query");
        logger.debug("  "+statementString);
        PreparedStatement statement = connection.prepareStatement(statementString.toString());
        for (SqlQualificationParameter parameter : qualification.getParameters()) {
            // Retrieve the parameter value
            String parameterValue = request.getParameter(parameter.getName());
            // If there is a reference to a parameter that was not passed
            if (parameterValue == null) {
                throw new BridgeError("Unable to parse qualification, "+
                    "the '"+parameter.getName()+"' parameter was "+
                    "referenced but not provided.");
            }
            
            // Log each parameter value in the query
            logger.trace("  "+ Integer.toString(parameter.getIndex()+1) + " (" +parameter.getName()+") : "+parameterValue);
            
            // Set the value for the parameter in the SQL statement.
            statement.setObject(parameter.getIndex(), parameterValue);
        }

        // Return the statement
        return statement;
    }

    @Override
    public void destroy() {}

    private Long getNumericalMetadata(Object metadata) {
        Long result = null;
        if (metadata != null) {
            result = Long.valueOf(metadata.toString());
        }
        return result;
    }

    protected void closeResource(Object resource) {
        if (resource != null) {
            try {
                if (resource instanceof ResultSet) {
                    ((ResultSet) resource).close();
                }
                else if (resource instanceof Statement) {
                    ((Statement) resource).close();
                }
                else if (resource instanceof Connection) {
                    ((Connection) resource).close();
                }
            } catch (SQLException e) {
                logger.warn("Failed to close the {} resource", resource.getClass().getSimpleName(), e);
            }
        }
    }

    protected Record buildRecord(ResultSet resultSet, ResultSetMetaData resultSetMetadata, List<String> fields)
        throws java.sql.SQLException, BridgeError
    {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        for (int i=1; i<=resultSetMetadata.getColumnCount(); i++) {
            String value = null;
            String fieldName = (fields.size() >= i)
                ? fields.get(i-1)
                : resultSetMetadata.getColumnName(i);
            if (resultSetMetadata.getColumnType(i) == Types.BLOB) {
                Blob blob = resultSet.getBlob(i);
                if (blob != null) {
                    byte[] bdata = blob.getBytes(1, (int) blob.length());
                    value = new String(bdata);
                }
            } else if (resultSetMetadata.getColumnType(i) == Types.CLOB) {
                Clob clob = resultSet.getClob(i);
                if (clob != null) {
                    InputStream in = clob.getAsciiStream();
                    try {
                        value = IOUtils.toString(in);
                    } catch (IOException e) {
                        throw new BridgeError("An error occurred while converting a Clob field to a String.", e);
                    } finally {
                        IOUtils.closeQuietly(in);
                    }
                }
            } else if (resultSetMetadata.getColumnType(i) == Types.NCLOB) {
                NClob nClob = resultSet.getNClob(i);
                if (nClob != null) {
                    InputStream in = nClob.getAsciiStream();
                    try {
                        value = IOUtils.toString(in);
                    } catch (IOException e) {
                        throw new BridgeError("An error occurred while converting a NClob field to a String.", e);
                    } finally {
                        IOUtils.closeQuietly(in);
                    }
                }
            } else {
                value = resultSet.getString(i);
            }
            result.put(fieldName, value);
        }
        return new Record(result);
    }

}
