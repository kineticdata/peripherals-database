package com.kineticdata.bridgehub.adapter.sql;

// Import the necessary core Java classes
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;

public class SqlQualificationParser {
    public static String PARAMETER_PATTERN = "<%=\\s*parameter\\[\\\"?(.*?)\\\"?\\]\\s*%>";

    /**
     * Since the order by is specified in metadata, which can be configured
     * manually by the user, it needs to be scrubbed to ensure that SQL 
     * injection attacks can not be used.
     * 
     * @param columns
     * @param order
     * @return
     */
    public static String buildOrderByClause(List<String> columns, String order) {
        List<String> cleansedSegments = new ArrayList();
        Set<String> columnSet = new HashSet(columns);
        
        List<String> segments = new ArrayList<String>();
        if (order.replaceAll(" ","").matches("<%=field\\[\".*?\"\\]%>.*")) {
            try {
                for (Map.Entry<String,String> entry : BridgeUtils.parseOrder(order).entrySet()) {
                    segments.add(StringUtils.isNotEmpty(entry.getValue())
                        ? entry.getKey() + " " + entry.getValue()
                        : entry.getKey()
                    );
                }
            } catch (BridgeError e) {
                throw new RuntimeException("There was an error encountered when attempting to parse the SQL Order (make sure the order is in the form of <%=field[\"FIELD_NAME\"]%>:ASC)",e);
            }
        } else {
            // Split the ORDER BY clause into segments.  IE:
            //   column1, column2 ASC, column3 DESC
            // becomes
            //   new String[] {"column1", "column2 ASC", "column3 DESC"};
            segments = Arrays.asList(order.split(",\\s*"));
        }

        // Initialize a
        List<String> invalidColumns = new ArrayList();

        // For each of the segments
        for (String segment : segments) {
            // Initialize the column name and direction
            String column = null;
            String direction = "ASC";
            // Check for an ASC or DESC at the end
            if (segment.endsWith(" ASC")) {
                column = StringUtils.removeEnd(segment, " ASC");
            } else if (segment.endsWith(" DESC")) {
                column = StringUtils.removeEnd(segment, " DESC");
                direction = "DESC";
            } else {
                column = segment;
            }
            // Check if the column name exists
            if (columnSet.contains(column)) {
                cleansedSegments.add(column+" "+direction);
            } else {
                invalidColumns.add(column);
            }
        }

        // If there are any invalid columns
        if (invalidColumns.size()>0) {
            throw new RuntimeException("Unable to build SQL ORDER BY clause, "+
                "columns specified in ORDER BY must also be specified as a "+
                "bridge field.  The following columns were included in the "+
                "'order' parameter, but not the 'fields' parameter (this is "+
                "most likely this is due to the column not being mapped to a "+
                "model attribute): "+StringUtils.join(invalidColumns, ", "));
        }

        // Return the cleansed order by clause
        return StringUtils.join(cleansedSegments, ", ");
    }

    public static SqlQualification parse(String query) {
        // Initialize
        List<SqlQualificationParameter> parameters = new ArrayList();

        // Create the pattern and pattern matcher
        Pattern pattern = Pattern.compile(PARAMETER_PATTERN);
        Matcher matcher = pattern.matcher(query);

        // Build up the results string
        StringBuffer buffer = new StringBuffer();
        while(matcher.find()) {
            // Retrieve the necessary values
            String parameterName = matcher.group(1);
            // Add the parameter to the parameters list
            parameters.add(new SqlQualificationParameter(parameters.size()+1, parameterName));
            // Append any part of the qualification that exists before the match
            matcher.appendReplacement(buffer, Matcher.quoteReplacement("?"));
        }
        // Append any part of the qualification remaining after the last match
        matcher.appendTail(buffer);

        return new SqlQualification(buffer.toString(), parameters);
    }

    public static void main(String[] args) {
        List<String> columns = new ArrayList();
        columns.add("column1");
        columns.add("column2");
        columns.add("column3");

        String order = "column1,column2 ASC,  column3 DESC";
        String orderResult = buildOrderByClause(columns, order);
        System.out.println(orderResult);

        String invalidOrder = "column1;SELECT * FROM USERS";
        String invalidOrderResult = buildOrderByClause(columns, invalidOrder);
        System.out.println(invalidOrderResult);
    }
}