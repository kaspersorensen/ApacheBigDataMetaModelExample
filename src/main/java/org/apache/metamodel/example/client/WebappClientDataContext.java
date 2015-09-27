package org.apache.metamodel.example.client;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.InMemoryDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ColumnTypeImpl;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.TableType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.UrlEscapers;

public class WebappClientDataContext extends QueryPostprocessDataContext {

    private final String _baseUrl;
    private final CloseableHttpClient _httpClient;

    public WebappClientDataContext(String serverUrl, String datastoreName) {
        _baseUrl = serverUrl + "/" + datastoreName;
        _httpClient = HttpClients.createSystem();
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        // build a query string to send to the server
        final Query q = new Query().from(table).select(columns);
        if (maxRows > 0) {
            q.setMaxRows(maxRows);
        }
        final String queryString = q.toSql();

        // invoke the query via a HTTP request
        final String escapedQuery = UrlEscapers.urlPathSegmentEscaper().escape(queryString);
        final Map<String, Object> resultMap = invokeHttpGetAndParseMap(_baseUrl + "/query/" + escapedQuery);

        // wrap the HTTP response as a DataSet
        @SuppressWarnings("unchecked")
        final List<List<Object>> rowValues = (List<List<Object>>) resultMap.get("data");

        final List<Row> rows = new ArrayList<>();
        final DataSetHeader header = new SimpleDataSetHeader(columns);
        for (List<Object> values : rowValues) {
            rows.add(new DefaultRow(header, values.toArray()));
        }
        return new InMemoryDataSet(header, rows);
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final MutableSchema schema = getSchemaBasics();

        addTableData(schema);

        return schema;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> invokeHttpGetAndParseMap(String url) throws MetaModelException {
        try {
            final CloseableHttpResponse response = _httpClient.execute(new HttpGet(url));
            try {
                final InputStream inputStream = response.getEntity().getContent();
                final Map<String, Object> responseMap = new ObjectMapper().readValue(inputStream, Map.class);
                return responseMap;
            } finally {
                response.close();
            }
        } catch (Exception e) {
            throw new MetaModelException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private void addTableData(MutableSchema schema) {
        final Map<String, Object> schemaMap = invokeHttpGetAndParseMap(_baseUrl + "/metadata/" + schema.getName());
        final List<String> tableList = (List<String>) schemaMap.get("tables");
        for (String tableName : tableList) {
            final MutableTable table = new MutableTable(tableName, schema);

            final Map<String, Object> tableMap = invokeHttpGetAndParseMap(_baseUrl + "/metadata/" + schema.getName()
                    + "/" + tableName);
            table.setType(TableType.getTableType((String) tableMap.get("type")));
            table.setRemarks((String) tableMap.get("remarks"));

            final List<Map<String, Object>> columnsInformation = (List<Map<String, Object>>) tableMap.get("columns");
            for (Map<String, Object> columnMap : columnsInformation) {
                final String columnName = (String) columnMap.get("name");
                final ColumnType columnType = ColumnTypeImpl.valueOf((String) columnMap.get("type"));
                final String nativeType = (String) columnMap.get("native_type");
                final Number columnNumber = (Number) columnMap.get("number");
                final Integer size = (Integer) columnMap.get("size");
                final String remarks = (String) columnMap.get("remarks");
                final Boolean nullable = (Boolean) columnMap.get("nullable");
                final boolean indexed = (Boolean) columnMap.get("indexed");
                final MutableColumn column = new MutableColumn(columnName, columnType, table, columnNumber.intValue(),
                        size, nativeType, nullable, remarks, indexed, null);
                table.addColumn(column);
            }

            schema.addTable(table);
        }
    }

    @SuppressWarnings("unchecked")
    private MutableSchema getSchemaBasics() {
        final MutableSchema schema = new MutableSchema();

        final Map<String, Object> responseMap = invokeHttpGetAndParseMap(_baseUrl + "/metadata");

        final List<Map<String, Object>> schemasList = (List<Map<String, Object>>) responseMap.get("schemas");

        for (Map<String, Object> map : schemasList) {
            final Object defaultProperty = map.get("default");
            if (((Boolean) defaultProperty).booleanValue()) {
                schema.setName((String) map.get("name"));
            }
        }

        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return getMainSchema().getName();
    }
}
