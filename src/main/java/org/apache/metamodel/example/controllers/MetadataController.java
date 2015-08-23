package org.apache.metamodel.example.controllers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.CollectionUtils;
import org.apache.metamodel.util.HasNameMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class MetadataController {

    @Autowired
    ApplicationContext _applicationContext;

    @RequestMapping(value = "/{datastoreName}/metadata", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Map<String, Object> datastoreDetails(@PathVariable("datastoreName") String datastoreName) {
        final DataContext dataContext = getDataContext(datastoreName);

        final List<Map<String, Object>> schemasInformation = new ArrayList<>();
        final Schema defaultSchema = dataContext.getDefaultSchema();
        final Schema[] schemas = dataContext.getSchemas();
        for (Schema schema : schemas) {
            final Map<String, Object> schemaInfo = new LinkedHashMap<String, Object>();
            schemaInfo.put("name", schema.getName());
            schemaInfo.put("default", schema == defaultSchema);
            schemaInfo.put("table_count", schema.getTableCount());
            schemasInformation.add(schemaInfo);
        }

        final Map<String, Object> response = new LinkedHashMap<String, Object>();
        response.put("schemas", schemasInformation);

        return response;
    }

    @RequestMapping(value = "/{datastoreName}/metadata/{schemaName:.+}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Map<String, Object> schemaDetails(@PathVariable("datastoreName") String datastoreName,
            @PathVariable("schemaName") String schemaName) {
        final DataContext dataContext = getDataContext(datastoreName);
        final Schema schema = dataContext.getSchemaByName(schemaName);

        final Map<String, Object> response = new LinkedHashMap<String, Object>();
        response.put("name", schema.getName());
        response.put("tables", schema.getTableNames());
        return response;
    }

    @RequestMapping(value = "/{datastoreName}/metadata/{schemaName:.+}/{tableName:.+}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Map<String, Object> tableDetails(@PathVariable("datastoreName") String datastoreName,
            @PathVariable("schemaName") String schemaName, @PathVariable("tableName") String tableName) {
        final DataContext dataContext = getDataContext(datastoreName);
        final Schema schema = dataContext.getSchemaByName(schemaName);
        final Table table = schema.getTableByName(tableName);

        final Column[] columns = table.getColumns();
        final List<Map<String, Object>> columnsInformation = new ArrayList<>();
        for (Column column : columns) {
            final Map<String, Object> columnMap = getColumnMap(column);
            columnsInformation.add(columnMap);
        }

        final Map<String, Object> response = new LinkedHashMap<String, Object>();
        response.put("name", table.getName());
        response.put("type", table.getType());
        response.put("remarks", table.getRemarks());
        final Column[] primaryKeys = table.getPrimaryKeys();
        response.put("primary_keys", CollectionUtils.map(primaryKeys, new HasNameMapper()));
        response.put("columns", columnsInformation);

        return response;
    }

    @RequestMapping(value = "/{datastoreName}/metadata/{schemaName:.+}/{tableName:.+}/{columnName:.+}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Map<String, Object> columnDetails(@PathVariable("datastoreName") String datastoreName,
            @PathVariable("schemaName") String schemaName, @PathVariable("tableName") String tableName,
            @PathVariable("columnName") String columnName) {
        final DataContext dataContext = getDataContext(datastoreName);
        final Schema schema = dataContext.getSchemaByName(schemaName);
        final Table table = schema.getTableByName(tableName);
        final Column column = table.getColumnByName(columnName);

        final Map<String, Object> response = getColumnMap(column);

        return response;
    }

    private Map<String, Object> getColumnMap(final Column column) {
        final Map<String, Object> response = new LinkedHashMap<String, Object>();
        response.put("name", column.getName());
        response.put("type", column.getType() == null ? null : column.getType().getName());
        response.put("native_type", column.getNativeType());
        response.put("number", column.getColumnNumber());
        response.put("size", column.getColumnSize());
        response.put("remarks", column.getRemarks());
        response.put("indexed", column.isIndexed());
        response.put("nullable", column.isNullable());
        return response;
    }

    private DataContext getDataContext(String datastoreName) {
        final Object bean = _applicationContext.getBean(datastoreName);
        if (!(bean instanceof DataContext)) {
            throw new IllegalArgumentException();
        }

        final DataContext dataContext = (DataContext) bean;
        return dataContext;
    }
}
