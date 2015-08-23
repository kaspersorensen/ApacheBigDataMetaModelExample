package org.apache.metamodel.example.client;

import java.util.Arrays;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class Main {

    public static void main(String[] args) {
        final String baseUrl;
        if (args.length > 0) {
            baseUrl = args[0];
        } else {
            baseUrl = "http://localhost:8080/ApacheBigDataMetaModelExample";
        }

        final String datastoreName;
        if (args.length > 1) {
            datastoreName = args[1];
        } else {
            datastoreName = "CSVex";
        }

        final DataContext dataContext = new WebappClientDataContext(baseUrl, datastoreName);

        final Schema defaultSchema = dataContext.getDefaultSchema();
        System.out.println("Default schema: " + defaultSchema.getName());

        final Table[] tables = defaultSchema.getTables();
        for (Table table : tables) {
            System.out.println(" Table: " + table.getName());
            final Column[] columns = table.getColumns();
            for (Column column : columns) {
                System.out.println("  - " + column.toString());
            }
        }

        final String query;
        if (args.length > 2) {
            final StringBuilder sb = new StringBuilder();
            for (int i = 2; i < args.length; i++) {
                if (sb.length() != 0) {
                    sb.append(' ');
                }
                sb.append(args[i]);
            }
            query = sb.toString();
        } else {
            query = "SELECT * FROM " + defaultSchema.getTable(0).getName();
        }

        final DataSet dataSet = dataContext.executeQuery(query);
        try {
            while (dataSet.next()) {
                final Row row = dataSet.getRow();
                System.out.println(Arrays.toString(row.getValues()));
            }
        } finally {
            dataSet.close();
        }
    }
}
