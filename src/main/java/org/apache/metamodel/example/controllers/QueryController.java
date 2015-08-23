package org.apache.metamodel.example.controllers;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.parser.QueryParserException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class QueryController {

    @Autowired
    ApplicationContext _applicationContext;

    @RequestMapping(value = "/{datastoreName}/query", produces = MediaType.APPLICATION_JSON_VALUE, method = RequestMethod.POST)
    @ResponseBody
    public Map<String, Object> queryPost(@PathVariable("datastoreName") String datastoreName,
            @RequestParam("query") String queryString) {
        return query(datastoreName, queryString);
    }

    @RequestMapping(value = "/{datastoreName}/query/{query:.+}", produces = MediaType.APPLICATION_JSON_VALUE, method = RequestMethod.GET)
    @ResponseBody
    public Map<String, Object> queryGet(@PathVariable("datastoreName") String datastoreName,
            @PathVariable("query") String queryString) {
        return query(datastoreName, queryString);
    }

    private Map<String, Object> query(String datastoreName, String queryString) {
        final DataContext dataContext = getDataContext(datastoreName);


        final Map<String, Object> response = new LinkedHashMap<String, Object>();
        final Map<String, Object> queryInformation = new LinkedHashMap<String, Object>();
        response.put("query", queryInformation);

        queryInformation.put("input", queryString);

        final Query query;

        try {
            query = dataContext.parseQuery(queryString);
            queryInformation.put("parsed", query.toSql());
        } catch (QueryParserException e) {
            response.put("error", e.getMessage());
            return response;
        }

        final DataSet dataSet = dataContext.executeQuery(query);
        try {
            response.put("data", dataSet.toObjectArrays());
        } finally {
            dataSet.close();
        }
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
