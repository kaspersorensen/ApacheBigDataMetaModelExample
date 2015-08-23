package org.apache.metamodel.example.controllers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletContext;

import org.apache.metamodel.DataContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class IndexController {

    @Autowired
    ApplicationContext _applicationContext;

    @Autowired
    ServletContext _servletContext;

    @RequestMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Map<String, Object> index() {
        final Map<String, DataContext> dataContextBeans = _applicationContext.getBeansOfType(DataContext.class);

        final Map<String, Object> response = new LinkedHashMap<String, Object>();
        final Set<String> datastoreNames = dataContextBeans.keySet();

        final List<Map<String, Object>> datastoresInformation = new ArrayList<>();
        for (String datastoreName : datastoreNames) {
            final Map<String, Object> datastoreInfo = new LinkedHashMap<String, Object>();
            datastoreInfo.put("name", datastoreName);
            datastoreInfo.put("type", dataContextBeans.get(datastoreName).getClass().getSimpleName());
            datastoreInfo.put("metadata_url", _servletContext.getContextPath() + '/' + datastoreName + "/metadata");
            datastoreInfo.put("query_url", _servletContext.getContextPath() + '/' + datastoreName + "/query");
            datastoresInformation.add(datastoreInfo);
        }

        response.put("datastores", datastoresInformation);
        return response;
    }
}
