package org.apache.metamodel.example;

import java.util.HashMap;
import java.util.Map;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class HelloController {

    @RequestMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> index() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("hello", "world");
        return map;
    }
}
