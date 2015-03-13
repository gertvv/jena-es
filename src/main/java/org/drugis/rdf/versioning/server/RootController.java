package org.drugis.rdf.versioning.server;

import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.*;
import org.springframework.web.bind.annotation.*;

@Controller
@EnableAutoConfiguration
@ComponentScan
public class RootController {

    @RequestMapping(value="/", produces="text/html")
    @ResponseBody
    String home() {
        return "<html><a href=\"/datasets\">Datasets</a></html>";
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(RootController.class, args);
    }
}
