package com.dgindusoft.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String logger(@RequestParam("param") String jsonLog){
//        System.out.println(jsonLog);
        log.info(jsonLog);

        //将数据发送至Kafka ODS主题
        kafkaTemplate.send("ods_base_log",jsonLog);

        return "success";
    }
}
