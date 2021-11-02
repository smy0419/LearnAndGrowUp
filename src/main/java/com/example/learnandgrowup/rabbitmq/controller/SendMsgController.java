package com.example.learnandgrowup.rabbitmq.controller;

import com.example.learnandgrowup.rabbitmq.constants.RabbitConsts;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController()
@Slf4j
public class SendMsgController {
    @Autowired
    private RabbitTemplate rabbitTemplate;

    @GetMapping("sendMsg")
    public String sendMsg(String msg) {
        log.info("发送消息: {}", msg);
        rabbitTemplate.convertAndSend(RabbitConsts.DELAY_MODE_QUEUE, RabbitConsts.DELAY_QUEUE, msg, message -> {
            message.getMessageProperties().setHeader("x-delay", 6000);
            return message;
        });
        return "SUCCESS";
    }
}
