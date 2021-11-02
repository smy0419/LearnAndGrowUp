package com.example.learnandgrowup.rabbitmq.handler;

import cn.hutool.json.JSONUtil;
import com.example.learnandgrowup.rabbitmq.constants.RabbitConsts;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * <p>
 * 延迟队列处理器
 * </p>
 *
 * @author yangkai.shen
 * @date Created in 2019-01-04 17:42
 */
@Slf4j
@Component
public class DelayQueueHandler {

    //    @RabbitHandler
    @RabbitListener(queues = RabbitConsts.DELAY_QUEUE)
    public void directHandlerManualAck(Message message, Channel channel) {
        log.info("message : {}", message);
        //  如果手动ACK,消息会被监听消费,但是消息在队列中依旧存在,如果 未配置 acknowledge-mode 默认是会在消费完毕后自动ACK掉
        final long deliveryTag = message.getMessageProperties().getDeliveryTag();
        try {
            log.info("延迟队列，手动ACK，接收消息：{}", JSONUtil.toJsonStr(message.getBody()));
            // 通知 MQ 消息已被成功消费,可以ACK了
            channel.basicAck(deliveryTag, false);
        } catch (IOException e) {
            try {
                // 处理失败,重新压入MQ
                channel.basicRecover();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }
}
