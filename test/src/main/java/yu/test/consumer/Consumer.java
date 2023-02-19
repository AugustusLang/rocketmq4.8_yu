package yu.test.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class Consumer {

    public static void main(String [] args){
        //实例化消息的生产者consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("Consumer_Group_test");
        //nameServer 地址
        consumer.setNamesrvAddr("127.0.0.1:9876");
        try {
            //订阅Topic
            consumer.subscribe("Topic_test","*");
            //从哪里开始消费
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            //设置回调函数  处理消息
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        String topic = msg.getTopic();
                        String msgBody = null;
                        try {
                            msgBody = new String(msg.getBody(), "utf-8");
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                            //发生异常  等会消费
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                        String tags = msg.getTags();
                        System.out.printf("收到消息: %S%S%S%S", topic, msgBody, tags);
                    }
                    //消费成功
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            consumer.start();
            //关闭producer
          //  consumer.shutdown();

        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}
