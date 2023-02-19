package yu.test.consumer.orderly;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class OrderlyConsumer {

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
            consumer.registerMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    context.setAutoCommit(true); //设置自动提交
                    //每个Queue都有唯一的consume来消费，订单对每个queue（分区）有序
                    for (MessageExt msg : msgs) {
                        String topic = msg.getTopic();
                        String msgBody = null;
                        try {
                            msgBody = new String(msg.getBody(), "utf-8");
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                            //发生异常  等会消费 而不是放到充实队列里
                            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                        }
                        String tags = msg.getTags();
                        System.out.printf("收到消息: %S%S%S%S", topic, msgBody, tags);
                    }
                    //消费成功
                    return ConsumeOrderlyStatus.SUCCESS;
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
