package yu.test.producer;


import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class ASyncProducer {
    public static void main(String[] args) {
        //实例化消息的生产者producer
        DefaultMQProducer producer = new DefaultMQProducer("Group_test");
        //nameServer 地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        //是否延迟发送
        producer.setSendLatencyFaultEnable(false);

        try {
            producer.start();
            for (int i = 0; i < 10; i++) {
                Message msg = new Message("default_topic", "tag", ("Hello_" + i).getBytes());
                //异步发送
                SendCallback callback = new SendCallback() {

                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.printf("%S%S%S%S", sendResult.getSendStatus(), sendResult.getMsgId(),
                                sendResult.getQueueOffset(), sendResult.getRegionId());
                    }

                    @Override
                    public void onException(Throwable e) {
                        e.printStackTrace();
                    }
                };
                //发送消息
                producer.send(msg, callback);
                System.out.println(callback);

                Thread.sleep(500);
            }

            //关闭producer
            producer.shutdown();

        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
