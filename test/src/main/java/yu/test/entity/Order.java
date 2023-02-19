package yu.test.entity;

public class Order {
    private long orderId;
    private String address;

    public long getOrderId() {
        return orderId;
    }

    public String getAddress() {
        return address;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId=" + orderId +
                ", address='" + address + '\'' +
                '}';
    }
}
