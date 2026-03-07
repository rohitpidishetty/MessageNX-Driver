import java.util.Map;

// javac -cp ".;libs\jars\*" .\Consumer.java
//  java -cp ".;libs\jars\*" Consumer
public class Consumer {

  public static void main(String[] args) throws Exception {
    MessageNX mnx = new MessageNX();
    mnx.setChannel("141983614");
    mnx.consume(
      new MessageNX.process() {
        @Override
        public void processor(Map<String, Object> topic) {
          System.out.println(topic.get("name"));
        }
      }
    );
  }
}
