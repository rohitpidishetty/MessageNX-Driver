# MessageNX-Driver
Driver code for MessageNX, implemented in Java and Python. You can directly download these package and link it to your code via pre-implemented methods.
MessageNX is a lightweight producer-consumer messaging library that enables applications to communicate through isolated message channels. Producers can publish messages to a channel while consumers fetch and process them asynchronously.

---

## Method Summary

| Method | Type | Description |
|------|------|-------------|
| `appName(String)` | MessageNX | Sets the application name for the MessageNX instance. This method follows the builder pattern and returns the MessageNX object allowing chained method calls before registering the application. |
| `register()` | String | Registers the application name with MessageNX and returns the generated application ID. This method should be called after `appName()`. |
| `setThreshold(int)` | void | Sets the consumption threshold for the consumer. This defines the number of messages to be fetched and processed from the MessageNX queue in one batch. |
| `setChannel(String)` | void | Connects the consumer to a specific isolated MessageNX queue (channel) and enables it to read messages produced to that channel. |
| `produce(Map<String,Object>, String)` | int | Produces a message to the MessageNX queue. The first parameter represents the message topic payload and the second parameter specifies the application channel where the message will be published. |
| `consume(process)` | void | Consumes messages from the configured MessageNX channel based on the threshold value and processes them using the callback function passed as a parameter. |

---

## Installation

Download the following dependencies:

- `jackson-annotations-2.15.4.jar`
- `jackson-core-2.15.4.jar`
- `jackson-databind-2.15.4.jar`

Place them inside the following folder:

libs/jars/


Example project structure:


# Project File Structure


project
│
├── libs
│ └── jars
│ ├── jackson-annotations-2.15.4.jar
│ ├── jackson-core-2.15.4.jar
│ └── jackson-databind-2.15.4.jar
│
├── Producer.java
└── Consumer.java



---

## Compile

```bash
javac -cp "libs/jars/*" source.java

Example:

javac -cp "libs/jars/*" Producer.java
javac -cp "libs/jars/*" Consumer.java
Run
java -cp ".;libs/jars/*" classname

Example:

java -cp ".;libs/jars/*" Producer
java -cp ".;libs/jars/*" Consumer
Producer Example
import javap.mnx.MessageNX;
import java.util.HashMap;
import java.util.Map;

public class Producer {

    public static void main(String[] args) throws Exception {

        MessageNX mnx = new MessageNX();

        Map<String, Object> topic = new HashMap<>();
        topic.put("name", "Rohit");
        topic.put("age", 26);

        mnx.produce(topic, "106524789");
    }
}
Consumer Example
import javap.mnx.MessageNX;
import java.util.Map;

public class Consumer {

    public static void main(String[] args) throws Exception {

        MessageNX mnx = new MessageNX();

        mnx.setChannel("106524789");

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
