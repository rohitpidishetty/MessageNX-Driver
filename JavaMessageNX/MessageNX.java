import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * MessageNX - High-Performance Producer-Consumer Message Processing System
 *
 * <p>
 * MessageNX is a scalable and efficient message processing application developed under
 * the NFRAC  initiative. It provides
 * a reliable mechanism for producers to push jobs to isolated channels and for consumers
 * to fetch, queue, and process these jobs concurrently with configurable thresholds and timeouts.
 * </p>
 *
 * <p>
 * Features include:
 * <ul>
 *   <li>Asynchronous job fetching from multiple channels.</li>
 *   <li>Local queue management with configurable threshold to optimize processing.</li>
 *   <li>JSON-based message handling, automatically mapped to {@code Map<String, Object>}.</li>
 *   <li>Timeout and retry logic for robust connectivity with producers.</li>
 *   <li>Flexible processing via callback interface to allow custom handling of jobs.</li>
 * </ul>
 * </p>
 *
 * <p>
 * MessageNX is designed for enterprise-level task orchestration where multiple
 * producer-consumer pairs need isolated channels but centralized management of job metadata.
 * </p>
 *
 * <p><b>Author:</b> Er. P. Rohit V. Acharya</p>
 * <p><b>Organization:</b> NFRAC</p>
 * <p><b>Version:</b> 1.0</p>
 * <p><b>Date:</b> 2026-03-06</p>
 *
 * <p><b>Producer Example usage:</b></p>
 * <pre>
 * // Initialize the MessageNX producer
 * MessageNX mnx = new MessageNX();
 *
 * // Optional: register an app ID
 * // String appId = mnx.appName("sample-app3").register();
 *
 * // Channel ID for sending jobs
 * String channelId = "106524789";
 *
 * // Iterate over jobs and send each to the channel
 * for (Map<String, Object> job : jobs) {
 *     int res = mnx.produce(job, channelId);
 *     System.out.println(res);
 * }
 * </pre>
 *
 * <p><b>Consumer Usage Example:</b></p>
 * <pre>
 * public class Consumer {
 *
 *     public static void main(String[] args) throws Exception {
 *         // Initialize MessageNX consumer
 *         MessageNX mnx = new MessageNX();
 *
 *         // Set the channel ID to consume from
 *         mnx.setChannel("106524789");
 *
 *         // Start consuming messages with a processor callback
 *         mnx.consume(new MessageNX.process() {
 *             @Override
 *             public void processor(Map<String, Object> topic) {
 *                 // Process each message (here we just print the "name" field)
 *                 System.out.println(topic.get("name"));
 *             }
 *         });
 *     }
 * }
 * </pre>
 */
final class MessageNX {

  static interface process {
    void processor(Map<String, Object> topic);
  }

  private String channel =
    "https://messagenx-a5f0h8c5gehjd8ab.canadacentral-01.azurewebsites.net/message-nx-consumer/listen";
  private int threshold = 5;
  private int timeout = 60;
  private Queue<Map<String, Object>> localQueue;
  private Long previousTimeStamp = null;
  private ArrayList<String> producerDetails;
  private String channelId = null;
  private HttpClient client;
  private HttpRequest request;
  private HttpResponse<String> response;
  private final ObjectMapper mapper;

  public MessageNX() {
    this.localQueue = new ArrayDeque<>();
    this.producerDetails = new ArrayList<>();
    this.client = HttpClient.newHttpClient();
    this.mapper = new ObjectMapper();
  }

  public MessageNX appName(String name) {
    this.producerDetails.add(name);
    return this;
  }

  public String register() {
    String content = String.join("", this.producerDetails);
    this.request = HttpRequest.newBuilder()
      .uri(
        URI.create(
          "https://messagenx-a5f0h8c5gehjd8ab.canadacentral-01.azurewebsites.net/message-nx-producer/id?content=" +
            content
        )
      )
      .GET()
      .header("Content-Type", "application/json")
      .build();
    try {
      this.response = this.client.send(
        this.request,
        HttpResponse.BodyHandlers.ofString()
      );
      if (this.response.statusCode() == 409) {
        System.out.println("ID already exists. Try a different combination.");
        System.exit(1);
      } else if (this.response.statusCode() != 200) {
        System.out.println(this.response.body());
        return null;
      }
      System.out.println(
        this.response.body() +
          " is the id registered for this particular app. Use this on consumer end, to read messages"
      );

      return this.response.body();
    } catch (Exception e) {
      System.out.println("Error, try again later");
      System.exit(1);
    }
    return null;
  }

  public int produce(Map<String, Object> topic, String channel) {
    Map<String, Object> json = new HashMap<>();
    json.put("topic", topic);
    json.put("channel", channel);
    try {
      String stringify = this.mapper.writeValueAsString(json);
      this.request = HttpRequest.newBuilder()
        .uri(
          URI.create(
            "https://messagenx-a5f0h8c5gehjd8ab.canadacentral-01.azurewebsites.net/message-nx-producer/listen"
          )
        )
        .header("Content-Type", "application/json")
        .POST(BodyPublishers.ofString(stringify))
        .build();
      return this.client.send(
        this.request,
        HttpResponse.BodyHandlers.ofString()
      ).statusCode();
    } catch (Exception e) {
      System.out.println(e.getMessage());
      return -1;
    }
  }

  public void setThreshold(int threshold) {
    this.threshold = threshold;
  }

  public void setChannel(String channelId) {
    this.channelId = channelId;
  }

  public void consume(process processor) throws Exception {
    if (this.channelId == null) {
      System.out.println("Set a channel ID before consuming messages.");
      return;
    }

    while (true) {
      while (this.localQueue.size() < this.threshold) {
        this.request = HttpRequest.newBuilder()
          .uri(URI.create(this.channel + "?channel=" + this.channelId))
          .GET()
          .header("Content-Type", "application/json")
          .build();
        this.response = this.client.send(
          this.request,
          HttpResponse.BodyHandlers.ofString()
        );
        if (this.response.statusCode() == 202) {
          this.localQueue.offer(
            this.mapper.readValue(this.response.body(), Map.class)
          );
          this.previousTimeStamp = null;
        } else {
          if (this.previousTimeStamp == null) {
            this.previousTimeStamp = System.currentTimeMillis();
          }
          if (
            this.previousTimeStamp != null &&
            (System.currentTimeMillis() - this.previousTimeStamp) > this.timeout
          ) {
            this.previousTimeStamp = null;
            break;
          }
          try {
            Thread.sleep(5000);
          } catch (Exception e) {}
        }
      }

      while (this.localQueue.size() > 0) {
        processor.processor(this.localQueue.poll());
      }
    }
  }
}
