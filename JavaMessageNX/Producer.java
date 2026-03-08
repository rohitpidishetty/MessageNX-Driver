import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// javac -cp "libs/jars/*" Producer.java MessageNX.java
// java -cp ".;libs\jars\*" Producer

public class Producer {

  public static void main(String[] args) {
    List<Map<String, Object>> students = new ArrayList<>();

    Map<String, Object> s1 = new HashMap<>();
    s1.put("name", "Rohit");
    s1.put("branch", "Computer Science");
    students.add(s1);

    Map<String, Object> s2 = new HashMap<>();
    s2.put("name", "Ananya");
    s2.put("branch", "Electronics");
    students.add(s2);

    Map<String, Object> s3 = new HashMap<>();
    s3.put("name", "Vikram");
    s3.put("branch", "Mechanical");
    students.add(s3);

    Map<String, Object> s4 = new HashMap<>();
    s4.put("name", "Sneha");
    s4.put("branch", "Civil");
    students.add(s4);

    MessageNX mnx = new MessageNX();
    // String appId = mnx.appName("app-name").register();

    for (Map<String, Object> job : students) {
      int res = mnx.produce(job, "...paste appId");
      System.out.println(res);
    }
  }
}
