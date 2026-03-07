# MessageNX-Driver
Driver code for MessageNX, implemented in Java and Python. You can directly download these package and link it to your code via pre-implemented methods.

Method	Type	Description
appName(String)	MessageNX	Sets the application name for the MessageNX instance. This method follows the builder pattern and returns the MessageNX object allowing chained method calls before registering the application.
register()	String	Registers the application name with MessageNX and returns the generated application ID. This method should be called after appName().
setThreshold(int)	void	Sets the consumption threshold for the consumer. This defines the number of messages to be fetched and processed from the MessageNX queue in one batch.
setChannel(String)	void	Connects the consumer to a specific isolated MessageNX queue (channel) and enables it to read messages produced to that channel.
produce(Map<String,Object>, String)	int	Produces a message to the MessageNX queue. The first parameter represents the message topic payload and the second parameter specifies the application channel where the message will be published.
consume(process)	void	Consumes messages from the configured MessageNX channel based on the threshold value and processes them using the callback function passed as a parameter.
Important Notes	:	While working with this package make sure you have jackson-annotations-2.15.4.jar, jackson-core-2.15.4.jar, jackson-databind-2.15.4.jar downloaded on your machine and place them inside libs:Folder\jars:Folder\[Here]:Jars. Command to compile javac -cp "libs/jars/*" source.java, to run java -cp ".;libs\jars\*" classname.
