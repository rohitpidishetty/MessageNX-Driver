const MessageNX = require("./MessageNX");

(async () => {
  const students = [
    { name: "Rohit", branch: "Computer Science" },
    { name: "Ananya", branch: "Electronics" },
    { name: "Vikram", branch: "Mechanical" },
    { name: "Sneha", branch: "Civil" },
  ];

  const mnx = new MessageNX();

  // const appId = await mnx.appName("app-name").register();

  const channelId = "...paste appId";

  for (const job of students) {
    const res = await mnx.produce(job, channelId);
    console.log(res);
  }
})();