const MessageNX = require("./MessageNX");

(async () => {
  const mnx = new MessageNX();
  mnx.setChannel("app Id");

  await mnx.consume((topic) => {
    if (topic.name) console.log(topic.name);
  });
})();