const axios = require("axios");

class MessageNX {
  constructor() {
    this.channel =
      "https://messagenx-a5f0h8c5gehjd8ab.canadacentral-01.azurewebsites.net/message-nx-consumer/listen";
    this.threshold = 5;
    this.timeout = 60;
    this.localQueue = [];
    this.previousTimeStamp = null;
    this.producerDetails = [];
    this.channelId = null;
  }

  appName(name) {
    this.producerDetails.push(name);
    return this;
  }

  async register() {
    const content = this.producerDetails.join("");
    const url = `https://messagenx-a5f0h8c5gehjd8ab.canadacentral-01.azurewebsites.net/message-nx-producer/id?content=${content}`;
    try {
      const res = await axios.get(url);
      if (res.status === 409) {
        console.log("ID already exists. Try a different combination.");
        process.exit(1);
      } else if (res.status !== 200) {
        console.log(res.data);
        return null;
      }
      console.log(
        `${res.data} is the id registered for this particular app. Use this on consumer end, to read messages`
      );
      return res.data;
    } catch (err) {
      console.log("Error, try again later");
      process.exit(1);
    }
  }

  async produce(topic, channelId) {
    try {
      const res = await axios.post(
        "https://messagenx-a5f0h8c5gehjd8ab.canadacentral-01.azurewebsites.net/message-nx-producer/listen",
        { topic, channel: channelId },
        { headers: { "Content-Type": "application/json" } }
      );
      return res.status;
    } catch (err) {
      console.log(err.message);
      return -1;
    }
  }

  setThreshold(threshold) {
    this.threshold = threshold;
  }

  setChannel(channelId) {
    this.channelId = channelId;
  }

  async consume(processCallback) {
    if (!this.channelId) {
      console.log("Set a channel ID before consuming messages.");
      return;
    }

    while (true) {
      while (this.localQueue.length < this.threshold) {
        try {
          const res = await axios.get(`${this.channel}?channel=${this.channelId}`);
          if (res.status === 202) {
            this.localQueue.push(res.data);
            this.previousTimeStamp = null;
          } else {
            if (!this.previousTimeStamp) this.previousTimeStamp = Date.now();
            if (this.previousTimeStamp && (Date.now() - this.previousTimeStamp) / 1000 > this.timeout) {
              this.previousTimeStamp = null;
              break;
            }
            await new Promise((r) => setTimeout(r, 5000));
          }
        } catch (err) {
          console.log(err.message);
          await new Promise((r) => setTimeout(r, 5000));
        }
      }

      while (this.localQueue.length > 0) {
        const msg = this.localQueue.shift();
        processCallback(msg);
      }
    }
  }
}

module.exports = MessageNX;