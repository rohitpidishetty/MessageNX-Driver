using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Threading;

/// <summary>
/// MessageNX - High-Performance Producer-Consumer Message Processing System
/// 
/// MessageNX is a scalable and efficient message processing library developed under NFRAC initiative.
/// It provides a reliable mechanism for producers to push jobs to isolated channels and for consumers
/// to fetch, queue, and process these jobs concurrently with configurable thresholds and timeouts.
/// </summary>
public class MessageNX
{
    private string channel = "https://messagenx-a5f0h8c5gehjd8ab.canadacentral-01.azurewebsites.net/message-nx-consumer/listen";
    private int threshold = 5;
    private int timeout = 60; // seconds
    private Queue<Dictionary<string, object>> localQueue;
    private long? previousTimeStamp = null;
    private List<string> producerDetails;
    private string channelId = null;

    private static readonly HttpClient client = new HttpClient();
    private readonly JsonSerializerOptions jsonOptions;

    public MessageNX()
    {
        localQueue = new Queue<Dictionary<string, object>>();
        producerDetails = new List<string>();
        jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
    }

    public MessageNX AppName(string name)
    {
        producerDetails.Add(name);
        return this;
    }

    public async Task<string> RegisterAsync()
    {
        string content = string.Join("", producerDetails);
        string url = $"https://messagenx-a5f0h8c5gehjd8ab.canadacentral-01.azurewebsites.net/message-nx-producer/id?content={content}";

        try
        {
            var res = await client.GetAsync(url);
            if (res.StatusCode == System.Net.HttpStatusCode.Conflict)
            {
                Console.WriteLine("ID already exists. Try a different combination.");
                Environment.Exit(1);
            }
            else if (!res.IsSuccessStatusCode)
            {
                Console.WriteLine(await res.Content.ReadAsStringAsync());
                return null;
            }

            string id = await res.Content.ReadAsStringAsync();
            Console.WriteLine($"{id} is the id registered for this particular app. Use this on consumer end, to read messages");
            return id;
        }
        catch
        {
            Console.WriteLine("Error, try again later.");
            Environment.Exit(1);
        }

        return null;
    }

    public async Task<int> ProduceAsync(Dictionary<string, object> topic, string channel)
    {
        var payload = new Dictionary<string, object>
        {
            { "topic", topic },
            { "channel", channel }
        };

        try
        {
            string json = JsonSerializer.Serialize(payload, jsonOptions);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var res = await client.PostAsync(
                "https://messagenx-a5f0h8c5gehjd8ab.canadacentral-01.azurewebsites.net/message-nx-producer/listen",
                content
            );
            return (int)res.StatusCode;
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
            return -1;
        }
    }

    public void SetThreshold(int threshold) => this.threshold = threshold;
    public void SetChannel(string channelId) => this.channelId = channelId;

    public async Task ConsumeAsync(Action<Dictionary<string, object>> processor)
    {
        if (channelId == null)
        {
            Console.WriteLine("Set a channel ID before consuming messages.");
            return;
        }

        while (true)
        {
            while (localQueue.Count < threshold)
            {
                var res = await client.GetAsync($"{channel}?channel={channelId}");
                string contentStr = await res.Content.ReadAsStringAsync();

                if (res.StatusCode == System.Net.HttpStatusCode.Accepted)
                {
                    var message = JsonSerializer.Deserialize<Dictionary<string, object>>(contentStr, jsonOptions);
                    localQueue.Enqueue(message);
                    previousTimeStamp = null;
                }
                else
                {
                    if (previousTimeStamp == null)
                        previousTimeStamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                    if (previousTimeStamp != null &&
                        (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - previousTimeStamp) > timeout)
                    {
                        previousTimeStamp = null;
                        break;
                    }
                    await Task.Delay(5000);
                }
            }

            while (localQueue.Count > 0)
            {
                var msg = localQueue.Dequeue();
                processor(msg);
            }
        }
    }
}