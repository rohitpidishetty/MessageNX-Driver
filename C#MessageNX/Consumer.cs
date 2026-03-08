using System;
using System.Collections.Generic;
using System.Threading.Tasks;

class Consumer
{
    static async Task Main(string[] args)
    {
        var mnx = new MessageNX();
        mnx.SetChannel("141983614");
        await mnx.ConsumeAsync(topic =>
        {
            if (topic.ContainsKey("name"))
            {
                Console.WriteLine(topic["name"]);
            }
        });
    }
}