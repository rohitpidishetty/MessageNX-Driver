using System;
using System.Collections.Generic;
using System.Threading.Tasks;

class Producer
{
    static async Task Main(string[] args)
    {
        var students = new List<Dictionary<string, object>>();

        var s1 = new Dictionary<string, object> { { "name", "Rohit" }, { "branch", "Computer Science" } };
        var s2 = new Dictionary<string, object> { { "name", "Ananya" }, { "branch", "Electronics" } };
        var s3 = new Dictionary<string, object> { { "name", "Vikram" }, { "branch", "Mechanical" } };
        var s4 = new Dictionary<string, object> { { "name", "Sneha" }, { "branch", "Civil" } };

        students.Add(s1);
        students.Add(s2);
        students.Add(s3);
        students.Add(s4);

        var mnx = new MessageNX();

        // string appId = await mnx.AppName("sample-app3").RegisterAsync();

        string channelId = "141983614";

        foreach (var job in students)
        {
            int res = await mnx.ProduceAsync(job, channelId);
            Console.WriteLine(res);
        }
    }
}