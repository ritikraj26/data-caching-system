using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;

public static class RedisCommandHandler
{
    public static Dictionary<string, (string value, DateTime? expiry)> data = new Dictionary<string, (string, DateTime?)>();

    public static void HandleCommand(string[] lines, Socket clientSocket)
    {
        switch (lines[2].ToUpper())
        {
            case "ECHO":
                HandleEcho(lines, clientSocket);
                break;
            case "PING":
                HandlePing(lines, clientSocket);
                break;
            case "SET":
                HandleSet(lines, clientSocket);
                break;
            case "GET":
                HandleGet(lines, clientSocket);
                break;
            case "CONFIG":
                HandleConfig(lines, clientSocket);
                break;
            case "KEYS":
                HandleKeys(lines, clientSocket);
                break;
            case "INFO":
                HandleInfo(lines, clientSocket);
                break;
            default:
                SendErrorResponse(clientSocket, "ERR unknown command");
                break;
        }
    }

    private static void HandleEcho(string[] lines, Socket clientSocket)
    {
        var echoMessage = lines[4];
        SendResponse(clientSocket, echoMessage);
    }

    private static void HandlePing(string[] lines, Socket clientSocket)
    {
        if (lines.Length == 5)
        {
            SendResponse(clientSocket, lines[4]);
        }
        else if (lines.Length == 3)
        {
            SendResponse(clientSocket, "PONG");
        }
    }

    private static void HandleSet(string[] lines, Socket clientSocket)
    {
        var key = lines[4];
        var value = lines[6];
        DateTime? expiry = null;
        if (lines.Length >= 10 && lines[8].ToUpper() == "PX")
        {
            if (int.TryParse(lines[10], out int expiryMs))
            {
                expiry = DateTime.UtcNow.AddMilliseconds(expiryMs);
            }
        }

        data[key] = (value, expiry);
        var message = "OK";
        SendResponse(clientSocket, message);
    }

    private static void HandleGet(string[] lines, Socket clientSocket)
    {
        var getKey = lines[4];
        Console.WriteLine("GET key: " + getKey);
        // iterate over data dictionary and print
        Console.WriteLine("count: " + data.Count);
        foreach (var kvp in data)
        {
            Console.WriteLine("Key: " + kvp.Key);
            Console.WriteLine("Value: " + kvp.Value);
        }


        if (data.TryGetValue(getKey, out var getValue))
        {
            var (storedValue, storedExpiry) = getValue;
            Console.WriteLine("Stored value: " + storedValue);
            if (storedExpiry == null || storedExpiry > DateTime.UtcNow)
            {
                SendResponse(clientSocket, storedValue);
            }
            else
            {
                data.Remove(getKey);
                SendNullResponse(clientSocket);
            }
        }
        else
        {
            SendNullResponse(clientSocket);
        }
    }

    private static void HandleConfig(string[] lines, Socket clientSocket)
    {
        if (lines[4].ToUpper() == "GET")
        {
            var configKey = lines[6].ToLower();
            string configValue = null;

            if (configKey == "dir")
            {
                configValue = ReadArgs.Dir;
            }
            else if (configKey == "dbfilename")
            {
                configValue = ReadArgs.DbFilename;
            }

            if (configValue != null)
            {
                SendArrayResponse(clientSocket, new[] { configKey, configValue });
            }
            else
            {
                SendNullResponse(clientSocket);
            }
        }
    }

    private static void HandleKeys(string[] lines, Socket clientSocket)
    {
        var pattern = lines[4];
        var filteredKeys = new List<string>();

        if (pattern == "*")
        {
            foreach (var kvp in data)
            {
                var dataKey = kvp.Key;
                var (_, dataExpiry) = kvp.Value;
                if (dataExpiry == null || dataExpiry > DateTime.UtcNow)
                {
                    filteredKeys.Add(dataKey);
                }
            }
        }
        else
        {
            // Pattern matching not implemented
        }

        SendArrayResponse(clientSocket, filteredKeys.ToArray());
    }

    private static void HandleInfo (string []lines, Socket clientSocket) {
        var infoCommand = lines[4];
        if (infoCommand == "replication") {
            string message = "role:master";
            var response = Encoding.UTF8.GetBytes($"+{message}\r\n");
            clientSocket.Send(response);
        }
    }

    private static void SendResponse(Socket clientSocket, string message)
    {
        var response = Encoding.UTF8.GetBytes($"+{message}\r\n");
        clientSocket.Send(response);
    }

    private static void SendErrorResponse(Socket clientSocket, string message)
    {
        var response = Encoding.UTF8.GetBytes($"-{message}\r\n");
        clientSocket.Send(response);
    }

    private static void SendNullResponse(Socket clientSocket)
    {
        var response = Encoding.UTF8.GetBytes("$-1\r\n");
        clientSocket.Send(response);
    }

    private static void SendArrayResponse(Socket clientSocket, string[] items)
    {
        var response = new StringBuilder();
        response.Append($"*{items.Length}\r\n");
        foreach (var item in items)
        {
            response.Append($"${item.Length}\r\n{item}\r\n");
        }
        clientSocket.Send(Encoding.UTF8.GetBytes(response.ToString()));
    }
}