using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

public class RdbReader
{
    private const string RDB_VERSION_INDICATOR = "REDIS";

    private static void SeekToByte(BinaryReader br, byte b)
    {
        byte currentByte;
        do{
            currentByte = br.ReadByte();
        } while (currentByte != b);
    }

    private static object ReadSizeEncodedValue(BinaryReader br) {
        byte first = br.ReadByte();
        if ((first & 0b11000000) == 0) {
            // 6 bit integer
            return first;
        } else if ((first & 0b11000000) == 0b01000000) {
            byte second = br.ReadByte();
            byte firstPart = (byte)(first & 0b00111111);
            byte[] bytes = [firstPart, second];
            Array.Reverse(bytes);
            return BitConverter.ToInt16(bytes);
        } else if ((first & 0b11000000) == 0b10000000) {
            // 32-bit integer
            byte[] bytes = br.ReadBytes(4);
            Array.Reverse(bytes);
            return BitConverter.ToInt32(bytes);
        } else {
            first &= 0b00111111;
            return ReadStringEncodedValue(first, br);
        }
    }

    private static object ReadStringEncodedValue(byte first, BinaryReader br) {
        if (first == 0xC0) {
            // 8-bit integer
            return (int)br.ReadByte();
        } else if (first == 0xC1) {
            // 16-bit integer
            byte[] bytes = br.ReadBytes(2);
            return BitConverter.ToInt16(bytes);
        } else if (first == 0xC2) {
            // 32-bit integer
            byte[] bytes = br.ReadBytes(4);
            return BitConverter.ToInt32(bytes);
        } else if (first == 0xC3) {
            // compressed string
            return "";
        } else {
            byte[] bytes = br.ReadBytes(first);
            return Encoding.UTF8.GetString(bytes);
        }
    }

    private static object ReadStringEncodedValue(BinaryReader br) {
        byte first = br.ReadByte();
        return ReadStringEncodedValue(first, br);
    }

    public static Dictionary<string, (string value, DateTime? expiry)> ReadKeysFromRdbFile(string filePath)
    {
        Dictionary<string, (string value, DateTime? expiry)> keys = new Dictionary<string, (string value, DateTime? expiry)>();
        BinaryReader br = null;
        try {
            FileInfo file = new FileInfo(filePath);
            FileStream fs = file.OpenRead();
            // skip the header
            fs.Seek(9, SeekOrigin.Begin);
            br = new BinaryReader(fs);
        } catch (Exception ex) {
            Console.WriteLine("RDB file is empty or not found");
            return keys;
        }

        Console.WriteLine("Reading RDB file...");
        
        Console.WriteLine("1");
        SeekToByte(br, 0xFE);

        Console.WriteLine("2");
        SeekToByte(br, 0xFB);

        Console.WriteLine("3");
        int keyValueSize = Convert.ToInt32(ReadSizeEncodedValue(br));

        Console.WriteLine("4");
        int expirySize = Convert.ToInt32(ReadSizeEncodedValue(br));

        for (int i = 0; i < keyValueSize; i++) {
            Console.WriteLine($"Reading key-value pair {i + 1}");

            byte type = br.ReadByte();
            DateTime? expiry = null;
            if (type == 0xFC) {
                expiry = DateTimeOffset.FromUnixTimeMilliseconds((long)BitConverter.ToUInt64(br.ReadBytes(8))).DateTime;
                type = br.ReadByte(); 
            } else if (type == 0xFD) {
                expiry = DateTimeOffset.FromUnixTimeSeconds(BitConverter.ToUInt32(br.ReadBytes(4))).DateTime;
                type = br.ReadByte();
            }

            string key = Convert.ToString(ReadStringEncodedValue(br));
            string value = type switch {
                0x00 => Convert.ToString(ReadStringEncodedValue(br)),
                _ => throw new NotImplementedException()
            };

            keys.Add(key, (value, expiry));
        }

        return keys;
    }
}

public class Program
{
    static Dictionary<string, (string value, DateTime? expiry)> data = new Dictionary<string, (string, DateTime?)>();
    static string dir = "/tmp";
    static string dbfilename = "dump.rdb";

    // geting the port number from the environment variable
    static int port = 6379;

    public static void Main(string[] args)
    {
        Console.WriteLine("Logs from your program will appear here!");

        // Parse command line arguments
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] == "--dir" && i + 1 < args.Length)
            {
                dir = args[i + 1];
            }
            else if (args[i] == "--dbfilename" && i + 1 < args.Length)
            {
                dbfilename = args[i + 1];
            } else if(args[i] == "--port" && i + 1 < args.Length) {
                port = int.Parse(args[i + 1]);
            }
        }

        // Load keys from RDB file
        LoadKeysFromRdbFile();

        TcpListener server = new TcpListener(IPAddress.Any, port);
        server.Start();

        while (true)
        {
            var clientSocket = server.AcceptSocket();
            _ = HandleClient(clientSocket);
        }
    }

    static void LoadKeysFromRdbFile()
    {
        string rdbFilePath = Path.Combine(dir, dbfilename);
        if (File.Exists(rdbFilePath))
        {
            try
            {
                data = RdbReader.ReadKeysFromRdbFile(rdbFilePath);
                // Console.WriteLine($"Added {keys.Count} keys from RDB file.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading RDB file: {ex.Message}");
            }
        }
        else
        {
            Console.WriteLine("RDB file not found. Starting with empty dataset.");
        }
    }

    static async Task HandleClient(Socket clientSocket)
    {
        const string responseString = "+PONG\r\n";
        while (clientSocket.Connected)
        {
            var buffer = new byte[1024];
            int bytesRead = await clientSocket.ReceiveAsync(buffer, SocketFlags.None);
            var requestString = Encoding.UTF8.GetString(buffer, 0, bytesRead);
            Console.WriteLine(requestString);

            var lines = requestString.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);

            if (lines.Length <= 2)
            {
                var message = "ERR invalid command\r\n";
                var response = Encoding.UTF8.GetBytes(message);
                clientSocket.Send(response);
            }
            else
            {
                switch (lines[2].ToUpper())
                {
                    case "ECHO":
                        var echoMessage = lines[4];
                        var echoResponse = Encoding.UTF8.GetBytes($"+{echoMessage}\r\n");
                        clientSocket.Send(echoResponse);
                        break;

                    case "PING":
                        if (lines.Length == 5)
                        {
                            var pingMessage = lines[4];
                            var pingResponse = Encoding.UTF8.GetBytes($"+{pingMessage}\r\n");
                            clientSocket.Send(pingResponse);
                        }
                        else if (lines.Length == 3)
                        {
                            var pingResponse = Encoding.UTF8.GetBytes(responseString);
                            clientSocket.Send(pingResponse);
                        }
                        break;

                    case "SET":
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
                        var setResponse = Encoding.UTF8.GetBytes("+OK\r\n");
                        clientSocket.Send(setResponse);
                        break;

                    case "GET":
                        var getKey = lines[4];
                        if (data.TryGetValue(getKey, out var getValue))
                        {
                            var (storedValue, storedExpiry) = getValue;
                            if (storedExpiry == null || storedExpiry > DateTime.UtcNow)
                            {
                                var getResponse = Encoding.UTF8.GetBytes($"+{storedValue}\r\n");
                                clientSocket.Send(getResponse);
                            }
                            else
                            {
                                data.Remove(getKey);
                                var getResponse = Encoding.UTF8.GetBytes("$-1\r\n");
                                clientSocket.Send(getResponse);
                            }
                        }
                        else
                        {
                            var getResponse = Encoding.UTF8.GetBytes("$-1\r\n");
                            clientSocket.Send(getResponse);
                        }
                        break;

                    case "CONFIG":
                        if (lines[4].ToUpper() == "GET")
                        {
                            var configKey = lines[6].ToLower();
                            string configValue = null;

                            if (configKey == "dir")
                            {
                                configValue = dir;
                            }
                            else if (configKey == "dbfilename")
                            {
                                configValue = dbfilename;
                            }

                            if (configValue != null)
                            {
                                var configResponse = Encoding.UTF8.GetBytes($"*2\r\n${configKey.Length}\r\n{configKey}\r\n${configValue.Length}\r\n{configValue}\r\n");
                                clientSocket.Send(configResponse);
                            }
                            else
                            {
                                var configResponse = Encoding.UTF8.GetBytes("$-1\r\n");
                                clientSocket.Send(configResponse);
                            }
                        }
                        break;

                    case "KEYS":
                        var pattern = lines[4];
                        Dictionary<string, (string value, DateTime? expiry)> filteredData = new Dictionary<string, (string, DateTime?)>();

                        if (pattern == "*")
                        {
                            Console.WriteLine("Returning all keys");
                            Console.WriteLine("Data count: " + data.Count);
                            foreach (var kvp in data)
                            {
                                var dataKey = kvp.Key;
                                var (dataValue, dataExpiry) = kvp.Value;
                                if (dataExpiry == null || dataExpiry > DateTime.UtcNow)
                                {
                                    filteredData[dataKey] = (dataValue, dataExpiry);
                                }
                            }
                        }
                        else
                        {
                            // yet to implement
                        }
                        Console.WriteLine("Filtered data count: " + filteredData.Count);
                        var keysResponse = new StringBuilder();
                        keysResponse.Append("*").Append(filteredData.Count).Append("\r\n");
                        foreach (var kvp in filteredData)
                        {
                            var filteredKey = kvp.Key;
                            keysResponse.Append("$").Append(filteredKey.Length).Append("\r\n").Append(filteredKey).Append("\r\n");
                        }
                        clientSocket.Send(Encoding.UTF8.GetBytes(keysResponse.ToString()));
                        break;

                    default:
                        var errorResponse = Encoding.UTF8.GetBytes("-ERR unknown command\r\n");
                        clientSocket.Send(errorResponse);
                        break;
                }
            }
        }

        clientSocket.Close();
    }
}