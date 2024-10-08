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

    public static Dictionary<string, (string value, DateTime? expiry)> ReadKeysFromRdbFile(string filePath)
    {
        Dictionary<string, (string value, DateTime? expiry)> keys = new Dictionary<string, (string value, DateTime? expiry)>();

        using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read))
        using (BinaryReader reader = new BinaryReader(fs))
        {
            Console.WriteLine("Reading RDB file...");
            // 1. Read and verify the header
            string headerName = new string(reader.ReadChars(5));
            Console.WriteLine($"Header: {headerName}");
            if (headerName != RDB_VERSION_INDICATOR)
            {
                throw new Exception("Invalid RDB file format");
            }

            // Read the version (unused)
            string versionNo = new string(reader.ReadChars(4));
            Console.WriteLine($"Version: {versionNo}");

            // 2. Skip metadata section (simplified)
            // In a full implementation, you'd parse auxiliary fields here

            // 3. Read the database section
            while (true)
            {
                byte opcode = reader.ReadByte();

                if (opcode == 0xFF) // EOF marker
                {
                    // yet to implement the checksum at the end of the file
                    // read 8byte CRC64 checksum for the entire file
                    long checksum = reader.ReadInt64();
                    Console.WriteLine($"Checksum: {checksum}");
                    break;
                }

                // reading the header section
                if (opcode == 0xFA) {
                    Console.WriteLine("Reading header section...");
                    int headerLen = reader.ReadByte();
                    Console.WriteLine($"Header length: {headerLen}");
                    string header = new string(reader.ReadChars(headerLen));
                    Console.WriteLine($"Header: {header}");
                    int metadataLen = reader.ReadByte();
                    Console.WriteLine($"Metadata length: {metadataLen}");
                    string metadata = new string(reader.ReadChars(metadataLen));
                    Console.WriteLine($"Metadata: {metadata}");
                    // advance the opcode
                    opcode = reader.ReadByte();
                    continue;
                }


                if (opcode == 0xFE) // Select DB opcode
                {
                    // Read database number (we're not using it in this simple implementation)
                    Console.WriteLine("Reading database section...");
                    ReadDatabaseSection(reader, keys);
                }
            }

            // 4. End of file section (already handled by breaking the loop)
        }
        Console.WriteLine("Keys Length: " + keys.Count);
        return keys;
    }

    private static void ReadDatabaseSection(BinaryReader reader, Dictionary<string, (string value, DateTime? expiry)> keys)
    {
        uint index = reader.ReadByte();
        Console.WriteLine($"Database index: {index}");

        byte opcode = reader.ReadByte();
        if (opcode == 0xFB) {
            // reading the hash table size information
            int hashTableSize = reader.ReadByte();
            Console.WriteLine($"Hash table size: {hashTableSize}");
            int keysWithExpiry = reader.ReadByte();
            Console.WriteLine($"Keys with expiry: {keysWithExpiry}");
            
            while(hashTableSize > 0) {
                hashTableSize--;
                int valueType = reader.ReadByte();
                // valueType 0 means its string
                if (valueType == 0) {
                    int keyLen = reader.ReadByte();
                    string key = new string(reader.ReadChars(keyLen));
                    int valueLen = reader.ReadByte();
                    string value = new string(reader.ReadChars(valueLen));
                    Console.WriteLine($"Key: {key}, Value: {value}");

                    if (keysWithExpiry > 0) {
                        byte expirycode = reader.ReadByte();
                        if (expirycode == 0xFC || expirycode == 0xFD) {
                            int expiryLen = reader.ReadByte();
                            string expiry = new string(reader.ReadChars(expiryLen));
                            Console.WriteLine($"Expiry: {expiry}");
                            keys[key] = (value, DateTime.Parse(expiry));
                            keysWithExpiry--;
                        } else {
                            keys[key] = (value, null);
                            // go back one byte
                            reader.BaseStream.Position -= 1;
                        }
                        // adding key,value and expiry to the dictionary

                    } else {
                        // adding key and value to the dictionary
                        keys[key] = (value, null);
                    }
                } else {
                    throw new Exception("Invalid value type");
                }
            }
        } else {
            throw new Exception("Invalid database section");
        }
    }
}

public class Program
{
    static Dictionary<string, (string value, DateTime? expiry)> data = new Dictionary<string, (string, DateTime?)>();
    static string dir = "/tmp";
    static string dbfilename = "dump.rdb";

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
            }
        }

        // Load keys from RDB file
        LoadKeysFromRdbFile();

        TcpListener server = new TcpListener(IPAddress.Any, 6379);
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