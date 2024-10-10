using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

public class RdbReader
{
    private const string RDB_VERSION_INDICATOR = "REDIS";

    public static Dictionary<string, (string value, DateTime? expiry)> ReadKeysFromRdbFile(string filePath)
    {
        Dictionary<string, (string value, DateTime? expiry)> keys = new Dictionary<string, (string value, DateTime? expiry)>();
        BinaryReader br = null;
        try
        {
            FileInfo file = new FileInfo(filePath);
            FileStream fs = file.OpenRead();
            // skip the header
            fs.Seek(9, SeekOrigin.Begin);
            br = new BinaryReader(fs);
        }
        catch (Exception ex)
        {
            Console.WriteLine("RDB file is empty or not found");
            return keys;
        }

        Console.WriteLine("Reading RDB file...");
        
        SeekToByte(br, 0xFE);
        SeekToByte(br, 0xFB);

        int keyValueSize = Convert.ToInt32(ReadSizeEncodedValue(br));
        int expirySize = Convert.ToInt32(ReadSizeEncodedValue(br));

        for (int i = 0; i < keyValueSize; i++)
        {
            Console.WriteLine($"Reading key-value pair {i + 1}");

            byte type = br.ReadByte();
            DateTime? expiry = null;
            if (type == 0xFC)
            {
                expiry = DateTimeOffset.FromUnixTimeMilliseconds((long)BitConverter.ToUInt64(br.ReadBytes(8))).DateTime;
                type = br.ReadByte(); 
            }
            else if (type == 0xFD)
            {
                expiry = DateTimeOffset.FromUnixTimeSeconds(BitConverter.ToUInt32(br.ReadBytes(4))).DateTime;
                type = br.ReadByte();
            }

            string key = Convert.ToString(ReadStringEncodedValue(br));
            string value = type switch
            {
                0x00 => Convert.ToString(ReadStringEncodedValue(br)),
                _ => throw new NotImplementedException()
            };

            Console.WriteLine($"Key: {key}, Value: {value}, Expiry: {expiry}");
            keys.Add(key, (value, expiry));
        }

        return keys;
    }

    private static void SeekToByte(BinaryReader br, byte b)
    {
        byte currentByte;
        do
        {
            currentByte = br.ReadByte();
        } while (currentByte != b);
    }

    private static object ReadSizeEncodedValue(BinaryReader br)
    {
        byte first = br.ReadByte();
        if ((first & 0b11000000) == 0)
        {
            // 6 bit integer
            return first;
        }
        else if ((first & 0b11000000) == 0b01000000)
        {
            byte second = br.ReadByte();
            byte firstPart = (byte)(first & 0b00111111);
            byte[] bytes = new byte[] { firstPart, second };
            Array.Reverse(bytes);
            return BitConverter.ToInt16(bytes);
        }
        else if ((first & 0b11000000) == 0b10000000)
        {
            // 32-bit integer
            byte[] bytes = br.ReadBytes(4);
            Array.Reverse(bytes);
            return BitConverter.ToInt32(bytes);
        }
        else
        {
            first &= 0b00111111;
            return ReadStringEncodedValue(first, br);
        }
    }

    private static object ReadStringEncodedValue(byte first, BinaryReader br)
    {
        if (first == 0xC0)
        {
            // 8-bit integer
            return (int)br.ReadByte();
        }
        else if (first == 0xC1)
        {
            // 16-bit integer
            byte[] bytes = br.ReadBytes(2);
            return BitConverter.ToInt16(bytes);
        }
        else if (first == 0xC2)
        {
            // 32-bit integer
            byte[] bytes = br.ReadBytes(4);
            return BitConverter.ToInt32(bytes);
        }
        else if (first == 0xC3)
        {
            // compressed string
            return "";
        }
        else
        {
            byte[] bytes = br.ReadBytes(first);
            return Encoding.UTF8.GetString(bytes);
        }
    }

    private static object ReadStringEncodedValue(BinaryReader br)
    {
        byte first = br.ReadByte();
        return ReadStringEncodedValue(first, br);
    }

    public static Dictionary<string, (string value, DateTime? expiry)> LoadKeysFromRdbFile(string dir, string dbfilename)
    {
        string rdbFilePath = Path.Combine(dir, dbfilename);
        Dictionary<string, (string value, DateTime? expiry)> keys = new Dictionary<string, (string value, DateTime? expiry)>();
        if (File.Exists(rdbFilePath))
        {
            try
            {
                keys = ReadKeysFromRdbFile(rdbFilePath);
                // Console.WriteLine($"Added {keys.Count} keys from RDB file.");
                return keys;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading RDB file: {ex.Message}");
                return keys;
            }
        }
        else
        {
            Console.WriteLine("RDB file not found. Starting with empty dataset.");
            return keys;
        }
    }
}