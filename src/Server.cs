using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

public class Server
{
    private static int port = 6379;
    private static string dir = "/tmp";
    private static string dbfilename = "dump.rdb";

    public static void Main(string[] args)
    {
        Console.WriteLine("Logs from your program will appear here!");

        // ReadArgs.ParseCommandLineArguments(args, ref dir, ref dbfilename, ref port);
        ReadArgs.ParseCommandLineArguments(args);

        RedisCommandHandler.data = RdbReader.LoadKeysFromRdbFile(ReadArgs.Dir, ReadArgs.DbFilename);

        TcpListener server = new TcpListener(IPAddress.Any, ReadArgs.Port);
        server.Start();

        while (true)
        {
            var clientSocket = server.AcceptSocket();
            _ = HandleClient(clientSocket);
        }
    }

    static async Task HandleClient(Socket clientSocket)
    {
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
                RedisCommandHandler.HandleCommand(lines, clientSocket);
            }
        }

        clientSocket.Close();
    }
}