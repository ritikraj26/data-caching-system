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

        // sending PING to Master
        if (ReadArgs.IsReplica) {

            // handshake 1/3
            var pingMessage = "*1\r\n$4\r\nPING\r\n";
            var pingMessageBytes = Encoding.UTF8.GetBytes(pingMessage);

            var masterSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            masterSocket.Connect(ReadArgs.MasterHost, ReadArgs.MasterPort);
            masterSocket.Send(pingMessageBytes);

            Console.WriteLine("Sent PING to Master");

            // handshake 2/3
            pingMessage=$"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{ReadArgs.Port}\r\n";
            pingMessageBytes = Encoding.UTF8.GetBytes(pingMessage);
            masterSocket.Send(pingMessageBytes);

            pingMessage="*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
            pingMessageBytes = Encoding.UTF8.GetBytes(pingMessage);
            masterSocket.Send(pingMessageBytes);

            // Console.WriteLine("Sent REPLCONF to Master");

            // masterSocket.Close();
        }

        TcpListener server = new TcpListener(IPAddress.Any, ReadArgs.Port);
        server.Start();

        while (true)
        {
            var clientSocket = server.AcceptSocket();
            // Console.WriteLine("Accepted connection from " + clientSocket.RemoteEndPoint);
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