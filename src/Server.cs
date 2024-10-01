using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;  


// You can use print statements as follows for debugging, they'll be visible when running tests.
// Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();
Dictionary<string, string> data = new Dictionary<string, string>();

// creating a new web socket connection
while(true){
    var clientSocket = server.AcceptSocket(); // wait for client
    _ = HandleClient(clientSocket);
}

async Task HandleClient(Socket clientSocket){
    const string responseString = "+PONG\r\n";
    while(clientSocket.Connected){
        // for request
        var buffer = new byte[1024];
        int bytesRead = await clientSocket.ReceiveAsync(buffer, SocketFlags.None);
        var requestString = Encoding.UTF8.GetString(buffer, 0, bytesRead);
        Console.WriteLine(requestString);

        // for response
        var lines = requestString.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
        if(lines.Length <= 2){
            var message = "ERR invalid command\r\n";
            var response = Encoding.UTF8.GetBytes(message);
            clientSocket.Send(response);
        } else {
            if(lines[2].ToUpper() == "ECHO"){
                var message = lines[4];
                var response = Encoding.UTF8.GetBytes($"+{message}\r\n");
                clientSocket.Send(response);
            } else if(lines[2].ToUpper() == "PING"){
                if(lines.Length == 5){
                    var message = lines[4];
                    var response = Encoding.UTF8.GetBytes($"+{message}\r\n");
                    clientSocket.Send(response);
                } else if(lines.Length == 3){
                    var response = Encoding.UTF8.GetBytes(responseString);
                    clientSocket.Send(response);
                }
            } else if(lines[2].ToUpper() == "SET"){
                var key = lines[4];
                var value = lines[6];
                data[key] = value;
                var response = Encoding.UTF8.GetBytes("+OK\r\n");
                clientSocket.Send(response);
            } else if(lines[2].ToUpper() == "GET"){
                var key = lines[4];
                if(data.ContainsKey(key)){
                    var value = data[key];
                    var response = Encoding.UTF8.GetBytes($"+{value}\r\n");
                    clientSocket.Send(response);
                } else {
                    var response = Encoding.UTF8.GetBytes("$-1\r\n");
                    clientSocket.Send(response);
                }
            }
        }
    }

    clientSocket.Close();
}
server.Stop();