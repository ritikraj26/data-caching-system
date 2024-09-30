using System.Net;
using System.Net.Sockets;
using System.Text;

// You can use print statements as follows for debugging, they'll be visible when running tests.
// Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();

// creating a new web socket connection
var clientSocket = server.AcceptSocket(); // wait for client
const string responseString = "+PONG\r\n";
while(clientSocket.Connected){
    // for request
    var buffer = new byte[1024];
    await clientSocket.ReceiveAsync(buffer);

    // for response
    var response = Encoding.UTF8.GetBytes(responseString);
    clientSocket.Send(response);
}

clientSocket.Close();
server.Stop();