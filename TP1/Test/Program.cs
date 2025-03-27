using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

class TestServer
{
	private static readonly int port = 5000; // Port for listening to Wavy connections

	public static void Main(string[] args)
	{
		TcpListener listener = new TcpListener(IPAddress.Any, port);
		listener.Start();
		Console.WriteLine("Test server listening...");

		while (true)
		{
			try
			{
				using (TcpClient client = listener.AcceptTcpClient())
				using (NetworkStream stream = client.GetStream())
				{
					byte[] buffer = new byte[1024];
					int bytesRead = stream.Read(buffer, 0, buffer.Length);
					string receivedData = Encoding.UTF8.GetString(buffer, 0, bytesRead);

					Console.WriteLine("Received data:");
					Console.WriteLine(receivedData);

					// Send confirmation back to Wavy
					SendConfirmation(stream, 401);
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine("Error: " + ex.Message);
			}
		}
	}

	/// <summary>
	/// Sends a confirmation message with the specified code.
	/// </summary>
	/// <param name="stream">The network stream to send the confirmation to</param>
	/// <param name="code">The confirmation code (e.g., 401 for Wavy data confirmation)</param>
	private static void SendConfirmation(NetworkStream stream, int code)
	{
		byte[] codeBytes = BitConverter.GetBytes(code);
		stream.Write(codeBytes, 0, codeBytes.Length);
	}
}
