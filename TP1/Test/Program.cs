using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Text.Json;
using System.Collections.Generic;

class TestServer
{
	private static readonly int port = 5000; // Port for listening to Wavy connections
	private static readonly Dictionary<string, DateTime> connectedWavys = new Dictionary<string, DateTime>();
	private static bool verboseMode = true; // Toggle for detailed logging

	public static void Main(string[] args)
	{
		Console.WriteLine("Test server starting in debug mode...");
		Console.WriteLine("Press 'V' to toggle verbose mode");
		Console.WriteLine("Press 'C' to show currently connected Wavy clients");
		Console.WriteLine("Press 'Q' to quit");

		// Start keyboard monitoring thread
		Thread keyboardThread = new Thread(MonitorKeyboard);
		keyboardThread.IsBackground = true;
		keyboardThread.Start();

		TcpListener listener = new TcpListener(IPAddress.Any, port);
		listener.Start();
		Console.WriteLine($"Test server listening on port {port}...");

		while (true)
		{
			try
			{
				TcpClient client = listener.AcceptTcpClient();
				Thread clientThread = new Thread(() => HandleClient(client));
				clientThread.IsBackground = true;
				clientThread.Start();
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error accepting client: {ex.Message}");
			}
		}
	}

	private static void MonitorKeyboard()
	{
		while (true)
		{
			if (Console.KeyAvailable)
			{
				var key = Console.ReadKey(true).Key;
				if (key == ConsoleKey.V)
				{
					verboseMode = !verboseMode;
					Console.WriteLine($"Verbose mode: {(verboseMode ? "ON" : "OFF")}");
				}
				else if (key == ConsoleKey.C)
				{
					ShowConnectedClients();
				}
				else if (key == ConsoleKey.Q)
				{
					Console.WriteLine("Shutting down server...");
					Environment.Exit(0);
				}
			}
			Thread.Sleep(100);
		}
	}

	private static void ShowConnectedClients()
	{
		Console.WriteLine("\n=== Connected Wavy Clients ===");
		if (connectedWavys.Count == 0)
		{
			Console.WriteLine("No clients connected.");
		}
		else
		{
			foreach (var kvp in connectedWavys)
			{
				TimeSpan connectedFor = DateTime.Now - kvp.Value;
				Console.WriteLine($"Wavy ID: {kvp.Key}, Connected for: {connectedFor.Minutes}m {connectedFor.Seconds}s");
			}
		}
		Console.WriteLine("============================\n");
	}

	private static void HandleClient(TcpClient client)
	{
		string clientIp = ((IPEndPoint)client.Client.RemoteEndPoint).Address.ToString();
		Console.WriteLine($"New connection from {clientIp}");

		try
		{
			using (client)
			using (NetworkStream stream = client.GetStream())
			{
				while (client.Connected)
				{
					// If no data available, wait a bit and try again
					if (!stream.DataAvailable)
					{
						Thread.Sleep(100);
						continue;
					}

					byte[] buffer = new byte[4096]; // Larger buffer for larger payloads
					int bytesRead = stream.Read(buffer, 0, buffer.Length);

					if (bytesRead <= 0)
						break;

					// First 4 bytes are the message code
					int messageCode = BitConverter.ToInt32(buffer, 0);

					Console.WriteLine($"\n[{DateTime.Now:HH:mm:ss.fff}] Received message with code: {messageCode}");

					switch (messageCode)
					{
						case 101: // Connection
							ProcessConnection(buffer, bytesRead, stream);
							break;
						case 201: // Initial data send
						case 202: // Data resend
							ProcessData(buffer, bytesRead, stream, messageCode);
							break;
						case 501: // Disconnection
							ProcessDisconnection(buffer, bytesRead, stream);
							break;
						default:
							if (verboseMode)
							{
								Console.WriteLine($"Unknown message code: {messageCode}");
								string receivedData = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);
								Console.WriteLine($"Payload: {receivedData}");
							}
							break;
					}

					// Always send confirmation regardless of what was received
					SendConfirmation(stream, 401);
				}
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error handling client {clientIp}: {ex.Message}");
		}

		Console.WriteLine($"Connection closed from {clientIp}");
	}

	private static void ProcessConnection(byte[] buffer, int bytesRead, NetworkStream stream)
	{
		string wavyId = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);
		connectedWavys[wavyId] = DateTime.Now;
		Console.WriteLine($"Wavy connection request from ID: {wavyId}");
	}

	private static void ProcessDisconnection(byte[] buffer, int bytesRead, NetworkStream stream)
	{
		string wavyId = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);
		if (connectedWavys.ContainsKey(wavyId))
		{
			connectedWavys.Remove(wavyId);
		}
		Console.WriteLine($"Wavy disconnection request from ID: {wavyId}");
	}

	private static void ProcessData(byte[] buffer, int bytesRead, NetworkStream stream, int messageCode)
	{
		string jsonData = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);

		try
		{
			// Parse the JSON to extract structured data
			using (JsonDocument doc = JsonDocument.Parse(jsonData))
			{
				JsonElement root = doc.RootElement;
				string wavyId = root.GetProperty("WavyId").GetString();

				Console.WriteLine($"[{messageCode}] Data received from Wavy ID: {wavyId}");

				if (verboseMode)
				{
					Console.WriteLine("Sensor Data:");
					JsonElement dataArray = root.GetProperty("Data");
					foreach (JsonElement item in dataArray.EnumerateArray())
					{
						string dataType = item.GetProperty("DataType").GetString();
						long value = item.GetProperty("Value").GetInt64();
						Console.WriteLine($"  - {dataType}: {value}");
					}
				}
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error parsing JSON data: {ex.Message}");
			if (verboseMode)
			{
				Console.WriteLine($"Raw data: {jsonData}");
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
		if (verboseMode)
		{
			Console.WriteLine($"Sent confirmation code: {code}");
		}
	}
}
