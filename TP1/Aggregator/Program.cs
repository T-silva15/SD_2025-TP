using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;

/// <summary>
/// Aggregator class responsible for collecting data from Wavy clients,
/// aggregating it by data types, and periodically sending the combined data to a server.
/// <para>Implements communication protocols with specific codes for connection, data transfer, and confirmation.</para>
/// </summary>
class Aggregator
{
	// Dictionary to store data received from Wavy clients, organized by data type
	private static readonly ConcurrentDictionary<string, ConcurrentBag<string>> dataStore = new ConcurrentDictionary<string, ConcurrentBag<string>>();

	// Network configuration
	private static readonly int port = 5000;                 // Port for listening to Wavy connections
	private static readonly string serverIp = "127.0.0.1";   // IP address of the server
	private static readonly int serverPort = 6000;           // Port for server connections

	// Unique identifier for this aggregator instance
	private static readonly string aggregatorId = Guid.NewGuid().ToString();

	/// <summary>
	/// Entry point for the Aggregator application.
	/// <para>Starts two threads: one for listening to incoming Wavy connections and another for periodically sending aggregated data to the server.</para>
	/// </summary>
	/// <param name="args">Command line arguments (not used)</param>
	public static void Main(string[] args)
	{
		Thread listenerThread = new Thread(StartListener);
		Thread senderThread = new Thread(SendDataToServer);

		listenerThread.Start();
		senderThread.Start();
	}

	/// <summary>
	/// Listens for incoming connections from Wavy clients.
	/// <para>Processes different message codes:</para>
	/// <para>101: Wavy connection</para>
	/// <para>201: Data received from Wavy</para>
	/// <para>501: Wavy disconnection</para>
	/// </summary>
	private static void StartListener()
	{
		TcpListener listener = new TcpListener(IPAddress.Any, port);
		listener.Start();
		Console.WriteLine("Aggregator listening...");

		while (true)
		{
			using (TcpClient client = listener.AcceptTcpClient())
			using (NetworkStream stream = client.GetStream())
			{
				// Read message from client
				byte[] buffer = new byte[1024];
				int bytesRead = stream.Read(buffer, 0, buffer.Length);
				int code = BitConverter.ToInt32(buffer, 0);

				switch (code)
				{
					case 101:
						// Handle Wavy connection
						Console.WriteLine("101 - Wavy connected");
						break;

					case 201:
						// Handle data from Wavy
						string jsonData = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);
						var data = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonData);
						if (data != null && data.TryGetValue("DataType", out var dataTypeObj) && dataTypeObj is string dataType)
						{
							// Create storage for this data type if it doesn't exist
							if (!dataStore.ContainsKey(dataType))
							{
								dataStore[dataType] = new ConcurrentBag<string>();
							}

							// Store the received data
							dataStore[dataType].Add(jsonData);
							Console.WriteLine("401 - Data received.");

							// Send confirmation back to Wavy
							SendConfirmation(stream, 401);
						}
						break;

					case 501:
						// Handle Wavy disconnection
						Console.WriteLine("501 - Wavy disconnected");
						break;
				}
			}
		}
	}

	/// <summary>
	/// Periodically sends aggregated data to the server.
	/// <para>Runs every hour</para>
	/// <para>Aggregates data by data type</para>
	/// <para>Sends data using code 301</para>
	/// <para>Waits for confirmation (code 402)</para>
	/// <para>Resends if confirmation not received</para>
	/// </summary>
	private static void SendDataToServer()
	{
		while (true)
		{
			// Wait for 1 hour between data transmissions
			Thread.Sleep(3600000);

			// Copy current data for sending and clear original store to separate new incoming data
			var dataToSend = new ConcurrentDictionary<string, ConcurrentBag<string>>(dataStore);
			dataStore.Clear();

			var aggregatedDataList = new List<object>();

			// Aggregate data for each data type
			foreach (var dataType in dataToSend.Keys)
			{
				var dataList = dataToSend[dataType];
				var aggregatedData = new
				{
					DataType = dataType,
					// Sum up all values for this data type
					TotalValue = dataList.Sum(data =>
					{
						var deserializedData = JsonSerializer.Deserialize<Dictionary<string, object>>(data);
						return deserializedData != null && deserializedData.TryGetValue("Value", out var valueObj) && valueObj is long value ? value : 0;
					}),
					// Collect unique Wavy IDs that contributed to this data type
					WavyIds = dataList.Select(data =>
					{
						var deserializedData = JsonSerializer.Deserialize<Dictionary<string, object>>(data);
						return deserializedData != null && deserializedData.TryGetValue("WavyId", out var wavyIdObj) && wavyIdObj is string wavyId ? wavyId : null;
					}).Where(wavyId => wavyId != null).Distinct().ToList()
				};

				aggregatedDataList.Add(aggregatedData);
			}

			// Create combined data message with aggregator ID and timestamp
			var combinedData = new
			{
				AggregatorId = aggregatorId,
				Timestamp = DateTime.UtcNow,
				Data = aggregatedDataList
			};

			string jsonData = JsonSerializer.Serialize(combinedData);

			// Send data with retry mechanism until confirmation is received
			bool confirmed = false;
			while (!confirmed)
			{
				try
				{
					using (TcpClient client = new TcpClient(serverIp, serverPort))
					using (NetworkStream stream = client.GetStream())
					{
						// Send aggregated data to server
						SendMessage(stream, jsonData, 301);
						Console.WriteLine($"301 - Data sent to server: {jsonData}");

						// Wait for confirmation from server
						confirmed = WaitForConfirmation(stream, 402);
						if (!confirmed)
						{
							Console.WriteLine("No confirmation received from server, resending data...");
							Thread.Sleep(180000); // Wait 3 minutes before resending
						}
					}
				}
				catch (Exception ex)
				{
					Console.WriteLine($"Error sending data to server: {ex.Message}");
				}
			}
		}
	}

	/// <summary>
	/// Sends a message with the specified code to the provided network stream.
	/// <para>The message format is: [4-byte code][message content]</para>
	/// </summary>
	/// <param name="stream">The network stream to send the message to</param>
	/// <param name="message">The message content to send</param>
	/// <param name="code">The protocol code indicating the message type</param>
	private static void SendMessage(NetworkStream stream, string message, int code)
	{
		// Convert code to bytes
		byte[] codeBytes = BitConverter.GetBytes(code);

		// Convert message to bytes
		byte[] messageBytes = Encoding.UTF8.GetBytes(message);

		// Combine code and message into a single byte array
		byte[] data = new byte[codeBytes.Length + messageBytes.Length];
		Buffer.BlockCopy(codeBytes, 0, data, 0, codeBytes.Length);
		Buffer.BlockCopy(messageBytes, 0, data, codeBytes.Length, messageBytes.Length);

		// Send the combined data
		stream.Write(data, 0, data.Length);
	}

	/// <summary>
	/// Sends a confirmation message with the specified code.
	/// <para>Used to acknowledge the receipt of data from Wavy clients.</para>
	/// </summary>
	/// <param name="stream">The network stream to send the confirmation to</param>
	/// <param name="code">The confirmation code (e.g., 401 for Wavy data confirmation)</param>
	private static void SendConfirmation(NetworkStream stream, int code)
	{
		// For control messages, only the code is sent
		byte[] codeBytes = BitConverter.GetBytes(code);
		stream.Write(codeBytes, 0, codeBytes.Length);
	}

	/// <summary>
	/// Waits for a confirmation message with the expected code.
	/// </summary>
	/// <param name="stream">The network stream to read the confirmation from</param>
	/// <param name="expectedCode">The expected confirmation code</param>
	/// <returns>True if the received code matches the expected code, otherwise false</returns>
	private static bool WaitForConfirmation(NetworkStream stream, int expectedCode)
	{
		// Read exactly 4 bytes for the code
		byte[] buffer = new byte[4];
		int bytesRead = stream.Read(buffer, 0, buffer.Length);
		int code = BitConverter.ToInt32(buffer, 0);

		// Return whether the received code matches the expected code
		return code == expectedCode;
	}
}
