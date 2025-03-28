using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
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
	#region Protocol Constants

	/// <summary>
	/// Message codes used in the communication protocol between Wavy, Aggregator, and Server
	/// </summary>
	private static class MessageCodes
	{
		// Existing codes...
		public const int WavyConnect = 101;
		public const int AggregatorConnect = 102; // Add this for Aggregator-Server connection
		public const int WavyDataHttpInitial = 200;
		public const int WavyDataInitial = 201;
		public const int WavyDataResend = 202;
		public const int AggregatorData = 301;
		public const int AggregatorDataResend = 302; // Add this for data resend to server
		public const int WavyConfirmation = 401;
		public const int ServerConfirmation = 402;
		public const int WavyDisconnect = 501;
		public const int AggregatorDisconnect = 502; // Add this for Aggregator-Server disconnection
	}


	#endregion

	#region Configuration

	/// <summary>
	/// Configuration settings for the Aggregator
	/// </summary>
	private static class Config
	{
		// Network settings
		public static int ListeningPort { get; private set; } = 5000;
		public static string ServerIp { get; private set; } = "127.0.0.1";
		public static int ServerPort { get; private set; } = 6000;

		// Timeouts and intervals
		public static int ConfirmationTimeoutMs { get; private set; } = 10000; // 10 seconds
		public static int DataTransmissionIntervalSec { get; private set; } = 10; // 10 seconds (TEMP VALUE)
		public static int RetryIntervalSec { get; private set; } = 3; // 3 seconds (TEMP VALUE)
		public static int MaxRetryAttempts { get; private set; } = 5;
		public static int ClientPollIntervalMs { get; private set; } = 100;

		// Buffer sizes
		public static int ReceiveBufferSize { get; private set; } = 4096;

		// Debug settings
		public static bool DefaultVerboseMode { get; private set; } = true;

		// File path for configuration
		private static readonly string ConfigFilePath = "aggregator.config.json";

		static Config()
		{
			// Try to load configuration from file
			try
			{
				if (File.Exists(ConfigFilePath))
				{
					string json = File.ReadAllText(ConfigFilePath);
					var config = JsonSerializer.Deserialize<Dictionary<string, object>>(json);

					// Apply configuration values if they exist
					if (config != null)
					{
						if (config.TryGetValue("ListeningPort", out var port) && port is JsonElement portElement && portElement.ValueKind == JsonValueKind.Number)
							ListeningPort = portElement.GetInt32();

						if (config.TryGetValue("ServerIp", out var ip) && ip is JsonElement ipElement && ipElement.ValueKind == JsonValueKind.String)
							ServerIp = ipElement.GetString();

						if (config.TryGetValue("ServerPort", out var serverPort) && serverPort is JsonElement serverPortElement && serverPortElement.ValueKind == JsonValueKind.Number)
							ServerPort = serverPortElement.GetInt32();

						if (config.TryGetValue("ConfirmationTimeoutMs", out var timeout) && timeout is JsonElement timeoutElement && timeoutElement.ValueKind == JsonValueKind.Number)
							ConfirmationTimeoutMs = timeoutElement.GetInt32();

						if (config.TryGetValue("DataTransmissionIntervalSec", out var interval) && interval is JsonElement intervalElement && intervalElement.ValueKind == JsonValueKind.Number)
							DataTransmissionIntervalSec = intervalElement.GetInt32();

						if (config.TryGetValue("RetryIntervalSec", out var retry) && retry is JsonElement retryElement && retryElement.ValueKind == JsonValueKind.Number)
							RetryIntervalSec = retryElement.GetInt32();

						if (config.TryGetValue("MaxRetryAttempts", out var maxRetry) && maxRetry is JsonElement maxRetryElement && maxRetryElement.ValueKind == JsonValueKind.Number)
							MaxRetryAttempts = maxRetryElement.GetInt32();

						if (config.TryGetValue("DefaultVerboseMode", out var verbose) && verbose is JsonElement verboseElement && verboseElement.ValueKind == JsonValueKind.True)
							DefaultVerboseMode = true;
						else if (config.TryGetValue("DefaultVerboseMode", out verbose) && verbose is JsonElement verboseElement2 && verboseElement2.ValueKind == JsonValueKind.False)
							DefaultVerboseMode = false;
					}

					Console.WriteLine("Configuration loaded from file.");
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error loading configuration: {ex.Message}");
				Console.WriteLine("Using default configuration values.");
			}
		}

		/// <summary>
		/// Saves the current configuration to a file
		/// </summary>
		public static void SaveToFile()
		{
			try
			{
				var config = new Dictionary<string, object>
			{
				{ "ListeningPort", ListeningPort },
				{ "ServerIp", ServerIp },
				{ "ServerPort", ServerPort },
				{ "ConfirmationTimeoutMs", ConfirmationTimeoutMs },
				{ "DataTransmissionIntervalSec", DataTransmissionIntervalSec },
				{ "RetryIntervalSec", RetryIntervalSec },
				{ "MaxRetryAttempts", MaxRetryAttempts },
				{ "DefaultVerboseMode", DefaultVerboseMode }
			};

				string json = JsonSerializer.Serialize(config, new JsonSerializerOptions { WriteIndented = true });
				File.WriteAllText(ConfigFilePath, json);
				Console.WriteLine("Configuration saved to file.");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error saving configuration: {ex.Message}");
			}
		}
	}


	#endregion

	#region State Management

	// Dictionary to store data received from Wavy clients, organized by data type
	private static readonly ConcurrentDictionary<string, ConcurrentBag<string>> dataStore = new ConcurrentDictionary<string, ConcurrentBag<string>>();

	// Unique identifier for this aggregator instance
	private static readonly string aggregatorId = Guid.NewGuid().ToString();

	// Track active client threads
	private static readonly ConcurrentDictionary<Thread, DateTime> activeThreads = new ConcurrentDictionary<Thread, DateTime>();

	// Debugging features
	private static readonly Dictionary<string, DateTime> connectedWavys = new Dictionary<string, DateTime>();
	private static bool verboseMode = Config.DefaultVerboseMode;
	private static bool isRunning = true;

	#endregion

	#region Entry Point and Main Thread Management

	/// <summary>
	/// Entry point for the Aggregator application.
	/// <para>Starts threads for listening to Wavy connections, sending data to server, and monitoring keyboard input.</para>
	/// </summary>
	/// <param name="args">Command line arguments (not used)</param>
	public static void Main(string[] args)
	{
		AppDomain.CurrentDomain.ProcessExit += OnProcessExit;

		Console.WriteLine($"Aggregator {aggregatorId} starting in debug mode...");
		Console.WriteLine($"Listening on port {Config.ListeningPort}, sending to {Config.ServerIp}:{Config.ServerPort}");
		Console.WriteLine("\nAvailable Commands:");
		Console.WriteLine("Press 'V' to toggle verbose mode");
		Console.WriteLine("Press 'C' to show currently connected Wavy clients");
		Console.WriteLine("Press 'D' to show current data store");
		Console.WriteLine("Press 'T' to show active threads");
		Console.WriteLine("Press 'S' to save current configuration");
		Console.WriteLine("Press 'Q' to quit");

		// Start keyboard monitoring thread
		Thread keyboardThread = new Thread(MonitorKeyboard) { IsBackground = true, Name = "KeyboardMonitor" };
		RegisterThread(keyboardThread);
		keyboardThread.Start();

		// Start listener thread for Wavy connections
		Thread listenerThread = new Thread(StartListener) { Name = "WavyListener" };
		RegisterThread(listenerThread);
		listenerThread.Start();

		// Start data sender thread for server communication
		Thread senderThread = new Thread(SendDataToServer) { Name = "ServerSender" };
		RegisterThread(senderThread);
		senderThread.Start();

		// Keep main thread alive until program is stopped
		while (isRunning)
		{
			Thread.Sleep(1000);
		}

		ShutdownThreads();
	}



	private static void OnProcessExit(object sender, EventArgs e)
	{
		isRunning = false;
		Console.WriteLine("Process terminating, shutting down threads...");
		ShutdownThreads();
	}

	private static void ShutdownThreads()
	{
		isRunning = false;
		Console.WriteLine("Shutting down threads...");

		// Wait for threads to terminate
		foreach (var threadEntry in activeThreads)
		{
			Thread thread = threadEntry.Key;
			if (thread != Thread.CurrentThread && thread.IsAlive)
			{
				Console.WriteLine($"Waiting for thread '{thread.Name}' to terminate...");
				if (!thread.Join(2000))
				{
					Console.WriteLine($"Thread '{thread.Name}' did not terminate gracefully.");
				}
			}
		}

		// Send disconnect notification to the server
		try
		{
			using (TcpClient client = new TcpClient(Config.ServerIp, Config.ServerPort))
			using (NetworkStream stream = client.GetStream())
			{
				SendServerDisconnection(stream);
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error during shutdown: {ex.Message}");
		}

		Console.WriteLine("Shutdown complete.");
	}


	private static void RegisterThread(Thread thread)
	{
		activeThreads.TryAdd(thread, DateTime.Now);
	}

	private static void UnregisterThread(Thread thread)
	{
		activeThreads.TryRemove(thread, out _);
	}

	#endregion

	#region Debug Interface

	/// <summary>
	/// Monitors keyboard input for debugging commands.
	/// </summary>
	private static void MonitorKeyboard()
	{
		try
		{
			while (isRunning)
			{
				if (Console.KeyAvailable)
				{
					var key = Console.ReadKey(true).Key;
					switch (key)
					{
						case ConsoleKey.V:
							verboseMode = !verboseMode;
							Console.WriteLine($"Verbose mode: {(verboseMode ? "ON" : "OFF")}");
							break;
						case ConsoleKey.C:
							ShowConnectedClients();
							break;
						case ConsoleKey.D:
							ShowDataStore();
							break;
						case ConsoleKey.T:
							ShowActiveThreads();
							break;
						case ConsoleKey.S:
							Config.SaveToFile();
							break;
						case ConsoleKey.Q:
							Console.WriteLine("Shutting down aggregator...");
							isRunning = false;
							Thread.Sleep(500); // Give time for message to display
							Environment.Exit(0);
							break;
					}
				}
				Thread.Sleep(Config.ClientPollIntervalMs);
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error in keyboard monitoring: {ex.Message}");
		}
		finally
		{
			UnregisterThread(Thread.CurrentThread);
		}
	}

	/// <summary>
	/// Displays information about currently connected Wavy clients.
	/// </summary>
	private static void ShowConnectedClients()
	{
		Console.WriteLine("\n=== Connected Wavy Clients ===");
		lock (connectedWavys)
		{
			if (connectedWavys.Count == 0)
			{
				Console.WriteLine("No clients connected.");
			}
			else
			{
				foreach (var kvp in connectedWavys)
				{
					TimeSpan connectedFor = DateTime.Now - kvp.Value;
					Console.WriteLine($"Wavy ID: {kvp.Key}");
					Console.WriteLine($"  Connected since: {kvp.Value:HH:mm:ss}");
					Console.WriteLine($"  Connected for: {connectedFor.Hours}h {connectedFor.Minutes}m {connectedFor.Seconds}s");
				}
			}
		}
		Console.WriteLine("============================\n");
	}

	/// <summary>
	/// Displays the current contents of the data store.
	/// </summary>
	private static void ShowDataStore()
	{
		Console.WriteLine("\n=== Current Data Store ===");
		if (dataStore.IsEmpty)
		{
			Console.WriteLine("Data store is empty.");
		}
		else
		{
			foreach (var dataType in dataStore.Keys)
			{
				var dataList = dataStore[dataType];
				Console.WriteLine($"Data Type: {dataType}, Records: {dataList.Count}");

				if (verboseMode && dataList.Count > 0)
				{
					int i = 0;
					foreach (var dataItem in dataList.Take(5)) // Show at most 5 items per type to avoid flooding
					{
						Console.WriteLine($"  - Item {++i}: {dataItem}");
					}
					if (dataList.Count > 5)
					{
						Console.WriteLine($"  ... and {dataList.Count - 5} more items");
					}
				}
			}
		}
		Console.WriteLine("=======================\n");
	}

	/// <summary>
	/// Displays information about active threads.
	/// </summary>
	private static void ShowActiveThreads()
	{
		Console.WriteLine("\n=== Active Threads ===");
		if (activeThreads.IsEmpty)
		{
			Console.WriteLine("No active threads.");
		}
		else
		{
			foreach (var threadEntry in activeThreads)
			{
				Thread thread = threadEntry.Key;
				DateTime startTime = threadEntry.Value;
				TimeSpan runningFor = DateTime.Now - startTime;

				Console.WriteLine($"Thread: {thread.Name ?? "Unnamed"}");
				Console.WriteLine($"  ID: {thread.ManagedThreadId}, State: {thread.ThreadState}");
				Console.WriteLine($"  Running for: {runningFor.Hours}h {runningFor.Minutes}m {runningFor.Seconds}s");
			}
		}
		Console.WriteLine("===================\n");
	}

	#endregion

	#region Network Listener and Client Handling

	/// <summary>
	/// Listens for incoming connections from Wavy clients.
	/// <para>Processes different message codes and maintains client connections.</para>
	/// </summary>
	private static void StartListener()
	{
		try
		{
			TcpListener listener = new TcpListener(IPAddress.Any, Config.ListeningPort);
			listener.Start();
			Console.WriteLine($"Aggregator listening on port {Config.ListeningPort}...");

			while (isRunning)
			{
				try
				{
					if (listener.Pending())
					{
						TcpClient client = listener.AcceptTcpClient();
						Thread clientThread = new Thread(() => HandleClient(client))
						{
							IsBackground = true,
							Name = $"Client-{DateTime.Now.Ticks}"
						};

						RegisterThread(clientThread);
						clientThread.Start();
					}
					else
					{
						Thread.Sleep(Config.ClientPollIntervalMs);
					}
				}
				catch (Exception ex)
				{
					Console.WriteLine($"Error accepting client: {ex.Message}");
					Thread.Sleep(1000); // Delay before retry
				}
			}

			listener.Stop();
			Console.WriteLine("Listener stopped.");
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Fatal error in listener: {ex.Message}");
		}
		finally
		{
			UnregisterThread(Thread.CurrentThread);
		}
	}

	/// <summary>
	/// Handles an individual client connection.
	/// </summary>
	/// <param name="client">The TCP client to handle</param>
	private static void HandleClient(TcpClient client)
	{
		string clientIp = "Unknown";

		try
		{
			clientIp = ((IPEndPoint)client.Client.RemoteEndPoint).Address.ToString();
			Console.WriteLine($"New connection from {clientIp}");

			using (client)
			using (NetworkStream stream = client.GetStream())
			{
				while (client.Connected && isRunning)
				{
					// If no data available, wait a bit and try again
					if (!stream.DataAvailable)
					{
						Thread.Sleep(Config.ClientPollIntervalMs);
						continue;
					}

					byte[] buffer = new byte[Config.ReceiveBufferSize];
					int bytesRead = stream.Read(buffer, 0, buffer.Length);

					if (bytesRead <= 0)
						break;

					// First 4 bytes are the message code
					int messageCode = BitConverter.ToInt32(buffer, 0);

					Console.WriteLine($"\n[{DateTime.Now:HH:mm:ss.fff}] Received message with code: {messageCode}");

					switch (messageCode)
					{
						case MessageCodes.WavyConnect:
							ProcessConnection(buffer, bytesRead, stream);
							break;
						case MessageCodes.WavyDataHttpInitial:
						case MessageCodes.WavyDataInitial:
						case MessageCodes.WavyDataResend:
							ProcessData(buffer, bytesRead, stream, messageCode);
							break;
						case MessageCodes.AggregatorData:
							ProcessAggregatedData(buffer, bytesRead, stream);
							break;
						case MessageCodes.WavyDisconnect:
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
					SendConfirmation(stream, MessageCodes.WavyConfirmation);
				}
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error handling client {clientIp}: {ex.Message}");
		}
		finally
		{
			Console.WriteLine($"Connection closed from {clientIp}");
			UnregisterThread(Thread.CurrentThread);
		}
	}

	#endregion

	#region Message Processing

	/// <summary>
	/// Processes a connection request from a Wavy client.
	/// </summary>
	private static void ProcessConnection(byte[] buffer, int bytesRead, NetworkStream stream)
	{
		string wavyId = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);
		lock (connectedWavys)
		{
			connectedWavys[wavyId] = DateTime.Now;
		}
		Console.WriteLine($"Wavy connection request from ID: {wavyId}");
	}

	/// <summary>
	/// Processes a disconnection request from a Wavy client.
	/// </summary>
	private static void ProcessDisconnection(byte[] buffer, int bytesRead, NetworkStream stream)
	{
		string wavyId = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);
		lock (connectedWavys)
		{
			if (connectedWavys.ContainsKey(wavyId))
			{
				connectedWavys.Remove(wavyId);
			}
		}
		Console.WriteLine($"Wavy disconnection request from ID: {wavyId}");
	}

	/// <summary>
	/// Processes data sent from a Wavy client.
	/// </summary>
	private static void ProcessData(byte[] buffer, int bytesRead, NetworkStream stream, int messageCode)
	{
		string jsonData = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);

		try
		{
			// Parse the JSON to extract structured data
			using (JsonDocument doc = JsonDocument.Parse(jsonData))
			{
				JsonElement root = doc.RootElement;

				if (!root.TryGetProperty("WavyId", out JsonElement wavyIdElement) ||
					!root.TryGetProperty("Data", out JsonElement dataArray) ||
					dataArray.ValueKind != JsonValueKind.Array)
				{
					Console.WriteLine("Received malformed data packet - missing required properties");
					if (verboseMode)
					{
						Console.WriteLine($"Raw data: {jsonData}");
					}
					return;
				}

				string wavyId = wavyIdElement.GetString();
				Console.WriteLine($"[{messageCode}] Data received from Wavy ID: {wavyId}");

				if (verboseMode)
				{
					Console.WriteLine("Sensor Data:");
				}

				foreach (JsonElement item in dataArray.EnumerateArray())
				{
					ProcessAndStoreDataItem(item, wavyId);
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
	/// Processes aggregated data sent from Wavy (HTTP-like mode).
	/// </summary>
	private static void ProcessAggregatedData(byte[] buffer, int bytesRead, NetworkStream stream)
	{
		string jsonData = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);

		try
		{
			// Parse the JSON to extract structured data
			using (JsonDocument doc = JsonDocument.Parse(jsonData))
			{
				JsonElement root = doc.RootElement;

				string wavyId = null;
				if (root.TryGetProperty("WavyId", out JsonElement wavyIdElement))
				{
					wavyId = wavyIdElement.GetString();
					Console.WriteLine($"[{MessageCodes.AggregatorData}] Aggregated data received from Wavy ID: {wavyId}");
				}
				else
				{
					Console.WriteLine($"[{MessageCodes.AggregatorData}] Aggregated data received (no Wavy ID found)");
				}

				if (root.TryGetProperty("Data", out JsonElement dataArray) && dataArray.ValueKind == JsonValueKind.Array)
				{
					if (verboseMode)
					{
						Console.WriteLine("Aggregated Data:");
					}

					foreach (JsonElement item in dataArray.EnumerateArray())
					{
						ProcessAndStoreDataItem(item, wavyId);
					}
				}
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error parsing aggregated JSON data: {ex.Message}");
			if (verboseMode)
			{
				Console.WriteLine($"Raw data: {jsonData}");
			}
		}
	}

	/// <summary>
	/// Processes and stores a single data item
	/// </summary>
	private static void ProcessAndStoreDataItem(JsonElement item, string wavyId = null)
	{
		try
		{
			if (!item.TryGetProperty("DataType", out JsonElement dataTypeElement))
				return;

			string dataType = dataTypeElement.GetString();

			// Store the data in the data store
			if (!dataStore.ContainsKey(dataType))
			{
				dataStore[dataType] = new ConcurrentBag<string>();
			}

			// Add the data item to the store
			dataStore[dataType].Add(item.GetRawText());

			// Output details in verbose mode
			if (verboseMode && item.TryGetProperty("Value", out JsonElement valueElement))
			{
				long value = valueElement.GetInt64();
				Console.WriteLine($"  - {dataType}: {value}");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error processing data item: {ex.Message}");
		}
	}

	#endregion

	#region Data Transmission to Server

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
		try
		{
			while (isRunning)
			{
				Console.WriteLine($"Waiting for {Config.DataTransmissionIntervalSec} seconds before sending data...");

				// Wait for configured interval between data transmissions
				for (int i = 0; i < Config.DataTransmissionIntervalSec && isRunning; i++)
				{
					Thread.Sleep(1000); // Check isRunning flag every second
				}

				if (!isRunning) break;

				Console.WriteLine($"\n[{DateTime.Now:HH:mm:ss.fff}] Preparing to send aggregated data to server");

				// Copy current data for sending and clear original store to separate new incoming data
				var dataToSend = new ConcurrentDictionary<string, ConcurrentBag<string>>(dataStore);
				dataStore.Clear();

				if (dataToSend.IsEmpty)
				{
					Console.WriteLine("No data to send to server.");
					continue;
				}

				// Establish connection, send data, and disconnect
				SendAggregatedData(dataToSend);
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Fatal error in data sender: {ex.Message}");
		}
		finally
		{
			UnregisterThread(Thread.CurrentThread);
		}
	}

	private static void SendAggregatedData(ConcurrentDictionary<string, ConcurrentBag<string>> dataToSend)
	{
		// Existing aggregation code...
		var aggregatedDataList = new List<object>();

		// Aggregate data for each data type
		foreach (var dataType in dataToSend.Keys)
		{
			// Perform aggregation logic here
			var aggregatedData = dataToSend[dataType].ToList();
			aggregatedDataList.Add(new { DataType = dataType, Data = aggregatedData });
		}

		if (aggregatedDataList.Count == 0)
		{
			Console.WriteLine("No data could be aggregated.");
			return;
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
		int retryCount = 0;

		try
		{
			// Create a TCP client and connect to the server
			using (TcpClient client = new TcpClient(Config.ServerIp, Config.ServerPort))
			using (NetworkStream stream = client.GetStream())
			{
				// Send connection request first (code 102)
				EstablishServerConnection(stream);

				// Send data and handle retries
				while (!confirmed && isRunning && retryCount < Config.MaxRetryAttempts)
				{
					try
					{
						// Choose the correct code based on retry count
						int messageCode = (retryCount == 0) ?
							MessageCodes.AggregatorData :
							MessageCodes.AggregatorDataResend;

						// Send aggregated data to server
						SendMessage(stream, jsonData, messageCode);
						Console.WriteLine($"[{messageCode}] Data sent to server:");
						if (verboseMode)
						{
							Console.WriteLine(jsonData);
						}

						// Wait for confirmation from server
						confirmed = WaitForConfirmation(stream, MessageCodes.ServerConfirmation);
						if (!confirmed)
						{
							retryCount++;
							Console.WriteLine($"No confirmation received from server, retry {retryCount}/{Config.MaxRetryAttempts}...");

							if (retryCount < Config.MaxRetryAttempts)
							{
								for (int i = 0; i < Config.RetryIntervalSec && isRunning; i++) // Wait before resending
								{
									Thread.Sleep(1000);
								}
							}
						}
						else
						{
							Console.WriteLine($"Server confirmed data receipt (code {MessageCodes.ServerConfirmation})");
						}
					}
					catch (Exception ex)
					{
						retryCount++;
						Console.WriteLine($"Error sending data to server: {ex.Message}");

						if (retryCount < Config.MaxRetryAttempts)
						{
							Console.WriteLine($"Retry {retryCount}/{Config.MaxRetryAttempts} in {Config.RetryIntervalSec} seconds...");
							for (int i = 0; i < Config.RetryIntervalSec && isRunning; i++) // Wait before resending
							{
								Thread.Sleep(1000);
							}
						}
					}
				}

				// Send disconnection notification (code 502) when done
				SendServerDisconnection(stream);
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error connecting to server: {ex.Message}");
		}

		if (!confirmed && retryCount >= Config.MaxRetryAttempts)
		{
			Console.WriteLine($"Failed to send data to server after {Config.MaxRetryAttempts} attempts. Data discarded.");
		}
	}

	#endregion

	#region Network Utilities

	/// <summary>
	/// Sends a message with the specified code to the provided network stream.
	/// <para>The message format is: [4-byte code][message content]</para>
	/// </summary>
	/// <param name="stream">The network stream to send the message to</param>
	/// <param name="message">The message content to send</param>
	/// <param name="code">The protocol code indicating the message type</param>
	private static void SendMessage(NetworkStream stream, string message, int code)
	{
		try
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

			if (verboseMode)
			{
				Console.WriteLine($"Sent message with code: {code}");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error sending message: {ex.Message}");
			throw; // Rethrow to let caller handle
		}
	}

	/// <summary>
	/// Sends a confirmation message with the specified code.
	/// <para>Used to acknowledge the receipt of data from Wavy clients.</para>
	/// </summary>
	/// <param name="stream">The network stream to send the confirmation to</param>
	/// <param name="code">The confirmation code (e.g., 401 for Wavy data confirmation)</param>
	private static void SendConfirmation(NetworkStream stream, int code)
	{
		try
		{
			// For control messages, only the code is sent
			byte[] codeBytes = BitConverter.GetBytes(code);
			stream.Write(codeBytes, 0, codeBytes.Length);

			if (verboseMode)
			{
				Console.WriteLine($"Sent confirmation code: {code}");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error sending confirmation: {ex.Message}");
		}
	}

	/// <summary>
	/// Waits for a confirmation message with the expected code.
	/// </summary>
	/// <param name="stream">The network stream to read the confirmation from</param>
	/// <param name="expectedCode">The expected confirmation code</param>
	/// <returns>True if the received code matches the expected code, otherwise false</returns>
	private static bool WaitForConfirmation(NetworkStream stream, int expectedCode)
	{
		try
		{
			// Set a read timeout to prevent hanging indefinitely
			stream.ReadTimeout = Config.ConfirmationTimeoutMs;

			// Read exactly 4 bytes for the code
			byte[] buffer = new byte[4];
			int bytesRead = stream.Read(buffer, 0, buffer.Length);

			if (bytesRead == 4)
			{
				int code = BitConverter.ToInt32(buffer, 0);

				if (verboseMode)
				{
					Console.WriteLine($"Received confirmation code: {code}, expected: {expectedCode}");
				}

				// Return whether the received code matches the expected code
				return code == expectedCode;
			}

			if (verboseMode)
			{
				Console.WriteLine($"Received incomplete confirmation: {bytesRead} bytes, expected 4 bytes");
			}

			return false;
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error waiting for confirmation: {ex.Message}");
			return false;
		}
	}

	/// <summary>
	/// Establishes a connection with the server using code 102
	/// </summary>
	private static void EstablishServerConnection(NetworkStream stream)
	{
		try
		{
			// Prepare connection message with aggregator ID
			string connectionData = JsonSerializer.Serialize(new { AggregatorId = aggregatorId });

			// Send connection request
			SendMessage(stream, connectionData, MessageCodes.AggregatorConnect);
			Console.WriteLine($"[{MessageCodes.AggregatorConnect}] Connection request sent to server");

			// Wait for confirmation from server
			bool confirmed = WaitForConfirmation(stream, MessageCodes.ServerConfirmation);
			if (confirmed)
			{
				Console.WriteLine("Server confirmed connection");
			}
			else
			{
				Console.WriteLine("Warning: Server did not confirm connection");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error establishing server connection: {ex.Message}");
			throw; // Rethrow to let caller handle
		}
	}


	/// <summary>
	/// Sends a disconnection notification to the server using code 502
	/// </summary>
	private static void SendServerDisconnection(NetworkStream stream)
	{
		try
		{
			// Prepare disconnection message with aggregator ID
			string disconnectionData = JsonSerializer.Serialize(new { AggregatorId = aggregatorId });

			// Send disconnection notification
			SendMessage(stream, disconnectionData, MessageCodes.AggregatorDisconnect);
			Console.WriteLine($"[{MessageCodes.AggregatorDisconnect}] Disconnection notification sent to server");

			// Wait for confirmation from server (with shorter timeout)
			stream.ReadTimeout = Math.Min(Config.ConfirmationTimeoutMs, 3000); // Use a shorter timeout for disconnection
			bool confirmed = WaitForConfirmation(stream, MessageCodes.ServerConfirmation);
			if (confirmed)
			{
				Console.WriteLine("Server confirmed disconnection");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error sending disconnection notification: {ex.Message}");
			// Don't rethrow - disconnection errors shouldn't stop the application
		}
	}



	#endregion
}
