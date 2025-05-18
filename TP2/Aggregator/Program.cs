using OceanMonitorSystem;
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
/// Middle-tier component of the OceanMonitor system that collects data from Wavy clients,
/// aggregates it by data type, and periodically forwards it to the central server.
/// </summary>
class Aggregator
{
	#region Protocol Constants

	/// <summary>
	/// Message codes used in the communication protocol 
	/// </summary>
	private static class MessageCodes
	{
		// Wavy-Aggregator communication codes
		public const int WavyConnect = 101;
		public const int WavyDataHttpInitial = 200;
		public const int WavyDataInitial = 201;
		public const int WavyDataResend = 202;
		public const int WavyDisconnect = 501;
		public const int WavyConfirmation = 401;

		// Aggregator-Server communication codes
		public const int AggregatorConnect = 102;
		public const int AggregatorData = 301;
		public const int AggregatorDataResend = 302;
		public const int AggregatorDisconnect = 502;
		public const int ServerConfirmation = 402;

	}

	#endregion

	#region Configuration

	/// <summary>
	/// Manages runtime configuration settings for the Aggregator
	/// </summary>
	private static class Config
	{
		// Network settings
		public static int ListeningPort { get; private set; } = 5000;
		public static string ServerIp { get; private set; } = "127.0.0.1";
		public static int ServerPort { get; private set; } = 6000;
		public static readonly string VALIDATION_SERVER_ADDRESS = "http://localhost:50052";
		public static DataValidationClient _validationClient;

		// Timeouts and intervals
		public static int ConfirmationTimeoutMs { get; private set; } = 10000;
		public static int DataTransmissionIntervalSec { get; private set; } = 30;
		public static int RetryIntervalSec { get; private set; } = 3;
		public static int MaxRetryAttempts { get; private set; } = 5;
		public static int ClientPollIntervalMs { get; private set; } = 100;

		// Buffer sizes
		public static int ReceiveBufferSize { get; private set; } = 4096;

		// Debug settings
		public static bool DefaultVerboseMode { get; private set; } = false;

		// File path for configuration
		private static readonly string ConfigFilePath = "aggregator.config.json";

		/// <summary>
		/// Static constructor that loads configuration from file if available
		/// </summary>
		static Config()
		{
			LoadConfigFromFile();


			// Initialize the validation client
			_validationClient = new DataValidationClient(VALIDATION_SERVER_ADDRESS, DefaultVerboseMode);
		}

		/// <summary>
		/// Loads configuration values from JSON file
		/// </summary>
		private static void LoadConfigFromFile()
		{
			try
			{
				if (File.Exists(ConfigFilePath))
				{
					string json = File.ReadAllText(ConfigFilePath);
					var config = JsonSerializer.Deserialize<Dictionary<string, object>>(json);

					if (config != null)
					{
						ApplyConfigValues(config);
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
		/// Applies configuration values from a dictionary
		/// </summary>
		private static void ApplyConfigValues(Dictionary<string, object> config)
		{
			if (TryGetJsonValue(config, "ListeningPort", out JsonElement portValue) && portValue.ValueKind == JsonValueKind.Number)
				ListeningPort = portValue.GetInt32();

			if (TryGetJsonValue(config, "ServerIp", out JsonElement ipValue) && ipValue.ValueKind == JsonValueKind.String)
				ServerIp = ipValue.GetString();

			if (TryGetJsonValue(config, "ServerPort", out JsonElement serverPortValue) && serverPortValue.ValueKind == JsonValueKind.Number)
				ServerPort = serverPortValue.GetInt32();

			if (TryGetJsonValue(config, "ConfirmationTimeoutMs", out JsonElement timeoutValue) && timeoutValue.ValueKind == JsonValueKind.Number)
				ConfirmationTimeoutMs = timeoutValue.GetInt32();

			if (TryGetJsonValue(config, "DataTransmissionIntervalSec", out JsonElement intervalValue) && intervalValue.ValueKind == JsonValueKind.Number)
				DataTransmissionIntervalSec = intervalValue.GetInt32();

			if (TryGetJsonValue(config, "RetryIntervalSec", out JsonElement retryValue) && retryValue.ValueKind == JsonValueKind.Number)
				RetryIntervalSec = retryValue.GetInt32();

			if (TryGetJsonValue(config, "MaxRetryAttempts", out JsonElement maxRetryValue) && maxRetryValue.ValueKind == JsonValueKind.Number)
				MaxRetryAttempts = maxRetryValue.GetInt32();

			if (TryGetJsonValue(config, "DefaultVerboseMode", out JsonElement verboseValue))
			{
				if (verboseValue.ValueKind == JsonValueKind.True)
					DefaultVerboseMode = true;
				else if (verboseValue.ValueKind == JsonValueKind.False)
					DefaultVerboseMode = false;
			}
		}

		/// <summary>
		/// Helper method to extract JsonElement values from a dictionary
		/// </summary>
		private static bool TryGetJsonValue(Dictionary<string, object> config, string key, out JsonElement value)
		{
			value = default;
			if (config.TryGetValue(key, out var obj) && obj is JsonElement element)
			{
				value = element;
				return true;
			}
			return false;
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

		/// <summary>
		/// Deletes the configuration file if it exists
		/// </summary>
		public static void DeleteConfigFile()
		{
			try
			{
				if (File.Exists(ConfigFilePath))
				{
					File.Delete(ConfigFilePath);
					Console.WriteLine("Configuration file deleted successfully.");
					Console.WriteLine("Default settings will be used on next restart.");
				}
				else
				{
					Console.WriteLine("Configuration file does not exist.");
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error deleting configuration file: {ex.Message}");
			}
		}
	}

	#endregion

	#region State Management

	// Storage for data received from Wavy clients
	private static readonly ConcurrentDictionary<string, ConcurrentBag<string>> dataStore =
		new ConcurrentDictionary<string, ConcurrentBag<string>>();

	// Unique identifier for this aggregator instance
	private static readonly string aggregatorId = Guid.NewGuid().ToString();

	// Active thread registry
	private static readonly ConcurrentDictionary<Thread, DateTime> activeThreads =
		new ConcurrentDictionary<Thread, DateTime>();

	// Registry of connected Wavy devices
	private static readonly Dictionary<string, DateTime> connectedWavys =
		new Dictionary<string, DateTime>();

	// Detailed logging control
	private static bool verboseMode = Config.DefaultVerboseMode;

	// Application lifecycle control
	private static bool isRunning = true;

	#endregion

	#region Entry Point and Main Thread Management

	/// <summary>
	/// Application entry point - initializes components and starts processing threads
	/// </summary>
	public static void Main(string[] args)
	{
		AppDomain.CurrentDomain.ProcessExit += OnProcessExit;

		Console.WriteLine($"Aggregator {aggregatorId} starting...");
		Console.WriteLine($"Listening on port {Config.ListeningPort}, sending to {Config.ServerIp}:{Config.ServerPort}");
		DisplayCommandMenu();

		InitializeThreads();

		// Keep main thread alive until shutdown
		while (isRunning)
		{
			Thread.Sleep(1000);
		}

		ShutdownThreads();
	}

	/// <summary>
	/// Displays available commands in the console
	/// </summary>
	private static void DisplayCommandMenu()
	{
		Console.WriteLine("\nAvailable Commands:");
		Console.WriteLine("Press 'V' to toggle verbose mode");
		Console.WriteLine("Press 'C' to show currently connected Wavy clients");
		Console.WriteLine("Press 'D' to show current data store");
		Console.WriteLine("Press 'T' to show active threads");
		Console.WriteLine("Press 'S' to save current configuration");
		Console.WriteLine("Press 'X' to delete configuration file");
		Console.WriteLine("Press 'W' to clear screen");
		Console.WriteLine("Press 'Q' to quit");
	}

	/// <summary>
	/// Initializes and starts worker threads
	/// </summary>
	private static void InitializeThreads()
	{
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
	}

	/// <summary>
	/// Handler for process exit event to ensure graceful shutdown
	/// </summary>
	private static void OnProcessExit(object sender, EventArgs e)
	{
		isRunning = false;
		Console.WriteLine("Process terminating, shutting down threads...");
		ShutdownThreads();
	}

	/// <summary>
	/// Shuts down all threads and performs cleanup
	/// </summary>
	private static void ShutdownThreads()
	{
		isRunning = false;
		Console.WriteLine("Shutting down threads...");

		WaitForThreadsToTerminate();
		SendFinalServerDisconnection();

		Console.WriteLine("Shutdown complete.");
	}

	/// <summary>
	/// Waits for worker threads to terminate
	/// </summary>
	private static void WaitForThreadsToTerminate()
	{
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
	}

	/// <summary>
	/// Sends final disconnection notification to the server
	/// </summary>
	private static void SendFinalServerDisconnection()
	{
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
	}

	/// <summary>
	/// Registers a thread in the active thread registry
	/// </summary>
	private static void RegisterThread(Thread thread)
	{
		activeThreads.TryAdd(thread, DateTime.Now);
	}

	/// <summary>
	/// Unregisters a thread from the active thread registry
	/// </summary>
	private static void UnregisterThread(Thread thread)
	{
		activeThreads.TryRemove(thread, out _);
	}

	#endregion

	#region User Interface

	/// <summary>
	/// Monitors keyboard input for user commands and handles command processing
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
					ProcessKeyCommand(key);
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
	/// Processes a keyboard command
	/// </summary>
	/// <param name="key">The console key that was pressed</param>
	private static void ProcessKeyCommand(ConsoleKey key)
	{
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
			case ConsoleKey.X:
				Config.DeleteConfigFile();
				break;
			case ConsoleKey.Q:
				Console.WriteLine("Shutting down aggregator...");
				isRunning = false;
				Thread.Sleep(500); // Give time for message to display
				Environment.Exit(0);
				break;
			case ConsoleKey.W:
				ClearScreenAndShowMenu();
				break;
		}
	}

	/// <summary>
	/// Displays information about connected Wavy clients
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
	/// Displays the current contents of the data store
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
					DisplayDataSample(dataList);
				}
			}
		}
		Console.WriteLine("=======================\n");
	}

	/// <summary>
	/// Displays a sample of data items (up to 5) from a collection
	/// </summary>
	/// <param name="dataList">The collection of data items to sample</param>
	private static void DisplayDataSample(ConcurrentBag<string> dataList)
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

	/// <summary>
	/// Displays information about active threads
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

	/// <summary>
	/// Clears the console screen and redisplays the menu
	/// </summary>
	static void ClearScreenAndShowMenu()
	{
		Console.Clear();
		Console.WriteLine($"Aggregator {aggregatorId} running...");
		Console.WriteLine($"Listening on port {Config.ListeningPort}, sending to {Config.ServerIp}:{Config.ServerPort}");
		DisplayCommandMenu();
	}

	#endregion

	#region Network Listener and Client Handling

	/// <summary>
	/// Listens for incoming connections from Wavy clients
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
						AcceptClient(listener);
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
	/// Accepts a new client connection and creates a thread to handle it
	/// </summary>
	/// <param name="listener">The TCP listener to accept clients from</param>
	private static void AcceptClient(TcpListener listener)
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

	/// <summary>
	/// Handles communication with a connected Wavy client
	/// </summary>
	/// <param name="client">The TCP client to handle</param>
	private static async void HandleClient(TcpClient client)
	{
		string clientIp = "Unknown";

		try
		{
			clientIp = ((IPEndPoint)client.Client.RemoteEndPoint).Address.ToString();
			Console.WriteLine($"New connection from {clientIp}");

			using (client)
			using (NetworkStream stream = client.GetStream())
			{
				bool isHttpLikeRequest = false;

				while (client.Connected && isRunning)
				{
					if (!stream.DataAvailable)
					{
						// Exit after processing HTTP-like request
						if (isHttpLikeRequest)
							break;

						Thread.Sleep(Config.ClientPollIntervalMs);
						continue;
					}

					byte[] buffer = new byte[Config.ReceiveBufferSize];
					int bytesRead = stream.Read(buffer, 0, buffer.Length);

					if (bytesRead <= 0)
						break;

					int messageCode = BitConverter.ToInt32(buffer, 0);
					Console.WriteLine($"\n[{DateTime.Now:HH:mm:ss.fff}] Received message with code: {messageCode}");

					// Identify HTTP-like requests
					if (messageCode == MessageCodes.WavyDataHttpInitial)
						isHttpLikeRequest = true;

					// Process message based on code and await the result
					await ProcessMessageAsync(buffer, bytesRead, stream, messageCode);

					// Handle disconnection
					if (messageCode == MessageCodes.WavyDisconnect)
						return;

					// Send confirmation for all messages (after processing is complete)
					SendConfirmation(stream, MessageCodes.WavyConfirmation);

					// For HTTP-like requests, after sending confirmation, we break from the loop
					if (isHttpLikeRequest)
						break;
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

	/// <summary>
	/// Processes a message based on its code asynchronously
	/// </summary>
	/// <param name="buffer">The received data buffer</param>
	/// <param name="bytesRead">Number of bytes read from the buffer</param>
	/// <param name="stream">The network stream for response</param>
	/// <param name="messageCode">The message code identifying the type of message</param>
	private static async Task ProcessMessageAsync(byte[] buffer, int bytesRead, NetworkStream stream, int messageCode)
	{
		switch (messageCode)
		{
			case MessageCodes.WavyConnect:
				ProcessConnection(buffer, bytesRead, stream);
				break;
			case MessageCodes.WavyDataHttpInitial:
			case MessageCodes.WavyDataInitial:
			case MessageCodes.WavyDataResend:
				await ProcessDataAsync(buffer, bytesRead, stream, messageCode);
				break;
			case MessageCodes.AggregatorData:
				ProcessAggregatedData(buffer, bytesRead, stream);
				break;
			case MessageCodes.WavyDisconnect:
				ProcessDisconnection(buffer, bytesRead, stream);
				break;
			default:
				LogUnknownMessage(buffer, bytesRead, messageCode);
				break;
		}
	}

	/// <summary>
	/// Processes data sent from a Wavy client with validation
	/// </summary>
	private static async Task ProcessDataAsync(byte[] buffer, int bytesRead, NetworkStream stream, int messageCode)
	{
		string jsonData = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);
		string wavyId = "Unknown";

		try
		{
			using (JsonDocument doc = JsonDocument.Parse(jsonData))
			{
				JsonElement root = doc.RootElement;

				// Extract Wavy ID
				if (root.TryGetProperty("WavyId", out JsonElement wavyIdElement))
				{
					wavyId = wavyIdElement.GetString();
				}

				Console.WriteLine($"[{messageCode}] Data received from Wavy ID: {wavyId}");

				// Validate and clean data using remote service
				var (isValid, cleanedData) = await Config._validationClient.ValidateAndCleanDataAsync(wavyId, jsonData);

				if (!isValid)
				{
					Console.WriteLine($"[WARNING] Validation failed for data from Wavy ID: {wavyId}");
					return;
				}

				// Use the cleaned data instead of the original
				jsonData = cleanedData;
			}

			// Store the cleaned data in the data store
			StoreInDataStore(jsonData);
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error processing data: {ex.Message}");
			if (verboseMode)
			{
				Console.WriteLine($"Raw data: {jsonData}");
			}
		}
	}

	/// <summary>
	/// Logs information about unknown message types
	/// </summary>
	/// <param name="buffer">The message buffer</param>
	/// <param name="bytesRead">The number of bytes read</param>
	/// <param name="messageCode">The unknown message code</param>
	private static void LogUnknownMessage(byte[] buffer, int bytesRead, int messageCode)
	{
		if (verboseMode)
		{
			Console.WriteLine($"Unknown message code: {messageCode}");
			string receivedData = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);
			Console.WriteLine($"Payload: {receivedData}");
		}
	}

	#endregion

	#region Message Processing

	/// <summary>
	/// Processes a connection request from a Wavy client and registers it
	/// </summary>
	/// <param name="buffer">The received data buffer</param>
	/// <param name="bytesRead">Number of bytes read from the buffer</param>
	/// <param name="stream">The network stream for response</param>
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
	/// Processes a disconnection request from a Wavy client and removes it from registry
	/// </summary>
	/// <param name="buffer">The received data buffer</param>
	/// <param name="bytesRead">Number of bytes read from the buffer</param>
	/// <param name="stream">The network stream for response</param>
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

	// Helper method to store data in the data store
	private static void StoreInDataStore(string jsonData)
	{
		try
		{
			using (JsonDocument doc = JsonDocument.Parse(jsonData))
			{
				JsonElement root = doc.RootElement;

				// Validate required fields
				if (!ValidateWavyDataFormat(root))
				{
					LogInvalidDataFormat(jsonData);
					return;
				}

				string wavyId = root.GetProperty("WavyId").GetString();

				// Store each data item
				foreach (JsonElement item in root.GetProperty("Data").EnumerateArray())
				{
					ProcessAndStoreDataItem(item, wavyId);
				}
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error storing cleaned data: {ex.Message}");
		}
	}

	/// <summary>
	/// Validates that the Wavy data has the required format
	/// </summary>
	/// <param name="root">The root JSON element to validate</param>
	/// <returns>True if the data has valid format, false otherwise</returns>
	private static bool ValidateWavyDataFormat(JsonElement root)
	{
		return root.TryGetProperty("WavyId", out JsonElement _) &&
			   root.TryGetProperty("Data", out JsonElement dataArray) &&
			   dataArray.ValueKind == JsonValueKind.Array;
	}

	/// <summary>
	/// Logs information about invalid data format received
	/// </summary>
	/// <param name="jsonData">The raw JSON data that was invalid</param>
	private static void LogInvalidDataFormat(string jsonData)
	{
		Console.WriteLine("Received malformed data packet - missing required properties");
		if (verboseMode)
		{
			Console.WriteLine($"Raw data: {jsonData}");
		}
	}

	/// <summary>
	/// Processes aggregated data sent in HTTP-like mode
	/// </summary>
	/// <param name="buffer">The received data buffer</param>
	/// <param name="bytesRead">Number of bytes read from the buffer</param>
	/// <param name="stream">The network stream for response</param>
	private static void ProcessAggregatedData(byte[] buffer, int bytesRead, NetworkStream stream)
	{
		string jsonData = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);

		try
		{
			using (JsonDocument doc = JsonDocument.Parse(jsonData))
			{
				JsonElement root = doc.RootElement;

				// Extract WavyId if present
				string wavyId = ExtractWavyId(root);

				// Process data array if present
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
	/// Extracts the Wavy ID from the root JSON element if present
	/// </summary>
	/// <param name="root">The root JSON element to extract from</param>
	/// <returns>The Wavy ID string or null if not found</returns>
	private static string ExtractWavyId(JsonElement root)
	{
		if (root.TryGetProperty("WavyId", out JsonElement wavyIdElement))
		{
			string wavyId = wavyIdElement.GetString();
			Console.WriteLine($"[{MessageCodes.AggregatorData}] Aggregated data received from Wavy ID: {wavyId}");
			return wavyId;
		}

		Console.WriteLine($"[{MessageCodes.AggregatorData}] Aggregated data received (no Wavy ID found)");
		return null;
	}

	/// <summary>
	/// Processes and stores a single data item
	/// </summary>
	/// <param name="item">The JSON element containing the data item</param>
	/// <param name="wavyId">The Wavy ID associated with this data (can be null)</param>
	private static void ProcessAndStoreDataItem(JsonElement item, string wavyId = null)
	{
		try
		{
			// Ensure required DataType property exists
			if (!item.TryGetProperty("DataType", out JsonElement dataTypeElement))
				return;

			string dataType = dataTypeElement.GetString();

			// Ensure container exists for this data type
			EnsureDataStoreContainer(dataType);

			// Store the data item with Wavy ID
			StoreDataItem(item, dataType, wavyId);

			// Log in verbose mode
			LogDataItemDetails(item, dataType, wavyId);
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error processing data item: {ex.Message}");
		}
	}

	/// <summary>
	/// Ensures a container exists in the data store for a particular data type
	/// </summary>
	/// <param name="dataType">The data type to create a container for</param>
	private static void EnsureDataStoreContainer(string dataType)
	{
		if (!dataStore.ContainsKey(dataType))
		{
			dataStore[dataType] = new ConcurrentBag<string>();
		}
	}

	/// <summary>
	/// Stores a data item in the data store
	/// </summary>
	/// <param name="item">The JSON element containing the data item</param>
	/// <param name="dataType">The type of data being stored</param>
	/// <param name="wavyId">The Wavy ID to associate with the data (can be null)</param>
	private static void StoreDataItem(JsonElement item, string dataType, string wavyId)
	{
		if (wavyId != null)
		{
			// Add WavyId to the data item
			var dataWithWavyId = CreateDataItemWithWavyId(item, wavyId);
			string jsonData = JsonSerializer.Serialize(dataWithWavyId);
			dataStore[dataType].Add(jsonData);
		}
		else
		{
			// Store as-is
			dataStore[dataType].Add(item.GetRawText());
		}
	}

	/// <summary>
	/// Creates a dictionary with a Wavy ID added to an existing JSON element
	/// </summary>
	/// <param name="item">The source JSON element</param>
	/// <param name="wavyId">The Wavy ID to add</param>
	/// <returns>A dictionary with all properties from the JSON element plus the Wavy ID</returns>
	private static Dictionary<string, object> CreateDataItemWithWavyId(JsonElement item, string wavyId)
	{
		var dataWithWavyId = new Dictionary<string, object>();

		// Copy all existing properties
		foreach (JsonProperty prop in item.EnumerateObject())
		{
			if (prop.Name == "DataType")
				dataWithWavyId["DataType"] = prop.Value.GetString();
			else if (prop.Name == "Value")
				dataWithWavyId["Value"] = prop.Value.GetDouble();
			else
				dataWithWavyId[prop.Name] = JsonSerializer.Deserialize<object>(prop.Value.GetRawText());
		}

		// Add the WavyId
		dataWithWavyId["WavyId"] = wavyId;

		return dataWithWavyId;
	}

	/// <summary>
	/// Logs details about a data item in verbose mode
	/// </summary>
	/// <param name="item">The JSON element containing the data item</param>
	/// <param name="dataType">The type of data</param>
	/// <param name="wavyId">The Wavy ID associated with this data (can be null)</param>
	private static void LogDataItemDetails(JsonElement item, string dataType, string wavyId)
	{
		if (verboseMode && item.TryGetProperty("Value", out JsonElement valueElement))
		{
			double value = valueElement.GetDouble();
			Console.WriteLine($"  - {dataType}: {value}{(wavyId != null ? $" (Wavy: {wavyId})" : "")}");
		}
	}

	#endregion

	#region Data Transmission to Server

	/// <summary>
	/// Periodically sends aggregated data to the server based on configured interval
	/// </summary>
	private static void SendDataToServer()
	{
		try
		{
			while (isRunning)
			{
				Console.WriteLine($"Waiting for {Config.DataTransmissionIntervalSec} seconds before sending data...");

				// Wait for the next transmission window
				for (int i = 0; i < Config.DataTransmissionIntervalSec && isRunning; i++)
				{
					Thread.Sleep(1000); // Check isRunning flag every second
				}

				if (!isRunning) break;

				Console.WriteLine($"\n[{DateTime.Now:HH:mm:ss.fff}] Preparing to send aggregated data to server");

				// Capture current data for transmission and reset the store
				var dataToSend = PrepareDataForTransmission();

				if (dataToSend.IsEmpty)
				{
					Console.WriteLine("No data to send to server.");
					continue;
				}

				// Transmit the data
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

	/// <summary>
	/// Prepares data for transmission by copying and clearing the data store
	/// </summary>
	/// <returns>A copy of the data store before clearing</returns>
	private static ConcurrentDictionary<string, ConcurrentBag<string>> PrepareDataForTransmission()
	{
		var dataToSend = new ConcurrentDictionary<string, ConcurrentBag<string>>(dataStore);
		dataStore.Clear();
		return dataToSend;
	}

	/// <summary>
	/// Sends aggregated data to the server
	/// </summary>
	/// <param name="dataToSend">The data to send, organized by data type</param>
	private static void SendAggregatedData(ConcurrentDictionary<string, ConcurrentBag<string>> dataToSend)
	{
		// Prepare the aggregated data payload
		var aggregatedDataList = PrepareAggregatedDataList(dataToSend);

		if (aggregatedDataList.Count == 0)
		{
			Console.WriteLine("No data could be aggregated.");
			return;
		}

		// Create the message payload
		var payload = CreateMessagePayload(aggregatedDataList);
		string jsonData = JsonSerializer.Serialize(payload);

		// Transmit with retry
		TransmitToServerWithRetry(jsonData);
	}

	/// <summary>
	/// Creates a list of aggregated data items organized by data type
	/// </summary>
	/// <param name="dataToSend">The raw data to aggregate</param>
	/// <returns>A list of data type objects, each containing parsed items</returns>
	private static List<object> PrepareAggregatedDataList(ConcurrentDictionary<string, ConcurrentBag<string>> dataToSend)
	{
		var aggregatedDataList = new List<object>();

		foreach (var dataType in dataToSend.Keys)
		{
			var parsedItems = ParseDataItems(dataToSend[dataType]);

			if (parsedItems.Count > 0)
			{
				aggregatedDataList.Add(new { DataType = dataType, Data = parsedItems });
			}
		}

		return aggregatedDataList;
	}

	/// <summary>
	/// Parses JSON strings into objects
	/// </summary>
	/// <param name="jsonStrings">Collection of JSON strings to parse</param>
	/// <returns>List of parsed objects</returns>
	private static List<object> ParseDataItems(ConcurrentBag<string> jsonStrings)
	{
		var parsedItems = new List<object>();

		foreach (var jsonString in jsonStrings)
		{
			try
			{
				var dataItem = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonString);
				if (dataItem != null)
				{
					parsedItems.Add(dataItem);
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error parsing data item: {ex.Message}");
			}
		}

		return parsedItems;
	}

	/// <summary>
	/// Creates the complete message payload with metadata
	/// </summary>
	/// <param name="aggregatedDataList">The aggregated data to include</param>
	/// <returns>An object representing the complete message payload</returns>
	private static object CreateMessagePayload(List<object> aggregatedDataList)
	{
		return new
		{
			AggregatorId = aggregatorId,
			Timestamp = DateTime.UtcNow,
			Data = aggregatedDataList
		};
	}

	/// <summary>
	/// Transmits data to the server with retry capability
	/// </summary>
	/// <param name="jsonData">The JSON data to transmit</param>
	private static void TransmitToServerWithRetry(string jsonData)
	{
		bool confirmed = false;
		int retryCount = 0;

		try
		{
			using (TcpClient client = new TcpClient(Config.ServerIp, Config.ServerPort))
			using (NetworkStream stream = client.GetStream())
			{
				// Establish initial connection
				EstablishServerConnection(stream);

				// Attempt to send data with retry
				while (!confirmed && isRunning && retryCount < Config.MaxRetryAttempts)
				{
					try
					{
						// Select appropriate message code
						int messageCode = (retryCount == 0) ?
							MessageCodes.AggregatorData :
							MessageCodes.AggregatorDataResend;

						// Send data to server
						SendMessage(stream, jsonData, messageCode);
						Console.WriteLine($"[{messageCode}] Data sent to server:");
						if (verboseMode)
						{
							Console.WriteLine(jsonData);
						}

						// Wait for confirmation
						confirmed = WaitForConfirmation(stream, MessageCodes.ServerConfirmation);

						if (!confirmed)
						{
							HandleRetryAttempt(ref retryCount);
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
							WaitForRetryInterval();
						}
					}
				}

				// Always send disconnection notification
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

	/// <summary>
	/// Handles a retry attempt, incrementing the counter and waiting
	/// </summary>
	/// <param name="retryCount">The retry counter to increment</param>
	private static void HandleRetryAttempt(ref int retryCount)
	{
		retryCount++;
		Console.WriteLine($"No confirmation received from server, retry {retryCount}/{Config.MaxRetryAttempts}...");

		if (retryCount < Config.MaxRetryAttempts)
		{
			WaitForRetryInterval();
		}
	}

	/// <summary>
	/// Waits for the configured retry interval
	/// </summary>
	private static void WaitForRetryInterval()
	{
		for (int i = 0; i < Config.RetryIntervalSec && isRunning; i++)
		{
			Thread.Sleep(1000);
		}
	}

	#endregion

	#region Network Utilities

	/// <summary>
	/// Sends a message with the specified code to the provided network stream
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
	/// Sends a confirmation message with the specified code
	/// </summary>
	/// <param name="stream">The network stream to send the confirmation to</param>
	/// <param name="code">The confirmation code to send</param>
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
	/// Waits for a confirmation message with the expected code
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
	/// Establishes a connection with the server
	/// </summary>
	/// <param name="stream">The network stream to use for the connection</param>
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
	/// Sends a disconnection notification to the server
	/// </summary>
	/// <param name="stream">The network stream to use for the disconnection</param>
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
