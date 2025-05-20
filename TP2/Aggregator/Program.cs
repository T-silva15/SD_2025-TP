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
using NetMQ;
using NetMQ.Sockets;

/// <summary>
/// Middle-tier component of the OceanMonitor system that collects data from Wavy clients 
/// through ZeroMQ publish-subscribe mechanism, aggregates it by data type, and 
/// periodically forwards it to the central server.
/// </summary>
class Aggregator
{
	#region Protocol Constants

	/// <summary>
	/// Message codes used in the communication protocol with the server
	/// </summary>
	private static class MessageCodes
	{
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
		// Server connection settings
		public static string ServerIp { get; private set; } = "127.0.0.1";
		public static int ServerPort { get; private set; } = 6000;
		public static readonly string VALIDATION_SERVER_ADDRESS = "http://localhost:50052";
		public static DataValidationClient _validationClient = null!;

		// Timeouts and intervals
		public static int ConfirmationTimeoutMs { get; private set; } = 10000;
		public static int DataTransmissionIntervalSec { get; private set; } = 30;
		public static int RetryIntervalSec { get; private set; } = 3;
		public static int MaxRetryAttempts { get; private set; } = 5;
		public static int ClientPollIntervalMs { get; private set; } = 100;

		// Buffer sizes
		public static int ReceiveBufferSize { get; private set; } = 4096;

		// Debug settings
		public static bool VerboseMode { get; private set; } = false;

		// File path for configuration
		private static readonly string ConfigFilePath = "aggregator.config.json";

		// Default data types to subscribe to
		public static readonly List<string> DefaultSubscribedDataTypes = new List<string>
		{
			"Temperature",
			"Frequency"
		};

		/// <summary>
		/// Static constructor that loads configuration from file if available
		/// </summary>
		static Config()
		{
			LoadConfigFromFile();

			// Initialize the validation client
			_validationClient = new DataValidationClient(VALIDATION_SERVER_ADDRESS, VerboseMode);
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
			if (TryGetJsonValue(config, "ServerIp", out JsonElement ipValue) && ipValue.ValueKind == JsonValueKind.String)
				ServerIp = ipValue.GetString() ?? "127.0.0.1";

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

			if (TryGetJsonValue(config, "VerboseMode", out JsonElement verboseValue))
			{
				if (verboseValue.ValueKind == JsonValueKind.True)
					VerboseMode = true;
				else if (verboseValue.ValueKind == JsonValueKind.False)
					VerboseMode = false;
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
					{ "ServerIp", ServerIp },
					{ "ServerPort", ServerPort },
					{ "ConfirmationTimeoutMs", ConfirmationTimeoutMs },
					{ "DataTransmissionIntervalSec", DataTransmissionIntervalSec },
					{ "RetryIntervalSec", RetryIntervalSec },
					{ "MaxRetryAttempts", MaxRetryAttempts },
					{ "VerboseMode", VerboseMode }
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

			try
			{
				if (File.Exists(PubSubConfig.SubscriptionFilePath))
				{
					File.Delete(PubSubConfig.SubscriptionFilePath);
					Console.WriteLine("Subscription file deleted successfully.");
					Console.WriteLine("Default settings will be used on next restart.");
				}
				else
				{
					Console.WriteLine("Subscription file does not exist.");
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error deleting subscription file: {ex.Message}");
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

	// Detailed logging control
	private static bool verboseMode = Config.VerboseMode;

	// Application lifecycle control
	private static bool isRunning = true;

	#endregion

	#region Entry Point and Main Thread Management

	/// <summary>
	/// Application entry point - initializes components and starts processing threads
	/// </summary>
	public static void Main(string[] args)
	{
		AppDomain.CurrentDomain.ProcessExit += (sender, e) => OnProcessExit(sender!, e);

		Console.WriteLine($"Aggregator {aggregatorId} starting...");
		Console.WriteLine($"Receiving data via ZeroMQ, sending to {Config.ServerIp}:{Config.ServerPort}");
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
		Console.WriteLine("Press 'D' to show current data store");
		Console.WriteLine("Press 'T' to show active threads");
		Console.WriteLine("Press 'M' to manage subscriptions");
		Console.WriteLine("Press 'S' to save current configuration");
		Console.WriteLine("Press 'X' to delete configuration and subscription files");
		Console.WriteLine("Press 'W' to clear screen");
		Console.WriteLine("Press 'Q' to quit");
	}

	/// <summary>
	/// Initializes and starts worker threads
	/// </summary>
	private static void InitializeThreads()
	{
		// Initialize ZeroMQ subscriber and load subscriptions
		PubSubConfig.LoadSubscriptions();

		// Start keyboard monitoring thread
		Thread keyboardThread = new Thread(MonitorKeyboard) { IsBackground = true, Name = "KeyboardMonitor" };
		RegisterThread(keyboardThread);
		keyboardThread.Start();

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

		// Cleanup ZeroMQ resources
		PubSubConfig.Cleanup();
		NetMQConfig.Cleanup();

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
			case ConsoleKey.M:
				ManageSubscriptions();
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
	/// Manages subscription settings through the console UI
	/// </summary>
	private static void ManageSubscriptions()
	{
		Console.WriteLine("\n=== Subscription Management ===");
		Console.WriteLine("Currently subscribed data types:");

		int index = 1;
		var dataTypes = new Dictionary<int, string>();

		foreach (var dataType in PubSubConfig.SubscribedDataTypes)
		{
			Console.WriteLine($"{index}. {dataType}");
			dataTypes[index++] = dataType;
		}

		Console.WriteLine("\nAvailable actions:");
		Console.WriteLine("A. Add a subscription");
		Console.WriteLine("R. Remove a subscription");
		Console.WriteLine("B. Back to main menu");

		Console.Write("\nEnter your choice: ");
		var key = Console.ReadKey().KeyChar;
		Console.WriteLine();

		switch (char.ToUpper(key))
		{
			case 'A':
				AddSubscription();
				break;
			case 'R':
				RemoveSubscription(dataTypes);
				break;
			case 'B':
				return;
		}

		Console.WriteLine("===========================\n");
	}

	/// <summary>
	/// UI for adding a new subscription
	/// </summary>
	private static void AddSubscription()
	{
		// List of valid data types that can be subscribed to
		var validDataTypes = new List<string> { "Temperature", "WindSpeed", "Frequency", "Decibels" };

		Console.WriteLine("\nAvailable data types:");
		for (int i = 0; i < validDataTypes.Count; i++)
		{
			Console.WriteLine($"{i + 1}. {validDataTypes[i]}");
		}

		Console.WriteLine("\nEnter data type number or name to subscribe to:");
		string input = Console.ReadLine()?.Trim() ?? "";

		string? selectedDataType = null;

		// Check if input is a number (index)
		if (int.TryParse(input, out int index) && index >= 1 && index <= validDataTypes.Count)
		{
			selectedDataType = validDataTypes[index - 1];
		}
		// Otherwise check if input matches a valid type name
		else
		{
			selectedDataType = validDataTypes.FirstOrDefault(dt =>
				dt.Equals(input, StringComparison.OrdinalIgnoreCase));
		}

		if (selectedDataType == null)
		{
			Console.WriteLine("Invalid data type. Please select from the available options.");
			return;
		}

		// Check if already subscribed
		if (PubSubConfig.SubscribedDataTypes.Contains(selectedDataType))
		{
			Console.WriteLine($"Already subscribed to {selectedDataType} data.");
			return;
		}

		// Proceed with subscription
		if (PubSubConfig.Subscribe(selectedDataType))
		{
			Console.WriteLine($"Successfully subscribed to {selectedDataType} data.");
		}
	}

	/// <summary>
	/// UI for removing an existing subscription
	/// </summary>
	private static void RemoveSubscription(Dictionary<int, string> dataTypes)
	{
		Console.WriteLine("\nEnter number of subscription to remove:");

		if (int.TryParse(Console.ReadLine() ?? "", out int choice) && dataTypes.TryGetValue(choice, out string dataType))
		{
			if (PubSubConfig.Unsubscribe(dataType))
			{
				Console.WriteLine($"Successfully unsubscribed from {dataType} data.");
			}
		}
		else
		{
			Console.WriteLine("Invalid selection.");
		}
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
		Console.WriteLine($"Receiving data via ZeroMQ, sending to {Config.ServerIp}:{Config.ServerPort}");
		DisplayCommandMenu();
	}

	#endregion

	#region Data Processing and Server Communication

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
			try
			{
				var dataItems = new List<object>();

				// Parse each JSON string into an object
				foreach (var jsonItem in dataToSend[dataType])
				{
					try
					{
						// Debug the JSON to see what's coming in
						if (verboseMode)
						{
							Console.WriteLine($"Processing item for {dataType}: {jsonItem}");
						}

						// Parse the JSON and create a simplified object with just the essential properties
						using (JsonDocument doc = JsonDocument.Parse(jsonItem))
						{
							var root = doc.RootElement;

							// Extract the key properties
							double value = 0;
							string wavyId = "Unknown";

							if (root.TryGetProperty("Value", out var valueElement))
							{
								// Explicitly handle different value kinds
								if (valueElement.ValueKind == JsonValueKind.Number)
								{
									value = valueElement.GetDouble();
								}
								else if (valueElement.ValueKind == JsonValueKind.String)
								{
									string valueStr = valueElement.GetString() ?? "0";
									double.TryParse(valueStr, out value);
								}
								Console.WriteLine($"Found value: {value}"); // Debug output
							}

							if (root.TryGetProperty("WavyId", out var wavyIdElement))
							{
								wavyId = wavyIdElement.GetString() ?? "Unknown";
							}

							// Create a simple, clean data item with just the essential fields
							// Use a concrete Dictionary instead of anonymous object to ensure correct serialization
							dataItems.Add(new Dictionary<string, object>
							{
								["Value"] = value, // Make sure "Value" is capitalized to match what the server expects
								["WavyId"] = wavyId // Make sure "WavyId" is capitalized to match what the server expects
							});
						}
					}
					catch (Exception ex)
					{
						Console.WriteLine($"Failed to parse data item: {ex.Message}");
						if (verboseMode)
						{
							Console.WriteLine($"Problem item: {jsonItem}");
						}
					}
				}

				if (dataItems.Count > 0)
				{
					// Create a properly formatted data object for this data type
					// Use a concrete Dictionary instead of anonymous object
					aggregatedDataList.Add(new Dictionary<string, object>
					{
						["DataType"] = dataType,
						["Data"] = dataItems.ToArray()  // Use an array for stable serialization
					});
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error aggregating {dataType} data: {ex.Message}");
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
		// Serialize using simpler types to avoid any serialization issues
		var payload = new Dictionary<string, object>
		{
			{ "AggregatorId", aggregatorId },
			{ "Timestamp", DateTime.UtcNow.ToString("o") },  // ISO 8601 string format
			{ "Data", aggregatedDataList }
		};

		return payload;
	}

	/// <summary>
	/// Transmits data to the server with retry capability
	/// </summary>
	/// <param name="jsonData">The JSON data to transmit</param>
	private static void TransmitToServerWithRetry(string jsonData)
	{
		bool confirmed = false;
		int retryCount = 0;

		// Do a quick check to make sure the JSON is valid before sending
		try
		{
			using (JsonDocument.Parse(jsonData)) { }
			Console.WriteLine("JSON validation passed");
		}
		catch (JsonException ex)
		{
			Console.WriteLine($"ERROR: Invalid JSON data, not sending: {ex.Message}");
			return;
		}

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
						Console.WriteLine($"[{messageCode}] Data sent to server, size: {jsonData.Length} bytes");

						if (verboseMode)
						{
							// Show only a sample in verbose mode to avoid flooding
							const int maxDisplayLength = 500;
							string displayJson = jsonData.Length > maxDisplayLength
								? jsonData.Substring(0, maxDisplayLength) + "..."
								: jsonData;
							Console.WriteLine(displayJson);
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

	#region Pub-Sub Communication

	/// <summary>
	/// Manages publish-subscribe communication settings and operations
	/// </summary>
	private static class PubSubConfig
	{
		// The Wavy publisher address to connect to
		public static string PublisherAddress { get; set; } = "tcp://127.0.0.1:5556";

		// The Wavy subscription management address
		public static string SubscriptionManagerAddress { get; set; } = "tcp://127.0.0.1:5557";

		// Data types this aggregator is interested in
		public static HashSet<string> SubscribedDataTypes { get; private set; } = new HashSet<string>();

		// Configuration file path
		public static readonly string SubscriptionFilePath = "aggregator_subscriptions.json";

		// Active subscribers for each data type
		private static Dictionary<string, SubscriberSocket> _subscribers = new Dictionary<string, SubscriberSocket>();

		/// <summary>
		/// Loads subscriptions from configuration file
		/// </summary>
		public static void LoadSubscriptions()
		{
			try
			{
				if (File.Exists(SubscriptionFilePath))
				{
					string json = File.ReadAllText(SubscriptionFilePath);
					var subscriptions = JsonSerializer.Deserialize<List<string>>(json);

					if (subscriptions != null)
					{
						SubscribedDataTypes.Clear();
						foreach (var dataType in subscriptions)
						{
							SubscribedDataTypes.Add(dataType);
						}
						Console.WriteLine($"Loaded {SubscribedDataTypes.Count} subscribed data types");

						if (Config.VerboseMode)
						{
							Console.WriteLine("Subscribed data types: " + string.Join(", ", SubscribedDataTypes));
						}
					}
				}
				else
				{
					// Use default subscriptions from Config class
					SubscribedDataTypes.Clear();
					foreach (var dataType in Config.DefaultSubscribedDataTypes)
					{
						SubscribedDataTypes.Add(dataType);
					}
					SaveSubscriptions();
					Console.WriteLine($"Created default subscriptions file with {SubscribedDataTypes.Count} data types");
				}

				// Subscribe to each data type
				foreach (var dataType in SubscribedDataTypes)
				{
					Subscribe(dataType);
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error loading subscriptions: {ex.Message}");

				// In case of error, fall back to Config defaults
				SubscribedDataTypes.Clear();
				foreach (var dataType in Config.DefaultSubscribedDataTypes)
				{
					SubscribedDataTypes.Add(dataType);
				}
				Console.WriteLine("Using default subscriptions from configuration");
			}
		}

		/// <summary>
		/// Subscribes to a specific data type
		/// </summary>
		public static bool Subscribe(string dataType)
		{
			try
			{
				// Create or verify local subscription
				bool alreadySubscribed = SubscribedDataTypes.Contains(dataType);

				// Add to local subscriptions list if not already there
				if (!alreadySubscribed)
				{
					SubscribedDataTypes.Add(dataType);
				}

				// Create subscriber socket if needed
				if (!_subscribers.TryGetValue(dataType, out var socket))
				{
					socket = new SubscriberSocket();
					Console.WriteLine($"Connecting subscriber to {PublisherAddress}");
					socket.Connect(PublisherAddress);
					Console.WriteLine($"Subscribing to topic '{dataType}'");
					socket.Subscribe(dataType);

					_subscribers[dataType] = socket;

					// Start a listener thread for this subscription
					StartSubscriptionListener(dataType, socket);
				}

				// ALWAYS notify the Wavy about this subscription with retries, even if already subscribed locally
				Console.WriteLine($"{(alreadySubscribed ? "Re-sending" : "Sending")} subscription request for {dataType} to Wavy");

				int maxRetries = 3;
				bool requestSent = false;

				for (int attempt = 0; attempt < maxRetries && !requestSent; attempt++)
				{
					try
					{
						Console.WriteLine($"Attempt {attempt + 1}/{maxRetries}: Connecting to {SubscriptionManagerAddress} for {dataType} subscription");

						using (var requestSocket = new RequestSocket())
						{
							// Set socket options for better reliability
							requestSocket.Options.Linger = TimeSpan.FromMilliseconds(500);

							requestSocket.Connect(SubscriptionManagerAddress);
							Console.WriteLine($"Connected to subscription manager");

							// Prepare the subscription request
							var request = new Dictionary<string, object>
							{
								{ "action", "subscribe" },
								{ "dataType", dataType }
							};
							string json = JsonSerializer.Serialize(request);

							Console.WriteLine($"Sending subscription request: {json}");
							requestSocket.SendFrame(json);

							Console.WriteLine($"Waiting for response...");

							try
							{
								// Just use the standard ReceiveFrameString without timeout
								// The socket will use its default timeout behavior
								string response = requestSocket.ReceiveFrameString();
								Console.WriteLine($"Subscription response: {response}");
								requestSent = true;
							}
							catch (NetMQException ex)
							{
								// Only log and continue, don't rethrow
								Console.WriteLine($"Failed to receive response: {ex.Message}");
							}
						}

						// Wait before retry if needed
						if (!requestSent && attempt < maxRetries - 1)
						{
							Console.WriteLine($"Waiting 1 second before retry...");
							Thread.Sleep(1000);
						}
					}
					catch (Exception ex)
					{
						Console.WriteLine($"Attempt {attempt + 1} failed: {ex.Message}");

						if (attempt < maxRetries - 1)
						{
							Console.WriteLine($"Waiting 1 second before retry...");
							Thread.Sleep(1000);
						}
					}
				}

				// Save subscriptions
				SaveSubscriptions();

				if (requestSent)
					Console.WriteLine($"Successfully {(alreadySubscribed ? "refreshed" : "subscribed to")} {dataType} data");
				else
					Console.WriteLine($"Warning: Could not notify Wavy about {dataType} subscription, but will listen for messages anyway");

				return true;
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error subscribing to {dataType}: {ex.Message}");
				Console.WriteLine($"Stack trace: {ex.StackTrace}");
				return false;
			}
		}

		/// <summary>
		/// Unsubscribes from a specific data type
		/// </summary>
		public static bool Unsubscribe(string dataType)
		{
			try
			{
				// Remove from subscriptions list
				if (!SubscribedDataTypes.Remove(dataType))
				{
					// Not subscribed
					return true;
				}

				// Remove subscriber socket if it exists
				if (_subscribers.TryGetValue(dataType, out var socket))
				{
					socket.Dispose();
					_subscribers.Remove(dataType);
				}

				// Notify the Wavy about unsubscribing
				using (var requestSocket = new RequestSocket())
				{
					requestSocket.Connect(SubscriptionManagerAddress);

					var request = new Dictionary<string, object>
					{
						{ "action", "unsubscribe" },
						{ "dataType", dataType }
					};

					string json = JsonSerializer.Serialize(request);
					requestSocket.SendFrame(json);

					string response = requestSocket.ReceiveFrameString();
					Console.WriteLine($"Unsubscription request response: {response}");
				}

				SaveSubscriptions();
				Console.WriteLine($"Unsubscribed from {dataType} data");
				return true;
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error unsubscribing from {dataType}: {ex.Message}");
				return false;
			}
		}

		/// <summary>
		/// Starts a listener thread for a specific data type subscription
		/// </summary>
		private static void StartSubscriptionListener(string dataType, SubscriberSocket socket)
		{
			Thread listenerThread = new Thread(() =>
			{
				try
				{
					Console.WriteLine($"Started listener for {dataType} data");

					while (isRunning)
					{
						try
						{
							string topic = socket.ReceiveFrameString();
							string messageJson = socket.ReceiveFrameString();

							if (verboseMode)
							{
								Console.WriteLine($"Received {topic} data: {messageJson}");
							}

							// Process the received data
							var data = JsonSerializer.Deserialize<Dictionary<string, object>>(messageJson);
							if (data != null)
							{
								// Store the data
								ProcessReceivedData(dataType, data);
							}
						}
						catch (Exception ex)
						{
							if (isRunning) // Only log if not shutting down
							{
								Console.WriteLine($"Error receiving {dataType} data: {ex.Message}");
							}
							Thread.Sleep(1000); // Avoid tight loop on error
						}
					}
				}
				catch (Exception ex)
				{
					Console.WriteLine($"Fatal error in {dataType} listener: {ex.Message}");
				}
				finally
				{
					UnregisterThread(Thread.CurrentThread);
				}
			})
			{
				IsBackground = true,
				Name = $"Subscriber-{dataType}"
			};

			RegisterThread(listenerThread);
			listenerThread.Start();
		}

		/// <summary>
		/// Saves current subscriptions to configuration file
		/// </summary>
		public static void SaveSubscriptions()
		{
			try
			{
				string json = JsonSerializer.Serialize(SubscribedDataTypes.ToList(),
					new JsonSerializerOptions { WriteIndented = true });

				File.WriteAllText(SubscriptionFilePath, json);
				Console.WriteLine("Subscription settings saved to file");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error saving subscriptions: {ex.Message}");
			}
		}

		/// <summary>
		/// Processes data received from a subscription
		/// </summary>
		private static async void ProcessReceivedData(string dataType, Dictionary<string, object> data)
		{
			try
			{
				// Extract Wavy ID if present or use "Unknown"
				string wavyId = "Unknown";
				if (data.TryGetValue("WavyId", out var wavyIdObj) && wavyIdObj != null)
				{
					wavyId = wavyIdObj.ToString() ?? "Unknown";
				}

				// Extract the value - handle JsonElement properly
				double value = 0.0;
				if (data.TryGetValue("Value", out var valueObj) && valueObj != null)
				{
					// Handle JsonElement conversion properly
					if (valueObj is JsonElement jsonElement)
					{
						if (jsonElement.ValueKind == JsonValueKind.Number)
						{
							value = jsonElement.GetDouble();
						}
						else if (jsonElement.ValueKind == JsonValueKind.String &&
								 !string.IsNullOrEmpty(jsonElement.GetString()) &&
								 double.TryParse(jsonElement.GetString(), out double parsedValue))
						{
							value = parsedValue;
						}
					}
					else
					{
						// Try regular conversion for other types
						try
						{
							value = Convert.ToDouble(valueObj);
						}
						catch (Exception convEx)
						{
							Console.WriteLine($"Value conversion error: {convEx.Message}. Using default value 0.");
						}
					}
				}

				// Debug output
				Console.WriteLine($"Extracted value for {dataType}: {value} (from {wavyId})");

				// Create validation data format expected by the gRPC validation service
				var validationData = new
				{
					Data = new[]
					{
				new
				{
					DataType = dataType,
					Value = value
				}
			}
				};

				string validationJson = JsonSerializer.Serialize(validationData);
				Console.WriteLine($"Validating data for {dataType}: {validationJson}");

				// Send data for validation
				try
				{
					var (isValid, cleanedData) = await Config._validationClient.ValidateAndCleanDataAsync(wavyId, validationJson);

					if (!isValid)
					{
						Console.WriteLine($"[WARNING] Validation failed for {dataType} data from {wavyId}");
						if (verboseMode)
						{
							Console.WriteLine($"  Invalid data: {validationJson}");
						}
						return; // Skip invalid data
					}

					Console.WriteLine($"Validation succeeded for {dataType} data from {wavyId}");

					// Create data to store - ensure proper structure with capitalized field names
					var storeDataItem = new Dictionary<string, object>
			{
				{ "DataType", dataType },
				{ "Value", value },  // Use capitalized "Value" to match what Server expects
                { "WavyId", wavyId },  // Use capitalized "WavyId" to match what Server expects
                { "Timestamp", DateTime.UtcNow }
			};

					string json = JsonSerializer.Serialize(storeDataItem);

					// Store the data
					if (!dataStore.TryGetValue(dataType, out var container))
					{
						container = new ConcurrentBag<string>();
						dataStore[dataType] = container;
					}

					container.Add(json);

					Console.WriteLine($"Stored {dataType} data point via ZeroMQ pub-sub with value {value}");
					if (verboseMode)
					{
						Console.WriteLine($"  Stored data: {json}");
					}
				}
				catch (Exception valEx)
				{
					Console.WriteLine($"Data validation error: {valEx.Message}");

					// Fall back to creating a simple data structure for storage without validation
					var simpleDataItem = new Dictionary<string, object>
			{
				{ "DataType", dataType },
				{ "Value", value },  // Use capitalized "Value"
                { "WavyId", wavyId },  // Use capitalized "WavyId"
                { "Timestamp", DateTime.UtcNow }
			};

					string json = JsonSerializer.Serialize(simpleDataItem);

					// Get or create container for this data type
					if (!dataStore.TryGetValue(dataType, out var container))
					{
						container = new ConcurrentBag<string>();
						dataStore[dataType] = container;
					}

					// Store unvalidated data
					container.Add(json);

					Console.WriteLine($"Stored unvalidated {dataType} data point via ZeroMQ pub-sub with value {value}");
					if (verboseMode)
					{
						Console.WriteLine($"  Stored data: {json}");
					}
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error processing ZeroMQ data for {dataType}: {ex.Message}");
				if (verboseMode)
				{
					Console.WriteLine($"Stack trace: {ex.StackTrace}");
				}
			}
		}

		/// <summary>
		/// Cleans up ZeroMQ resources
		/// </summary>
		public static void Cleanup()
		{
			foreach (var socket in _subscribers.Values)
			{
				socket.Dispose();
			}

			_subscribers.Clear();
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