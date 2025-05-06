using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;

/// <summary>
/// Edge component of the OceanMonitor system that generates sensor data and transmits it to the Aggregator.
/// Operates in either HTTP-like mode (connect-send-disconnect) or continuous connection mode.
/// </summary>
public class Wavy
{
	#region Protocol Constants

	/// <summary>
	/// Message codes used in the communication protocol between system components
	/// </summary>
	private static class MessageCodes
	{
		// Wavy to Aggregator communication codes
		public const int WavyConnect = 101;
		public const int WavyDataHttpInitial = 200;
		public const int WavyDataInitial = 201;
		public const int WavyDataResend = 202;
		public const int WavyDisconnect = 501;

		// Response codes
		public const int AggregatorConfirmation = 401;
	}

	#endregion

	#region Configuration

	/// <summary>
	/// Manages runtime configuration settings for the Wavy client
	/// </summary>
	private static class Config
	{
		// Network configuration
		public static string AggregatorIp { get; set; } = "127.0.0.1";
		public static int AggregatorPort { get; set; } = 5000;

		// Operation mode
		public static bool UseHttpLikeMode { get; set; } = false;

		// Communication parameters
		public static int ConfirmationTimeoutMs { get; set; } = 5000;
		public static int DataTransmissionIntervalMs { get; set; } = 7000;
		public static int RetryIntervalMs { get; set; } = 2000;
		public static int MaxRetryAttempts { get; set; } = 3;

		// Sensor simulation intervals
		public static int TemperatureIntervalMs { get; private set; } = 6000;
		public static int WindSpeedIntervalMs { get; private set; } = 3000;
		public static int FrequencyIntervalMs { get; private set; } = 2000;
		public static int DecibelsIntervalMs { get; private set; } = 1000;

		// Debug options
		public static bool VerboseMode { get; set; } = false;

		// Configuration storage
		public static readonly string ConfigFilePath = "wavy.config.json";

		/// <summary>
		/// Static constructor that loads configuration from file if available
		/// </summary>
		static Config()
		{
			LoadConfigFromFile();
		}

		/// <summary>
		/// Loads configuration settings from JSON file
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
		/// <param name="config">Dictionary containing configuration values</param>
		private static void ApplyConfigValues(Dictionary<string, object> config)
		{
			ExtractStringValue(config, "AggregatorIp", value => AggregatorIp = value);
			ExtractIntValue(config, "AggregatorPort", value => AggregatorPort = value);
			ExtractBoolValue(config, "UseHttpLikeMode", value => UseHttpLikeMode = value);
			ExtractIntValue(config, "ConfirmationTimeoutMs", value => ConfirmationTimeoutMs = value);
			ExtractIntValue(config, "DataTransmissionIntervalMs", value => DataTransmissionIntervalMs = value);
			ExtractIntValue(config, "RetryIntervalMs", value => RetryIntervalMs = value);
			ExtractIntValue(config, "MaxRetryAttempts", value => MaxRetryAttempts = value);
			ExtractIntValue(config, "TemperatureIntervalMs", value => TemperatureIntervalMs = value);
			ExtractIntValue(config, "WindSpeedIntervalMs", value => WindSpeedIntervalMs = value);
			ExtractIntValue(config, "FrequencyIntervalMs", value => FrequencyIntervalMs = value);
			ExtractIntValue(config, "DecibelsIntervalMs", value => DecibelsIntervalMs = value);
			ExtractBoolValue(config, "VerboseMode", value => VerboseMode = value);
		}

		/// <summary>
		/// Extracts a string value from configuration dictionary
		/// </summary>
		private static void ExtractStringValue(Dictionary<string, object> config, string key, Action<string> setter)
		{
			if (config.TryGetValue(key, out var value) &&
				value is JsonElement element &&
				element.ValueKind == JsonValueKind.String)
			{
				setter(element.GetString());
			}
		}

		/// <summary>
		/// Extracts an integer value from configuration dictionary
		/// </summary>
		private static void ExtractIntValue(Dictionary<string, object> config, string key, Action<int> setter)
		{
			if (config.TryGetValue(key, out var value) &&
				value is JsonElement element &&
				element.ValueKind == JsonValueKind.Number)
			{
				setter(element.GetInt32());
			}
		}

		/// <summary>
		/// Extracts a boolean value from configuration dictionary
		/// </summary>
		private static void ExtractBoolValue(Dictionary<string, object> config, string key, Action<bool> setter)
		{
			if (config.TryGetValue(key, out var value) && value is JsonElement element)
			{
				if (element.ValueKind == JsonValueKind.True)
					setter(true);
				else if (element.ValueKind == JsonValueKind.False)
					setter(false);
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
					{ "AggregatorIp", AggregatorIp },
					{ "AggregatorPort", AggregatorPort },
					{ "UseHttpLikeMode", UseHttpLikeMode },
					{ "ConfirmationTimeoutMs", ConfirmationTimeoutMs },
					{ "DataTransmissionIntervalMs", DataTransmissionIntervalMs },
					{ "RetryIntervalMs", RetryIntervalMs },
					{ "MaxRetryAttempts", MaxRetryAttempts },
					{ "TemperatureIntervalMs", TemperatureIntervalMs },
					{ "WindSpeedIntervalMs", WindSpeedIntervalMs },
					{ "FrequencyIntervalMs", FrequencyIntervalMs },
					{ "DecibelsIntervalMs", DecibelsIntervalMs },
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
		}
	}

	#endregion

	#region State Management

	// Unique identifier for this Wavy instance
	private static readonly string wavyId = Guid.NewGuid().ToString();

	// Registry of active worker threads
	private static readonly ConcurrentDictionary<Thread, DateTime> activeThreads =
		new ConcurrentDictionary<Thread, DateTime>();

	// Application lifecycle control
	private static bool isRunning = true;

	// Thread-local random number generator for thread safety
	[ThreadStatic]
	private static Random random;

	// Thread-safe access to random number generator
	private static Random Random => random ??= new Random(
		Thread.CurrentThread.ManagedThreadId * unchecked((int)DateTime.Now.Ticks));

	#endregion

	#region Entry Point and Main Thread Management

	/// <summary>
	/// Application entry point - initializes components and starts sensor data generation
	/// </summary>
	/// <param name="args">Command line arguments for configuration</param>
	public static void Main(string[] args)
	{
		AppDomain.CurrentDomain.ProcessExit += OnProcessExit;

		ParseCommandLineArgs(args);
		DisplayStartupInfo();
		InitializeThreads();

		// Keep main thread alive until shutdown
		while (isRunning)
		{
			Thread.Sleep(1000);
		}

		ShutdownThreads();
	}

	/// <summary>
	/// Parses command line arguments to configure the Wavy client
	/// </summary>
	/// <param name="args">Command line arguments array</param>
	private static void ParseCommandLineArgs(string[] args)
	{
		for (int i = 0; i < args.Length; i++)
		{
			string arg = args[i].ToLower();

			switch (arg)
			{
				case "--http":
				case "-h":
					Config.UseHttpLikeMode = true;
					break;
				case "--continuous":
				case "-c":
					Config.UseHttpLikeMode = false;
					break;
				case "--interval":
				case "-i":
					if (i + 1 < args.Length && int.TryParse(args[i + 1], out int interval))
					{
						Config.DataTransmissionIntervalMs = interval;
						i++; // Skip the value
					}
					break;
				case "--ip":
				case "-ip":
					if (i + 1 < args.Length)
					{
						Config.AggregatorIp = args[i + 1];
						i++; // Skip the value
					}
					break;
				case "--port":
				case "-p":
					if (i + 1 < args.Length && int.TryParse(args[i + 1], out int port))
					{
						Config.AggregatorPort = port;
						i++; // Skip the value
					}
					break;
			}
		}
	}

	/// <summary>
	/// Displays startup information and available commands
	/// </summary>
	private static void DisplayStartupInfo()
	{
		Console.WriteLine($"Wavy client starting (ID: {wavyId})");
		Console.WriteLine($"Mode: {(Config.UseHttpLikeMode ? "HTTP-like" : "Continuous connection")}");
		Console.WriteLine($"Connecting to Aggregator at {Config.AggregatorIp}:{Config.AggregatorPort}");
		DisplayCommandMenu();
	}

	/// <summary>
	/// Displays available commands in the console
	/// </summary>
	private static void DisplayCommandMenu()
	{
		Console.WriteLine("\nAvailable Commands:");
		Console.WriteLine("Press 'M' to toggle connection mode");
		Console.WriteLine("Press 'V' to toggle verbose mode");
		Console.WriteLine("Press 'T' to show active threads");
		Console.WriteLine("Press 'S' to save current configuration");
		Console.WriteLine("Press 'W' to clear screen");
		Console.WriteLine("Press 'Q' to quit");
	}

	/// <summary>
	/// Initializes and starts all worker threads
	/// </summary>
	private static void InitializeThreads()
	{
		// Create a data queue shared by all sensor threads
		var dataQueue = new ConcurrentQueue<Dictionary<string, object>>();

		// Start keyboard monitor thread
		StartKeyboardMonitor();

		// Start sensor data generator threads
		StartSensorThreads(dataQueue);

		// Start appropriate data sender thread based on mode
		StartDataSenderThread(dataQueue);
	}

	/// <summary>
	/// Starts the keyboard monitoring thread
	/// </summary>
	private static void StartKeyboardMonitor()
	{
		Thread keyboardThread = new Thread(MonitorKeyboard)
		{
			IsBackground = true,
			Name = "KeyboardMonitor"
		};
		RegisterThread(keyboardThread);
		keyboardThread.Start();
	}

	/// <summary>
	/// Starts threads for generating different types of sensor data
	/// </summary>
	/// <param name="dataQueue">The shared queue for storing generated data</param>
	private static void StartSensorThreads(ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		// Temperature sensor thread
		Thread temperatureThread = new Thread(() =>
			GenerateSensorData("Temperature", Config.TemperatureIntervalMs, dataQueue))
		{
			IsBackground = true,
			Name = "TemperatureSensor"
		};

		// Wind speed sensor thread
		Thread windSpeedThread = new Thread(() =>
			GenerateSensorData("WindSpeed", Config.WindSpeedIntervalMs, dataQueue))
		{
			IsBackground = true,
			Name = "WindSpeedSensor"
		};

		// Frequency sensor thread
		Thread frequencyThread = new Thread(() =>
			GenerateSensorData("Frequency", Config.FrequencyIntervalMs, dataQueue))
		{
			IsBackground = true,
			Name = "FrequencySensor"
		};

		// Decibels sensor thread
		Thread decibelsThread = new Thread(() =>
			GenerateSensorData("Decibels", Config.DecibelsIntervalMs, dataQueue))
		{
			IsBackground = true,
			Name = "DecibelsSensor"
		};

		// Register and start all sensor threads
		RegisterThread(temperatureThread);
		RegisterThread(windSpeedThread);
		RegisterThread(frequencyThread);
		RegisterThread(decibelsThread);

		temperatureThread.Start();
		windSpeedThread.Start();
		frequencyThread.Start();
		decibelsThread.Start();
	}

	/// <summary>
	/// Starts the appropriate data sender thread based on the configured mode
	/// </summary>
	/// <param name="dataQueue">The shared queue containing sensor data</param>
	private static void StartDataSenderThread(ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		Thread dataSenderThread;

		if (Config.UseHttpLikeMode)
		{
			dataSenderThread = new Thread(() => RunHttpLikeMode(dataQueue))
			{
				Name = "HttpLikeModeSender"
			};
		}
		else
		{
			dataSenderThread = new Thread(() => RunContinuousConnectionMode(dataQueue))
			{
				Name = "ContinuousConnectionSender"
			};
		}

		RegisterThread(dataSenderThread);
		dataSenderThread.Start();
	}

	/// <summary>
	/// Called when process is exiting to ensure graceful shutdown
	/// </summary>
	private static void OnProcessExit(object sender, EventArgs e)
	{
		isRunning = false;
		Console.WriteLine("Process terminating, shutting down threads...");
		ShutdownThreads();
	}

	/// <summary>
	/// Shuts down all threads and performs cleanup operations
	/// </summary>
	private static void ShutdownThreads()
	{
		isRunning = false;
		Console.WriteLine("Shutting down threads...");

		WaitForThreadsToTerminate();
		SendFinalDisconnection();

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
				if (!thread.Join(2000)) // Wait up to 2 seconds
				{
					Console.WriteLine($"Thread '{thread.Name}' did not terminate gracefully.");
				}
			}
		}
	}

	/// <summary>
	/// Sends final disconnection message if in continuous connection mode
	/// </summary>
	private static void SendFinalDisconnection()
	{
		if (!Config.UseHttpLikeMode)
		{
			try
			{
				using (TcpClient client = new TcpClient(Config.AggregatorIp, Config.AggregatorPort))
				using (NetworkStream stream = client.GetStream())
				{
					SendDisconnection(stream);
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error sending disconnection message: {ex.Message}");
			}
		}
	}


	/// <summary>
	/// Registers a thread for lifecycle management
	/// </summary>
	/// <param name="thread">The thread to register</param>
	private static void RegisterThread(Thread thread)
	{
		activeThreads.TryAdd(thread, DateTime.Now);
	}

	/// <summary>
	/// Unregisters a thread from lifecycle management
	/// </summary>
	/// <param name="thread">The thread to unregister</param>
	private static void UnregisterThread(Thread thread)
	{
		activeThreads.TryRemove(thread, out _);
	}

	#endregion

	#region User Interface

	/// <summary>
	/// Monitors keyboard input for user commands and processes them
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
				Thread.Sleep(100);
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
	/// Processes keyboard commands
	/// </summary>
	/// <param name="key">The key that was pressed</param>
	private static void ProcessKeyCommand(ConsoleKey key)
	{
		switch (key)
		{
			case ConsoleKey.M:
				Config.UseHttpLikeMode = !Config.UseHttpLikeMode;
				Console.WriteLine($"Connection mode changed to: {(Config.UseHttpLikeMode ? "HTTP-like" : "Continuous connection")}");
				Console.WriteLine("Restart application for the change to take effect.");
				break;
			case ConsoleKey.V:
				Config.VerboseMode = !Config.VerboseMode;
				Console.WriteLine($"Verbose mode: {(Config.VerboseMode ? "ON" : "OFF")}");
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
				Console.WriteLine("Shutting down Wavy client...");
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
	/// Displays information about active threads and their runtime
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
		Console.WriteLine($"Wavy client starting (ID: {wavyId})");
		Console.WriteLine($"Mode: {(Config.UseHttpLikeMode ? "HTTP-like" : "Continuous connection")}");
		Console.WriteLine($"Connecting to Aggregator at {Config.AggregatorIp}:{Config.AggregatorPort}");
		Console.WriteLine("\nAvailable Commands:");
		Console.WriteLine("Press 'M' to toggle connection mode");
		Console.WriteLine("Press 'V' to toggle verbose mode");
		Console.WriteLine("Press 'T' to show active threads");
		Console.WriteLine("Press 'S' to save current configuration");
		Console.WriteLine("Press 'W' to clear screen");
		Console.WriteLine("Press 'Q' to quit");
	}

	#endregion

	#region Sensor Data Generation

	/// <summary>
	/// Generates a random sensor value within a specified range
	/// </summary>
	/// <param name="max">Maximum value (inclusive)</param>
	/// <param name="min">Minimum value (inclusive)</param>
	/// <returns>Random float value between min and max</returns>
	private static float GenerateRandomValue(int max, int min)
	{
		return (float)Random.NextDouble() * (max - min) + min;
	}

	/// <summary>
	/// Creates a data structure representing a sensor reading
	/// </summary>
	/// <param name="dataType">The type of sensor data (e.g., Temperature, WindSpeed)</param>
	/// <param name="value">The sensor reading value</param>
	/// <returns>Dictionary containing the sensor data</returns>
	private static Dictionary<string, object> CreateSensorData(string dataType, float value)
	{
		return new Dictionary<string, object>
		{
			{ "DataType", dataType },
			{ "Value", (long)value } // Cast to long to match Aggregator expectation
        };
	}

	/// <summary>
	/// Continuously generates sensor data at the specified interval
	/// </summary>
	/// <param name="dataType">The type of sensor data to generate</param>
	/// <param name="interval">Interval between readings in milliseconds</param>
	/// <param name="dataQueue">Queue to store the generated data</param>
	private static void GenerateSensorData(string dataType, int interval, ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		try
		{
			while (isRunning)
			{
				// Generate sensor reading and add to queue
				float value = GenerateRandomValue(40, 0);
				var data = CreateSensorData(dataType, value);
				dataQueue.Enqueue(data);

				// Log if verbose mode enabled
				if (Config.VerboseMode)
				{
					Console.WriteLine($"Generated {dataType} sensor data: {value}");
				}

				// Wait for the next interval, checking isRunning periodically
				for (int i = 0; i < interval && isRunning; i += 100)
				{
					Thread.Sleep(100);
				}
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error in {dataType} sensor thread: {ex.Message}");
		}
		finally
		{
			UnregisterThread(Thread.CurrentThread);
		}
	}

	#endregion

	#region HTTP-Like Connection Mode

	/// <summary>
	/// Runs the Wavy client in HTTP-like mode where a new connection is established
	/// for each data transmission
	/// </summary>
	/// <param name="dataQueue">Queue containing sensor data to be transmitted</param>
	private static void RunHttpLikeMode(ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		try
		{
			Console.WriteLine("Running in HTTP-like mode");

			while (isRunning)
			{
				// Send data to aggregator
				SendAggregatedSensorDataHttpLike(dataQueue);

				// Wait for the next transmission interval, checking isRunning periodically
				for (int i = 0; i < Config.DataTransmissionIntervalMs && isRunning; i += 100)
				{
					Thread.Sleep(100);
				}
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error in HTTP-like mode thread: {ex.Message}");
		}
		finally
		{
			UnregisterThread(Thread.CurrentThread);
		}
	}

	/// <summary>
	/// Sends aggregated sensor data to the Aggregator using an HTTP-like pattern
	/// (connect, send, receive confirmation, disconnect)
	/// </summary>
	/// <param name="dataQueue">Queue containing sensor data to be transmitted</param>
	private static void SendAggregatedSensorDataHttpLike(ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		try
		{
			// Aggregate data from queue
			var aggregatedData = PrepareAggregatedData(dataQueue);

			// Skip sending if no data available
			if (aggregatedData["Data"] is List<Dictionary<string, object>> dataList && dataList.Count == 0)
			{
				if (Config.VerboseMode)
				{
					Console.WriteLine("No data to send");
				}
				return;
			}

			// Serialize data to JSON
			string json = JsonSerializer.Serialize(aggregatedData);

			// Establish connection to aggregator
			using (TcpClient client = new TcpClient(Config.AggregatorIp, Config.AggregatorPort))
			{
				Console.WriteLine($"Connected to Aggregator at {Config.AggregatorIp}:{Config.AggregatorPort}");

				using (NetworkStream stream = client.GetStream())
				{
					// Attempt to send data with retries if needed
					SendDataWithRetries(stream, json, MessageCodes.WavyDataHttpInitial);
				}
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error in HTTP-like mode data transmission: {ex.Message}");
		}
	}

	/// <summary>
	/// Sends data with retry mechanism until confirmation is received or retry limit reached
	/// </summary>
	/// <param name="stream">The network stream to send data through</param>
	/// <param name="json">The JSON data to send</param>
	/// <param name="initialCode">The message code for initial send</param>
	/// <returns>True if data was successfully sent and confirmed, false otherwise</returns>
	private static bool SendDataWithRetries(NetworkStream stream, string json, int initialCode)
	{
		bool confirmed = false;
		int retryCount = 0;

		// Send data with retry mechanism until confirmation is received or max retries reached
		while (!confirmed && retryCount < Config.MaxRetryAttempts && isRunning)
		{
			try
			{
				// Use appropriate message code (initial or resend)
				int messageCode = (retryCount == 0) ? initialCode : MessageCodes.WavyDataResend;

				// Send data
				SendMessage(stream, json, messageCode);
				Console.WriteLine("Sent aggregated sensor data to Aggregator.");

				if (Config.VerboseMode)
				{
					Console.WriteLine($"Data: {json}");
				}

				// Wait for confirmation
				confirmed = WaitForConfirmation(stream, MessageCodes.AggregatorConfirmation);

				// If not confirmed, prepare for retry
				if (!confirmed)
				{
					retryCount++;
					Console.WriteLine($"No confirmation received, retry {retryCount}/{Config.MaxRetryAttempts}...");
					Thread.Sleep(Config.RetryIntervalMs);
				}
			}
			catch (Exception ex)
			{
				retryCount++;
				Console.WriteLine($"Error sending data: {ex.Message}");
				if (retryCount < Config.MaxRetryAttempts)
				{
					Console.WriteLine($"Retrying in {Config.RetryIntervalMs / 1000} seconds...");
					Thread.Sleep(Config.RetryIntervalMs);
				}
			}
		}

		// Log if all attempts failed
		if (!confirmed && retryCount >= Config.MaxRetryAttempts)
		{
			Console.WriteLine($"Failed to send data after {Config.MaxRetryAttempts} attempts. Data discarded.");
		}

		return confirmed;
	}

	#endregion

	#region Continuous Connection Mode

	/// <summary>
	/// Runs the Wavy client in continuous connection mode where a single
	/// connection is maintained for multiple data transmissions
	/// </summary>
	/// <param name="dataQueue">Queue containing sensor data to be transmitted</param>
	private static void RunContinuousConnectionMode(ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		try
		{
			Console.WriteLine("Running in continuous connection mode");

			while (isRunning)
			{
				try
				{
					// Establish connection to aggregator
					using (TcpClient client = new TcpClient(Config.AggregatorIp, Config.AggregatorPort))
					{
						Console.WriteLine($"Connected to Aggregator at {Config.AggregatorIp}:{Config.AggregatorPort}");

						using (NetworkStream stream = client.GetStream())
						{
							// Send initial connection request
							SendConnection(stream);

							// Send data periodically while connection is maintained
							while (isRunning && client.Connected)
							{
								SendAggregatedSensorDataContinuous(stream, dataQueue);

								// Wait for next transmission interval
								for (int i = 0; i < Config.DataTransmissionIntervalMs && isRunning; i += 100)
								{
									Thread.Sleep(100);
								}
							}
						}
					}
				}
				catch (Exception ex)
				{
					Console.WriteLine($"Connection error: {ex.Message}");
					Console.WriteLine("Retrying connection in 5 seconds...");
					Thread.Sleep(5000);
				}
			}

			// Send disconnection notification when exiting
			SendFinalDisconnection();
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error in continuous connection mode thread: {ex.Message}");
		}
		finally
		{
			UnregisterThread(Thread.CurrentThread);
		}
	}

	/// <summary>
	/// Sends aggregated sensor data over an existing connection in continuous mode
	/// </summary>
	/// <param name="stream">The network stream to send data through</param>
	/// <param name="dataQueue">Queue containing sensor data to be transmitted</param>
	private static void SendAggregatedSensorDataContinuous(NetworkStream stream, ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		try
		{
			// Prepare aggregated data from queue
			var aggregatedData = PrepareAggregatedData(dataQueue);

			// Skip if no data to send
			if (aggregatedData["Data"] is List<Dictionary<string, object>> dataList && dataList.Count == 0)
			{
				if (Config.VerboseMode)
				{
					Console.WriteLine("No data to send");
				}
				return;
			}

			// Serialize the data to JSON
			string json = JsonSerializer.Serialize(aggregatedData);

			bool confirmed = false;
			int retryCount = 0;

			// Send data with retry mechanism until confirmation is received
			while (!confirmed && retryCount < Config.MaxRetryAttempts && isRunning)
			{
				try
				{
					// Use appropriate message code based on retry count
					int messageCode = (retryCount == 0) ?
						MessageCodes.WavyDataInitial :
						MessageCodes.WavyDataResend;

					SendMessage(stream, json, messageCode);
					Console.WriteLine("Sent aggregated sensor data to Aggregator.");

					if (Config.VerboseMode)
					{
						Console.WriteLine($"Data: {json}");
					}

					confirmed = WaitForConfirmation(stream, MessageCodes.AggregatorConfirmation);

					// Handle retry if not confirmed
					if (!confirmed)
					{
						retryCount++;
						Console.WriteLine($"No confirmation received, retry {retryCount}/{Config.MaxRetryAttempts}...");
						Thread.Sleep(Config.RetryIntervalMs);
					}
				}
				catch (Exception ex)
				{
					retryCount++;
					Console.WriteLine($"Error sending data: {ex.Message}");
					if (retryCount < Config.MaxRetryAttempts)
					{
						Console.WriteLine($"Retrying in {Config.RetryIntervalMs / 1000} seconds...");
						Thread.Sleep(Config.RetryIntervalMs);
					}
					else
					{
						throw; // Re-throw to signal connection loss
					}
				}
			}

			// Signal connection failure if max retries reached
			if (!confirmed && retryCount >= Config.MaxRetryAttempts)
			{
				Console.WriteLine($"Failed to send data after {Config.MaxRetryAttempts} attempts. Data discarded.");
				throw new Exception("Communication with Aggregator failed");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error in continuous connection mode data transmission: {ex.Message}");
			throw; // Re-throw to signal connection loss
		}
	}

	#endregion

	#region Network Utilities

	/// <summary>
	/// Aggregates and prepares sensor data from the queue for transmission
	/// </summary>
	/// <param name="dataQueue">Queue containing sensor data to be transmitted</param>
	/// <returns>Dictionary containing aggregated data ready for serialization</returns>
	private static Dictionary<string, object> PrepareAggregatedData(ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		// Create container for aggregated data
		var aggregatedData = new Dictionary<string, object>
		{
			{ "WavyId", wavyId },
			{ "Data", new List<Dictionary<string, object>>() }
		};

		var dataList = (List<Dictionary<string, object>>)aggregatedData["Data"];

		// Dequeue and process all available items
		while (dataQueue.TryDequeue(out var data))
		{
			// Try to find existing data of the same type to aggregate
			var existingData = dataList.Find(d => d["DataType"].ToString() == data["DataType"].ToString());
			if (existingData != null)
			{
				// Aggregate values for the same data type
				existingData["Value"] = (long)existingData["Value"] + (long)data["Value"];
			}
			else
			{
				// Add new data type to the list
				dataList.Add(data);
			}
		}

		return aggregatedData;
	}

	/// <summary>
	/// Sends a connection request to the Aggregator with the Wavy ID
	/// </summary>
	/// <param name="stream">The network stream to send the request through</param>
	private static void SendConnection(NetworkStream stream)
	{
		try
		{
			// Prepare message with code and Wavy ID
			byte[] codeBytes = BitConverter.GetBytes(MessageCodes.WavyConnect);
			byte[] idBytes = Encoding.UTF8.GetBytes(wavyId);

			// Combine code and ID into a single message
			byte[] data = new byte[codeBytes.Length + idBytes.Length];
			Buffer.BlockCopy(codeBytes, 0, data, 0, codeBytes.Length);
			Buffer.BlockCopy(idBytes, 0, data, codeBytes.Length, idBytes.Length);

			// Send the message
			stream.Write(data, 0, data.Length);
			Console.WriteLine($"Sent connection code with ID: {wavyId}");

			// Wait for acknowledgment from Aggregator
			bool confirmed = WaitForConfirmation(stream, MessageCodes.AggregatorConfirmation);
			if (confirmed)
			{
				Console.WriteLine("Connection acknowledged by Aggregator.");
			}
			else
			{
				Console.WriteLine("Connection not acknowledged by Aggregator.");
				throw new Exception("Connection not acknowledged");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error sending connection request: {ex.Message}");
			throw;
		}
	}

	/// <summary>
	/// Sends a disconnection request to the Aggregator
	/// </summary>
	/// <param name="stream">The network stream to send the request through</param>
	private static void SendDisconnection(NetworkStream stream)
	{
		try
		{
			// Prepare message with code and Wavy ID
			byte[] codeBytes = BitConverter.GetBytes(MessageCodes.WavyDisconnect);
			byte[] idBytes = Encoding.UTF8.GetBytes(wavyId);

			// Combine code and ID into a single message
			byte[] data = new byte[codeBytes.Length + idBytes.Length];
			Buffer.BlockCopy(codeBytes, 0, data, 0, codeBytes.Length);
			Buffer.BlockCopy(idBytes, 0, data, codeBytes.Length, idBytes.Length);

			// Send the message
			stream.Write(data, 0, data.Length);
			Console.WriteLine($"Sent disconnection code with ID: {wavyId}");

			// Wait for acknowledgment from Aggregator
			bool confirmed = WaitForConfirmation(stream, MessageCodes.AggregatorConfirmation);
			if (confirmed)
			{
				Console.WriteLine("Disconnection acknowledged by Aggregator.");
			}
			else
			{
				Console.WriteLine("Disconnection not acknowledged by Aggregator.");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error sending disconnection request: {ex.Message}");
			throw;
		}
	}

	/// <summary>
	/// Sends a message with a specific code to the Aggregator
	/// </summary>
	/// <param name="stream">The network stream to send the message through</param>
	/// <param name="message">The message content</param>
	/// <param name="code">The protocol message code</param>
	private static void SendMessage(NetworkStream stream, string message, int code)
	{
		try
		{
			// Prepare message with code and content
			byte[] codeBytes = BitConverter.GetBytes(code);
			byte[] messageBytes = Encoding.UTF8.GetBytes(message);

			// Combine code and content into a single message
			byte[] data = new byte[codeBytes.Length + messageBytes.Length];
			Buffer.BlockCopy(codeBytes, 0, data, 0, codeBytes.Length);
			Buffer.BlockCopy(messageBytes, 0, data, codeBytes.Length, messageBytes.Length);

			// Send the message
			stream.Write(data, 0, data.Length);

			if (Config.VerboseMode)
			{
				Console.WriteLine($"Sent message with code: {code}");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error sending message: {ex.Message}");
			throw;
		}
	}

	/// <summary>
	/// Waits for a confirmation message with the expected code from the Aggregator
	/// </summary>
	/// <param name="stream">The network stream to read from</param>
	/// <param name="expectedCode">The expected confirmation code</param>
	/// <returns>True if the expected code was received, false otherwise</returns>
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

				if (Config.VerboseMode)
				{
					Console.WriteLine($"Received confirmation code: {code}, expected: {expectedCode}");
				}

				return code == expectedCode;
			}

			if (Config.VerboseMode)
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

	#endregion
}

