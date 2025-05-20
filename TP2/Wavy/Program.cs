using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using NetMQ;
using NetMQ.Sockets;

/// <summary>
/// Edge component of the OceanMonitor system that generates sensor data and transmits it to the Aggregator
/// using ZeroMQ publish-subscribe mechanism.
/// </summary>
public class Wavy
{
	#region Configuration

	/// <summary>
	/// Manages runtime configuration settings for the Wavy client
	/// </summary>
	private static class Config
	{
		// ZeroMQ configuration
		public static string PublisherAddress { get; set; } = "tcp://127.0.0.1:5556";
		public static string SubscriptionManagerAddress { get; set; } = "tcp://127.0.0.1:5557";

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
			ExtractStringValue(config, "PublisherAddress", value => PublisherAddress = value);
			ExtractStringValue(config, "SubscriptionManagerAddress", value => SubscriptionManagerAddress = value);
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
					{ "PublisherAddress", PublisherAddress },
					{ "SubscriptionManagerAddress", SubscriptionManagerAddress },
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
				case "--pub":
				case "-p":
					if (i + 1 < args.Length)
					{
						Config.PublisherAddress = args[i + 1];
						i++; // Skip the value
					}
					break;
				case "--sub":
				case "-s":
					if (i + 1 < args.Length)
					{
						Config.SubscriptionManagerAddress = args[i + 1];
						i++; // Skip the value
					}
					break;
				case "--verbose":
				case "-v":
					Config.VerboseMode = true;
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
		Console.WriteLine($"Publishing on {Config.PublisherAddress}");
		Console.WriteLine($"Subscription manager on {Config.SubscriptionManagerAddress}");
		DisplayCommandMenu();
	}

	/// <summary>
	/// Displays available commands in the console
	/// </summary>
	private static void DisplayCommandMenu()
	{
		Console.WriteLine("\nAvailable Commands:");
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
		// Initialize ZeroMQ publisher and load subscriptions
		PubSubConfig.InitializePublisher();  // Initialize the publisher socket once
		PubSubConfig.LoadSubscriptions();

		// Start subscription listener
		StartSubscriptionListener();

		// Start keyboard monitor thread
		StartKeyboardMonitor();

		// Start sensor data generator threads
		StartSensorThreads();
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
	private static void StartSensorThreads()
	{
		// Temperature sensor thread
		Thread temperatureThread = new Thread(() =>
			GenerateSensorData("Temperature", Config.TemperatureIntervalMs))
		{
			IsBackground = true,
			Name = "TemperatureSensor"
		};

		// Wind speed sensor thread
		Thread windSpeedThread = new Thread(() =>
			GenerateSensorData("WindSpeed", Config.WindSpeedIntervalMs))
		{
			IsBackground = true,
			Name = "WindSpeedSensor"
		};

		// Frequency sensor thread
		Thread frequencyThread = new Thread(() =>
			GenerateSensorData("Frequency", Config.FrequencyIntervalMs))
		{
			IsBackground = true,
			Name = "FrequencySensor"
		};

		// Decibels sensor thread
		Thread decibelsThread = new Thread(() =>
			GenerateSensorData("Decibels", Config.DecibelsIntervalMs))
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
				if (!thread.Join(2000)) // Wait up to 2 seconds
				{
					Console.WriteLine($"Thread '{thread.Name}' did not terminate gracefully.");
				}
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

			// debug functions
			case ConsoleKey.F:
				Console.WriteLine("Forcing default subscriptions...");
				PubSubConfig.ForceAddDefaultSubscriptions();
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
		Console.WriteLine($"Publishing on {Config.PublisherAddress}");
		Console.WriteLine($"Subscription manager on {Config.SubscriptionManagerAddress}");
		DisplayCommandMenu();
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
			{ "Value", (long)value }, // Cast to long for consistent data type
            { "WavyId", wavyId }
		};
	}

	/// <summary>
	/// Continuously generates sensor data at the specified interval and publishes it via ZeroMQ
	/// </summary>
	/// <param name="dataType">The type of sensor data to generate</param>
	/// <param name="interval">Interval between readings in milliseconds</param>
	private static void GenerateSensorData(string dataType, int interval)
	{
		try
		{
			while (isRunning)
			{
				// Generate sensor reading
				float value = GenerateRandomValue(40, 0);
				var data = CreateSensorData(dataType, value);

				// Publish via ZeroMQ if there are subscribers
				PubSubConfig.PublishData(data);

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

	#region Pub-Sub Communication

	/// <summary>
	/// Manages publish-subscribe communication settings and operations
	/// </summary>
	private static class PubSubConfig
	{
		// List of all available data types the Wavy can produce
		public static readonly HashSet<string> AvailableDataTypes = new HashSet<string>(new string[]
		{
		"Temperature",
		"WindSpeed",
		"Frequency",
		"Decibels"
		});

		// List of data types that have subscribers (controlled by Aggregator)
		public static HashSet<string> SubscribedDataTypes { get; private set; } = new HashSet<string>();

		// Publisher socket for ZeroMQ communication
		private static PublisherSocket _publisherSocket;
		private static readonly object _socketLock = new object();

		/// <summary>
		/// Initializes the ZeroMQ publisher socket once during application startup
		/// </summary>
		public static void InitializePublisher()
		{
			lock (_socketLock)
			{
				if (_publisherSocket == null)
				{
					_publisherSocket = new PublisherSocket();
					_publisherSocket.Options.SendHighWatermark = 1000;
					_publisherSocket.Bind(Config.PublisherAddress);
					Console.WriteLine($"Publisher socket bound to {Config.PublisherAddress}");

					// Allow time for subscribers to connect
					Thread.Sleep(500);
				}
			}
		}

		/// <summary>
		/// Gets the ZeroMQ publisher socket (initializes it if needed)
		/// </summary>
		public static PublisherSocket PublisherSocket
		{
			get
			{
				lock (_socketLock)
				{
					if (_publisherSocket == null)
					{
						InitializePublisher();
					}
					return _publisherSocket;
				}
			}
		}

		// DEBUG
		public static void ForceAddDefaultSubscriptions()
		{
			// Add all available data types as subscribed
			foreach (var dataType in AvailableDataTypes)
			{
				UpdateSubscription(dataType, true);
				Console.WriteLine($"FORCE MODE: Added subscription for {dataType}");
			}
		}

		/// <summary>
		/// Initializes subscriptions - starts with an empty set. 
		/// All subscriptions will be controlled by the Aggregator.
		/// </summary>
		public static void LoadSubscriptions()
		{
			try
			{
				// Initialize publisher socket first to prevent multiple binds
				InitializePublisher();

				// Start with no subscriptions - Aggregator will send subscription requests
				SubscribedDataTypes.Clear();

				Console.WriteLine("Subscription system initialized. Waiting for Aggregator subscription requests.");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error initializing subscription system: {ex.Message}");
			}
		}

		/// <summary>
		/// Publishes a data item using ZeroMQ if there are subscribers for its type
		/// </summary>
		public static void PublishData(Dictionary<string, object> data)
		{
			if (data == null) return;

			string dataType = data["DataType"].ToString();

			// Only publish if this data type has subscribers
			if (SubscribedDataTypes.Contains(dataType))
			{
				try
				{
					// Add topic (data type) and serialized message
					string json = JsonSerializer.Serialize(data);

					lock (_socketLock)
					{
						// ZeroMQ multipart message: first frame is topic, second is data
						PublisherSocket.SendMoreFrame(dataType).SendFrame(json);
					}

					if (Config.VerboseMode)
					{
						Console.WriteLine($"Published {dataType} data: {json}");
					}
				}
				catch (Exception ex)
				{
					Console.WriteLine($"Error publishing {dataType} data: {ex.Message}");
				}
			}
			else if (Config.VerboseMode)
			{
				Console.WriteLine($"Skipped publishing {dataType} - no subscribers");
			}
		}

		/// <summary>
		/// Updates the list of subscribed data types based on Aggregator messages
		/// </summary>
		public static void UpdateSubscription(string dataType, bool isSubscribing)
		{
			if (isSubscribing)
			{
				if (SubscribedDataTypes.Add(dataType))
				{
					Console.WriteLine($"Added subscription for data type: {dataType}");
				}
			}
			else
			{
				if (SubscribedDataTypes.Remove(dataType))
				{
					Console.WriteLine($"Removed subscription for data type: {dataType}");
				}
			}
		}

		/// <summary>
		/// Clean up ZeroMQ resources
		/// </summary>
		public static void Cleanup()
		{
			lock (_socketLock)
			{
				if (_publisherSocket != null)
				{
					_publisherSocket.Dispose();
					_publisherSocket = null;
				}
			}
		}
	}

	/// <summary>
	/// Listens for subscription requests from Aggregators
	/// </summary>
	private static void StartSubscriptionListener()
	{
		Thread subscriptionThread = new Thread(() =>
		{
			try
			{
				using (var responseSocket = new ResponseSocket())
				{
					responseSocket.Bind(Config.SubscriptionManagerAddress);
					Console.WriteLine($"Subscription listener started on {Config.SubscriptionManagerAddress}");

					while (isRunning)
					{
						try
						{
							// This will block until a message is received
							string message = responseSocket.ReceiveFrameString();

							try
							{
								var request = JsonSerializer.Deserialize<Dictionary<string, object>>(message);
								if (request != null &&
									request.TryGetValue("action", out object actionObj) &&
									request.TryGetValue("dataType", out object dataTypeObj))
								{
									string action = actionObj.ToString();
									string dataType = dataTypeObj.ToString();

									if (action == "subscribe")
									{
										PubSubConfig.UpdateSubscription(dataType, true);
										responseSocket.SendFrame("Subscribed");
										Console.WriteLine($"Received subscription request for {dataType}");
									}
									else if (action == "unsubscribe")
									{
										PubSubConfig.UpdateSubscription(dataType, false);
										responseSocket.SendFrame("Unsubscribed");
										Console.WriteLine($"Received unsubscription request for {dataType}");
									}
									else
									{
										responseSocket.SendFrame("Unknown action");
									}
								}
								else
								{
									responseSocket.SendFrame("Invalid request format");
								}
							}
							catch (Exception ex)
							{
								Console.WriteLine($"Error processing subscription request: {ex.Message}");
								responseSocket.SendFrame("Error processing request");
							}
						}
						catch (Exception ex)
						{
							if (isRunning) // Only log if not shutting down
							{
								Console.WriteLine($"Socket error: {ex.Message}");
								Thread.Sleep(1000); // Avoid tight loop on error
							}
						}
					}
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Subscription listener error: {ex.Message}");
			}
			finally
			{
				UnregisterThread(Thread.CurrentThread);
			}
		})
		{
			IsBackground = true,
			Name = "SubscriptionListener"
		};

		RegisterThread(subscriptionThread);
		subscriptionThread.Start();
	}

	#endregion
}