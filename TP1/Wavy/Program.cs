using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;

/// <summary>
/// This class represents a Wavy client that sends sensor data to the Aggregator.
/// Each instance of Wavy generates random sensor data for different data types.
/// </summary>
public class Wavy
{
	#region Protocol Constants

	/// <summary>
	/// Message codes used in the communication protocol between Wavy, Aggregator, and Server
	/// </summary>
	private static class MessageCodes
	{
		// Wavy to Aggregator codes
		public const int WavyConnect = 101;
		public const int WavyDataHttpInitial = 200;
		public const int WavyDataInitial = 201;
		public const int WavyDataResend = 202;
		public const int WavyDisconnect = 501;

		// Confirmation codes
		public const int AggregatorConfirmation = 401;
	}

	#endregion

	#region Configuration

	/// <summary>
	/// Configuration settings for the Wavy client
	/// </summary>
	private static class Config
	{
		// Network settings
		public static string AggregatorIp { get; set; } = "127.0.0.1";
		public static int AggregatorPort { get; set; } = 5000;

		// Connection mode
		public static bool UseHttpLikeMode { get; set; } = true;

		// Timeouts and intervals
		public static int ConfirmationTimeoutMs { get; set; } = 5000; // 5 seconds
		public static int DataTransmissionIntervalMs { get; set; } = 7000; // For testing
		public static int RetryIntervalMs { get; set; } = 2000;
		public static int MaxRetryAttempts { get; set; } = 3;

		// Sensor generation intervals
		public static int TemperatureIntervalMs { get; private set; } = 6000;
		public static int WindSpeedIntervalMs { get; private set; } = 3000;
		public static int FrequencyIntervalMs { get; private set; } = 2000;
		public static int DecibelsIntervalMs { get; private set; } = 1000;

		// Debug settings
		public static bool VerboseMode { get; set; } = true;

		// File path for configuration
		public static readonly string ConfigFilePath = "wavy.config.json";

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
						if (config.TryGetValue("AggregatorIp", out var ip) && ip is JsonElement ipElement && ipElement.ValueKind == JsonValueKind.String)
							AggregatorIp = ipElement.GetString();

						if (config.TryGetValue("AggregatorPort", out var port) && port is JsonElement portElement && portElement.ValueKind == JsonValueKind.Number)
							AggregatorPort = portElement.GetInt32();

						if (config.TryGetValue("UseHttpLikeMode", out var useHttp) && useHttp is JsonElement useHttpElement)
						{
							if (useHttpElement.ValueKind == JsonValueKind.True)
								UseHttpLikeMode = true;
							else if (useHttpElement.ValueKind == JsonValueKind.False)
								UseHttpLikeMode = false;
						}

						if (config.TryGetValue("ConfirmationTimeoutMs", out var timeout) && timeout is JsonElement timeoutElement && timeoutElement.ValueKind == JsonValueKind.Number)
							ConfirmationTimeoutMs = timeoutElement.GetInt32();

						if (config.TryGetValue("DataTransmissionIntervalMs", out var interval) && interval is JsonElement intervalElement && intervalElement.ValueKind == JsonValueKind.Number)
							DataTransmissionIntervalMs = intervalElement.GetInt32();

						if (config.TryGetValue("RetryIntervalMs", out var retry) && retry is JsonElement retryElement && retryElement.ValueKind == JsonValueKind.Number)
							RetryIntervalMs = retryElement.GetInt32();

						if (config.TryGetValue("MaxRetryAttempts", out var maxRetry) && maxRetry is JsonElement maxRetryElement && maxRetryElement.ValueKind == JsonValueKind.Number)
							MaxRetryAttempts = maxRetryElement.GetInt32();

						if (config.TryGetValue("TemperatureIntervalMs", out var tempInterval) && tempInterval is JsonElement tempIntervalElement && tempIntervalElement.ValueKind == JsonValueKind.Number)
							TemperatureIntervalMs = tempIntervalElement.GetInt32();

						if (config.TryGetValue("WindSpeedIntervalMs", out var windInterval) && windInterval is JsonElement windIntervalElement && windIntervalElement.ValueKind == JsonValueKind.Number)
							WindSpeedIntervalMs = windIntervalElement.GetInt32();

						if (config.TryGetValue("FrequencyIntervalMs", out var freqInterval) && freqInterval is JsonElement freqIntervalElement && freqIntervalElement.ValueKind == JsonValueKind.Number)
							FrequencyIntervalMs = freqIntervalElement.GetInt32();

						if (config.TryGetValue("DecibelsIntervalMs", out var dbInterval) && dbInterval is JsonElement dbIntervalElement && dbIntervalElement.ValueKind == JsonValueKind.Number)
							DecibelsIntervalMs = dbIntervalElement.GetInt32();

						if (config.TryGetValue("VerboseMode", out var verbose) && verbose is JsonElement verboseElement)
						{
							if (verboseElement.ValueKind == JsonValueKind.True)
								VerboseMode = true;
							else if (verboseElement.ValueKind == JsonValueKind.False)
								VerboseMode = false;
						}
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
	}

	#endregion

	#region State Management

	// Unique ID for this Wavy instance
	private static readonly string wavyId = Guid.NewGuid().ToString();

	// Track active threads
	private static readonly ConcurrentDictionary<Thread, DateTime> activeThreads = new ConcurrentDictionary<Thread, DateTime>();

	// Flag to control application termination
	private static bool isRunning = true;

	// Random number generator (thread-safe by using ThreadStatic)
	[ThreadStatic]
	private static Random random;

	// Get thread-safe random instance
	private static Random Random => random ??= new Random(Thread.CurrentThread.ManagedThreadId * unchecked((int)DateTime.Now.Ticks));

	#endregion

	#region Entry Point and Main Thread Management

	/// <summary>
	/// Program entry point. Connects to the Aggregator and starts sending sensor data.
	/// </summary>
	public static void Main(string[] args)
	{
		AppDomain.CurrentDomain.ProcessExit += OnProcessExit;

		// Parse command line arguments
		ParseCommandLineArgs(args);

		// Show startup information
		Console.WriteLine($"Wavy client starting (ID: {wavyId})");
		Console.WriteLine($"Mode: {(Config.UseHttpLikeMode ? "HTTP-like" : "Continuous connection")}");
		Console.WriteLine($"Connecting to Aggregator at {Config.AggregatorIp}:{Config.AggregatorPort}");
		Console.WriteLine("\nAvailable Commands:");
		Console.WriteLine("Press 'M' to toggle connection mode");
		Console.WriteLine("Press 'V' to toggle verbose mode");
		Console.WriteLine("Press 'T' to show active threads");
		Console.WriteLine("Press 'S' to save current configuration");
		Console.WriteLine("Press 'Q' to quit");

		// Start keyboard monitoring thread
		Thread keyboardThread = new Thread(MonitorKeyboard) { IsBackground = true, Name = "KeyboardMonitor" };
		RegisterThread(keyboardThread);
		keyboardThread.Start();

		// Start sensor data threads
		var dataQueue = new ConcurrentQueue<Dictionary<string, object>>();

		Thread temperatureThread = new Thread(() => GenerateSensorData("Temperature", Config.TemperatureIntervalMs, dataQueue))
		{
			IsBackground = true,
			Name = "TemperatureSensor"
		};

		Thread windSpeedThread = new Thread(() => GenerateSensorData("WindSpeed", Config.WindSpeedIntervalMs, dataQueue))
		{
			IsBackground = true,
			Name = "WindSpeedSensor"
		};

		Thread frequencyThread = new Thread(() => GenerateSensorData("Frequency", Config.FrequencyIntervalMs, dataQueue))
		{
			IsBackground = true,
			Name = "FrequencySensor"
		};

		Thread decibelsThread = new Thread(() => GenerateSensorData("Decibels", Config.DecibelsIntervalMs, dataQueue))
		{
			IsBackground = true,
			Name = "DecibelsSensor"
		};

		RegisterThread(temperatureThread);
		RegisterThread(windSpeedThread);
		RegisterThread(frequencyThread);
		RegisterThread(decibelsThread);

		temperatureThread.Start();
		windSpeedThread.Start();
		frequencyThread.Start();
		decibelsThread.Start();

		// Start data sending thread based on connection mode
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

		// Keep main thread alive until program is stopped
		while (isRunning)
		{
			Thread.Sleep(1000);
		}

		ShutdownThreads();
	}

	private static void ParseCommandLineArgs(string[] args)
	{
		for (int i = 0; i < args.Length; i++)
		{
			string arg = args[i].ToLower();

			if (arg == "--http" || arg == "-h")
			{
				Config.UseHttpLikeMode = true;
			}
			else if (arg == "--continuous" || arg == "-c")
			{
				Config.UseHttpLikeMode = false;
			}
			else if ((arg == "--interval" || arg == "-i") && i + 1 < args.Length && int.TryParse(args[i + 1], out int interval))
			{
				Config.DataTransmissionIntervalMs = interval;
				i++; // Skip the next argument as it's the interval value
			}
			else if ((arg == "--ip" || arg == "-ip") && i + 1 < args.Length)
			{
				Config.AggregatorIp = args[i + 1];
				i++; // Skip the next argument as it's the IP
			}
			else if ((arg == "--port" || arg == "-p") && i + 1 < args.Length && int.TryParse(args[i + 1], out int port))
			{
				Config.AggregatorPort = port;
				i++; // Skip the next argument as it's the port
			}
		}
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
				if (!thread.Join(2000)) // Wait up to 2 seconds
				{
					Console.WriteLine($"Thread '{thread.Name}' did not terminate gracefully.");
				}
			}
		}

		// Send disconnection message if not in HTTP-like mode
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

	#region User Interface

	/// <summary>
	/// Monitors keyboard input for commands.
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
						case ConsoleKey.Q:
							Console.WriteLine("Shutting down Wavy client...");
							isRunning = false;
							Thread.Sleep(500); // Give time for message to display
							Environment.Exit(0);
							break;
					}
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

	#region Sensor Data Generation

	/// <summary>
	/// Generates random sensor data for different data types between a given range.
	/// </summary>
	private static float GenerateRandomValue(int max, int min)
	{
		return (float)Random.NextDouble() * (max - min) + min;
	}

	/// <summary>
	/// Creates a dictionary representing sensor data.
	/// </summary>
	private static Dictionary<string, object> CreateSensorData(string dataType, float value)
	{
		return new Dictionary<string, object>
		{
			{ "DataType", dataType },
			{ "Value", (long)value } // Cast to long to match Aggregator expectation
        };
	}

	/// <summary>
	/// Generates sensor data for a specific data type at a specified interval and adds it to the queue.
	/// </summary>
	private static void GenerateSensorData(string dataType, int interval, ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		try
		{
			while (isRunning)
			{
				float value = GenerateRandomValue(40, 0);
				var data = CreateSensorData(dataType, value);
				dataQueue.Enqueue(data);

				if (Config.VerboseMode)
				{
					Console.WriteLine($"Generated {dataType} sensor data: {value}");
				}

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
	/// Runs the Wavy client in HTTP-like mode, connecting for each data transmission.
	/// </summary>
	private static void RunHttpLikeMode(ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		try
		{
			Console.WriteLine("Running in HTTP-like mode");

			while (isRunning)
			{
				SendAggregatedSensorDataHttpLike(dataQueue);

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
	/// Sends aggregated sensor data to the Aggregator in HTTP-like mode.
	/// Connects, sends data, waits for confirmation, and disconnects.
	/// </summary>
	private static void SendAggregatedSensorDataHttpLike(ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		try
		{
			// Prepare aggregated data from queue
			var aggregatedData = PrepareAggregatedData(dataQueue);

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

			using (TcpClient client = new TcpClient(Config.AggregatorIp, Config.AggregatorPort))
			{
				Console.WriteLine($"Connected to Aggregator at {Config.AggregatorIp}:{Config.AggregatorPort}");

				using (NetworkStream stream = client.GetStream())
				{
					bool confirmed = false;
					int retryCount = 0;

					// Send data with retry mechanism until confirmation is received
					while (!confirmed && retryCount < Config.MaxRetryAttempts && isRunning)
					{
						try
						{
							SendMessage(stream, json, MessageCodes.WavyDataHttpInitial);
							Console.WriteLine("Sent aggregated sensor data to Aggregator.");

							if (Config.VerboseMode)
							{
								Console.WriteLine($"Data: {json}");
							}

							confirmed = WaitForConfirmation(stream, MessageCodes.AggregatorConfirmation);

							// If no confirmation received, re-send
							if (!confirmed)
							{
								retryCount++;
								Console.WriteLine($"No confirmation received, retry {retryCount}/{Config.MaxRetryAttempts}...");
								Thread.Sleep(Config.RetryIntervalMs);

								// For retry, use the data resend code
								SendMessage(stream, json, MessageCodes.WavyDataInitial);
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

					if (!confirmed && retryCount >= Config.MaxRetryAttempts)
					{
						Console.WriteLine($"Failed to send data after {Config.MaxRetryAttempts} attempts. Data discarded.");
					}
				}
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error in HTTP-like mode data transmission: {ex.Message}");
		}
	}

	#endregion

	#region Continuous Connection Mode

	/// <summary>
	/// Runs the Wavy client in continuous connection mode.
	/// </summary>
	private static void RunContinuousConnectionMode(ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		try
		{
			Console.WriteLine("Running in continuous connection mode");

			while (isRunning)
			{
				try
				{
					using (TcpClient client = new TcpClient(Config.AggregatorIp, Config.AggregatorPort))
					{
						Console.WriteLine($"Connected to Aggregator at {Config.AggregatorIp}:{Config.AggregatorPort}");

						using (NetworkStream stream = client.GetStream())
						{
							// Send connection request
							SendConnection(stream);

							// Send data periodically
							while (isRunning && client.Connected)
							{
								SendAggregatedSensorDataContinuous(stream, dataQueue);

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

			// Send disconnection on exit
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
				Console.WriteLine($"Error sending disconnection: {ex.Message}");
			}
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
	/// Sends aggregated sensor data to the Aggregator in continuous connection mode.
	/// </summary>
	private static void SendAggregatedSensorDataContinuous(NetworkStream stream, ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		try
		{
			// Prepare aggregated data from queue
			var aggregatedData = PrepareAggregatedData(dataQueue);

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
					SendMessage(stream, json, MessageCodes.WavyDataInitial);
					Console.WriteLine("Sent aggregated sensor data to Aggregator.");

					if (Config.VerboseMode)
					{
						Console.WriteLine($"Data: {json}");
					}

					confirmed = WaitForConfirmation(stream, MessageCodes.AggregatorConfirmation);

					// If no confirmation received, re-send
					if (!confirmed)
					{
						retryCount++;
						Console.WriteLine($"No confirmation received, retry {retryCount}/{Config.MaxRetryAttempts}...");
						Thread.Sleep(Config.RetryIntervalMs);

						// For retry, use the data resend code
						SendMessage(stream, json, MessageCodes.WavyDataResend);
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
	/// Prepares aggregated data from the queue.
	/// </summary>
	private static Dictionary<string, object> PrepareAggregatedData(ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		var aggregatedData = new Dictionary<string, object>
		{
			{ "WavyId", wavyId },
			{ "Data", new List<Dictionary<string, object>>() }
		};

		var dataList = (List<Dictionary<string, object>>)aggregatedData["Data"];

		while (dataQueue.TryDequeue(out var data))
		{
			var existingData = dataList.Find(d => d["DataType"].ToString() == data["DataType"].ToString());
			if (existingData != null)
			{
				existingData["Value"] = (long)existingData["Value"] + (long)data["Value"];
			}
			else
			{
				dataList.Add(data);
			}
		}

		return aggregatedData;
	}

	/// <summary>
	/// Sends a connection request to the Aggregator.
	/// </summary>
	private static void SendConnection(NetworkStream stream)
	{
		try
		{
			byte[] codeBytes = BitConverter.GetBytes(MessageCodes.WavyConnect);
			byte[] idBytes = Encoding.UTF8.GetBytes(wavyId);

			// Create a message with code and ID
			byte[] data = new byte[codeBytes.Length + idBytes.Length];
			Buffer.BlockCopy(codeBytes, 0, data, 0, codeBytes.Length);
			Buffer.BlockCopy(idBytes, 0, data, codeBytes.Length, idBytes.Length);

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
	/// Sends a disconnection request to the Aggregator.
	/// </summary>
	private static void SendDisconnection(NetworkStream stream)
	{
		try
		{
			byte[] codeBytes = BitConverter.GetBytes(MessageCodes.WavyDisconnect);
			byte[] idBytes = Encoding.UTF8.GetBytes(wavyId);

			// Create a message with code and ID
			byte[] data = new byte[codeBytes.Length + idBytes.Length];
			Buffer.BlockCopy(codeBytes, 0, data, 0, codeBytes.Length);
			Buffer.BlockCopy(idBytes, 0, data, codeBytes.Length, idBytes.Length);

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
	/// Sends a message to the Aggregator with code and content.
	/// </summary>
	private static void SendMessage(NetworkStream stream, string message, int code)
	{
		try
		{
			byte[] codeBytes = BitConverter.GetBytes(code);
			byte[] messageBytes = Encoding.UTF8.GetBytes(message);
			byte[] data = new byte[codeBytes.Length + messageBytes.Length];
			Buffer.BlockCopy(codeBytes, 0, data, 0, codeBytes.Length);
			Buffer.BlockCopy(messageBytes, 0, data, codeBytes.Length, messageBytes.Length);
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
	/// Waits for a confirmation message with the expected code.
	/// </summary>
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
