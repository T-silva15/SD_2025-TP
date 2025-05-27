using NetMQ;
using NetMQ.Sockets;
using Spectre.Console;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Xml;
using System.Xml.Serialization;

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
		public static int TemperatureIntervalMs { get; private set; } = 15000;
		public static int WindSpeedIntervalMs { get; private set; } = 10000;
		public static int FrequencyIntervalMs { get; private set; } = 7000;
		public static int DecibelsIntervalMs { get; private set; } = 5000;

		// Message Format
		public static string MessageFormat { get; set; } = "JSON"; // "JSON" or "XML"

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
			ExtractStringValue(config, "MessageFormat", value => MessageFormat = value);
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
				{ "VerboseMode", VerboseMode },
				{ "MessageFormat", MessageFormat }
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

	#region Serialization

	/// <summary>
	/// Handles serialization and deserialization between different formats
	/// </summary>
	private static class SerializationHelper
	{
		/// <summary>
		/// Serializes data to the currently selected format (JSON or XML)
		/// </summary>
		public static string Serialize(Dictionary<string, object> data)
		{
			return Config.MessageFormat.ToUpperInvariant() == "XML"
				? SerializeToXml(data)
				: SerializeToJson(data);
		}

		/// <summary>
		/// Deserializes data from the specified format
		/// </summary>
		public static Dictionary<string, object> Deserialize(string data, string format = null)
		{
			format ??= Config.MessageFormat;

			return format.ToUpperInvariant() == "XML"
				? DeserializeFromXml(data)
				: DeserializeFromJson(data);
		}

		/// <summary>
		/// Serializes data to JSON
		/// </summary>
		private static string SerializeToJson(Dictionary<string, object> data)
		{
			return JsonSerializer.Serialize(data);
		}

		/// <summary>
		/// Serializes data to XML
		/// </summary>
		private static string SerializeToXml(Dictionary<string, object> data)
		{
			using var stringWriter = new StringWriter();
			using var writer = XmlWriter.Create(stringWriter, new XmlWriterSettings { Indent = true });

			writer.WriteStartDocument();
			writer.WriteStartElement("SensorData");

			// Write WavyId as attribute
			if (data.TryGetValue("WavyId", out var wavyId))
			{
				writer.WriteAttributeString("WavyId", wavyId.ToString());
			}

			// Write all other elements
			foreach (var pair in data)
			{
				if (pair.Key != "WavyId") // Skip WavyId as it's already an attribute
				{
					writer.WriteElementString(pair.Key, pair.Value?.ToString());
				}
			}

			writer.WriteEndElement(); // SensorData
			writer.WriteEndDocument();

			return stringWriter.ToString();
		}

		/// <summary>
		/// Deserializes from JSON format
		/// </summary>
		private static Dictionary<string, object> DeserializeFromJson(string json)
		{
			try
			{
				return JsonSerializer.Deserialize<Dictionary<string, object>>(json);
			}
			catch (Exception ex)
			{
				Log($"JSON deserialization error: {ex.Message}", true);
				return new Dictionary<string, object>();
			}
		}

		/// <summary>
		/// Deserializes from XML format
		/// </summary>
		private static Dictionary<string, object> DeserializeFromXml(string xml)
		{
			var result = new Dictionary<string, object>();

			try
			{
				var doc = new XmlDocument();
				doc.LoadXml(xml);

				var root = doc.DocumentElement;
				if (root == null) return result;

				// Get WavyId from attribute
				if (root.HasAttribute("WavyId"))
				{
					result["WavyId"] = root.GetAttribute("WavyId");
				}

				// Get child elements
				foreach (XmlNode node in root.ChildNodes)
				{
					if (node is XmlElement element)
					{
						result[element.Name] = element.InnerText;
					}
				}
			}
			catch (Exception ex)
			{
				Log($"XML deserialization error: {ex.Message}", true);
			}

			return result;
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

	#region Logging System

	// Log queue to store messages that will be displayed when not in menu mode
	private static readonly ConcurrentQueue<string> logMessages = new ConcurrentQueue<string>();
	private static bool inMenuMode = false;
	private static Thread? logDisplayThread = null;

	/// <summary>
	/// Logs a message to the message queue for display
	/// </summary>
	private static void Log(string message, bool isVerbose = false)
	{
		if (isVerbose && !Config.VerboseMode)
			return;

		string formattedMessage = $"[{DateTime.Now:HH:mm:ss.fff}] {message}";
		logMessages.Enqueue(formattedMessage);

		// Only display immediately if not in menu mode
		if (!inMenuMode && logDisplayThread == null)
		{
			AnsiConsole.MarkupLine($"[gray]{EscapeMarkup(formattedMessage)}[/]");
		}
	}

	/// <summary>
	/// Escapes markup characters in a string for Spectre.Console
	/// </summary>
	private static string EscapeMarkup(string text)
	{
		return text.Replace("[", "[[").Replace("]", "]]");
	}

	/// <summary>
	/// Starts the log display thread that shows logs when not in menu mode
	/// </summary>
	private static void StartLogDisplayThread()
	{
		if (logDisplayThread != null)
			return;

		logDisplayThread = new Thread(() =>
		{
			try
			{
				while (isRunning)
				{
					if (!inMenuMode && logMessages.TryDequeue(out string? message))
					{
						// Use AnsiConsole for richer formatting
						AnsiConsole.MarkupLine($"[gray]{EscapeMarkup(message)}[/]");
					}
					else
					{
						Thread.Sleep(100);
					}
				}
			}
			catch (Exception ex)
			{
				AnsiConsole.MarkupLine($"[red]Error in log display thread: {ex.Message}[/]");
			}
			finally
			{
				UnregisterThread(Thread.CurrentThread);
			}
		})
		{
			IsBackground = true,
			Name = "LogDisplayThread"
		};

		RegisterThread(logDisplayThread);
		logDisplayThread.Start();
	}

	#endregion

	#region Entry Point and Main Thread Management

	/// <summary>
	/// Application entry point - initializes components and starts sensor data generation
	/// </summary>
	/// <param name="args">Command line arguments for configuration</param>
	public static void Main(string[] args)
	{
		AppDomain.CurrentDomain.ProcessExit += OnProcessExit;

		AnsiConsole.Clear();

		// Show a fancy title
		AnsiConsole.Write(
			new FigletText("Wavy")
				.Color(Color.Blue)
				.Centered());

		AnsiConsole.Write(new Rule("[blue]Ocean Monitor System[/]").RuleStyle("grey").Centered());

		ParseCommandLineArgs(args);
		DisplayStartupInfo();
		StartLogDisplayThread();
		InitializeThreads();

		// Keep main thread alive until shutdown
		while (isRunning)
		{
			Thread.Sleep(1000);
		}

		ShutdownThreads();
	}

	/// <summary>
	/// Displays startup information and available commands
	/// </summary>
	private static void DisplayStartupInfo()
	{
		var statusTable = new Table()
			.Border(TableBorder.Rounded)
			.BorderColor(Color.Grey)
			.AddColumn(new TableColumn("[blue]Property[/]"))
			.AddColumn(new TableColumn("[green]Value[/]"));

		statusTable.AddRow("Wavy ID", $"[yellow]{wavyId}[/]");
		statusTable.AddRow("Publisher Address", $"[yellow]{Config.PublisherAddress}[/]");
		statusTable.AddRow("Subscription Manager", $"[yellow]{Config.SubscriptionManagerAddress}[/]");
		statusTable.AddRow("Verbose Mode", Config.VerboseMode ? "[green]ON[/]" : "[grey]OFF[/]");

		AnsiConsole.Write(statusTable);
		AnsiConsole.WriteLine();

		DisplayCommandMenu();
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
	/// Displays available commands in the console using Spectre.Console
	/// </summary>
	private static void DisplayCommandMenu()
	{
		var table = new Table()
			.BorderColor(Color.Grey)
			.Title("[yellow]Available Commands[/]")
			.AddColumn(new TableColumn("[aqua]Key[/]").Centered())
			.AddColumn(new TableColumn("[green]Action[/]"));

		table.AddRow("[yellow]V[/]", "Toggle verbose mode");
		table.AddRow("[yellow]T[/]", "Show active threads");
		table.AddRow("[yellow]F[/]", "Toggle message format (JSON/XML)");
		table.AddRow("[yellow]S[/]", "Save current configuration");
		table.AddRow("[yellow]X[/]", "Delete configuration files");
		table.AddRow("[yellow]W[/]", "Clear screen");
		table.AddRow("[yellow]Q[/]", "Quit");

		AnsiConsole.Write(table);
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
				AnsiConsole.MarkupLine($"Verbose mode: {(Config.VerboseMode ? "[green]ON[/]" : "[grey]OFF[/]")}");
				break;
			case ConsoleKey.F:
				Config.MessageFormat = Config.MessageFormat.ToUpperInvariant() == "JSON" ? "XML" : "JSON";
				AnsiConsole.MarkupLine($"Message format switched to: [green]{Config.MessageFormat}[/]");
				break;
			case ConsoleKey.T:
				ShowActiveThreads();
				break;
			case ConsoleKey.S:
				SaveConfigWithStatus();
				break;
			case ConsoleKey.X:
				DeleteConfigWithStatus();
				break;
			case ConsoleKey.Q:
				AnsiConsole.MarkupLine("[yellow]Shutting down Wavy client...[/]");
				isRunning = false;
				Thread.Sleep(500);
				Environment.Exit(0);
				break;
			case ConsoleKey.W:
				ClearScreenAndShowMenu();
				break;
		}
	}

	/// <summary>
	/// Saves configuration with visual feedback
	/// </summary>
	private static void SaveConfigWithStatus()
	{
		try
		{
			inMenuMode = true;
			AnsiConsole.Status()
				.Start("Saving configuration...", ctx =>
				{
					ctx.Spinner(Spinner.Known.Dots);
					ctx.SpinnerStyle(Style.Parse("green"));

					// Save config
					Config.SaveToFile();

					Thread.Sleep(1000); // Show status for a moment
				});

			AnsiConsole.MarkupLine("[green]Configuration saved successfully.[/]");
		}
		finally
		{
			inMenuMode = false;
		}
	}

	/// <summary>
	/// Deletes configuration with visual feedback
	/// </summary>
	private static void DeleteConfigWithStatus()
	{
		try
		{
			inMenuMode = true;

			if (AnsiConsole.Confirm("Are you sure you want to delete the configuration file?", false))
			{
				AnsiConsole.Status()
					.Start("Deleting configuration...", ctx =>
					{
						ctx.Spinner(Spinner.Known.Dots);
						ctx.SpinnerStyle(Style.Parse("yellow"));

						// Delete config
						Config.DeleteConfigFile();

						Thread.Sleep(1000); // Show status for a moment
					});

				AnsiConsole.MarkupLine("[green]Configuration file deleted. Default settings will be used on restart.[/]");
			}
			else
			{
				AnsiConsole.MarkupLine("[yellow]Operation canceled.[/]");
			}
		}
		finally
		{
			inMenuMode = false;
		}
	}

	/// <summary>
	/// Displays information about active threads and their runtime
	/// </summary>
	private static void ShowActiveThreads()
	{
		try
		{
			inMenuMode = true;
			AnsiConsole.Clear();

			AnsiConsole.Write(new Rule("[yellow]Active Threads[/]").RuleStyle("blue").Centered());

			if (activeThreads.IsEmpty)
			{
				AnsiConsole.MarkupLine("[grey]No active threads.[/]");
			}
			else
			{
				var table = new Table()
					.BorderColor(Color.Blue)
					.AddColumn(new TableColumn("[green]Thread Name[/]"))
					.AddColumn(new TableColumn("[green]ID[/]").Centered())
					.AddColumn(new TableColumn("[green]State[/]"))
					.AddColumn(new TableColumn("[green]Running Time[/]"));

				foreach (var threadEntry in activeThreads)
				{
					Thread thread = threadEntry.Key;
					DateTime startTime = threadEntry.Value;
					TimeSpan runningFor = DateTime.Now - startTime;

					string state = thread.IsAlive ? "[green]Active[/]" : "[grey]Inactive[/]";
					string runtime = $"{runningFor.Hours}h {runningFor.Minutes}m {runningFor.Seconds}s";

					table.AddRow(
						$"[yellow]{thread.Name ?? "Unnamed"}[/]",
						$"[aqua]{thread.ManagedThreadId}[/]",
						state,
						runtime);
				}

				AnsiConsole.Write(table);
			}

			AnsiConsole.WriteLine();
			AnsiConsole.Markup("[yellow]Press any key to return to main menu...[/]");
			Console.ReadKey(true);
			ClearScreenAndShowMenu();
		}
		finally
		{
			inMenuMode = false;
		}
	}

	/// <summary>
	/// Clears the console screen and redisplays the menu
	/// </summary>
	static void ClearScreenAndShowMenu()
	{
		AnsiConsole.Clear();

		AnsiConsole.Write(
			new FigletText("Wavy")
				.Color(Color.Blue)
				.Centered());

		AnsiConsole.Write(new Rule("[blue]Ocean Monitor System[/]").RuleStyle("grey").Centered());

		// Display key info in a status panel
		var statusTable = new Table()
			.Border(TableBorder.Rounded)
			.BorderColor(Color.Grey)
			.AddColumn(new TableColumn("[blue]Property[/]"))
			.AddColumn(new TableColumn("[green]Value[/]"));

		statusTable.AddRow("Wavy ID", $"[yellow]{wavyId}[/]");
		statusTable.AddRow("Publisher Address", $"[yellow]{Config.PublisherAddress}[/]");
		statusTable.AddRow("Subscription Manager", $"[yellow]{Config.SubscriptionManagerAddress}[/]");
		statusTable.AddRow("Message Format", $"[yellow]{Config.MessageFormat}[/]");
		statusTable.AddRow("Verbose Mode", Config.VerboseMode ? "[green]ON[/]" : "[grey]OFF[/]");
		statusTable.AddRow("Active Subscriptions", $"[aqua]{PubSubConfig.SubscribedDataTypes.Count}[/] data types");

		AnsiConsole.Write(statusTable);
		AnsiConsole.WriteLine();

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
					Log($"Generated {dataType} sensor data: {value}", true);
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
			Log($"Error in {dataType} sensor thread: {ex.Message}");
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
					Log($"Publisher socket bound to {Config.PublisherAddress}");

					// Allow time for subscribers to connect
					Thread.Sleep(500);
				}
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

				Log("Subscription system initialized. Waiting for Aggregator subscription requests.");
			}
			catch (Exception ex)
			{
				Log($"Error initializing subscription system: {ex.Message}");
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
					Log($"Added subscription for data type: {dataType}");
				}
			}
			else
			{
				if (SubscribedDataTypes.Remove(dataType))
				{
					Log($"Removed subscription for data type: {dataType}");
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

		/// <summary>
		/// Publishes a data item using ZeroMQ if there are subscribers for its type
		/// </summary>
		public static void PublishData(Dictionary<string, object> data)
		{
			if (data == null) return;

			string? dataType = data["DataType"]?.ToString();
			if (dataType == null) return;

			// Only publish if this data type has subscribers
			if (SubscribedDataTypes.Contains(dataType))
			{
				try
				{
					// Get message format indicator
					string formatIndicator = Config.MessageFormat.ToUpperInvariant() == "XML" ? "XML:" : "JSON:";

					// Serialize the data according to current format
					string serializedData = SerializationHelper.Serialize(data);

					// Prepend with format indicator
					string message = formatIndicator + serializedData;

					lock (_socketLock)
					{
						// ZeroMQ multipart message: first frame is topic, second is data
						PublisherSocket.SendMoreFrame(dataType).SendFrame(message);
					}

					if (Config.VerboseMode)
					{
						Log($"Published {dataType} data with format {Config.MessageFormat}: {serializedData}", true);
					}
				}
				catch (Exception ex)
				{
					Log($"Error publishing {dataType} data: {ex.Message}");
				}
			}
			else if (Config.VerboseMode)
			{
				Log($"Skipped publishing {dataType} - no subscribers", true);
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
					Log($"Subscription listener started on {Config.SubscriptionManagerAddress}");

					while (isRunning)
					{
						try
						{
							// This will block until a message is received
							string message = responseSocket.ReceiveFrameString();

							// Detect format (JSON or XML)
							string format = "JSON"; // Default
							if (message.StartsWith("XML:"))
							{
								format = "XML";
								message = message.Substring(4); // Remove format indicator
							}
							else if (message.StartsWith("JSON:"))
							{
								message = message.Substring(5); // Remove format indicator
							}

							try
							{
								// Use the appropriate deserialization method based on format
								var request = SerializationHelper.Deserialize(message, format);

								if (request != null &&
									request.TryGetValue("action", out object actionObj) &&
									request.TryGetValue("dataType", out object dataTypeObj))
								{
									string action = actionObj.ToString();
									string dataType = dataTypeObj.ToString();

									if (action == "subscribe")
									{
										PubSubConfig.UpdateSubscription(dataType, true);

										// Send response in the same format as the request
										string responseFormat = format.ToUpperInvariant() == "XML" ? "XML" : "JSON";
										string formatIndicator = responseFormat == "XML" ? "XML:" : "JSON:";

										if (responseFormat == "XML")
										{
											responseSocket.SendFrame(formatIndicator + "<Response>Subscribed</Response>");
										}
										else
										{
											responseSocket.SendFrame(formatIndicator + "{\"status\":\"Subscribed\"}");
										}

										Log($"Received subscription request for {dataType} (format: {format})");
									}
									else if (action == "unsubscribe")
									{
										PubSubConfig.UpdateSubscription(dataType, false);

										// Send response in the same format as the request
										string responseFormat = format.ToUpperInvariant() == "XML" ? "XML" : "JSON";
										string formatIndicator = responseFormat == "XML" ? "XML:" : "JSON:";

										if (responseFormat == "XML")
										{
											responseSocket.SendFrame(formatIndicator + "<Response>Subscribed</Response>");
										}
										else
										{
											responseSocket.SendFrame(formatIndicator + "{\"status\":\"Subscribed\"}");
										}

										Log($"Received unsubscription request for {dataType} (format: {format})");
									}
									else
									{
										if (format.ToUpperInvariant() == "XML")
										{
											responseSocket.SendFrame("XML:<Response>Unknown action</Response>");
										}
										else
										{
											responseSocket.SendFrame("JSON:{\"error\":\"Unknown action\"}");
										}
									}
								}
								else
								{
									if (format.ToUpperInvariant() == "XML")
									{
										responseSocket.SendFrame("XML:<Response>Invalid request format</Response>");
									}
									else
									{
										responseSocket.SendFrame("JSON:{\"error\":\"Invalid request format\"}");
									}
								}
							}
							catch (Exception ex)
							{
								Log($"Error processing subscription request: {ex.Message}");

								if (format.ToUpperInvariant() == "XML")
								{
									responseSocket.SendFrame($"XML:<Error>Error processing request: {ex.Message}</Error>");
								}
								else
								{
									responseSocket.SendFrame($"JSON:{{\"error\":\"Error processing request: {ex.Message}\"}}");
								}
							}
						}
						catch (Exception ex)
						{
							if (isRunning)
							{
								Log($"Socket error: {ex.Message}");
								Thread.Sleep(1000); 
							}
						}
					}
				}
			}
			catch (Exception ex)
			{
				Log($"Subscription listener error: {ex.Message}");
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