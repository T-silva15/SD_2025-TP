using OceanMonitorSystem;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using NetMQ;
using NetMQ.Sockets;
using Spectre.Console;
using System.Xml;
using System.Xml.Serialization;

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


		// Then modify the ValidateAndCleanDataAsync method to respect this flag
		// Timeouts and intervals
		public static int ConfirmationTimeoutMs { get; private set; } = 10000;
		public static int DataTransmissionIntervalSec { get; private set; } = 30;
		public static int RetryIntervalSec { get; private set; } = 3;
		public static int MaxRetryAttempts { get; private set; } = 5;
		public static int ClientPollIntervalMs { get; private set; } = 100;

		// Message format preference for sending data
		public static string PreferredMessageFormat { get; set; } = "JSON";

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
			if (TryGetJsonValue(config, "PreferredMessageFormat", out JsonElement formatValue) && formatValue.ValueKind == JsonValueKind.String)
				PreferredMessageFormat = formatValue.GetString() ?? "JSON";

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
					{ "VerboseMode", VerboseMode },
					{ "PreferredMessageFormat", PreferredMessageFormat }
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

	#region Logging System

	// Log queue to store messages that will be displayed when not in menu mode
	private static readonly ConcurrentQueue<string> logMessages = new ConcurrentQueue<string>();
	private static bool inMenuMode = false;
	private static Thread? logDisplayThread = null;
	private static Process? grpcServerProcess = null;

	/// <summary>
	/// Logs a message to the message queue instead of directly to console
	/// </summary>
	private static void Log(string message, bool isVerbose = false)
	{
		if (isVerbose && !verboseMode)
			return;

		string formattedMessage = $"[{DateTime.Now:HH:mm:ss.fff}] {message}";
		logMessages.Enqueue(formattedMessage);

		// Only display immediately if not in menu mode and log display thread isn't active
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
	/// Application entry point - initializes components and starts processing threads
	/// </summary>
	public static void Main(string[] args)
	{
		AppDomain.CurrentDomain.ProcessExit += (sender, e) => OnProcessExit(sender!, e);

		// Clear the console and show a nice splash screen
		AnsiConsole.Clear();
		AnsiConsole.Write(
			new FigletText("Aggregator")
				.Color(Color.Aqua)
				.Centered());

		AnsiConsole.Write(new Rule("[blue]Ocean Monitor System[/]").RuleStyle("grey").Centered());

		AnsiConsole.MarkupLine($"[bold aqua]Aggregator[/] [yellow]{aggregatorId}[/] [bold green]starting...[/]");
		AnsiConsole.MarkupLine($"Receiving data via [blue]ZeroMQ[/], sending to [green]{Config.ServerIp}:{Config.ServerPort}[/]");

		StartLogDisplayThread();
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
		table.AddRow("[yellow]D[/]", "Show current data store");
		table.AddRow("[yellow]T[/]", "Show active threads");
		table.AddRow("[yellow]M[/]", "Manage subscriptions");
		table.AddRow("[yellow]F[/]", "Toggle message format (JSON/XML)");
		table.AddRow("[yellow]S[/]", "Save current configuration");
		table.AddRow("[yellow]X[/]", "Delete configuration and subscription files");
		table.AddRow("[yellow]R[/]", "Run Rust gRPC validation server");
		table.AddRow("[yellow]K[/]", "Kill Rust gRPC validation server");
		table.AddRow("[yellow]W[/]", "Clear screen");
		table.AddRow("[yellow]Q[/]", "Quit");

		AnsiConsole.Write(table);
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
		Log("Process terminating, shutting down threads...");

		// Kill the Rust server if it's running
		if (grpcServerProcess != null && !grpcServerProcess.HasExited)
		{
			try
			{
				grpcServerProcess.Kill(true);
			}
			catch (Exception ex)
			{
				Log($"Error shutting down Rust gRPC server: {ex.Message}");
			}
		}

		ShutdownThreads();
	}

	/// <summary>
	/// Shuts down all threads and performs cleanup
	/// </summary>
	private static void ShutdownThreads()
	{
		isRunning = false;
		Log("Shutting down threads...");

		// Kill the Rust server if it's running
		if (grpcServerProcess != null && !grpcServerProcess.HasExited)
		{
			try
			{
				Log("Terminating Rust gRPC server...");
				grpcServerProcess.Kill(true);
			}
			catch (Exception ex)
			{
				Log($"Error shutting down Rust gRPC server: {ex.Message}");
			}
		}

		WaitForThreadsToTerminate();
		SendFinalServerDisconnection();

		// Cleanup ZeroMQ resources
		PubSubConfig.Cleanup();
		NetMQConfig.Cleanup();

		Log("Shutdown complete.");
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
				Log($"Waiting for thread '{thread.Name}' to terminate...");
				if (!thread.Join(2000)) // Wait up to 2 seconds
				{
					Log($"Thread '{thread.Name}' did not terminate gracefully.");
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
				AnsiConsole.MarkupLine($"Verbose mode: {(verboseMode ? "[green]ON[/]" : "[grey]OFF[/]")}");
				break;
			case ConsoleKey.D:
				ShowDataStore();
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
			case ConsoleKey.M:
				ManageSubscriptions();
				break;
			case ConsoleKey.R:
				StartRustGrpcServer();
				break;
			case ConsoleKey.F:
				Config.PreferredMessageFormat = Config.PreferredMessageFormat.ToUpperInvariant() == "JSON" ? "XML" : "JSON";
				AnsiConsole.MarkupLine($"Message format switched to: [green]{Config.PreferredMessageFormat}[/]");
				break;
			case ConsoleKey.K:
				KillRustGrpcServer();
				break;
			case ConsoleKey.Q:
				AnsiConsole.MarkupLine("[yellow]Shutting down aggregator...[/]");
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
		try
		{
			inMenuMode = true;

			// Clear the screen to focus on the menu
			AnsiConsole.Clear();
			AnsiConsole.Write(new Rule("[yellow]Subscription Management[/]").RuleStyle("blue").Centered());

			var panel = new Panel("").Expand();
			AnsiConsole.MarkupLine("[bold]Currently subscribed data types:[/]");

			int index = 1;
			var dataTypes = new Dictionary<int, string>();

			var subscriptionTable = new Table().BorderColor(Color.Blue)
				.AddColumn(new TableColumn("Index").Centered())
				.AddColumn("Data Type");

			foreach (var dataType in PubSubConfig.SubscribedDataTypes)
			{
				subscriptionTable.AddRow($"[yellow]{index}[/]", $"[green]{dataType}[/]");
				dataTypes[index++] = dataType;
			}

			if (dataTypes.Count > 0)
				AnsiConsole.Write(subscriptionTable);
			else
				AnsiConsole.MarkupLine("[grey]No active subscriptions[/]");

			AnsiConsole.WriteLine();
			AnsiConsole.MarkupLine("[bold]Available actions:[/]");
			AnsiConsole.MarkupLine("[yellow]A[/]. Add a subscription");
			AnsiConsole.MarkupLine("[yellow]R[/]. Remove a subscription");
			AnsiConsole.MarkupLine("[yellow]B[/]. Back to main menu");

			AnsiConsole.Markup("\n[bold]Enter your choice:[/] ");
			var key = Console.ReadKey().KeyChar;
			AnsiConsole.WriteLine();

			switch (char.ToUpper(key))
			{
				case 'A':
					AddSubscription();
					break;
				case 'R':
					if (dataTypes.Count > 0)
						RemoveSubscription(dataTypes);
					else
						AnsiConsole.MarkupLine("[yellow]No subscriptions to remove[/]");
					break;
				case 'B':
					ClearScreenAndShowMenu();
					return;
			}

			AnsiConsole.WriteLine();
			AnsiConsole.Write(new Rule().RuleStyle("grey"));
			AnsiConsole.WriteLine();

			// Allow user to see the result before returning to main menu
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
	/// UI for adding a new subscription
	/// </summary>
	private static void AddSubscription()
	{
		// List of valid data types that can be subscribed to
		var validDataTypes = new List<string> { "Temperature", "WindSpeed", "Frequency", "Decibels" };

		AnsiConsole.Clear();
		AnsiConsole.Write(new Rule("[yellow]Add Subscription[/]").RuleStyle("blue").Centered());

		// Create a selection menu using Spectre.Console
		var dataType = AnsiConsole.Prompt(
			new SelectionPrompt<string>()
				.Title("[green]Choose a data type to subscribe to:[/]")
				.PageSize(10)
				.HighlightStyle(new Style(foreground: Color.Yellow))
				.AddChoices(validDataTypes)
		);

		// Check if already subscribed
		if (PubSubConfig.SubscribedDataTypes.Contains(dataType))
		{
			AnsiConsole.MarkupLine($"[yellow]Already subscribed to {dataType} data.[/]");
			return;
		}

		// Show a spinner while subscribing
		AnsiConsole.Status()
			.Start($"Subscribing to {dataType}...", ctx =>
			{
				ctx.Spinner(Spinner.Known.Dots);
				ctx.SpinnerStyle(Style.Parse("yellow"));

				// Proceed with subscription
				if (PubSubConfig.Subscribe(dataType))
				{
					AnsiConsole.MarkupLine($"[green]Successfully subscribed to {dataType} data.[/]");
				}
				else
				{
					AnsiConsole.MarkupLine($"[red]Failed to subscribe to {dataType} data.[/]");
				}

				Thread.Sleep(1000); // Pause to show the result
			});
	}

	/// <summary>
	/// UI for removing an existing subscription
	/// </summary>
	private static void RemoveSubscription(Dictionary<int, string> dataTypes)
	{
		AnsiConsole.Clear();
		AnsiConsole.Write(new Rule("[yellow]Remove Subscription[/]").RuleStyle("blue").Centered());

		// Create list of available subscriptions for selection
		var choices = dataTypes.Select(kvp => $"{kvp.Key}: {kvp.Value}").ToList();

		var selection = AnsiConsole.Prompt(
			new SelectionPrompt<string>()
				.Title("[green]Select subscription to remove:[/]")
				.PageSize(10)
				.HighlightStyle(new Style(foreground: Color.Yellow))
				.AddChoices(choices)
		);

		int key = int.Parse(selection.Split(':')[0]);
		string dataType = dataTypes[key];

		// Show a spinner while unsubscribing
		AnsiConsole.Status()
			.Start($"Unsubscribing from {dataType}...", ctx =>
			{
				ctx.Spinner(Spinner.Known.Dots);
				ctx.SpinnerStyle(Style.Parse("yellow"));

				if (PubSubConfig.Unsubscribe(dataType))
				{
					AnsiConsole.MarkupLine($"[green]Successfully unsubscribed from {dataType} data.[/]");
				}
				else
				{
					AnsiConsole.MarkupLine($"[red]Failed to unsubscribe from {dataType} data.[/]");
				}

				Thread.Sleep(1000); // Pause to show the result
			});
	}

	/// <summary>
	/// Displays the current contents of the data store
	/// </summary>
	private static void ShowDataStore()
	{
		try
		{
			inMenuMode = true;
			AnsiConsole.Clear();

			AnsiConsole.Write(new Rule("[yellow]Current Data Store[/]").RuleStyle("blue").Centered());

			if (dataStore.IsEmpty)
			{
				AnsiConsole.MarkupLine("[grey]Data store is empty.[/]");
			}
			else
			{
				var table = new Table()
					.BorderColor(Color.Blue)
					.AddColumn(new TableColumn("[green]Data Type[/]"))
					.AddColumn(new TableColumn("[green]Records[/]").Centered());

				foreach (var dataType in dataStore.Keys)
				{
					var dataList = dataStore[dataType];
					table.AddRow($"[yellow]{dataType}[/]", $"[aqua]{dataList.Count}[/]");
				}

				AnsiConsole.Write(table);

				// If in verbose mode, show the actual data samples in expandable panels
				if (verboseMode)
				{
					AnsiConsole.WriteLine();
					AnsiConsole.Write(new Rule("[yellow]Data Samples[/]").RuleStyle("grey"));

					foreach (var dataType in dataStore.Keys)
					{
						var dataList = dataStore[dataType];
						if (dataList.Count > 0)
						{
							var panel = new Panel(GetDataSampleText(dataList))
								.Header($"[blue]{dataType} Samples[/]")
								.BorderColor(Color.Grey)
								.Expand();

							AnsiConsole.Write(panel);
						}
					}
				}
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
	/// Gets a formatted text sample of data items
	/// </summary>
	private static string GetDataSampleText(ConcurrentBag<string> dataList)
	{
		var sb = new StringBuilder();

		int i = 0;
		foreach (var dataItem in dataList.Take(5)) // Show at most 5 items per type to avoid flooding
		{
			sb.AppendLine($"  - Item {++i}: {dataItem}");
		}

		if (dataList.Count > 5)
		{
			sb.AppendLine($"  ... and {dataList.Count - 5} more items");
		}

		return sb.ToString();
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

			if (AnsiConsole.Confirm("Are you sure you want to delete the configuration files?", false))
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

				AnsiConsole.MarkupLine("[green]Configuration files deleted. Default settings will be used on restart.[/]");
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
	/// Displays information about active threads
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

			// Add Rust server status info if applicable
			if (grpcServerProcess != null)
			{
				AnsiConsole.WriteLine();
				AnsiConsole.Write(new Rule("[yellow]External Processes[/]").RuleStyle("blue"));

				bool isRunning = !grpcServerProcess.HasExited;
				string status = isRunning ? "[green]Running[/]" : "[red]Exited[/]";

				var processTable = new Table()
					.BorderColor(Color.DarkBlue)
					.AddColumn(new TableColumn("[green]Process[/]"))
					.AddColumn(new TableColumn("[green]PID[/]").Centered())
					.AddColumn(new TableColumn("[green]Status[/]"));

				processTable.AddRow("[yellow]Rust gRPC Server[/]",
					$"[aqua]{grpcServerProcess.Id}[/]",
					status);

				AnsiConsole.Write(processTable);
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
			new FigletText("Aggregator")
				.Color(Color.Aqua)
				.Centered());

		AnsiConsole.Write(new Rule("[blue]Ocean Monitor System[/]").RuleStyle("grey").Centered());

		var statusTable = new Table()
		.Border(TableBorder.Rounded)
		.BorderColor(Color.Grey)
		.AddColumn(new TableColumn("[blue]Property[/]"))
		.AddColumn(new TableColumn("[green]Value[/]"));

		statusTable.AddRow("Aggregator ID", $"[yellow]{aggregatorId}[/]");
		statusTable.AddRow("Server Connection", $"[yellow]{Config.ServerIp}:{Config.ServerPort}[/]");
		statusTable.AddRow("Message Format", $"[yellow]{Config.PreferredMessageFormat}[/]");
		statusTable.AddRow("Verbose Mode", verboseMode ? "[green]ON[/]" : "[grey]OFF[/]");
		statusTable.AddRow("Subscriptions", $"[aqua]{PubSubConfig.SubscribedDataTypes.Count}[/] data types");
		statusTable.AddRow("Rust gRPC Server",
			grpcServerProcess != null && !grpcServerProcess.HasExited
			? $"[green]Running (PID: {grpcServerProcess.Id})[/]"
			: "[grey]Not running[/]");

		AnsiConsole.Write(statusTable);
		AnsiConsole.WriteLine();

		DisplayCommandMenu();
	}

	#endregion

	#region Data Processing and Server Communication

	/// <summary>
	/// Helper class for serialization and deserialization
	/// </summary>
	private static class SerializationHelper
	{
		/// <summary>
		/// Detects message format and extracts content
		/// </summary>
		public static string DetectFormat(string message, out string content)
		{
			if (message.StartsWith("XML:"))
			{
				content = message.Substring(4); // Remove XML: prefix
				return "XML";
			}
			else if (message.StartsWith("JSON:"))
			{
				content = message.Substring(5); // Remove JSON: prefix
				return "JSON";
			}
			else
			{
				// Default to JSON if no prefix
				content = message;
				return "JSON";
			}
		}

		/// <summary>
		/// Serializes data according to specified format
		/// </summary>
		public static string Serialize(Dictionary<string, object> data, string format = "JSON")
		{
			return format.ToUpperInvariant() == "XML"
				? SerializeToXml(data)
				: SerializeToJson(data);
		}

		/// <summary>
		/// Deserializes data from specified format
		/// </summary>
		public static Dictionary<string, object> Deserialize(string data, string format = "JSON")
		{
			return format.ToUpperInvariant() == "XML"
				? DeserializeFromXml(data)
				: DeserializeFromJson(data);
		}

		// JSON implementations
		private static string SerializeToJson(Dictionary<string, object> data)
		{
			return JsonSerializer.Serialize(data);
		}

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

		// XML implementations
		private static string SerializeToXml(Dictionary<string, object> data)
		{
			using var stringWriter = new StringWriter();
			using var writer = XmlWriter.Create(stringWriter, new XmlWriterSettings { Indent = true });

			writer.WriteStartDocument();

			if (data.ContainsKey("action") && data.ContainsKey("dataType"))
			{
				// Subscription request format
				writer.WriteStartElement("Request");
				foreach (var pair in data)
				{
					writer.WriteElementString(pair.Key, pair.Value?.ToString());
				}
			}
			else
			{
				// General data format
				writer.WriteStartElement("Data");

				if (data.TryGetValue("AggregatorId", out var aggregatorId))
				{
					writer.WriteAttributeString("AggregatorId", aggregatorId.ToString());
				}

				foreach (var pair in data)
				{
					if (pair.Key != "AggregatorId") // Skip AggregatorId as it's already an attribute
					{
						writer.WriteElementString(pair.Key, pair.Value?.ToString());
					}
				}
			}

			writer.WriteEndElement();
			writer.WriteEndDocument();

			return stringWriter.ToString();
		}

		private static Dictionary<string, object> DeserializeFromXml(string xml)
		{
			var result = new Dictionary<string, object>();

			try
			{
				var doc = new XmlDocument();
				doc.LoadXml(xml);

				var root = doc.DocumentElement;
				if (root == null) return result;

				// Process attributes as key-value pairs
				foreach (XmlAttribute attr in root.Attributes)
				{
					result[attr.Name] = attr.Value;
				}

				// Process child elements as key-value pairs
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

	/// <summary>
	/// Periodically sends aggregated data to the server based on configured interval
	/// </summary>
	private static void SendDataToServer()
	{
		try
		{
			while (isRunning)
			{
				Log($"Waiting for {Config.DataTransmissionIntervalSec} seconds before sending data...");

				// Wait for the next transmission window
				for (int i = 0; i < Config.DataTransmissionIntervalSec && isRunning; i++)
				{
					Thread.Sleep(1000); // Check isRunning flag every second
				}

				if (!isRunning) break;

				Log($"Preparing to send aggregated data to server");

				// Capture current data for transmission and reset the store
				var dataToSend = PrepareDataForTransmission();

				if (dataToSend.IsEmpty)
				{
					Log("No data to send to server.");
					continue;
				}

				// Transmit the data
				SendAggregatedData(dataToSend);
			}
		}
		catch (Exception ex)
		{
			Log($"Fatal error in data sender: {ex.Message}");
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

				// Create subscription message using preferred format
				var subscriptionMessage = new Dictionary<string, object>
		{
			{ "action", "subscribe" },
			{ "dataType", dataType }
		};

				// Serialize using preferred format
				string format = Config.PreferredMessageFormat;
				string serializedData = SerializationHelper.Serialize(subscriptionMessage, format);
				string formatPrefix = format.ToUpperInvariant() == "XML" ? "XML:" : "JSON:";
				string message = formatPrefix + serializedData;

				// Notify the Wavy about this subscription with retries
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

							Console.WriteLine($"Sending subscription request: {message}");
							requestSocket.SendFrame(message);

							Console.WriteLine($"Waiting for response...");

							try
							{
								// The socket will use its default timeout behavior
								string response = requestSocket.ReceiveFrameString();
								Console.WriteLine($"Subscription response: {response}");

								// Detect format of response
								string responseFormat = SerializationHelper.DetectFormat(response, out string responseContent);

								// Check for success regardless of format
								bool isSuccess = false;
								if (responseFormat == "XML")
								{
									isSuccess = responseContent.Contains("Subscribed") || responseContent.Contains("<Response>Subscribed</Response>");
								}
								else
								{
									isSuccess = responseContent.Contains("Subscribed") || responseContent.Contains("\"status\":\"Subscribed\"");
								}

								requestSent = isSuccess;
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

					// Use preferred format
					string format = Config.PreferredMessageFormat;
					string serializedData = SerializationHelper.Serialize(request, format);
					string formatPrefix = format.ToUpperInvariant() == "XML" ? "XML:" : "JSON:";
					string message = formatPrefix + serializedData;

					requestSocket.SendFrame(message);

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
							string message = socket.ReceiveFrameString();

							if (verboseMode)
							{
								Console.WriteLine($"Received {topic} data: {message}");
							}

							// Detect format and extract content
							string format = SerializationHelper.DetectFormat(message, out string content);

							// Deserialize using the detected format
							var data = SerializationHelper.Deserialize(content, format);

							// Process the received data
							if (data != null)
							{
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
		/// Processes data received from a subscription with improved output control
		/// </summary>
		private static async void ProcessReceivedData(string dataType, Dictionary<string, object> data)
		{
			// Skip all output processing while in menu mode
			if (inMenuMode)
			{
				// Store the data but don't display anything
				try
				{
					// Extract data values silently
					string wavyId = data.TryGetValue("WavyId", out var wavyIdObj) && wavyIdObj != null
						? wavyIdObj.ToString() ?? "Unknown"
						: "Unknown";

					double value = 0.0;
					if (data.TryGetValue("Value", out var valueObj) && valueObj != null)
					{
						if (valueObj is JsonElement jsonElement)
						{
							if (jsonElement.ValueKind == JsonValueKind.Number)
								value = jsonElement.GetDouble();
							else if (jsonElement.ValueKind == JsonValueKind.String &&
									!string.IsNullOrEmpty(jsonElement.GetString()) &&
									double.TryParse(jsonElement.GetString(), out double parsedValue))
								value = parsedValue;
						}
						else
						{
							try { value = Convert.ToDouble(valueObj); }
							catch { }
						}
					}

					// Create validation data
					var validationData = new { Data = new[] { new { DataType = dataType, Value = value } } };
					string validationJson = JsonSerializer.Serialize(validationData);

					// Send data for validation silently
					var (isValid, _) = await Config._validationClient.ValidateAndCleanDataAsync(wavyId, validationJson);

					if (isValid)
					{
						// Store valid data
						var storeDataItem = new Dictionary<string, object>
				{
					{ "DataType", dataType },
					{ "Value", value },
					{ "WavyId", wavyId },
					{ "Timestamp", DateTime.UtcNow }
				};

						string json = JsonSerializer.Serialize(storeDataItem);

						// Store without logging
						if (!dataStore.TryGetValue(dataType, out var container))
						{
							container = new ConcurrentBag<string>();
							dataStore[dataType] = container;
						}

						container.Add(json);
					}
					// No output in either case while in menu mode
				}
				catch
				{
					// Suppress all exceptions in menu mode
				}

				// Early return to avoid any console output
				return;
			}

			// Normal processing when not in menu mode
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
							Log($"Value conversion error: {convEx.Message}. Using default value 0.");
						}
					}
				}

				// Use Log instead of Console.WriteLine for controlled output
				Log($"Extracted value for {dataType}: {value} (from {wavyId})", verboseMode);

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
				Log($"Validating data for {dataType}: {validationJson}", verboseMode);

				// Send data for validation
				try
				{
					var (isValid, cleanedData) = await Config._validationClient.ValidateAndCleanDataAsync(wavyId, validationJson);

					if (!isValid)
					{
						Log($"[WARNING] Validation failed for {dataType} data from {wavyId}");
						if (verboseMode)
						{
							Log($"  Invalid data: {validationJson}", true);
						}
						return; // Skip invalid data
					}

					Log($"Validation succeeded for {dataType} data from {wavyId}", verboseMode);

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

					Log($"Stored {dataType} data point via ZeroMQ pub-sub with value {value}", verboseMode);
					if (verboseMode)
					{
						Log($"  Stored data: {json}", true);
					}
				}
				catch (Exception valEx)
				{
					Log($"Data validation error: {valEx.Message}");

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

					Log($"Stored unvalidated {dataType} data point via ZeroMQ pub-sub with value {value}", verboseMode);
					if (verboseMode)
					{
						Log($"  Stored data: {json}", true);
					}
				}
			}
			catch (Exception ex)
			{
				Log($"Error processing ZeroMQ data for {dataType}: {ex.Message}");
				if (verboseMode)
				{
					Log($"Stack trace: {ex.StackTrace}", true);
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
	/// Starts the Rust gRPC validation server using "cargo run"
	/// </summary>
	private static void StartRustGrpcServer()
	{
		try
		{
			inMenuMode = true;

			if (grpcServerProcess != null && !grpcServerProcess.HasExited)
			{
				AnsiConsole.MarkupLine("[yellow]Rust gRPC server is already running.[/]");
				return;
			}

			// Check if cargo is available
			bool cargoExists = false;
			try
			{
				using var testProcess = Process.Start(new ProcessStartInfo
				{
					FileName = "cargo",
					Arguments = "--version",
					UseShellExecute = false,
					RedirectStandardOutput = true,
					RedirectStandardError = true,
					CreateNoWindow = true
				});

				if (testProcess != null)
				{
					testProcess.WaitForExit();
					cargoExists = testProcess.ExitCode == 0;
				}
			}
			catch
			{
				cargoExists = false;
			}

			if (!cargoExists)
			{
				AnsiConsole.MarkupLine("[red]Error: Cargo (Rust build tool) not found in PATH.[/]");
				AnsiConsole.MarkupLine("[yellow]Please install Rust from https://rustup.rs/[/]");
				return;
			}

			// Use the current Aggregator directory or try to find the directory containing Cargo.toml
			string projectDir = Directory.GetCurrentDirectory();

			// If there's no Cargo.toml in the current directory, check if we're in the bin folder
			if (!File.Exists(Path.Combine(projectDir, "Cargo.toml")))
			{
				// Try the directory specified in the comment
				projectDir = Path.GetFullPath("C:\\code\\SD\\SD_2025-TP\\TP2\\Aggregator");

				if (!Directory.Exists(projectDir))
				{
					AnsiConsole.MarkupLine("[red]Could not find the Rust project directory.[/]");
					return;
				}
			}

			AnsiConsole.Status()
				.Start("Starting Rust gRPC validation server...", ctx =>
				{
					ctx.Spinner(Spinner.Known.Dots);
					ctx.SpinnerStyle(Style.Parse("green"));

					Log($"Running cargo in directory: {projectDir}");

					// Start the server using cargo run
					var startInfo = new ProcessStartInfo
					{
						FileName = "cargo",
						Arguments = "run",
						UseShellExecute = true, // Use shell so we can open new console window
						CreateNoWindow = false,
						WindowStyle = ProcessWindowStyle.Normal,
						WorkingDirectory = projectDir
					};

					grpcServerProcess = Process.Start(startInfo);

					if (grpcServerProcess != null)
					{
						Log($"Rust gRPC server started with process ID: {grpcServerProcess.Id}");
						Thread.Sleep(2000); // Give cargo some time to start
					}
					else
					{
						Log("Failed to start Rust gRPC server");
					}
				});

			if (grpcServerProcess != null && !grpcServerProcess.HasExited)
			{
				AnsiConsole.MarkupLine("[green]Rust gRPC validation server started successfully.[/]");
			}
			else
			{
				AnsiConsole.MarkupLine("[red]Failed to start Rust gRPC validation server.[/]");
			}
		}
		catch (Exception ex)
		{
			AnsiConsole.MarkupLine($"[red]Error starting Rust gRPC server: {ex.Message}[/]");
		}
		finally
		{
			inMenuMode = false;
		}
	}

	/// <summary>
	/// Kills the running Rust gRPC validation server
	/// </summary>
	private static void KillRustGrpcServer()
	{
		try
		{
			inMenuMode = true;

			if (grpcServerProcess == null || grpcServerProcess.HasExited)
			{
				AnsiConsole.MarkupLine("[yellow]No Rust gRPC server is currently running.[/]");
				return;
			}

			AnsiConsole.Status()
				.Start("Stopping Rust gRPC validation server...", ctx =>
				{
					ctx.Spinner(Spinner.Known.Dots);
					ctx.SpinnerStyle(Style.Parse("red"));

					try
					{
						if (!grpcServerProcess.HasExited)
						{
							// Kill the entire process tree since cargo might have child processes
							grpcServerProcess.Kill(true);
							grpcServerProcess.WaitForExit(3000); // Wait up to 3 seconds

							if (grpcServerProcess.HasExited)
							{
								Log("Successfully stopped Rust gRPC server");
							}
							else
							{
								Log("Failed to stop Rust gRPC server");
							}
						}
					}
					catch (Exception ex)
					{
						Log($"Error while stopping Rust gRPC server: {ex.Message}");
					}
				});

			if (grpcServerProcess.HasExited)
			{
				AnsiConsole.MarkupLine("[green]Rust gRPC validation server stopped successfully.[/]");
				grpcServerProcess = null;
			}
			else
			{
				AnsiConsole.MarkupLine("[red]Failed to stop Rust gRPC validation server.[/]");
			}
		}
		catch (Exception ex)
		{
			AnsiConsole.MarkupLine($"[red]Error stopping Rust gRPC server: {ex.Message}[/]");
		}
		finally
		{
			inMenuMode = false;
		}
	}

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