using Google.Protobuf.WellKnownTypes;
using Grpc.Net.Client;
using Microsoft.Data.SqlClient;
using Oceanmonitor;
using Spectre.Console;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace OceanMonitorSystem
{
	/// <summary>
	/// Server component of the OceanMonitor system responsible for receiving, processing,
	/// and storing data from Aggregator clients in a SQL database.
	/// </summary>
	public class Server
	{
		#region Constants and Fields

		private const string PYTHON_GRPC_SERVER_ADDRESS = "http://localhost:50051";
		private static readonly GrpcChannel channel = GrpcChannel.ForAddress(PYTHON_GRPC_SERVER_ADDRESS);
		private static readonly DataCalculationService.DataCalculationServiceClient grpcClient = new DataCalculationService.DataCalculationServiceClient(channel);
		private static Process? PythonServerProcess = null;


		/// <summary>
		/// Lock object to prevent concurrent database access issues
		/// </summary>
		static readonly object dbLock = new object();

		/// <summary>
		/// Server listening port for incoming connections from Aggregators
		/// </summary>
		const int PORT = 6000;

		/// <summary>
		/// Tracks connected aggregators by their ID and connection timestamp
		/// </summary>
		static readonly Dictionary<string, DateTime> connectedAggregators = new Dictionary<string, DateTime>();

		/// <summary>
		/// Controls application execution state
		/// </summary>
		static bool isRunning = true;

		/// <summary>
		/// Controls detailed logging output for debugging
		/// </summary>
		static bool verboseMode = false;

		/// <summary>
		/// Database connection string for SQL Server
		/// </summary>
		static string connectionString = "Server=(localdb)\\mssqllocaldb;Database=OceanMonitor;TPythoned_Connection=True;TPythonServerCertificate=True;";

		/// <summary>
		/// Protocol message codes used in the Aggregator-Server communication protocol
		/// </summary>
		private static class MessageCodes
		{
			public const int AggregatorConnect = 102;
			public const int AggregatorData = 301;
			public const int AggregatorDataResend = 302;
			public const int AggregatorDisconnect = 502;
			public const int ServerConfirmation = 402;
		}

		/// <summary>
		/// Determines when devices are considered inactive
		/// </summary>
		static readonly int InactivityThresholdHours = 1;

		#endregion

		#region State Management

		/// <summary>
		/// Controls UI display mode to prevent console output during menu interactions
		/// </summary>
		static bool inMenuMode = false;

		/// <summary>
		/// Queue to store log messages during menu mode
		/// </summary>
		static readonly ConcurrentQueue<string> logMessages = new ConcurrentQueue<string>();

		/// <summary>
		/// Thread for displaying logs in the background
		/// </summary>
		static Thread? logDisplayThread = null;

		/// <summary>
		/// Escapes markup characters in a string for Spectre.Console
		/// </summary>
		static string EscapeMarkup(string text)
		{
			return text.Replace("[", "[[").Replace("]", "]]");
		}

		/// <summary>
		/// Logs a message with controlled output based on the current UI mode
		/// </summary>
		static void Log(string message, bool isVerboseOnly = false)
		{
			if (isVerboseOnly && !verboseMode)
				return;

			string formattedMessage = $"[{DateTime.Now:HH:mm:ss.fff}] {message}";
			logMessages.Enqueue(formattedMessage);

			// Only display immediately if not in menu mode and log thread isn't active
			if (!inMenuMode && logDisplayThread == null)
			{
				AnsiConsole.MarkupLine($"[gray]{EscapeMarkup(formattedMessage)}[/]");
			}
		}

		/// <summary>
		/// Starts the log display thread that shows logs when not in menu mode
		/// </summary>
		static void StartLogDisplayThread()
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
			})
			{
				IsBackground = true,
				Name = "LogDisplayThread"
			};

			logDisplayThread.Start();
		}

		/// <summary>
		/// Clears any pending log messages in the queue
		/// </summary>
		static void ClearLogQueue()
		{
			while (logMessages.TryDequeue(out _)) { }
		}

		#endregion

		#region Entry Point and Main Thread Management

		/// <summary>
		/// Application entry point. Initializes server components and starts handling connections.
		/// </summary>
		static void Main()
		{
			Thread keyboardThread = new Thread(MonitorKeyboard) { IsBackground = true, Name = "KeyboardMonitor" };
			keyboardThread.Start();

			// Initialize log display thread
			StartLogDisplayThread();

			// Clear the console and show a nice splash screen
			AnsiConsole.Clear();
			AnsiConsole.Write(
				new FigletText("OceanServer")
					.Color(Color.Green)
					.Centered());

			AnsiConsole.Write(new Rule("[blue]Ocean Monitor System[/]").RuleStyle("grey").Centered());

			// Start the TCP listener
			TcpListener server = new TcpListener(IPAddress.Parse("127.0.0.1"), PORT);
			server.Start();

			Log($"Server listening on port {PORT}");
			AnsiConsole.MarkupLine($"[bold green]Server[/] started and listening on port [yellow]{PORT}[/]");

			DisplayCommandMenu();

			while (isRunning)
			{
				try
				{
					TcpClient client = server.AcceptTcpClient();
					Thread clientThread = new Thread(() => HandleAggregator(client))
					{
						IsBackground = true,
						Name = $"Aggregator-{DateTime.Now.Ticks}"
					};
					clientThread.Start();
				}
				catch (Exception ex)
				{
					if (isRunning) // Only log if not shutting down
					{
						Log($"Failed to accept client connection: {ex.Message}");
					}
					Thread.Sleep(1000);
				}
			}

			server.Stop();
			Log("Shutdown complete.");
		}

		/// <summary>
		/// Displays available commands in the console using Spectre.Console
		/// </summary>
		static void DisplayCommandMenu()
		{
			var table = new Table()
				.BorderColor(Color.Grey)
				.Title("[yellow]Available Commands[/]")
				.AddColumn(new TableColumn("[aqua]Key[/]").Centered())
				.AddColumn(new TableColumn("[green]Action[/]"));

			table.AddRow("[yellow]A[/]", "Show connected aggregators");
			table.AddRow("[yellow]D[/]", "Show database statistics");
			table.AddRow("[yellow]V[/]", "Toggle verbose mode");
			table.AddRow("[yellow]C[/]", "Modify connection string");
			table.AddRow("[yellow]I[/]", "Check for inactive devices");
			table.AddRow("[yellow]P[/]", "Run Python gRPC calculation server");
			table.AddRow("[yellow]K[/]", "Kill Python gRPC calculation server");
			table.AddRow("[yellow]W[/]", "Clear screen");
			table.AddRow("[yellow]Q[/]", "Quit");

			AnsiConsole.Write(table);
		}

		/// <summary>
		/// Monitors keyboard input for interactive commands
		/// </summary>
		static void MonitorKeyboard()
		{
			while (isRunning)
			{
				if (Console.KeyAvailable)
				{
					var key = Console.ReadKey(true).Key;
					switch (key)
					{
						case ConsoleKey.A:
							ShowConnectedAggregators();
							break;
						case ConsoleKey.D:
							ShowDatabaseStatistics();
							break;
						case ConsoleKey.V:
							verboseMode = !verboseMode;
							AnsiConsole.MarkupLine($"Verbose mode: {(verboseMode ? "[green]ON[/]" : "[grey]OFF[/]")}");
							break;
						case ConsoleKey.C:
							ModifyConnectionString();
							break;
						case ConsoleKey.I:
							CheckInactiveDevices();
							break;
						case ConsoleKey.P:
							StartPythonGrpcServer();
							break;
						case ConsoleKey.K:
							KillPythonGrpcServer();
							break;
						case ConsoleKey.Q:
							AnsiConsole.MarkupLine("[yellow]Shutting down server...[/]");
							isRunning = false;
							Thread.Sleep(500);
							break;
						case ConsoleKey.W:
							ClearScreenAndShowMenu();
							break;
					}
				}
				Thread.Sleep(100);
			}
		}

		/// <summary>
		/// Displays information about connected aggregators
		/// </summary>
		static void ShowConnectedAggregators()
		{
			try
			{
				inMenuMode = true;
				ClearLogQueue();
				AnsiConsole.Clear();

				AnsiConsole.Write(new Rule("[yellow]Connected Aggregators[/]").RuleStyle("blue").Centered());

				lock (connectedAggregators)
				{
					if (connectedAggregators.Count == 0)
					{
						AnsiConsole.MarkupLine("[grey]No aggregators connected.[/]");
					}
					else
					{
						var table = new Table()
							.BorderColor(Color.Blue)
							.AddColumn(new TableColumn("[green]Aggregator ID[/]"))
							.AddColumn(new TableColumn("[green]Connected Since[/]"))
							.AddColumn(new TableColumn("[green]Connected For[/]"));

						foreach (var kvp in connectedAggregators)
						{
							TimeSpan connectedFor = DateTime.Now - kvp.Value;
							string runtime = $"{connectedFor.Hours}h {connectedFor.Minutes}m {connectedFor.Seconds}s";

							table.AddRow(
								$"[yellow]{kvp.Key}[/]",
								$"[aqua]{kvp.Value:HH:mm:ss}[/]",
								runtime);
						}

						AnsiConsole.Write(table);
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
		/// Displays database statistics (counts of records in tables)
		/// </summary>
		static void ShowDatabaseStatistics()
		{
			try
			{
				inMenuMode = true;
				ClearLogQueue();
				AnsiConsole.Clear();

				AnsiConsole.Write(new Rule("[yellow]Database Statistics[/]").RuleStyle("blue").Centered());

				try
				{
					using (var connection = new SqlConnection(connectionString))
					{
						connection.Open();
						var tableStats = new Dictionary<string, int>();
						string[] tableNames = { "Aggregators", "Wavys", "WavyAggregatorMapping", "Measurements", "CalculatedMetrics" };

						foreach (var tableName in tableNames)
						{
							using (var cmd = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", connection))
							{
								try
								{
									int count = (int)cmd.ExecuteScalar();
									tableStats[tableName] = count;
								}
								catch
								{
									tableStats[tableName] = -1; // Table doesn't exist
								}
							}
						}

						var statsTable = new Table()
							.BorderColor(Color.Blue)
							.AddColumn(new TableColumn("[green]Table Name[/]"))
							.AddColumn(new TableColumn("[green]Records[/]").Centered());

						foreach (var stat in tableStats)
						{
							if (stat.Value >= 0)
							{
								statsTable.AddRow($"[yellow]{stat.Key}[/]", $"[aqua]{stat.Value:N0}[/]");
							}
							else
							{
								statsTable.AddRow($"[yellow]{stat.Key}[/]", "[grey]Not found[/]");
							}
						}

						AnsiConsole.Write(statsTable);
					}
				}
				catch (Exception ex)
				{
					AnsiConsole.MarkupLine($"[red]Error retrieving database statistics: {ex.Message}[/]");
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
		/// Allows modifying the database connection string
		/// </summary>
		static void ModifyConnectionString()
		{
			try
			{
				inMenuMode = true;
				ClearLogQueue();
				AnsiConsole.Clear();

				AnsiConsole.Write(new Rule("[yellow]Database Connection[/]").RuleStyle("blue").Centered());
				AnsiConsole.MarkupLine($"[bold]Current connection string:[/]");

				var panel = new Panel(connectionString)
					.Header("Connection String")
					.Border(BoxBorder.Rounded)
					.BorderColor(Color.Grey);
				AnsiConsole.Write(panel);

				string input = AnsiConsole.Ask<string>("[yellow]Enter new connection string[/] [grey](leave empty to cancel):[/]");

				if (!string.IsNullOrWhiteSpace(input))
				{
					AnsiConsole.Status()
						.Start("Testing connection...", ctx =>
						{
							ctx.Spinner(Spinner.Known.Dots);
							ctx.SpinnerStyle(Style.Parse("yellow"));

							try
							{
								using (var connection = new SqlConnection(input))
								{
									connection.Open();
									connectionString = input;
									Thread.Sleep(1000); // Give time to see the status
								}

								AnsiConsole.MarkupLine("[green]Connection string updated successfully.[/]");
							}
							catch (Exception ex)
							{
								AnsiConsole.MarkupLine($"[red]Error with new connection string: {ex.Message}[/]");
								AnsiConsole.MarkupLine("[red]Connection string not updated.[/]");
							}
						});
				}
				else
				{
					AnsiConsole.MarkupLine("[yellow]Operation cancelled. Connection string not changed.[/]");
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
		/// Identifies and optionally removes inactive devices from the database
		/// </summary>
		static void CheckInactiveDevices()
		{
			try
			{
				inMenuMode = true;
				ClearLogQueue();
				AnsiConsole.Clear();

				AnsiConsole.Write(new Rule("[yellow]Inactive Device Management[/]").RuleStyle("blue").Centered());
				DateTime inactiveThreshold = DateTime.Now.AddHours(-InactivityThresholdHours);

				AnsiConsole.MarkupLine($"Devices not seen since: [yellow]{inactiveThreshold:yyyy-MM-dd HH:mm:ss}[/]");
				AnsiConsole.MarkupLine($"Inactivity threshold: [yellow]{InactivityThresholdHours} hours[/]");

				lock (dbLock)
				{
					using (var connection = new SqlConnection(connectionString))
					{
						try
						{
							connection.Open();
							using (var transaction = connection.BeginTransaction())
							{
								try
								{
									// Count inactive devices
									int inactiveAggregatorsCount = CountInactiveAggregators(connection, transaction, inactiveThreshold);
									int inactiveWavysCount = CountInactiveWavys(connection, transaction, inactiveThreshold);

									var table = new Table()
										.BorderColor(Color.Blue)
										.AddColumn(new TableColumn("[green]Device Type[/]"))
										.AddColumn(new TableColumn("[green]Count[/]").Centered());

									table.AddRow("[yellow]Inactive Aggregators[/]", $"[aqua]{inactiveAggregatorsCount}[/]");
									table.AddRow("[yellow]Inactive Wavys[/]", $"[aqua]{inactiveWavysCount}[/]");

									AnsiConsole.Write(table);
									AnsiConsole.WriteLine();

									if (inactiveAggregatorsCount > 0 || inactiveWavysCount > 0)
									{
										if (AnsiConsole.Confirm("Do you want to remove these inactive devices?", false))
										{
											AnsiConsole.Status()
												.Start("Removing inactive devices...", ctx =>
												{
													ctx.Spinner(Spinner.Known.Dots);
													ctx.SpinnerStyle(Style.Parse("yellow"));

													DeleteInactiveDeviceData(connection, transaction, inactiveThreshold);
													transaction.Commit();
													Thread.Sleep(1000); // Show status for a moment
												});

											AnsiConsole.MarkupLine("[green]Inactive devices successfully removed from the database.[/]");
										}
										else
										{
											AnsiConsole.MarkupLine("[yellow]Operation cancelled. No devices were removed.[/]");
											transaction.Rollback();
										}
									}
									else
									{
										AnsiConsole.MarkupLine("[green]No inactive devices found. Nothing to remove.[/]");
										transaction.Rollback();
									}
								}
								catch (Exception ex)
								{
									transaction.Rollback();
									throw new Exception($"Transaction rolled back: {ex.Message}", ex);
								}
							}
						}
						catch (Exception ex)
						{
							AnsiConsole.MarkupLine($"[red]Failed to check/remove inactive devices: {ex.Message}[/]");
							if (ex.InnerException != null)
							{
								AnsiConsole.MarkupLine($"[red]Inner exception: {ex.InnerException.Message}[/]");
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
		/// Clears the console screen and displays the menu again
		/// </summary>
		static void ClearScreenAndShowMenu()
		{
			AnsiConsole.Clear();

			AnsiConsole.Write(
				new FigletText("OceanServer")
					.Color(Color.Green)
					.Centered());

			AnsiConsole.Write(new Rule("[blue]Ocean Monitor System[/]").RuleStyle("grey").Centered());

			var statusTable = new Table()
				.Border(TableBorder.Rounded)
				.BorderColor(Color.Grey)
				.AddColumn(new TableColumn("[blue]Property[/]"))
				.AddColumn(new TableColumn("[green]Value[/]"));

			lock (connectedAggregators)
			{
				statusTable.AddRow("Server Port", $"[yellow]{PORT}[/]");
				statusTable.AddRow("Active Aggregators", $"[yellow]{connectedAggregators.Count}[/]");
				statusTable.AddRow("Verbose Mode", verboseMode ? "[green]ON[/]" : "[grey]OFF[/]");
				statusTable.AddRow("Python gRPC Server",
					PythonServerProcess != null && !PythonServerProcess.HasExited
					? $"[green]Running (PID: {PythonServerProcess.Id})[/]"
					: "[grey]Not running[/]");
			}

			AnsiConsole.Write(statusTable);
			AnsiConsole.WriteLine();

			DisplayCommandMenu();
		}

		/// <summary>
		/// Counts the number of inactive aggregators in the database
		/// </summary>
		static int CountInactiveAggregators(SqlConnection connection, SqlTransaction transaction, DateTime threshold)
		{
			using (var cmd = new SqlCommand(
				"SELECT COUNT(*) FROM Aggregators WHERE LastSeen < @threshold",
				connection, transaction))
			{
				cmd.Parameters.AddWithValue("@threshold", threshold);
				return (int)cmd.ExecuteScalar();
			}
		}

		/// <summary>
		/// Counts the number of inactive Wavy devices in the database
		/// </summary>
		static int CountInactiveWavys(SqlConnection connection, SqlTransaction transaction, DateTime threshold)
		{
			using (var cmd = new SqlCommand(
				"SELECT COUNT(*) FROM Wavys WHERE LastSeen < @threshold",
				connection, transaction))
			{
				cmd.Parameters.AddWithValue("@threshold", threshold);
				return (int)cmd.ExecuteScalar();
			}
		}

		/// <summary>
		/// Deletes all data related to inactive devices from the database
		/// </summary>
		static void DeleteInactiveDeviceData(SqlConnection connection, SqlTransaction transaction, DateTime threshold)
		{
			// Delete measurements from inactive Wavys
			string deleteMeasurementsFromWavys = @"
    DELETE FROM Measurements 
    WHERE WavyID IN (SELECT WavyID FROM Wavys WHERE LastSeen < @threshold)";
			using (var cmd = new SqlCommand(deleteMeasurementsFromWavys, connection, transaction))
			{
				cmd.Parameters.AddWithValue("@threshold", threshold);
				cmd.ExecuteNonQuery();
			}

			// Delete measurements from inactive Aggregators
			string deleteMeasurementsFromAggregators = @"
    DELETE FROM Measurements 
    WHERE AggregatorID IN (SELECT AggregatorID FROM Aggregators WHERE LastSeen < @threshold)";
			using (var cmd = new SqlCommand(deleteMeasurementsFromAggregators, connection, transaction))
			{
				cmd.Parameters.AddWithValue("@threshold", threshold);
				cmd.ExecuteNonQuery();
			}

			// Delete mappings involving inactive devices
			string deleteMappings = @"
    DELETE FROM WavyAggregatorMapping
    WHERE WavyID IN (SELECT WavyID FROM Wavys WHERE LastSeen < @threshold)
    OR AggregatorID IN (SELECT AggregatorID FROM Aggregators WHERE LastSeen < @threshold)";
			using (var cmd = new SqlCommand(deleteMappings, connection, transaction))
			{
				cmd.Parameters.AddWithValue("@threshold", threshold);
				cmd.ExecuteNonQuery();
			}

			// Delete inactive Wavys
			string deleteWavys = "DELETE FROM Wavys WHERE LastSeen < @threshold";
			using (var cmd = new SqlCommand(deleteWavys, connection, transaction))
			{
				cmd.Parameters.AddWithValue("@threshold", threshold);
				int removedWavys = cmd.ExecuteNonQuery();
				Console.WriteLine($"Removed {removedWavys} inactive Wavy devices");
			}

			// Delete inactive Aggregators
			string deleteAggregators = "DELETE FROM Aggregators WHERE LastSeen < @threshold";
			using (var cmd = new SqlCommand(deleteAggregators, connection, transaction))
			{
				cmd.Parameters.AddWithValue("@threshold", threshold);
				int removedAggregators = cmd.ExecuteNonQuery();
				Console.WriteLine($"Removed {removedAggregators} inactive Aggregator devices");
			}
		}

		#endregion

		#region Aggregator Connection Handling

		/// <summary>
		/// Handles communication with an Aggregator client, processing incoming messages
		/// and maintaining the connection until closed.
		/// </summary>
		/// <param name="client">TCP client connection from an Aggregator</param>
		static void HandleAggregator(TcpClient client)
		{
			string aggregatorId = "Unknown";
			string clientIp = ((IPEndPoint)client.Client.RemoteEndPoint).Address.ToString();

			try
			{
				using (NetworkStream stream = client.GetStream())
				{
					bool keepAlive = false;

					do
					{
						byte[] buffer = new byte[4096];
						int bytesRead = stream.Read(buffer, 0, buffer.Length);

						if (bytesRead <= 0)
							break;

						int code = BitConverter.ToInt32(buffer, 0);
						Log($"Received message with code: {code} from {clientIp}");

						switch (code)
						{
							case MessageCodes.AggregatorConnect:
								aggregatorId = ProcessConnection(buffer, bytesRead, stream, clientIp);
								keepAlive = true;
								break;

							case MessageCodes.AggregatorData:
							case MessageCodes.AggregatorDataResend:
								ProcessData(buffer, bytesRead, stream, code, ref aggregatorId);
								break;

							case MessageCodes.AggregatorDisconnect:
								ProcessDisconnection(buffer, bytesRead, stream, ref aggregatorId);
								keepAlive = false;
								break;

							default:
								Log($"[WARNING] Unknown message code: {code} from {clientIp}");
								break;
						}

						if (!keepAlive)
							break;

					} while (client.Connected);
				}

				client.Close();
				Log($"Connection closed with {(aggregatorId != "Unknown" ? "Aggregator " + aggregatorId : clientIp)}");
			}
			catch (Exception ex)
			{
				Log($"[ERROR] {ex.Message} from {(aggregatorId != "Unknown" ? "Aggregator " + aggregatorId : clientIp)}");

				if (aggregatorId != "Unknown")
				{
					lock (connectedAggregators)
					{
						if (connectedAggregators.ContainsKey(aggregatorId))
							connectedAggregators.Remove(aggregatorId);
					}
				}
			}
		}

		/// <summary>
		/// Processes an Aggregator connection request and registers the connection
		/// </summary>
		/// <returns>The Aggregator ID from the connection request</returns>
		static string ProcessConnection(byte[] buffer, int bytesRead, NetworkStream stream, string clientIp)
		{
			string aggregatorId = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);

			lock (connectedAggregators)
			{
				connectedAggregators[aggregatorId] = DateTime.Now;
			}

			Log($"[102] Connection established with Aggregator {aggregatorId} from {clientIp}");
			SendConfirmation(stream, MessageCodes.ServerConfirmation);

			return aggregatorId;
		}

		/// <summary>
		/// Processes an Aggregator disconnection request and removes the connection registration
		/// </summary>
		static void ProcessDisconnection(byte[] buffer, int bytesRead, NetworkStream stream, ref string aggregatorId)
		{
			string receivedId = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);

			if (aggregatorId == "Unknown")
				aggregatorId = receivedId;

			lock (connectedAggregators)
			{
				if (connectedAggregators.ContainsKey(aggregatorId))
					connectedAggregators.Remove(aggregatorId);
			}

			Log($"[502] Disconnection request from Aggregator {aggregatorId}");
			SendConfirmation(stream, MessageCodes.ServerConfirmation);
		}

		/// <summary>
		/// Processes data received from an Aggregator, deserializes it, and stores it in the database
		/// </summary>
		static void ProcessData(byte[] buffer, int bytesRead, NetworkStream stream, int code, ref string aggregatorId)
		{
			string jsonData = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);
			Log($"[{code}] Data received from {(aggregatorId != "Unknown" ? "Aggregator " + aggregatorId : "unknown aggregator")}");

			if (verboseMode)
			{
				Log($"[DATA] {jsonData}", true);
			}

			try
			{
				var data = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonData);
				if (data != null)
				{
					if (aggregatorId == "Unknown" && data.ContainsKey("AggregatorId"))
						aggregatorId = data["AggregatorId"].ToString();

					SaveToDatabase(data);
					SendConfirmation(stream, MessageCodes.ServerConfirmation);
					Log($"[402] Confirmation sent to Aggregator {aggregatorId}");
				}
			}
			catch (Exception ex)
			{
				Log($"[ERROR] Failed to process data: {ex.Message}");
				if (verboseMode && ex.InnerException != null)
				{
					Log($"[ERROR] Inner exception: {ex.InnerException.Message}", true);
				}
			}
		}

		/// <summary>
		/// Sends a confirmation code to an Aggregator
		/// </summary>
		static void SendConfirmation(NetworkStream stream, int code)
		{
			try
			{
				byte[] confirm = BitConverter.GetBytes(code);
				stream.Write(confirm, 0, confirm.Length);
			}
			catch (Exception ex)
			{
				Log($"[ERROR] Failed to send confirmation: {ex.Message}");
			}
		}

		#endregion

		#region Database Operations

		/// <summary>
		/// Stores received data from an Aggregator in the database
		/// </summary>
		/// <param name="data">Dictionary containing deserialized aggregator data</param>
		static void SaveToDatabase(Dictionary<string, object> data)
		{
			lock (dbLock)
			{
				using (var connection = new SqlConnection(connectionString))
				{
					try
					{
						connection.Open();
						using (var transaction = connection.BeginTransaction())
						{
							try
							{
								string aggregatorId = data["AggregatorId"].ToString();
								DateTime timestamp = DateTime.Parse(data["Timestamp"].ToString());

								// Update or insert Aggregator record
								UpsertAggregator(connection, transaction, aggregatorId, timestamp);

								// Process the data array
								var dataElement = (JsonElement)data["Data"];
								ProcessDataArray(connection, transaction, dataElement, aggregatorId, timestamp);

								transaction.Commit();
								Console.WriteLine("[DB] Data successfully inserted into database tables.");
							}
							catch (Exception ex)
							{
								transaction.Rollback();
								throw new Exception($"Transaction rolled back: {ex.Message}", ex);
							}
						}
					}
					catch (Exception ex)
					{
						Console.WriteLine($"[ERROR] Database operation failed: {ex.Message}");
						if (ex.InnerException != null)
						{
							Console.WriteLine($"[ERROR] Inner exception: {ex.InnerException.Message}");
						}
					}
				}
			}
		}

		/// <summary>
		/// Updates an existing Aggregator record or inserts a new one if it doesn't exist
		/// </summary>
		static void UpsertAggregator(SqlConnection connection, SqlTransaction transaction, string aggregatorId, DateTime timestamp)
		{
			string updateAggregator = @"
        IF EXISTS (SELECT 1 FROM Aggregators WHERE AggregatorID = @id)
            UPDATE Aggregators SET LastSeen = @ts WHERE AggregatorID = @id
        ELSE
            INSERT INTO Aggregators (AggregatorID, LastSeen) VALUES (@id, @ts)";

			using (var cmd = new SqlCommand(updateAggregator, connection, transaction))
			{
				cmd.Parameters.AddWithValue("@id", aggregatorId);
				cmd.Parameters.AddWithValue("@ts", timestamp);
				cmd.ExecuteNonQuery();
			}
		}

		/// <summary>
		/// Processes the array of data items from an Aggregator message
		/// </summary>
		static void ProcessDataArray(SqlConnection connection, SqlTransaction transaction,
									JsonElement dataElement, string aggregatorId, DateTime timestamp)
		{
			foreach (JsonElement item in dataElement.EnumerateArray())
			{
				string dataType = item.GetProperty("DataType").GetString();

				if (item.TryGetProperty("Data", out JsonElement dataArray))
				{
					if (dataArray.ValueKind == JsonValueKind.Array)
					{
						foreach (JsonElement dataItem in dataArray.EnumerateArray())
						{
							ProcessMeasurement(connection, transaction, dataItem, dataType, aggregatorId, timestamp);
						}
					}
					else if (dataArray.ValueKind == JsonValueKind.String)
					{
						string dataJson = dataArray.GetString();
						try
						{
							using (JsonDocument doc = JsonDocument.Parse(dataJson))
							{
								ProcessMeasurement(connection, transaction, doc.RootElement, dataType, aggregatorId, timestamp);
							}
						}
						catch (JsonException)
						{
							Console.WriteLine($"[WARNING] Could not parse Data string as JSON: {dataJson}");
						}
					}
				}
				else
				{
					Console.WriteLine($"[WARNING] Data property not found for data type {dataType}");
				}
			}
		}

		/// <summary>
		/// Processes a measurement data item and inserts it into the database with calculated metrics
		/// </summary>
		static void ProcessMeasurement(SqlConnection connection, SqlTransaction transaction,
									JsonElement element, string dataType, string aggregatorId, DateTime timestamp)
		{
			try
			{
				// Handle nested parsing for string-encoded JSON elements
				if (element.ValueKind == JsonValueKind.String)
				{
					string json = element.GetString();
					using (JsonDocument doc = JsonDocument.Parse(json))
					{
						ProcessMeasurement(connection, transaction, doc.RootElement, dataType, aggregatorId, timestamp);
					}
					return;
				}

				// Extract the required data from the element
				string wavyId = ExtractWavyId(element);
				if (!TryExtractValue(element, out double value))
				{
					Console.WriteLine("[WARNING] No valid value found in measurement data");
					return;
				}

				// Ensure the Wavy device exists in the database
				EnsureWavyExists(connection, transaction, wavyId, timestamp);

				// Update or create the mapping between Wavy and Aggregator
				UpdateWavyAggregatorMapping(connection, transaction, wavyId, aggregatorId, timestamp);

				// Process with gRPC first, then fall back to normal insertion if it fails
				var task = ProcessMeasurementWithGrpc(connection, transaction, wavyId, aggregatorId, dataType, value, timestamp);
				task.Wait(); // Wait for the async task to complete

				if (verboseMode && !task.Result)
				{
					Console.WriteLine("[WARNING] gRPC calculation failed, using basic measurement storage");
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"[WARNING] Error processing measurement: {ex.Message}");
			}
		}


		/// <summary>
		/// Extracts the Wavy ID from a measurement element
		/// </summary>
		static string ExtractWavyId(JsonElement element)
		{
			if (element.TryGetProperty("WavyId", out JsonElement wavyIdElement) &&
				wavyIdElement.ValueKind == JsonValueKind.String)
			{
				string wavyId = wavyIdElement.GetString();
				return string.IsNullOrEmpty(wavyId) ? "Unknown" : wavyId;
			}
			else if (element.TryGetProperty("wavyId", out wavyIdElement) &&
					 wavyIdElement.ValueKind == JsonValueKind.String)
			{
				string wavyId = wavyIdElement.GetString();
				return string.IsNullOrEmpty(wavyId) ? "Unknown" : wavyId;
			}

			if (verboseMode)
			{
				Console.WriteLine($"[WARNING] No Wavy ID found in measurement: {element.GetRawText()}");
			}
			return "Unknown";
		}

		/// <summary>
		/// Tries to extract a numeric value from a measurement element
		/// </summary>
		static bool TryExtractValue(JsonElement element, out double value)
		{
			// Debug the incoming JSON
			if (verboseMode)
			{
				Console.WriteLine($"Extracting value from element: {element.GetRawText()}");
			}

			// First try the capitalized property name
			if (element.TryGetProperty("Value", out JsonElement valueElement))
			{
				bool success = TryGetDoubleValue(valueElement, out value);
				if (success)
				{
					Console.WriteLine($"Extracted Value: {value}");
				}
				return success;
			}
			// Then try lowercase
			else if (element.TryGetProperty("value", out valueElement))
			{
				bool success = TryGetDoubleValue(valueElement, out value);
				if (success)
				{
					Console.WriteLine($"Extracted value: {value}");
				}
				return success;
			}

			// If no property found, try direct JSON value if the element itself is a number
			if (element.ValueKind == JsonValueKind.Number)
			{
				value = element.GetDouble();
				Console.WriteLine($"Extracted direct value: {value}");
				return true;
			}

			Console.WriteLine($"[WARNING] No value property found in: {element.GetRawText()}");
			value = 0;
			return false;
		}

		/// <summary>
		/// Ensures a Wavy device record exists in the database
		/// </summary>
		static void EnsureWavyExists(SqlConnection connection, SqlTransaction transaction, string wavyId, DateTime timestamp)
		{
			string ensureWavy = @"
        IF NOT EXISTS (SELECT 1 FROM Wavys WHERE WavyID = @id)
            INSERT INTO Wavys (WavyID, LastSeen) VALUES (@id, @ts)
        ELSE
            UPDATE Wavys SET LastSeen = @ts WHERE WavyID = @id";

			using (var cmd = new SqlCommand(ensureWavy, connection, transaction))
			{
				cmd.Parameters.AddWithValue("@id", wavyId);
				cmd.Parameters.AddWithValue("@ts", timestamp);
				cmd.ExecuteNonQuery();
			}
		}

		/// <summary>
		/// Updates or creates a mapping between a Wavy device and an Aggregator
		/// </summary>
		static void UpdateWavyAggregatorMapping(SqlConnection connection, SqlTransaction transaction,
											 string wavyId, string aggregatorId, DateTime timestamp)
		{
			string updateMapping = @"
        IF EXISTS (SELECT 1 FROM WavyAggregatorMapping WHERE WavyID = @wid AND AggregatorID = @aid)
            UPDATE WavyAggregatorMapping SET LastConnected = @ts WHERE WavyID = @wid AND AggregatorID = @aid
        ELSE
            INSERT INTO WavyAggregatorMapping (WavyID, AggregatorID, FirstConnected, LastConnected) 
            VALUES (@wid, @aid, @ts, @ts)";

			using (var cmd = new SqlCommand(updateMapping, connection, transaction))
			{
				cmd.Parameters.AddWithValue("@wid", wavyId);
				cmd.Parameters.AddWithValue("@aid", aggregatorId);
				cmd.Parameters.AddWithValue("@ts", timestamp);
				cmd.ExecuteNonQuery();
			}
		}

		/// <summary>
		/// Inserts a measurement record into the database
		/// </summary>
		static void InsertMeasurement(SqlConnection connection, SqlTransaction transaction,
								   string wavyId, string aggregatorId, string dataType, double value, DateTime timestamp)
		{
			string insertMeasurement = @"
        INSERT INTO Measurements (WavyID, AggregatorID, DataType, Value, Timestamp)
        VALUES (@wid, @aid, @type, @val, @ts)";

			using (var cmd = new SqlCommand(insertMeasurement, connection, transaction))
			{
				cmd.Parameters.AddWithValue("@wid", wavyId);
				cmd.Parameters.AddWithValue("@aid", aggregatorId);
				cmd.Parameters.AddWithValue("@type", dataType);
				cmd.Parameters.AddWithValue("@val", value);
				cmd.Parameters.AddWithValue("@ts", timestamp);
				cmd.ExecuteNonQuery();
			}
		}

		/// <summary>
		/// Extracts a double value from a JSON element, handling different types
		/// </summary>
		static bool TryGetDoubleValue(JsonElement element, out double value)
		{
			value = 0;

			switch (element.ValueKind)
			{
				case JsonValueKind.Number:
					value = element.GetDouble();
					return true;

				case JsonValueKind.String:
					return double.TryParse(element.GetString(), out value);

				default:
					return false;
			}
		}

		#endregion

		#region gRPC Operations

		private static void StartPythonGrpcServer()
		{
			try
			{
				inMenuMode = true;

				if (PythonServerProcess != null && !PythonServerProcess.HasExited)
				{
					AnsiConsole.MarkupLine("[yellow]Python gRPC server is already running.[/]");
					return;
				}

				// Check if cargo is available
				bool cargoExists = false;
				try
				{
					using var testProcess = Process.Start(new ProcessStartInfo
					{
						FileName = "python",
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
					AnsiConsole.MarkupLine("[yellow]Please install Python from https://www.python.org/");
					return;
				}

				// Use the current Aggregator directory or try to find the directory containing grpc_calc_server.py
				string projectDir = Directory.GetCurrentDirectory();

				// If there's no grpc_calc_server.py in the current directory, check if we're in the bin folder
				if (!File.Exists(Path.Combine(projectDir, "grpc_calc_server.py")))
				{
					// Try the directory specified in the comment
					projectDir = Path.GetFullPath("C:\\code\\SD\\SD_2025-TP\\TP2\\Server");

					if (!Directory.Exists(projectDir))
					{
						AnsiConsole.MarkupLine("[red]Could not find the Python project directory.[/]");
						return;
					}
				}

				AnsiConsole.Status()
					.Start("Starting Python gRPC validation server...", ctx =>
					{
						ctx.Spinner(Spinner.Known.Dots);
						ctx.SpinnerStyle(Style.Parse("green"));

						Log($"Running python in directory: {projectDir}");

						// Start the server using cargo run
						var startInfo = new ProcessStartInfo
						{
							FileName = "python",
							Arguments = "grpc_calc_server.py",
							UseShellExecute = true, 
							CreateNoWindow = false,
							WindowStyle = ProcessWindowStyle.Normal,
							WorkingDirectory = projectDir
						};

						PythonServerProcess = Process.Start(startInfo);

						if (PythonServerProcess != null)
						{
							Log($"Python gRPC server started with process ID: {PythonServerProcess.Id}");
							Thread.Sleep(2000); // Give cargo some time to start
						}
						else
						{
							Log("Failed to start Python gRPC server");
						}
					});

				if (PythonServerProcess != null && !PythonServerProcess.HasExited)
				{
					AnsiConsole.MarkupLine("[green]Python gRPC validation server started successfully.[/]");
				}
				else
				{
					AnsiConsole.MarkupLine("[red]Failed to start Python gRPC validation server.[/]");
				}
			}
			catch (Exception ex)
			{
				AnsiConsole.MarkupLine($"[red]Error starting Python gRPC server: {ex.Message}[/]");
			}
			finally
			{
				inMenuMode = false;
			}
		}

		/// <summary>
		/// Kills the running Python gRPC calculation server
		/// </summary>
		static void KillPythonGrpcServer()
		{
			try
			{
				inMenuMode = true;

				if (PythonServerProcess == null || PythonServerProcess.HasExited)
				{
					AnsiConsole.MarkupLine("[yellow]No Python gRPC server is currently running.[/]");
					return;
				}

				AnsiConsole.Status()
					.Start("Stopping Python gRPC calculation server...", ctx =>
					{
						ctx.Spinner(Spinner.Known.Dots);
						ctx.SpinnerStyle(Style.Parse("red"));

						try
						{
							if (!PythonServerProcess.HasExited)
							{
								PythonServerProcess.Kill(true); // Force kill
								PythonServerProcess.WaitForExit(3000); // Wait up to 3 seconds

								if (PythonServerProcess.HasExited)
								{
									Log("Successfully stopped Python gRPC server");
								}
								else
								{
									Log("Failed to stop Python gRPC server");
								}
							}
						}
						catch (Exception ex)
						{
							Log($"Error while stopping Python gRPC server: {ex.Message}");
						}
					});

				if (PythonServerProcess.HasExited)
				{
					AnsiConsole.MarkupLine("[green]Python gRPC calculation server stopped successfully.[/]");
					PythonServerProcess = null;
				}
				else
				{
					AnsiConsole.MarkupLine("[red]Failed to stop Python gRPC calculation server.[/]");
				}
			}
			catch (Exception ex)
			{
				AnsiConsole.MarkupLine($"[red]Error stopping Python gRPC server: {ex.Message}[/]");
			}
			finally
			{
				inMenuMode = false;
			}
		}

		/// <summary>
		/// Sends measurement data to Python server for calculation and stores the results
		/// </summary>
		static async Task<bool> ProcessMeasurementWithGrpc(SqlConnection connection, SqlTransaction transaction,
			string wavyId, string aggregatorId, string dataType, double value, DateTime timestamp)
		{
			try
			{
				// Don't show output during menu mode
				if (!inMenuMode)
					Log($"[GRPC] Sending data to calculation server: WavyID={wavyId}, Type={dataType}, Value={value}");

				var calculationRequest = new DataCalculationRequest
				{
					WavyId = wavyId,
					DataType = dataType,
					Value = value,
					Timestamp = ((DateTimeOffset)timestamp).ToUnixTimeMilliseconds()
				};

				// Call the gRPC service
				var response = await grpcClient.CalculateMetricsAsync(calculationRequest);

				if (response != null)
				{
					if (!inMenuMode)
						Log($"[GRPC] Received calculation results for {wavyId}");

					// Store both original measurement and calculated metrics
					InsertMeasurement(connection, transaction, wavyId, aggregatorId, dataType, value, timestamp);

					// Store the calculated metrics
					InsertCalculatedMetrics(connection, transaction, response, aggregatorId);

					return true;
				}

				return false;
			}
			catch (Exception ex)
			{
				if (!inMenuMode)
					Log($"[ERROR] gRPC calculation failed: {ex.Message}");

				// Fall back to normal insertion without calculated metrics
				InsertMeasurement(connection, transaction, wavyId, aggregatorId, dataType, value, timestamp);
				return false;
			}
		}

		/// <summary>
		/// Inserts calculated metrics into the database
		/// </summary>
		static void InsertCalculatedMetrics(SqlConnection connection, SqlTransaction transaction,
			DataCalculationResponse metrics, string aggregatorId)
		{
			// Create timestamp from Unix time
			DateTime timestamp = DateTimeOffset.FromUnixTimeMilliseconds(metrics.Timestamp).DateTime;

			string insertMetrics = @"
INSERT INTO CalculatedMetrics 
(WavyID, AggregatorID, DataType, OriginalValue, AverageValue, Variance, 
TrendCoefficient, NormalizedValue, AnomalyScore, Timestamp)
VALUES 
(@wid, @aid, @type, @orig, @avg, @var, @trend, @norm, @anom, @ts)";

			using (var cmd = new SqlCommand(insertMetrics, connection, transaction))
			{
				cmd.Parameters.AddWithValue("@wid", metrics.WavyId);
				cmd.Parameters.AddWithValue("@aid", aggregatorId);
				cmd.Parameters.AddWithValue("@type", metrics.DataType);
				cmd.Parameters.AddWithValue("@orig", metrics.OriginalValue);
				cmd.Parameters.AddWithValue("@avg", metrics.AverageValue);
				cmd.Parameters.AddWithValue("@var", metrics.Variance);
				cmd.Parameters.AddWithValue("@trend", metrics.TrendCoefficient);
				cmd.Parameters.AddWithValue("@norm", metrics.NormalizedValue);
				cmd.Parameters.AddWithValue("@anom", metrics.AnomalyScore);
				cmd.Parameters.AddWithValue("@ts", timestamp);
				cmd.ExecuteNonQuery();
			}

			if (verboseMode && !inMenuMode)
			{
				Log($"[DB] Calculated metrics saved: WavyID={metrics.WavyId}, Type={metrics.DataType}", true);
				Log($"     Average={metrics.AverageValue:F2}, Variance={metrics.Variance:F2}, Anomaly={metrics.AnomalyScore:F2}", true);
			}
		}

		#endregion
	}
}
