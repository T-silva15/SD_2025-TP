using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using Microsoft.Data.SqlClient;

/// <summary>
/// Server class responsible for receiving and processing data from Aggregator clients.
/// Handles network connections, data processing, and database storage for the OceanMonitor system.
/// </summary>
class Server
{
	#region Constants and Fields

	/// <summary>
	/// Lock object to prevent concurrent database access issues
	/// </summary>
	static readonly object dbLock = new object();

	/// <summary>
	/// Server listening port for incoming connections from Aggregators
	/// </summary>
	const int PORT = 6000;

	/// <summary>
	/// Dictionary to track connected aggregators by their ID
	/// </summary>
	static readonly Dictionary<string, DateTime> connectedAggregators = new Dictionary<string, DateTime>();

	/// <summary>
	/// Flag to control application execution
	/// </summary>
	static bool isRunning = true;

	/// <summary>
	/// Flag to enable detailed logging
	/// </summary>
	static bool verboseMode = false;

	/// <summary>
	/// Database connection string
	/// </summary>
	static string connectionString = "Server=(localdb)\\mssqllocaldb;Database=OceanMonitor;Trusted_Connection=True;TrustServerCertificate=True;";

	/// <summary>
	/// Protocol message codes used in communication
	/// </summary>
	private static class MessageCodes
	{
		// Aggregator to Server codes
		public const int AggregatorConnect = 102;
		public const int AggregatorData = 301;
		public const int AggregatorDataResend = 302;
		public const int AggregatorDisconnect = 502;

		// Server to Aggregator confirmation codes
		public const int ServerConfirmation = 402;
	}

	/// <summary>
	/// Inactivity threshold in hours before marking a device as inactive
	/// </summary>
	static readonly int InactivityThresholdHours = 1; // Temporary value for testing - 1 hour

	#endregion

	#region Entry Point and Main Thread Management

	/// <summary>
	/// Entry point for the server application.
	/// Starts a TCP listener and continuously accepts connections from Aggregators.
	/// </summary>
	static void Main()
	{
		// Start keyboard monitoring thread for interactive commands
		Thread keyboardThread = new Thread(MonitorKeyboard) { IsBackground = true, Name = "KeyboardMonitor" };
		keyboardThread.Start();

		// Initialize and start the TCP listener for incoming connections
		TcpListener server = new TcpListener(IPAddress.Parse("127.0.0.1"), PORT);
		server.Start();
		Console.WriteLine($"[SERVER] Listening on port {PORT}...");
		Console.WriteLine("\nAvailable Commands:");
		Console.WriteLine("Press 'A' to show connected aggregators");
		Console.WriteLine("Press 'D' to show database statistics");
		Console.WriteLine("Press 'V' to toggle verbose mode");
		Console.WriteLine("Press 'C' to modify connection string");
		Console.WriteLine("Press 'I' to check for inactive devices");
		Console.WriteLine("Press 'Q' to quit");

		// Main server loop
		while (isRunning)
		{
			try
			{
				// Accept incoming client connections and handle each in a separate thread
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
				Console.WriteLine($"[ERROR] Failed to accept client connection: {ex.Message}");
				// Short delay to prevent CPU usage spiking in case of repeated errors
				Thread.Sleep(1000);
			}
		}

		// Clean up resources when shutting down
		server.Stop();
		Console.WriteLine("[SERVER] Shutdown complete.");
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
						Console.WriteLine($"Verbose mode: {(verboseMode ? "ON" : "OFF")}");
						break;
					case ConsoleKey.C:
						ModifyConnectionString();
						break;
					case ConsoleKey.I:
						CheckInactiveDevices();
						break;
					case ConsoleKey.Q:
						Console.WriteLine("Shutting down server...");
						isRunning = false;
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
		Console.WriteLine("\n=== Connected Aggregators ===");
		lock (connectedAggregators)
		{
			if (connectedAggregators.Count == 0)
			{
				Console.WriteLine("No aggregators connected.");
			}
			else
			{
				foreach (var kvp in connectedAggregators)
				{
					TimeSpan connectedFor = DateTime.Now - kvp.Value;
					Console.WriteLine($"Aggregator ID: {kvp.Key}");
					Console.WriteLine($"  Connected since: {kvp.Value:HH:mm:ss}");
					Console.WriteLine($"  Connected for: {connectedFor.Hours}h {connectedFor.Minutes}m {connectedFor.Seconds}s");
				}
			}
		}
		Console.WriteLine("============================\n");
	}

	/// <summary>
	/// Displays database statistics (counts of records in tables)
	/// </summary>
	static void ShowDatabaseStatistics()
	{
		Console.WriteLine("\n=== Database Statistics ===");
		try
		{
			using (var connection = new SqlConnection(connectionString))
			{
				connection.Open();

				// Get counts from main tables
				var tableStats = new Dictionary<string, int>();

				string[] tables = { "Aggregators", "Wavys", "WavyAggregatorMapping", "Measurements" };
				foreach (var table in tables)
				{
					using (var cmd = new SqlCommand($"SELECT COUNT(*) FROM {table}", connection))
					{
						try
						{
							int count = (int)cmd.ExecuteScalar();
							tableStats[table] = count;
						}
						catch
						{
							tableStats[table] = -1; // Table doesn't exist
						}
					}
				}

				// Display statistics
				foreach (var stat in tableStats)
				{
					if (stat.Value >= 0)
					{
						Console.WriteLine($"Table {stat.Key}: {stat.Value} records");
					}
					else
					{
						Console.WriteLine($"Table {stat.Key}: Not found");
					}
				}
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error retrieving database statistics: {ex.Message}");
		}
		Console.WriteLine("==========================\n");
	}

	/// <summary>
	/// Allows modifying the database connection string
	/// </summary>
	static void ModifyConnectionString()
	{
		Console.WriteLine($"\nCurrent connection string: {connectionString}");
		Console.WriteLine("Enter new connection string (or leave empty to cancel):");
		string input = Console.ReadLine();

		if (!string.IsNullOrWhiteSpace(input))
		{
			// Test the connection string before saving
			try
			{
				using (var connection = new SqlConnection(input))
				{
					connection.Open();
					connectionString = input;
					Console.WriteLine("Connection string updated successfully.");
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error with new connection string: {ex.Message}");
				Console.WriteLine("Connection string not updated.");
			}
		}
		else
		{
			Console.WriteLine("Operation cancelled. Connection string not changed.");
		}
	}

	/// <summary>
	/// Checks for and removes inactive Aggregators and Wavys from the database
	/// based on the defined InactivityThresholdHours threshold
	/// </summary>
	static void CheckInactiveDevices()
	{
		Console.WriteLine("\n=== Checking for Inactive Devices ===");
		DateTime inactiveThreshold = DateTime.Now.AddHours(-InactivityThresholdHours);

		Console.WriteLine($"Removing devices not seen since: {inactiveThreshold:yyyy-MM-dd HH:mm:ss}");
		Console.WriteLine($"Inactivity threshold: {InactivityThresholdHours} hours");

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
							// Count inactive devices before deletion
							int inactiveAggregatorsCount = 0;
							int inactiveWavysCount = 0;

							// Count inactive Aggregators
							using (var cmd = new SqlCommand(
								"SELECT COUNT(*) FROM Aggregators WHERE LastSeen < @threshold",
								connection, transaction))
							{
								cmd.Parameters.AddWithValue("@threshold", inactiveThreshold);
								inactiveAggregatorsCount = (int)cmd.ExecuteScalar();
							}

							// Count inactive Wavys
							using (var cmd = new SqlCommand(
								"SELECT COUNT(*) FROM Wavys WHERE LastSeen < @threshold",
								connection, transaction))
							{
								cmd.Parameters.AddWithValue("@threshold", inactiveThreshold);
								inactiveWavysCount = (int)cmd.ExecuteScalar();
							}

							Console.WriteLine($"Found {inactiveAggregatorsCount} inactive Aggregators and {inactiveWavysCount} inactive Wavys");

							if (inactiveAggregatorsCount > 0 || inactiveWavysCount > 0)
							{
								Console.Write("Do you want to remove these inactive devices? (y/n): ");
								string answer = Console.ReadLine()?.ToLower();

								if (answer == "y" || answer == "yes")
								{
									// First, delete measurements from inactive Wavys
									string deleteMeasurementsFromWavys = @"
                                    DELETE FROM Measurements 
                                    WHERE WavyID IN (SELECT WavyID FROM Wavys WHERE LastSeen < @threshold)";
									using (var cmd = new SqlCommand(deleteMeasurementsFromWavys, connection, transaction))
									{
										cmd.Parameters.AddWithValue("@threshold", inactiveThreshold);
										cmd.ExecuteNonQuery();
									}

									// Delete measurements from inactive Aggregators
									string deleteMeasurementsFromAggregators = @"
                                    DELETE FROM Measurements 
                                    WHERE AggregatorID IN (SELECT AggregatorID FROM Aggregators WHERE LastSeen < @threshold)";
									using (var cmd = new SqlCommand(deleteMeasurementsFromAggregators, connection, transaction))
									{
										cmd.Parameters.AddWithValue("@threshold", inactiveThreshold);
										cmd.ExecuteNonQuery();
									}

									// Delete mappings involving inactive devices
									string deleteMappings = @"
                                    DELETE FROM WavyAggregatorMapping
                                    WHERE WavyID IN (SELECT WavyID FROM Wavys WHERE LastSeen < @threshold)
                                    OR AggregatorID IN (SELECT AggregatorID FROM Aggregators WHERE LastSeen < @threshold)";
									using (var cmd = new SqlCommand(deleteMappings, connection, transaction))
									{
										cmd.Parameters.AddWithValue("@threshold", inactiveThreshold);
										cmd.ExecuteNonQuery();
									}

									// Delete inactive Wavys
									string deleteWavys = "DELETE FROM Wavys WHERE LastSeen < @threshold";
									using (var cmd = new SqlCommand(deleteWavys, connection, transaction))
									{
										cmd.Parameters.AddWithValue("@threshold", inactiveThreshold);
										int removedWavys = cmd.ExecuteNonQuery();
										Console.WriteLine($"Removed {removedWavys} inactive Wavy devices");
									}

									// Delete inactive Aggregators
									string deleteAggregators = "DELETE FROM Aggregators WHERE LastSeen < @threshold";
									using (var cmd = new SqlCommand(deleteAggregators, connection, transaction))
									{
										cmd.Parameters.AddWithValue("@threshold", inactiveThreshold);
										int removedAggregators = cmd.ExecuteNonQuery();
										Console.WriteLine($"Removed {removedAggregators} inactive Aggregator devices");
									}

									transaction.Commit();
									Console.WriteLine("Inactive devices successfully removed from the database.");
								}
								else
								{
									Console.WriteLine("Operation cancelled. No devices were removed.");
									transaction.Rollback();
								}
							}
							else
							{
								Console.WriteLine("No inactive devices found. Nothing to remove.");
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
					Console.WriteLine($"[ERROR] Failed to check/remove inactive devices: {ex.Message}");
					if (ex.InnerException != null)
					{
						Console.WriteLine($"[ERROR] Inner exception: {ex.InnerException.Message}");
					}
				}
			}
		}

		Console.WriteLine("===============================\n");
	}


	#endregion

	#region Aggregator Connection Handling

	/// <summary>
	/// Handles communication with an individual Aggregator client.
	/// Processes incoming data and sends confirmation responses.
	/// </summary>
	/// <param name="client">The TcpClient representing the connected Aggregator</param>
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
					Console.WriteLine($"[SERVER] Received message with code: {code} from {clientIp}");

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
							Console.WriteLine($"[WARNING] Unknown message code: {code} from {clientIp}");
							break;
					}

					if (!keepAlive)
						break;

				} while (client.Connected);
			}

			client.Close();
			Console.WriteLine($"[SERVER] Connection closed with {(aggregatorId != "Unknown" ? "Aggregator " + aggregatorId : clientIp)}");
		}
		catch (Exception ex)
		{
			Console.WriteLine($"[ERROR] {ex.Message} from {(aggregatorId != "Unknown" ? "Aggregator " + aggregatorId : clientIp)}");

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
	/// Processes a connection request from an Aggregator.
	/// </summary>
	/// <param name="buffer">The received data buffer</param>
	/// <param name="bytesRead">Number of bytes read from the buffer</param>
	/// <param name="stream">The network stream for sending responses</param>
	/// <param name="clientIp">The client's IP address</param>
	/// <returns>The Aggregator ID extracted from the connection request</returns>
	static string ProcessConnection(byte[] buffer, int bytesRead, NetworkStream stream, string clientIp)
	{
		string aggregatorId = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);

		lock (connectedAggregators)
		{
			connectedAggregators[aggregatorId] = DateTime.Now;
		}

		Console.WriteLine($"[102] Connection established with Aggregator {aggregatorId} from {clientIp}");
		SendConfirmation(stream, MessageCodes.ServerConfirmation);

		return aggregatorId;
	}

	/// <summary>
	/// Processes a disconnection request from an Aggregator.
	/// </summary>
	/// <param name="buffer">The received data buffer</param>
	/// <param name="bytesRead">Number of bytes read from the buffer</param>
	/// <param name="stream">The network stream for sending responses</param>
	/// <param name="aggregatorId">Reference to the Aggregator ID</param>
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

		Console.WriteLine($"[502] Disconnection request from Aggregator {aggregatorId}");
		SendConfirmation(stream, MessageCodes.ServerConfirmation);
	}

	/// <summary>
	/// Processes data message from an Aggregator.
	/// </summary>
	/// <param name="buffer">The received data buffer</param>
	/// <param name="bytesRead">Number of bytes read from the buffer</param>
	/// <param name="stream">The network stream for sending responses</param>
	/// <param name="code">The received message code</param>
	/// <param name="aggregatorId">Reference to the Aggregator ID</param>
	static void ProcessData(byte[] buffer, int bytesRead, NetworkStream stream, int code, ref string aggregatorId)
	{
		string jsonData = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);
		Console.WriteLine($"[{code}] Data received from {(aggregatorId != "Unknown" ? "Aggregator " + aggregatorId : "unknown aggregator")}");

		if (verboseMode)
		{
			Console.WriteLine($"[DATA] {jsonData}");
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
				Console.WriteLine($"[402] Confirmation sent to Aggregator {aggregatorId}");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"[ERROR] Failed to process data: {ex.Message}");
			if (verboseMode && ex.InnerException != null)
			{
				Console.WriteLine($"[ERROR] Inner exception: {ex.InnerException.Message}");
			}
		}
	}

	/// <summary>
	/// Sends a confirmation message with the specified code.
	/// </summary>
	/// <param name="stream">The network stream to send the confirmation to</param>
	/// <param name="code">The confirmation code to send</param>
	static void SendConfirmation(NetworkStream stream, int code)
	{
		try
		{
			byte[] confirm = BitConverter.GetBytes(code);
			stream.Write(confirm, 0, confirm.Length);
		}
		catch (Exception ex)
		{
			Console.WriteLine($"[ERROR] Failed to send confirmation: {ex.Message}");
		}
	}

	#endregion

	#region Database Operations

	/// <summary>
	/// Stores the received aggregator data in the database.
	/// Uses a lock to prevent concurrent database access.
	/// </summary>
	/// <param name="data">Dictionary containing aggregator data</param>
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
							string updateAggregator = @"
                            IF EXISTS (SELECT 1 FROM Aggregators WHERE AggregatorID = @id)
                                UPDATE Aggregators SET LastSeen = @ts, Status = 'Active' WHERE AggregatorID = @id
                            ELSE
                                INSERT INTO Aggregators (AggregatorID, LastSeen, Status) VALUES (@id, @ts, 'Active')";

							using (var cmd = new SqlCommand(updateAggregator, connection, transaction))
							{
								cmd.Parameters.AddWithValue("@id", aggregatorId);
								cmd.Parameters.AddWithValue("@ts", timestamp);
								cmd.ExecuteNonQuery();
							}

							// Get Data as JsonElement
							var dataElement = (JsonElement)data["Data"];

							// Process each data item in the list
							foreach (JsonElement item in dataElement.EnumerateArray())
							{
								string dataType = item.GetProperty("DataType").GetString();
								JsonElement dataArray;

								// Handle Data property which could be either a string or an array
								if (item.TryGetProperty("Data", out dataArray))
								{
									// Handle the case where Data is an array
									if (dataArray.ValueKind == JsonValueKind.Array)
									{
										foreach (JsonElement dataItem in dataArray.EnumerateArray())
										{
											// Process each data item which could be a string or an object
											ProcessMeasurement(connection, transaction, dataItem, dataType, aggregatorId, timestamp);
										}
									}
									// Handle case where Data might be a single string that needs parsing
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
	/// Helper method to process a measurement and insert it into the database.
	/// </summary>
	/// <param name="connection">The SQL connection</param>
	/// <param name="transaction">The SQL transaction</param>
	/// <param name="element">The JSON element containing measurement data</param>
	/// <param name="dataType">The type of data (e.g., "Temperature")</param>
	/// <param name="aggregatorId">The Aggregator ID</param>
	/// <param name="timestamp">The timestamp for the measurement</param>
	static void ProcessMeasurement(SqlConnection connection, SqlTransaction transaction,
								  JsonElement element, string dataType, string aggregatorId, DateTime timestamp)
	{
		try
		{
			// If element is a string, try to parse it as JSON
			if (element.ValueKind == JsonValueKind.String)
			{
				string json = element.GetString();
				using (JsonDocument doc = JsonDocument.Parse(json))
				{
					ProcessMeasurement(connection, transaction, doc.RootElement, dataType, aggregatorId, timestamp);
				}
				return;
			}

			// Extract WavyId and Value
			string wavyId = "Unknown";
			double value = 0;
			bool valueFound = false;

			// Extract WavyId - check different possible property names
			if (element.TryGetProperty("WavyId", out JsonElement wavyIdElement))
			{
				wavyId = wavyIdElement.GetString();
			}
			else if (element.TryGetProperty("wavyId", out wavyIdElement))
			{
				wavyId = wavyIdElement.GetString();
			}

			// Extract Value - check different possible property names and types
			if (element.TryGetProperty("Value", out JsonElement valueElement))
			{
				valueFound = TryGetDoubleValue(valueElement, out value);
			}
			else if (element.TryGetProperty("value", out valueElement))
			{
				valueFound = TryGetDoubleValue(valueElement, out value);
			}

			if (!valueFound)
			{
				Console.WriteLine($"[WARNING] No valid value found in measurement data");
				return;
			}

			// Skip unknown Wavy IDs
			if (wavyId == "Unknown")
			{
				Console.WriteLine($"[WARNING] No Wavy ID found for measurement, using 'Unknown'");
			}

			// Ensure the Wavy device exists
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

			// Update or insert mapping between Wavy and Aggregator
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

			// Insert measurement
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

			if (verboseMode)
			{
				Console.WriteLine($"[DB] Measurement saved: WavyID={wavyId}, Type={dataType}, Value={value}");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"[WARNING] Error processing measurement: {ex.Message}");
		}
	}

	/// <summary>
	/// Helper method to safely extract double values from different JSON types
	/// </summary>
	/// <param name="element">The JSON element containing the value</param>
	/// <param name="value">The extracted double value (output)</param>
	/// <returns>True if value was successfully extracted, false otherwise</returns>
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
}
