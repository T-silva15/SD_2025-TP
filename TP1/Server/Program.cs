using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using Microsoft.Data.SqlClient;

/// <summary>
/// Server component of the OceanMonitor system responsible for receiving, processing,
/// and storing data from Aggregator clients in a SQL database.
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
	static string connectionString = "Server=(localdb)\\mssqllocaldb;Database=OceanMonitor;Trusted_Connection=True;TrustServerCertificate=True;";

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

	#region Entry Point and Main Thread Management

	/// <summary>
	/// Application entry point. Initializes server components and starts handling connections.
	/// </summary>
	static void Main()
	{
		Thread keyboardThread = new Thread(MonitorKeyboard) { IsBackground = true, Name = "KeyboardMonitor" };
		keyboardThread.Start();

		TcpListener server = new TcpListener(IPAddress.Parse("127.0.0.1"), PORT);
		server.Start();
		Console.WriteLine($"[SERVER] Listening on port {PORT}...");
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
				Console.WriteLine($"[ERROR] Failed to accept client connection: {ex.Message}");
				Thread.Sleep(1000);
			}
		}

		server.Stop();
		Console.WriteLine("[SERVER] Shutdown complete.");
	}

	/// <summary>
	/// Displays the available commands in the console
	/// </summary>
	static void DisplayCommandMenu()
	{
		Console.WriteLine("\nAvailable Commands:");
		Console.WriteLine("Press 'A' to show connected aggregators");
		Console.WriteLine("Press 'D' to show database statistics");
		Console.WriteLine("Press 'V' to toggle verbose mode");
		Console.WriteLine("Press 'C' to modify connection string");
		Console.WriteLine("Press 'I' to check for inactive devices");
		Console.WriteLine("Press 'W' to clear screen");
		Console.WriteLine("Press 'Q' to quit");
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
	/// Identifies and optionally removes inactive devices from the database
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
							// Count inactive devices
							int inactiveAggregatorsCount = CountInactiveAggregators(connection, transaction, inactiveThreshold);
							int inactiveWavysCount = CountInactiveWavys(connection, transaction, inactiveThreshold);

							Console.WriteLine($"Found {inactiveAggregatorsCount} inactive Aggregators and {inactiveWavysCount} inactive Wavys");

							if (inactiveAggregatorsCount > 0 || inactiveWavysCount > 0)
							{
								if (ConfirmDeletion())
								{
									DeleteInactiveDeviceData(connection, transaction, inactiveThreshold);
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
	/// Asks for user confirmation before deleting inactive devices
	/// </summary>
	static bool ConfirmDeletion()
	{
		Console.Write("Do you want to remove these inactive devices? (y/n): ");
		string answer = Console.ReadLine()?.ToLower();
		return answer == "y" || answer == "yes";
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

	/// <summary>
	/// Clears the console screen and displays the menu again
	/// </summary>
	static void ClearScreenAndShowMenu()
	{
		Console.Clear();
		Console.WriteLine($"[SERVER] Listening on port {PORT}...");
		DisplayCommandMenu();
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

		Console.WriteLine($"[102] Connection established with Aggregator {aggregatorId} from {clientIp}");
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

		Console.WriteLine($"[502] Disconnection request from Aggregator {aggregatorId}");
		SendConfirmation(stream, MessageCodes.ServerConfirmation);
	}

	/// <summary>
	/// Processes data received from an Aggregator, deserializes it, and stores it in the database
	/// </summary>
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
			Console.WriteLine($"[ERROR] Failed to send confirmation: {ex.Message}");
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
	/// Processes a measurement data item and inserts it into the database
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

			// Insert the measurement record
			InsertMeasurement(connection, transaction, wavyId, aggregatorId, dataType, value, timestamp);

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
	/// Extracts the Wavy ID from a measurement element
	/// </summary>
	static string ExtractWavyId(JsonElement element)
	{
		if (element.TryGetProperty("WavyId", out JsonElement wavyIdElement))
		{
			return wavyIdElement.GetString();
		}
		else if (element.TryGetProperty("wavyId", out wavyIdElement))
		{
			return wavyIdElement.GetString();
		}

		Console.WriteLine("[WARNING] No Wavy ID found in measurement, using 'Unknown'");
		return "Unknown";
	}

	/// <summary>
	/// Tries to extract a numeric value from a measurement element
	/// </summary>
	static bool TryExtractValue(JsonElement element, out double value)
	{
		if (element.TryGetProperty("Value", out JsonElement valueElement))
		{
			return TryGetDoubleValue(valueElement, out value);
		}
		else if (element.TryGetProperty("value", out valueElement))
		{
			return TryGetDoubleValue(valueElement, out value);
		}

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
}
