using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using Microsoft.Data.SqlClient;

class Server
{
	static readonly object dbLock = new object();
	const int PORT = 6000;
	static readonly Dictionary<string, DateTime> connectedAggregators = new Dictionary<string, DateTime>();

	private static class MessageCodes
	{
		public const int AggregatorConnect = 102;
		public const int AggregatorData = 301;
		public const int AggregatorDataResend = 302;
		public const int AggregatorDisconnect = 502;
		public const int ServerConfirmation = 402;
	}

	static void Main()
	{
		TcpListener server = new TcpListener(IPAddress.Parse("127.0.0.1"), PORT);
		server.Start();
		Console.WriteLine($"[SERVER] Listening on port {PORT}...");

		while (true)
		{
			TcpClient client = server.AcceptTcpClient();
			Thread clientThread = new Thread(() => HandleAggregator(client));
			clientThread.Start();
		}
	}

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

	static void ProcessData(byte[] buffer, int bytesRead, NetworkStream stream, int code, ref string aggregatorId)
	{
		string jsonData = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);
		Console.WriteLine($"[{code}] Data received from {(aggregatorId != "Unknown" ? "Aggregator " + aggregatorId : "unknown aggregator")}");

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
		}
	}

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

	static void SaveToDatabase(Dictionary<string, object> data)
	{
		lock (dbLock)
		{
			string connectionString = "Server=(localdb)\\mssqllocaldb;Database=OceanMonitor;Trusted_Connection=True;TrustServerCertificate=True;";
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

	// Helper method to process a measurement and insert it into the new database structure
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

			Console.WriteLine($"[DB] Measurement saved: WavyID={wavyId}, Type={dataType}, Value={value}");
		}
		catch (Exception ex)
		{
			Console.WriteLine($"[WARNING] Error processing measurement: {ex.Message}");
		}
	}


	// Helper method to safely extract double values from different JSON types
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
}
