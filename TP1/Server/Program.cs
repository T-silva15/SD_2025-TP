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
			string connectionString = "Server=localhost;Database=OceanMonitor;Trusted_Connection=True;";
			using (var connection = new SqlConnection(connectionString))
			{
				connection.Open();

				string aggregatorId = data["AggregatorId"].ToString();
				DateTime timestamp = DateTime.Parse(data["Timestamp"].ToString());
				var dataList = JsonSerializer.Deserialize<List<Dictionary<string, object>>>(data["Data"].ToString());

				foreach (var item in dataList)
				{
					string dataType = item["DataType"].ToString();
					double totalValue = Convert.ToDouble(item["TotalValue"]);
					var wavyIds = JsonSerializer.Deserialize<List<string>>(item["WavyIds"].ToString());

					foreach (var wavyId in wavyIds)
					{
						UpdateOrInsertWavy(connection, wavyId, dataType, totalValue);

						string sqlAgr = @"
                            IF NOT EXISTS (SELECT 1 FROM Agregadores WHERE AgrID = @ag)
                            INSERT INTO Agregadores (AgrID, WavyID, Timestamp)
                            VALUES (@ag, @wavy, @ts)";

						using (var cmd = new SqlCommand(sqlAgr, connection))
						{
							cmd.Parameters.AddWithValue("@ag", aggregatorId);
							cmd.Parameters.AddWithValue("@wavy", wavyId);
							cmd.Parameters.AddWithValue("@ts", timestamp);
							cmd.ExecuteNonQuery();
						}
					}
				}

				Console.WriteLine("[DB] Data inserted into WAVYS and AGREGADORES tables.");
			}
		}
	}

	static void UpdateOrInsertWavy(SqlConnection connection, string wavyId, string dataType, double value)
	{
		string checkSql = "SELECT COUNT(*) FROM Wavys WHERE WavyID = @id";
		using (var checkCmd = new SqlCommand(checkSql, connection))
		{
			checkCmd.Parameters.AddWithValue("@id", wavyId);
			int count = (int)checkCmd.ExecuteScalar();

			string column = dataType switch
			{
				"Temperature" => "Temperatura",
				"Velocity" => "Velocidade",
				"Decibels" => "Decibeis",
				"Frequency" => "Frequencia",
				_ => null
			};

			if (column == null) return;

			if (count > 0)
			{
				string updateSql = $"UPDATE Wavys SET {column} = @val WHERE WavyID = @id";
				using (var updateCmd = new SqlCommand(updateSql, connection))
				{
					updateCmd.Parameters.AddWithValue("@val", value);
					updateCmd.Parameters.AddWithValue("@id", wavyId);
					updateCmd.ExecuteNonQuery();
				}
			}
			else
			{
				string insertSql = $@"
                    INSERT INTO Wavys (WavyID, {column}) 
                    VALUES (@id, @val)";
				using (var insertCmd = new SqlCommand(insertSql, connection))
				{
					insertCmd.Parameters.AddWithValue("@id", wavyId);
					insertCmd.Parameters.AddWithValue("@val", value);
					insertCmd.ExecuteNonQuery();
				}
			}
		}
	}
}
