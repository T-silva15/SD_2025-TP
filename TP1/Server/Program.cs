using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Data.SqlClient;

class Servidor
{
	static readonly object dbLock = new object(); // Mutex para acesso à base de dados
	private static readonly int PORT = 6000;

	static void Main()
	{
		TcpListener servidor = new TcpListener(IPAddress.Any, PORT);
		servidor.Start();
		Console.WriteLine($"[SERVIDOR] A ouvir na porta {PORT}...");

		while (true)
		{
			TcpClient cliente = servidor.AcceptTcpClient();
			Thread threadCliente = new Thread(() => LidarComAgregador(cliente));
			threadCliente.Start();
		}
	}

	static void LidarComAgregador(TcpClient client)
	{
		try
		{
			NetworkStream stream = client.GetStream();
			byte[] buffer = new byte[4096];
			int bytesRead = stream.Read(buffer, 0, buffer.Length);

			int code = BitConverter.ToInt32(buffer, 0);

			if (code == 301)
			{
				string jsonData = Encoding.UTF8.GetString(buffer, 4, bytesRead - 4);
				Console.WriteLine($"[301] Dados recebidos do AGREGADOR:\n{jsonData}");

				var dados = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonData);
				if (dados != null)
				{
					GuardarNoBD(dados);

					// Enviar confirmação (código 402)
					byte[] confirm = BitConverter.GetBytes(402);
					stream.Write(confirm, 0, confirm.Length);
					Console.WriteLine("[402] Confirmação enviada ao agregador.");
				}
			}

			stream.Close();
			client.Close();
		}
		catch (Exception ex)
		{
			Console.WriteLine($"[ERRO] {ex.Message}");
		}
	}

	static void GuardarNoBD(Dictionary<string, object> dados)
	{
		lock (dbLock)
		{
			string connectionString = "Server=(localdb)\\mssqllocaldb;Database=OceanMonitor;Trusted_Connection=True;";
			using (var con = new SqlConnection(connectionString))
			{
				con.Open();

				string aggregatorId = dados["AggregatorId"]?.ToString() ?? string.Empty;
				DateTime timestamp = DateTime.Parse(dados["Timestamp"]?.ToString() ?? DateTime.MinValue.ToString());

				var jsonData = dados["Data"]?.ToString() ?? string.Empty;
				var dataList = JsonSerializer.Deserialize<List<Dictionary<string, object>>>(jsonData) ?? new List<Dictionary<string, object>>();

				foreach (var item in dataList)
				{
					string tipo = item["DataType"]?.ToString() ?? string.Empty;
					long totalValue = Convert.ToInt64(item["TotalValue"] ?? 0);

					var wavyIds = JsonSerializer.Deserialize<List<string>>(item["WavyIds"]?.ToString() ?? string.Empty) ?? new List<string>();
					string wavyIdsStr = string.Join(",", wavyIds);

					string sql = @"
                            INSERT INTO Dados (AggregatorId, DataType, TotalValue, Timestamp, WavyIds)
                            VALUES (@ag, @tp, @val, @ts, @wavy)";

					using (var cmd = new SqlCommand(sql, con))
					{
						cmd.Parameters.AddWithValue("@ag", aggregatorId);
						cmd.Parameters.AddWithValue("@tp", tipo);
						cmd.Parameters.AddWithValue("@val", totalValue);
						cmd.Parameters.AddWithValue("@ts", timestamp);
						cmd.Parameters.AddWithValue("@wavy", wavyIdsStr);
						cmd.ExecuteNonQuery();
					}
				}

				Console.WriteLine("[BD] Dados armazenados com sucesso.");
			}
		}
	}
}
