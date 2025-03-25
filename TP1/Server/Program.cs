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
    static readonly object dbLock = new object();
    const int PORT = 6000;

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
                Console.WriteLine($"[301] Dados recebidos:\n{jsonData}");

                var dados = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonData);
                if (dados != null)
                {
                    GuardarNasTabelas(dados);

                    // Enviar confirmação (402)
                    byte[] confirm = BitConverter.GetBytes(402);
                    stream.Write(confirm, 0, confirm.Length);
                    Console.WriteLine("[402] Confirmação enviada.");
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

    static void GuardarNasTabelas(Dictionary<string, object> dados)
    {
        lock (dbLock)
        {
            string connectionString = "Server=localhost;Database=OceanMonitor;Trusted_Connection=True;";
            using (var con = new SqlConnection(connectionString))
            {
                con.Open();

                string aggregatorId = dados["AggregatorId"].ToString();
                DateTime timestamp = DateTime.Parse(dados["Timestamp"].ToString());
                var dataList = JsonSerializer.Deserialize<List<Dictionary<string, object>>>(dados["Data"].ToString());

                foreach (var item in dataList)
                {
                    string dataType = item["DataType"].ToString();
                    double totalValue = Convert.ToDouble(item["TotalValue"]);
                    var wavyIds = JsonSerializer.Deserialize<List<string>>(item["WavyIds"].ToString());

                    foreach (var wavyId in wavyIds)
                    {
                        AtualizarOuInserirWavy(con, wavyId, dataType, totalValue);

                        string sqlAgr = @"
                            IF NOT EXISTS (SELECT 1 FROM Agregadores WHERE AgrID = @ag)
                            INSERT INTO Agregadores (AgrID, WavyID, Timestamp)
                            VALUES (@ag, @wavy, @ts)";

                        using (var cmd = new SqlCommand(sqlAgr, con))
                        {
                            cmd.Parameters.AddWithValue("@ag", aggregatorId);
                            cmd.Parameters.AddWithValue("@wavy", wavyId);
                            cmd.Parameters.AddWithValue("@ts", timestamp);
                            cmd.ExecuteNonQuery();
                        }
                    }
                }

                Console.WriteLine("[BD] Dados inseridos em WAVYS e AGREGADORES.");
            }
        }
    }

    static void AtualizarOuInserirWavy(SqlConnection con, string wavyId, string tipo, double valor)
    {
        string checkSql = "SELECT COUNT(*) FROM Wavys WHERE WavyID = @id";
        using (var checkCmd = new SqlCommand(checkSql, con))
        {
            checkCmd.Parameters.AddWithValue("@id", wavyId);
            int count = (int)checkCmd.ExecuteScalar();

            string col = tipo switch
            {
                "Temperatura" => "Temperatura",
                "Velocidade" => "Velocidade",
                "Decibeis" => "Decibeis",
                "Frequencia" => "Frequencia",
                _ => null
            };

            if (col == null) return;

            if (count > 0)
            {
                string updateSql = $"UPDATE Wavys SET {col} = @val WHERE WavyID = @id";
                using (var updateCmd = new SqlCommand(updateSql, con))
                {
                    updateCmd.Parameters.AddWithValue("@val", valor);
                    updateCmd.Parameters.AddWithValue("@id", wavyId);
                    updateCmd.ExecuteNonQuery();
                }
            }
            else
            {
                string insertSql = $@"
                    INSERT INTO Wavys (WavyID, {col}) 
                    VALUES (@id, @val)";
                using (var insertCmd = new SqlCommand(insertSql, con))
                {
                    insertCmd.Parameters.AddWithValue("@id", wavyId);
                    insertCmd.Parameters.AddWithValue("@val", valor);
                    insertCmd.ExecuteNonQuery();
                }
            }
        }
    }
}
