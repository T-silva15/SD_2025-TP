using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;

public class Wavy
{
    private static readonly string aggregatorIp = "127.0.0.1";
    private static readonly int aggregatorPort = 5000;
    private static readonly string wavyId = Guid.NewGuid().ToString();

    public static void Main(string[] args)
    {
        try
        {
            using (TcpClient client = new TcpClient(aggregatorIp, aggregatorPort))
            using (NetworkStream stream = client.GetStream())
            {
                Console.WriteLine($"Connected to Aggregator as Wavy ID: {wavyId}");
                SendCode(stream, 101); // Send connection code

                while (true)
                {
					SendSensorData(stream, "Temperature", GenerateTemp(40, 0));
					Thread.Sleep(60000);

					SendSensorData(stream, "WindSpeed", GenerateTemp(300, 0));
					Thread.Sleep(30000);

					SendSensorData(stream, "Frequency", GenerateTemp(108, 0));
                    Thread.Sleep(20000);

                    SendSensorData(stream, "Decibels", GenerateTemp(150, 0));
                    Thread.Sleep(10000);
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error: " + ex.Message);
        }
        finally
        {
            try
            {
                using (TcpClient client = new TcpClient(aggregatorIp, aggregatorPort))
                using (NetworkStream stream = client.GetStream())
                {
                    SendCode(stream, 501); // Send disconnection code
                    Console.WriteLine("Disconnected from Aggregator.");
                }
            }
            catch { }
        }
    }

    public static float GenerateTemp(int max, int min)
    {
        Random random = new Random();
        return (float)random.NextDouble() * (max - min) + min;
    }

    private static void SendSensorData(NetworkStream stream, string dataType, float value)
    {
        var data = new Dictionary<string, object>
        {
            { "WavyId", wavyId },
            { "DataType", dataType },
            { "Value", (long)value } // Cast to long to match Aggregator expectation
        };

        string json = JsonSerializer.Serialize(data);
        bool confirmed = false;

        while (!confirmed)
        {
            SendMessage(stream, json, 200); // Send with code 200 (initial send)
            Console.WriteLine($"Sent {dataType} data to Aggregator.");

            confirmed = WaitForConfirmation(stream, 401);
            if (!confirmed)
            {
                Console.WriteLine("No confirmation received, resending...");
                SendMessage(stream, json, 201); // Re-send with code 201
                Thread.Sleep(2000);
            }
        }
    }

    private static void SendCode(NetworkStream stream, int code)
    {
        byte[] codeBytes = BitConverter.GetBytes(code);
        stream.Write(codeBytes, 0, codeBytes.Length);
    }

    private static void SendMessage(NetworkStream stream, string message, int code)
    {
        byte[] codeBytes = BitConverter.GetBytes(code);
        byte[] messageBytes = Encoding.UTF8.GetBytes(message);
        byte[] data = new byte[codeBytes.Length + messageBytes.Length];
        Buffer.BlockCopy(codeBytes, 0, data, 0, codeBytes.Length);
        Buffer.BlockCopy(messageBytes, 0, data, codeBytes.Length, messageBytes.Length);
        stream.Write(data, 0, data.Length);
    }

    private static bool WaitForConfirmation(NetworkStream stream, int expectedCode)
    {
        try
        {
            byte[] buffer = new byte[4];
            int bytesRead = stream.Read(buffer, 0, buffer.Length);
            if (bytesRead == 4)
            {
                int code = BitConverter.ToInt32(buffer, 0);
                return code == expectedCode;
            }
        }
        catch { }
        return false;
    }
}
