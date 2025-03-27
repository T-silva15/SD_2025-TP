using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;

/// <summary>
/// This class represents a Wavy client that sends sensor data to the Aggregator.
/// Each instance of Wavy generates random sensor data for different data types.
/// </summary>
public class Wavy
{
	private static readonly string aggregatorIp = "127.0.0.1"; // IP address of the Aggregator
	private static readonly int aggregatorPort = 5000; // Port for connecting and send data to the Aggregator
	private static readonly string wavyId = Guid.NewGuid().ToString(); // Unique ID for this Wavy instance
	
		
	private static readonly bool useHttpLikeMode = false; // Flag to switch between modes

	/// <summary>
	/// Program entry point. Connects to the Aggregator and starts sending sensor data.
	/// </summary>
	public static void Main(string[] args)
	{
		if (useHttpLikeMode)
		{
			// HTTP-like mode
			var dataQueue = new ConcurrentQueue<Dictionary<string, object>>();

			Thread temperatureThread = new Thread(() => GenerateSensorData("Temperature", 60000, dataQueue));
			Thread windSpeedThread = new Thread(() => GenerateSensorData("WindSpeed", 30000, dataQueue));
			Thread frequencyThread = new Thread(() => GenerateSensorData("Frequency", 20000, dataQueue));
			Thread decibelsThread = new Thread(() => GenerateSensorData("Decibels", 10000, dataQueue));

			temperatureThread.Start();
			windSpeedThread.Start();
			frequencyThread.Start();
			decibelsThread.Start();

			while (true)
			{
				SendAggregatedSensorDataHttpLike(dataQueue);
				Thread.Sleep(60000); // Send data every minute
			}
		}
		else
		{
			// Continuous connection mode
			try
			{
				using (TcpClient client = new TcpClient(aggregatorIp, aggregatorPort))
				using (NetworkStream stream = client.GetStream())
				{
					Console.WriteLine($"Connected to Aggregator as Wavy ID: {wavyId}");
					SendCode(stream, 101); // Send connection code

					while (true)
					{
						SendSensorData(stream, "Temperature", GenerateRandomValue(40, 0));
						Thread.Sleep(60000);

						SendSensorData(stream, "WindSpeed", GenerateRandomValue(300, 0));
						Thread.Sleep(30000);

						SendSensorData(stream, "Frequency", GenerateRandomValue(108, 0));
						Thread.Sleep(20000);

						SendSensorData(stream, "Decibels", GenerateRandomValue(150, 0));
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
	}

	/// <summary>
	/// Generates random sensor data for different data types between a given range.
	/// </summary>
	public static float GenerateRandomValue(int max, int min)
	{
		Random random = new Random();
		return (float)random.NextDouble() * (max - min) + min;
	}

	/// <summary>
	/// Sends aggregated sensor data to the Aggregator in HTTP-like mode.
	/// Connects, sends data, waits for confirmation, and disconnects.
	/// </summary>
	private static void SendAggregatedSensorDataHttpLike(ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		try
		{
			using (TcpClient client = new TcpClient(aggregatorIp, aggregatorPort))
			using (NetworkStream stream = client.GetStream())
			{
				var aggregatedData = new Dictionary<string, object>
			{
				{ "WavyId", wavyId },
				{ "Data", new List<Dictionary<string, object>>() }
			};

				var dataList = (List<Dictionary<string, object>>)aggregatedData["Data"];

				while (dataQueue.TryDequeue(out var data))
				{
					var existingData = dataList.Find(d => d["DataType"].ToString() == data["DataType"].ToString());
					if (existingData != null)
					{
						existingData["Value"] = (long)existingData["Value"] + (long)data["Value"];
					}
					else
					{
						dataList.Add(data);
					}
				}

				if (dataList.Count == 0)
				{
					return; // No data to send
				}

				// Serialize the data to JSON
				string json = JsonSerializer.Serialize(aggregatedData);
				bool confirmed = false;

				// Send data with retry mechanism until confirmation is received
				while (!confirmed)
				{
					SendMessage(stream, json, 200); // Send with code 200 (initial send)
					Console.WriteLine("Sent aggregated sensor data to Aggregator.");

					confirmed = WaitForConfirmation(stream, 401);

					// If no confirmation received, re-send
					if (!confirmed)
					{
						Console.WriteLine("No confirmation received, resending...");
						SendMessage(stream, json, 201); // Re-send with code 201
						Thread.Sleep(2000);
					}
				}
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine("Error: " + ex.Message);
		}
	}

	/// <summary>
	/// Creates a dictionary representing sensor data. Used for connection mode.
	/// </summary>
	private static Dictionary<string, object> CreateSensorData(string dataType, float value)
	{
		return new Dictionary<string, object>
	{
		{ "DataType", dataType }, // Add data type
        { "Value", (long)value } // Cast to long to match Aggregator expectation
    };
	}

	/// <summary>
	/// Generates sensor data for a specific data type at a specified interval and adds it to the queue.
	/// </summary>
	private static void GenerateSensorData(string dataType, int interval, ConcurrentQueue<Dictionary<string, object>> dataQueue)
	{
		while (true)
		{
			var data = CreateSensorData(dataType, GenerateRandomValue(40, 0));
			dataQueue.Enqueue(data);
			Thread.Sleep(interval);
		}
	}

	/// <summary>
	/// Sends sensor data to the Aggregator in continuous connection mode.
	/// </summary>
	private static void SendSensorData(NetworkStream stream, string dataType, float value)
	{
		// Create a dictionary to hold the data to be sent
		var data = new Dictionary<string, object>
		{
			{ "WavyId", wavyId }, // Add Wavy ID
            { "DataType", dataType }, // Add data type
            { "Value", (long)value } // Cast to long to match Aggregator expectation
        };

		// Serialize the data to JSON
		string json = JsonSerializer.Serialize(data);
		bool confirmed = false;

		// Send data with retry mechanism until confirmation is received
		while (!confirmed)
		{
			SendMessage(stream, json, 200); // Send with code 200 (initial send)
			Console.WriteLine($"Sent {dataType} data to Aggregator.");

			confirmed = WaitForConfirmation(stream, 401);

			// If no confirmation received, re-send
			if (!confirmed)
			{
				Console.WriteLine("No confirmation received, resending...");
				SendMessage(stream, json, 201); // Re-send with code 201
				Thread.Sleep(2000);
			}
		}
	}

	/// <summary>
	/// Sends a code to the Aggregator without any message.
	/// </summary>
	private static void SendCode(NetworkStream stream, int code)
	{
		byte[] codeBytes = BitConverter.GetBytes(code);
		stream.Write(codeBytes, 0, codeBytes.Length);
	}

	/// <summary>
	/// Sends a message to the Aggregator with code and content.
	/// </summary>
	private static void SendMessage(NetworkStream stream, string message, int code)
	{
		byte[] codeBytes = BitConverter.GetBytes(code);
		byte[] messageBytes = Encoding.UTF8.GetBytes(message);
		byte[] data = new byte[codeBytes.Length + messageBytes.Length];
		Buffer.BlockCopy(codeBytes, 0, data, 0, codeBytes.Length);
		Buffer.BlockCopy(messageBytes, 0, data, codeBytes.Length, messageBytes.Length);
		stream.Write(data, 0, data.Length);
	}

	/// <summary>
	/// Waits for a confirmation (4 bytes with the confirmation code).
	/// </summary>
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

