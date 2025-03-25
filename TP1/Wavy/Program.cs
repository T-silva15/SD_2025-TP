using System;
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
    private static readonly string wavyId = Guid.NewGuid().ToString(); //id for this Wavy instance (unique) to simulate multiple Wavy instances


    /// <summary>
    /// program entry point, here we connect to the Aggregator and start sending sensor data:
    /// 1. Tries to connect to the Aggregator
    /// 2. Sends connection code to the Aggregator (101)
    /// 3. gets into an infinite loop that sends sensor data to the Aggregator every 10 seconds:
    /// 4. Sends disconnection code to the Aggregator (501)
    /// </summary>
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
                    SendSensorData(stream, "Frequency", GenerateTemp(108, 0));
                    Thread.Sleep(10000);

                    SendSensorData(stream, "Decibels", GenerateTemp(150, 0));
                    Thread.Sleep(10000);

                    SendSensorData(stream, "WindSpeed", GenerateTemp(300, 0));
                    Thread.Sleep(10000);

                    SendSensorData(stream, "Temperature", GenerateTemp(40, 0));
                    Thread.Sleep(30000);
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


    /// <summary>
    /// simulates generating random sensor data for different data
    /// types between a given range
    public static float GenerateTemp(int max, int min)
    {
        Random random = new Random();
        return (float)random.NextDouble() * (max - min) + min;
    }

    /// <summary>
    /// this method sends sensor data to the Aggregator, 
    /// waits for confirmation, and re-sends if necessary
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
    /// Sends a code to the Aggregator without any message
    /// Per example: 101 for connection, 501 for disconnection
    private static void SendCode(NetworkStream stream, int code)
    {
        byte[] codeBytes = BitConverter.GetBytes(code);
        stream.Write(codeBytes, 0, codeBytes.Length);
    }


    /// <summary>
    /// Message sending method that sends a message to the Aggregator
    /// with code and content
    /// -
    /// - converts the code (4 bytes) and the message (UTF8) to bytes
    /// - unite the code and message bytes into a single byte array and send it by the stream
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
    /// waits for a confirmation (4 bytes with the confirmation code)
    /// if the confirmation code is the expected code (usually 401), returns true
    /// if not, returns false
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
