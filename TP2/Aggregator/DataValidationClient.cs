// DataValidationClient.cs
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Grpc.Net.Client;
using Oceanmonitor;

namespace OceanMonitorSystem
{
	/// <summary>
	/// Client for the remote data validation gRPC service
	/// </summary>
	public class DataValidationClient : IDisposable
	{
		private readonly GrpcChannel _channel;
		private readonly bool _verboseMode;

		// This will be initialized after the generated gRPC classes are available
		private readonly dynamic _client;

		/// <summary>
		/// Creates a new instance of the DataValidationClient
		/// </summary>
		/// <param name="serverAddress">The address of the gRPC validation server</param>
		/// <param name="verboseMode">Whether to log detailed information</param>
		public DataValidationClient(string serverAddress, bool verboseMode = false)
		{
			_verboseMode = verboseMode;

			// Configure gRPC channel
			var channelOptions = new GrpcChannelOptions
			{
				MaxReceiveMessageSize = 16 * 1024 * 1024, // 16 MB
				MaxSendMessageSize = 4 * 1024 * 1024      // 4 MB
			};

			Console.WriteLine($"[VALIDATION] Initializing connection to validation service at {serverAddress}");

			// Create the channel but defer actual client creation
			_channel = GrpcChannel.ForAddress(serverAddress, channelOptions);

			// Note: We'll handle the actual service connection dynamically
			// to avoid the compile-time dependency on the generated classes

			Console.WriteLine($"[VALIDATION] Channel created for validation service");
		}

		/// <summary>
		/// Validates and cleans data received from a Wavy client
		/// </summary>
		/// <param name="wavyId">The ID of the Wavy device</param>
		/// <param name="dataJson">The JSON data to validate</param>
		/// <returns>Tuple containing success status and cleaned data</returns>
		public async Task<(bool IsValid, string CleanedData)> ValidateAndCleanDataAsync(string wavyId, string dataJson)
		{
			try
			{
				Console.WriteLine("\n=== Data Validation Process ===");
				Console.WriteLine($"Validating data for Wavy: {wavyId}");

				if (_verboseMode)
				{
					Console.WriteLine($"Original data: {dataJson}");
				}

				// Parse the incoming JSON
				using JsonDocument doc = JsonDocument.Parse(dataJson);
				JsonElement root = doc.RootElement;

				// Extract the data array
				if (!root.TryGetProperty("Data", out JsonElement dataArray) ||
					dataArray.ValueKind != JsonValueKind.Array)
				{
					Console.WriteLine("[ERROR] Invalid data format: 'Data' array not found");
					return (false, dataJson);
				}

				// Build the validation request
				var request = new ValidationRequest { WavyId = wavyId };
				int itemCount = 0;

				foreach (JsonElement item in dataArray.EnumerateArray())
				{
					if (item.TryGetProperty("DataType", out JsonElement dataTypeElement) &&
						item.TryGetProperty("Value", out JsonElement valueElement))
					{
						string dataType = dataTypeElement.GetString();
						double value = valueElement.GetDouble();

						Console.WriteLine($"→ Item {++itemCount}: {dataType} = {value}");

						request.Data.Add(new DataItem
						{
							DataType = dataType,
							Value = value,
							Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
						});
					}
				}

				Console.WriteLine($"Sending {request.Data.Count} items to validation service...");

				// Create the gRPC client and send request
				var client = new DataValidationService.DataValidationServiceClient(_channel);
				var response = await client.ValidateDataAsync(request);

				// Process and display response information
				Console.ForegroundColor = response.Status == ValidationResponse.Types.Status.Ok ?
					ConsoleColor.Green :
					(response.Status == ValidationResponse.Types.Status.PartiallyValid ? ConsoleColor.Yellow : ConsoleColor.Red);

				Console.WriteLine($"Validation Status: {response.Status}");
				Console.WriteLine($"Message: {response.Message}");
				Console.ResetColor();

				// Check if validation failed entirely
				if (response.Status == ValidationResponse.Types.Status.Invalid)
				{
					Console.WriteLine("[WARNING] Validation failed - cannot use this data");
					return (false, dataJson);
				}

				// Display detailed validation results
				Console.WriteLine("\nValidation Results:");
				int modifiedCount = 0;

				foreach (var item in response.Data)
				{
					string statusSymbol = item.WasModified ? "✓" : "✗";
					ConsoleColor itemColor = item.WasModified ? ConsoleColor.Yellow : ConsoleColor.Green;

					Console.ForegroundColor = itemColor;

					if (item.WasModified)
					{
						modifiedCount++;
						Console.WriteLine($"  {statusSymbol} {item.DataType}: {item.OriginalValue:F2} → {item.CleanedValue:F2}");
						Console.WriteLine($"     Reason: {item.ValidationMessage}");
					}
					else if (_verboseMode)
					{
						Console.WriteLine($"  {statusSymbol} {item.DataType}: {item.CleanedValue:F2} (valid)");
					}

					Console.ResetColor();
				}

				// Summary
				Console.WriteLine($"\nSummary: {modifiedCount} of {response.Data.Count} items required cleaning");

				// Reconstruct the data with cleaned values
				var cleanedData = new
				{
					WavyId = wavyId,
					Data = response.Data.Select(item => new
					{
						DataType = item.DataType,
						Value = item.CleanedValue,
						WasModified = item.WasModified,
						ValidationMessage = item.ValidationMessage
					}).ToArray()
				};

				string cleanedJson = JsonSerializer.Serialize(cleanedData, new JsonSerializerOptions { WriteIndented = true });

				if (_verboseMode)
				{
					Console.WriteLine($"Cleaned data: {cleanedJson}");
				}

				Console.WriteLine("=== Validation Complete ===\n");
				return (true, cleanedJson);
			}
			catch (Exception ex)
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine($"[VALIDATION ERROR] {ex.Message}");
				Console.WriteLine($"Stack trace: {ex.StackTrace}");
				Console.ResetColor();
				return (false, dataJson);
			}
		}

		/// <summary>
		/// Disposes the gRPC channel
		/// </summary>
		public void Dispose()
		{
			_channel?.Dispose();
		}
	}
}
