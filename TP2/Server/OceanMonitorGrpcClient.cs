// Server/OceanMonitorGrpcClient.cs
using System;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using Oceanmonitor;

namespace Server
{
	public class OceanMonitorGrpcClient : IDisposable
	{
		private readonly GrpcChannel channel;
		private readonly DataCalculationService.DataCalculationServiceClient client;
		private bool disposed = false;

		public OceanMonitorGrpcClient(string address)
		{
			channel = GrpcChannel.ForAddress(address);
			client = new DataCalculationService.DataCalculationServiceClient(channel);
		}

		public DataCalculationService.DataCalculationServiceClient GetClient()
		{
			return client;
		}

		public async Task<DataCalculationResponse> CalculateMetricsAsync(string wavyId, string dataType, double value, long timestamp)
		{
			var request = new DataCalculationRequest
			{
				WavyId = wavyId,
				DataType = dataType,
				Value = value,
				Timestamp = timestamp
			};

			return await client.CalculateMetricsAsync(request);
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing)
		{
			if (!disposed)
			{
				if (disposing)
				{
					channel.Dispose();
				}
				disposed = true;
			}
		}
	}
}
