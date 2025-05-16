# grpc_calc_server.py
import time
import math
import numpy as np
from datetime import datetime
from concurrent import futures
import logging

import grpc
import ocean_monitor_pb2
import ocean_monitor_pb2_grpc

# Dictionary to keep track of recent values for calculations
recent_values = {}
max_history_size = 100

class DataCalculationServiceServicer(ocean_monitor_pb2_grpc.DataCalculationServiceServicer):
    def CalculateMetrics(self, request, context):
        """Calculate various metrics based on received sensor data"""
        
        wavy_id = request.wavy_id
        data_type = request.data_type
        value = request.value
        timestamp = request.timestamp
        
        # Create a unique key for this wavy + data type combination
        key = f"{wavy_id}:{data_type}"
        
        # Initialize history for this key if it doesn't exist
        if key not in recent_values:
            recent_values[key] = []
        
        # Add the new value to history
        recent_values[key].append(value)
        
        # Trim history if it's too large
        if len(recent_values[key]) > max_history_size:
            recent_values[key] = recent_values[key][-max_history_size:]
        
        # Get the history for calculations
        history = recent_values[key]
        
        # Calculate metrics
        average_value = np.mean(history) if len(history) > 0 else value
        variance = np.var(history) if len(history) > 1 else 0.0
        
        # Calculate trend coefficient (slope of linear regression)
        trend_coefficient = 0.0
        if len(history) > 2:
            x = np.arange(len(history))
            y = np.array(history)
            trend_coefficient = np.polyfit(x, y, 1)[0]
        
        # Calculate normalized value (z-score)
        normalized_value = 0.0
        if len(history) > 1 and variance > 0:
            normalized_value = (value - average_value) / math.sqrt(variance)
        
        # Calculate anomaly score (absolute z-score)
        anomaly_score = abs(normalized_value)
        
        # Log the calculation
        logging.info(f"Calculated metrics for {key}: avg={average_value:.2f}, var={variance:.2f}, trend={trend_coefficient:.2f}")
        
        # Create and return the response
        return ocean_monitor_pb2.DataCalculationResponse(
            wavy_id=wavy_id,
            data_type=data_type,
            original_value=value,
            average_value=average_value,
            variance=variance,
            trend_coefficient=trend_coefficient,
            normalized_value=normalized_value,
            anomaly_score=anomaly_score,
            timestamp=timestamp
        )

def serve():
    """Start the gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ocean_monitor_pb2_grpc.add_DataCalculationServiceServicer_to_server(
        DataCalculationServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Python calculation server started on port 50051")
    try:
        while True:
            time.sleep(60 * 60 * 24)  # Run for 24 hours
    except KeyboardInterrupt:
        server.stop(0)
        print("Server stopped")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    serve()
