// lib.rs
use tonic::{transport::Server, Request, Response, Status};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Import the generated proto code
pub mod oceanmonitor {
    tonic::include_proto!("oceanmonitor");
}

use oceanmonitor::{
    ValidationRequest, ValidationResponse, DataItem, CleanedDataItem,
    validation_response::Status as ValidationStatus,
};

// Service implementation
pub struct DataValidationService {
    // Data ranges for different measurement types
    validation_ranges: Arc<Mutex<HashMap<String, (f64, f64)>>>,
    // Track history of measurements for each Wavy ID and type
    measurement_history: Arc<Mutex<HashMap<String, HashMap<String, Vec<f64>>>>>, 
}

impl Default for DataValidationService {
    fn default() -> Self {
        let mut ranges = HashMap::new();
        // Define reasonable ranges for each data type (min, max)
        ranges.insert("Temperature".to_string(), (-10.0, 50.0));
        ranges.insert("Frequency".to_string(), (10.0, 1000.0));
        ranges.insert("WindSpeed".to_string(), (0.0, 150.0));
        ranges.insert("Decibels".to_string(), (0.0, 200.0));
        
        DataValidationService {
            validation_ranges: Arc::new(Mutex::new(ranges)),
            measurement_history: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

// Helper methods for data validation
impl DataValidationService {
    // Validate and clean a single data item
    fn validate_data_item(&self, item: &DataItem) -> CleanedDataItem {
        let data_type = &item.data_type;
        let value = item.value;
        let timestamp = item.timestamp;
        let mut cleaned_value = value;
        let mut was_modified = false;
        let mut message = "Valid data".to_string();
        
        // Get validation range for this data type
        let ranges = self.validation_ranges.lock().unwrap();
        let (min, max) = ranges.get(data_type).unwrap_or(&(-f64::INFINITY, f64::INFINITY));
        
        // Check if value is outside acceptable range
        if value < *min {
            cleaned_value = *min;
            was_modified = true;
            message = format!("Value below minimum ({}), set to minimum", min);
        } else if value > *max {
            cleaned_value = *max;
            was_modified = true;
            message = format!("Value above maximum ({}), set to maximum", max);
        }
        
        // Check for NaN or Infinity
        if value.is_nan() || value.is_infinite() {
            cleaned_value = self.get_average_for_type(&data_type).unwrap_or(0.0);
            was_modified = true;
            message = "Invalid value (NaN or Infinity), replaced with average".to_string();
        }
        
        // Return cleaned data item
        CleanedDataItem {
            data_type: data_type.clone(),
            original_value: value,
            cleaned_value,
            was_modified,
            validation_message: message,
            timestamp,
        }
    }
    
    // Get average value for a data type from history
    fn get_average_for_type(&self, data_type: &str) -> Option<f64> {
        let history = self.measurement_history.lock().unwrap();
        
        let mut values: Vec<f64> = Vec::new();
        for (_, type_history) in history.iter() {
            if let Some(measurements) = type_history.get(data_type) {
                values.extend(measurements);
            }
        }
        
        if values.is_empty() {
            None
        } else {
            let sum: f64 = values.iter().sum();
            Some(sum / values.len() as f64)
        }
    }
    
    // Update measurement history
    fn update_history(&self, wavy_id: &str, data_item: &CleanedDataItem) {
        let mut history = self.measurement_history.lock().unwrap();
        
        let wavy_history = history.entry(wavy_id.to_string()).or_insert_with(HashMap::new);
        let measurements = wavy_history.entry(data_item.data_type.clone()).or_insert_with(Vec::new);
        
        // Keep last 100 measurements
        if measurements.len() >= 100 {
            measurements.remove(0);
        }
        
        // Only add valid measurements to history
        if !data_item.was_modified {
            measurements.push(data_item.cleaned_value);
        }
    }
    
    // Check for anomalies by comparing with recent history
    fn detect_anomalies(&self, wavy_id: &str, data_type: &str, value: f64) -> Option<String> {
        let history = self.measurement_history.lock().unwrap();
        
        if let Some(wavy_history) = history.get(wavy_id) {
            if let Some(measurements) = wavy_history.get(data_type) {
                if measurements.len() < 5 {
                    return None; // Not enough history to detect anomalies
                }
                
                // Calculate mean and standard deviation
                let sum: f64 = measurements.iter().sum();
                let mean = sum / measurements.len() as f64;
                
                let variance: f64 = measurements.iter()
                    .map(|x| (*x - mean).powi(2))
                    .sum::<f64>() / measurements.len() as f64;
                
                let std_dev = variance.sqrt();
                
                // Check if value is an outlier (more than 3 standard deviations from mean)
                if (value - mean).abs() > 3.0 * std_dev {
                    return Some(format!("Possible anomaly: value deviates significantly from history (mean: {:.2}, stddev: {:.2})", mean, std_dev));
                }
            }
        }
        
        None
    }
}

// Implement the gRPC service
#[tonic::async_trait]
impl oceanmonitor::data_validation_service_server::DataValidationService for DataValidationService {
    async fn validate_data(
        &self,
        request: Request<ValidationRequest>
    ) -> Result<Response<ValidationResponse>, Status> {
        println!("Received validation request from: {:?}", request.remote_addr());
        
        let req = request.into_inner();
        let wavy_id = req.wavy_id.clone();
        
        println!("Processing data for Wavy ID: {}", wavy_id);
        println!("Received {} data items", req.data.len());
        
        // Process each data item
        let mut cleaned_data = Vec::new();
        let mut has_modifications = false;
        
        for item in req.data.iter() {
            println!("Validating {} value: {}", item.data_type, item.value);
            
            // Check for anomalies
            if let Some(anomaly_msg) = self.detect_anomalies(&wavy_id, &item.data_type, item.value) {
                println!("Anomaly detected: {}", anomaly_msg);
            }
            
            // Clean and validate the item
            let cleaned_item = self.validate_data_item(item);
            
            if cleaned_item.was_modified {
                has_modifications = true;
                println!("Modified {} from {} to {} - {}", 
                    cleaned_item.data_type, 
                    cleaned_item.original_value,
                    cleaned_item.cleaned_value,
                    cleaned_item.validation_message
                );
            }
            
            // Update history with the cleaned value
            self.update_history(&wavy_id, &cleaned_item);
            
            cleaned_data.push(cleaned_item);
        }
        
        // Determine overall validation status
        let status = if cleaned_data.is_empty() {
            ValidationStatus::Invalid
        } else if has_modifications {
            ValidationStatus::PartiallyValid
        } else {
            ValidationStatus::Ok
        };
        
        // Create response
        let message = match status {
            ValidationStatus::Ok => "All data valid".to_string(),
            ValidationStatus::PartiallyValid => "Some data items required correction".to_string(),
            ValidationStatus::Invalid => "Invalid data detected".to_string(),
        };
        
        let response = ValidationResponse {
            status: status as i32,
            wavy_id,
            data: cleaned_data,
            message,
        };
        
        println!("Validation complete with status: {:?}", status);
        println!("-------------------------------------");
        Ok(Response::new(response))
    }
}

// Server startup helper
pub async fn run_server(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse()?;
    let validation_service = DataValidationService::default();
    
    println!("Starting validation server on {}", addr);
    
    Server::builder()
        .add_service(oceanmonitor::data_validation_service_server::DataValidationServiceServer::new(validation_service))
        .serve(addr)
        .await?;
    
    Ok(())
}
