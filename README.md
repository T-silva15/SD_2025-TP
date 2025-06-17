# 🌊 OceanMonitor - Distributed Data Monitoring System

[![C#](https://img.shields.io/badge/C%23-9.0-blue.svg)](https://docs.microsoft.com/en-us/dotnet/csharp/)
[![.NET](https://img.shields.io/badge/.NET-8.0-purple.svg)](https://dotnet.microsoft.com/)
[![Rust](https://img.shields.io/badge/Rust-1.70-orange.svg)](https://www.rust-lang.org/)
[![gRPC](https://img.shields.io/badge/gRPC-Protocol%20Buffers-orange.svg)](https://grpc.io/)
[![ZeroMQ](https://img.shields.io/badge/ZeroMQ-4.3.4-green.svg)](https://zeromq.org/)
[![Python](https://img.shields.io/badge/Python-3.11-yellow.svg)](https://www.python.org/)
[![SQL Server](https://img.shields.io/badge/SQL%20Server-2022-red.svg)](https://www.microsoft.com/sql-server)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Academic Project** | Distributed Systems | **Final Grade: TBD** 🏆

A scalable distributed system for collecting, aggregating, and analyzing ocean sensor data across multiple tiers using socket communication, ZeroMQ messaging, Rust microservices, and gRPC for advanced data processing.

## 📋 Table of Contents

- [🎯 Overview](#-overview)
- [🏗️ System Architecture](#️-system-architecture)
- [✨ Features](#-features)
- [📂 Project Structure](#-project-structure)
- [🚀 Getting Started](#-getting-started)
- [💻 Usage](#-usage)
- [🛠️ Technologies](#️-technologies)
- [📊 Implementation Phases](#-implementation-phases)
- [🔮 Future Enhancements](#-future-enhancements)
- [📄 License](#-license)

## 🎯 Overview

OceanMonitor is a comprehensive distributed system designed for monitoring and analyzing ocean environmental data. The system follows a three-tier architecture with edge sensors (Wavy), aggregation nodes (Aggregator), and a central server (Server) that provides storage, analysis, and visualization of the collected data.

### Key Highlights
- **Three-Tier Architecture**: Scalable design with clear separation of responsibilities
- **Custom Communication Protocol**: Reliable socket-based data exchange with acknowledgment
- **Multiple Messaging Paradigms**: Socket-based messaging (Phase 1) and ZeroMQ messaging (Phase 2)
- **Cross-Language Integration**: C# backend with Rust and Python microservices
- **Reliability Mechanisms**: Data persistence, retry logic, and failure recovery
- **Advanced Analytics**: Real-time statistical processing via multiple RPC mechanisms

## 🏗️ System Architecture

### Three-Tier Architecture
```
Phase 1:
┌───────────────┐      ┌─────────────────┐      ┌───────────────┐
│  Wavy Clients │ ───→ │ Aggregator Nodes│ ───→ │ Central Server│
│    (Edge)     │      │  (Middle Tier)  │      │  (Backend +   │
└───────────────┘      └─────────────────┘      │ Data Storage) │
                                                └───────────────┘
                     
Phase 2:
┌───────────────┐             ┌──────────────────┐        ┌──────────────────────┐
│  Wavy Clients │=============│ Aggregator Nodes │ ←────→ │ Rust Data Validation │
│    (Edge)     │    ZeroMQ   │  (Middle Tier)   │ (gRPC) │       Service        │
└───────────────┘  (Pub/Sub)  └────────┬─────────┘        └──────────────────────┘
                                       │
                                       ▼
                              ┌─────────────────┐        ┌──────────────────┐
                              │ Central Server  │ ←────→ │ Python Analytics │
                              │    (Backend)    │ (gRPC) │      Service     │
                              └─────────────────┘        └──────────────────┘
```

1. **Edge Layer (Wavy)**:
   - Simulates ocean environmental sensors
   - Collects temperature, wind speed, frequency, and decibel data
   - In Phase 1: Operates in HTTP-like or continuous connection modes
   - In Phase 2: Implements ZeroMQ publisher for efficient data distribution with a publish-subscribe network
   - Supports both JSON and XML data serialization in Phase 2

2. **Aggregation Layer (Aggregator)**:
   - Receives data from multiple Wavy sensors
   - In Phase 1: Direct socket communication with Wavy clients
   - In Phase 2: Acts as ZeroMQ subscriber to Wavy publishers
   - Integrates with Rust-based validation service via gRPC
   - Validates and cleans data through the Rust service before forwarding
   - Groups data by type and buffers for periodic transmission
   - Manages client connections and handles failure recovery

3. **Backend Layer (Server)**:
   - Stores all received data in SQL Server database
   - Processes incoming data from multiple Aggregator nodes
   - Communicates with Python gRPC service for advanced analytics
   - Handles statistical calculations and anomaly detection
   - Provides data visualization and system monitoring capabilities

## ✨ Features

### 🌐 Network Communication
- **Phase 1 - Custom Socket Protocol**: 
  - Reliable binary protocol with numeric message codes
  - Direct socket connections between all components
  - Acknowledgment system with guaranteed delivery
  
- **Phase 2 - ZeroMQ for Wavy-Aggregator**:
  - Publish-Subscribe pattern between Wavy (publisher) and Aggregator (subscriber)
  - High-performance asynchronous messaging
  - Separate TCP channels for data publication and subscription management
  - Multiple serialization options (JSON/XML)
  
- **Phase 2 - gRPC for Service Integration**:
  - Rust data cleaning/validation service integrated with Aggregator
  - Python analytics service integrated with Server
  - Protocol Buffers for efficient data serialization
  
- **Connection Management**:
  - Session tracking for clients and aggregators
  - Automatic retry with exponential backoff
  - Support for different connection modes in Phase 1:
    - HTTP-like mode (individual connections for unstable networks)
    - Persistent connection mode (for stable networks)

### 📊 Data Management
- **Real-time Data Collection**: Continuous sensor data ingestion
- **Data Aggregation**: Efficient grouping by type and sampling period
- **Persistent Storage**: SQL Server database integration
- **Data Integrity**: Transaction support and consistency checks
- **Historical Data Access**: Query capabilities for collected data

### 🧮 Advanced Analytics
- **Multi-Language Processing Ecosystem**: 
  - C# backend coordination
  - Rust high-performance data validation and transformation
  - Python scientific computing and analytics
- **Multiple RPC Systems**:
  - ZeroMQ for Rust service communication
  - gRPC for Python analytics services
- **Statistical Analysis**:
  - Moving averages calculation
  - Variance and standard deviation computation
  - Trend coefficient calculations
  - Linear regression for time series data
- **Anomaly Detection**:
  - Z-score based outlier identification
  - Anomaly scoring and classification
  - Time-window comparison for contextual anomalies
- **Data Cleaning**:
  - Automatic correction of out-of-range values
  - Classification into valid, partially valid, and invalid data
  - Modification tracking and logging
- **Real-time Calculations**: On-the-fly data processing

### 🛡️ System Reliability
- **Fault Tolerance**: Recovery from node and network failures
- **Data Persistence**: Local caching to prevent data loss
- **Connection Management**: Automatic reconnection on failure
- **Load Distribution**: Support for multiple aggregator nodes
- **Process Monitoring**: Health checks and system status reporting

## 🌐 Communication Protocols

### Phase 1: Custom Binary Protocol

The system uses a custom protocol with numeric message codes to identify message types:

- **101-102**: Connection (Wavy-Aggregator, Aggregator-Server)
- **200-202**: Sensor data (Wavy to Aggregator)
- **301-302**: Aggregated data (Aggregator to Server)
- **401-402**: Acknowledgments
- **501-502**: Disconnection

**Message Format**: 4-byte code (int32) + JSON payload

**Connection Modes**:
- **Independent Requests Mode (HTTP-like)**: Connection per transmission (ideal for unstable networks)
- **Continuous Connection Mode**: Persistent connection (efficient for stable networks)
- The Aggregator supports both modes simultaneously, allowing hybrid architectures

### Phase 2: ZeroMQ Pub-Sub & RPC Integration

**Wavy-Aggregator Communication (ZeroMQ)**:
- **Publisher-Subscriber Pattern**: 
  - Wavy acts as publisher using NetMQ PublisherSocket over TCP
  - Aggregator acts as subscriber to receive sensor data
- **Subscription Management**: Separate TCP channel using NetMQ ResponseSocket
- **Data Format Options**: Configurable between JSON and XML serialization
- **Built-in Reliability**: ZeroMQ's automatic retry mechanisms

**Aggregator-Rust Service Integration (gRPC)**:
- Aggregator sends data to Rust validation service for cleaning and validation
- Synchronous gRPC calls for data validation
- Returns cleaned data back to Aggregator before forwarding to Server

**Aggregator-Server Communication**:
- Maintains Phase 1 custom protocol for backward compatibility
- Enhanced data aggregation before transmission
- Sends only validated/cleaned data from Rust service

**Server-Python Integration (gRPC)**:
- Server sends data to Python analytics service
- Python service performs statistical analysis and returns results
- Moving average calculations, variance analysis, trend detection

## 📂 Project Structure

```
SD_2025-TP/
├── TP1/                        # Phase 1: Basic Distributed System
│   ├── Server/                # Central server component
│   │   ├── Program.cs        # Server implementation with SQL storage
│   │   └── Server.csproj     # Project configuration
│   ├── Aggregator/           # Middle-tier aggregation component
│   │   ├── Program.cs        # Aggregator implementation
│   │   └── Aggregator.csproj # Project configuration
│   ├── Wavy/                 # Edge sensor simulator component
│   │   ├── Program.cs        # Wavy client implementation
│   │   └── Wavy.csproj       # Project configuration
│   └── TP1.sln               # Solution file for Phase 1
│
└── TP2/                        # Phase 2: Enhanced with ZeroMQ and gRPC
    ├── Server/                # Enhanced server with multiple integrations
    │   ├── Program.cs        # Server implementation with analytics
    │   ├── OceanMonitorGrpcClient.cs # gRPC client wrapper
    │   ├── ocean_monitor.proto # Protocol Buffer definition
    │   ├── grpc_calc_server.py # Python analytics microservice
    │   └── Server.csproj     # Project configuration with dependencies
    ├── Aggregator/           # Enhanced aggregator with Rust and ZeroMQ
    │   ├── Program.cs        # Aggregator core implementation
    │   ├── Aggregator.csproj # Project configuration
    │   ├── src/              # Rust service integrated with aggregator
    │   │   ├── main.rs       # Entry point for Rust services
    │   │   └── lib.rs        # Data validation and processing service
    │   └── build.rs          # Rust build configuration
    ├── Wavy/                 # Enhanced edge sensor component
    │   ├── Program.cs        # Wavy client implementation with ZeroMQ
    │   └── Wavy.csproj       # Project configuration
    ├── Test/                 # Test components
    └── TP2.sln               # Solution file for Phase 2
```

## 🚀 Getting Started

### Prerequisites
- [.NET 8.0 SDK](https://dotnet.microsoft.com/download/dotnet/8.0) or later
- [SQL Server](https://www.microsoft.com/en-us/sql-server/sql-server-downloads) or [SQL Server Express LocalDB](https://docs.microsoft.com/en-us/sql/database-engine/configure-windows/sql-server-express-localdb)
- [Python 3.8+](https://www.python.org/downloads/) (for Phase 2 gRPC analytics)
- [Rust](https://www.rust-lang.org/tools/install) and Cargo (for Phase 2 Rust services)
- [ZeroMQ Library](https://zeromq.org/download/) (for Phase 2 messaging)
- [Visual Studio 2022](https://visualstudio.microsoft.com/) or [VS Code](https://code.visualstudio.com/)

### Database Setup (Phase 1 & 2)

The system requires a SQL Server database named "OceanMonitor" with the following schema:

1. **Database Schema Overview**

   - **Aggregators Table**: Tracks all aggregator nodes in the system
     - Stores AggregatorId, connection status, and last contact timestamp
   
   - **Wavys Table**: Registers all Wavy sensor devices
     - Stores WavyId, connection status, and last contact timestamp
   
   - **WavyAggregatorMapping Table**: Maps the relationship between sensors and aggregators
     - Establishes which Wavy is connected to which Aggregator
     - Records when the mapping was established
   
   - **Measurements Table**: Stores all sensor readings
     - Records WavyId, AggregatorId, data type, value, and timestamp
     - Includes validation flags to track data quality
     - Supports filtering by device ID and time ranges
   
   - **CalculatedMetrics Table**: Contains pre-calculated analytics
     - Stores derived metrics from the Python analytics service
     - Includes metric type, data type, value, time range, and calculation timestamp

2. **Setup Instructions**

   - Create a SQL Server database named "OceanMonitor"
   - Execute the database creation script located in `Scripts/CreateDatabase.sql`
   - Update the connection string in Server/Program.cs if needed:
     ```csharp
     static string connectionString = "Server=(localdb)\\mssqllocaldb;Database=OceanMonitor;Trusted_Connection=True;TrustServerCertificate=True;";
     ```

### Phase 2 Dependencies

1. **Install required Python packages**:
   ```bash
   cd TP2/Server
   pip install -r requirements.txt
   ```

2. **Build Rust services**:
   ```bash
   cd TP2/Aggregator
   cargo build --release
   ```

3. **Install ZeroMQ libraries** (platform-specific):
   
   For Ubuntu/Debian:
   ```bash
   sudo apt install libzmq3-dev
   ```
   
   For Windows (using vcpkg):
   ```bash
   vcpkg install zeromq
   ```

   For macOS:
   ```bash
   brew install zeromq
   ```

4. **Install additional Rust dependencies** (if not handled by Cargo):
   ```bash
   cargo install tonic-build
   cargo install protobuf
   ```

## 💻 Usage

### Phase 1 - Basic Distributed System

1. **Start the Server**:
   ```bash
   cd TP1/Server
   dotnet run
   ```

2. **Start one or more Aggregators**:
   ```bash
   cd TP1/Aggregator
   dotnet run
   ```

3. **Start multiple Wavy clients**:
   ```bash
   cd TP1/Wavy
   dotnet run
   ```

### Phase 2 - Enhanced with ZeroMQ, Rust, and gRPC Analytics

1. **Start the Server** (automatically launches Python gRPC service):
   ```bash
   cd TP2/Server
   dotnet run
   ```

2. **Start Aggregators** (enhanced version with Rust validation service):
   ```bash
   cd TP2/Aggregator
   dotnet run
   ```

3. **Start multiple Wavy clients with ZeroMQ publishing**:
   ```bash
   cd TP2/Wavy
   # Start with JSON format
   dotnet run -- --format json
   
   # Or with XML format
   dotnet run -- --format xml
   ```

4. **Available Interactive Commands**:

   In all components:
   ```
   status - Show connection status
   stats - Display data statistics
   clear - Clear the console
   config - Save current configuration
   exit - Exit application
   ```
   
   Wavy-specific commands:
   ```
   frequency <hz> - Set data generation frequency
   toggle - Toggle continuous/request mode
   ```

### Configuration Options:

Wavy client accepts command-line parameters and configuration files:
```bash
dotnet run -- --aggregatorIp 192.168.1.100 --mode continuous --verbose
```

## 🛠️ Technologies

### Core Technologies
- **C# 9.0** - Primary programming language for distributed components
- **.NET 8.0** - Modern cross-platform development framework
- **Rust 1.70+** - High-performance systems programming language (Phase 2)
- **ZeroMQ 4.3.4** - Advanced, high-performance message queue library (Phase 2)
- **Socket API** - Low-level network communication (Phase 1)
- **SQL Server** - Relational database for data persistence
- **Protocol Buffers** - Efficient binary serialization format (Phase 2)
- **gRPC** - High-performance RPC framework (Phase 2)
- **Python NumPy** - Scientific computing for analytics (Phase 2)

### Development Tools
- **Visual Studio 2022** - Primary IDE
- **Git** - Version control system
- **SQL Server Management Studio** - Database management

### Libraries & Frameworks
- **System.Net.Sockets** - .NET socket programming
- **System.Text.Json** - JSON serialization/deserialization
- **Microsoft.Data.SqlClient** - SQL Server data access
- **Grpc.Net.Client** - gRPC client for .NET (Phase 2)
- **Google.Protobuf** - Protocol Buffers support (Phase 2)
- **NumPy** - Scientific computing library for Python (Phase 2)
- **ZeroMQ (NetMQ)** - High-performance messaging library for .NET (Phase 2)
- **Tonic** - Rust gRPC framework (Phase 2)
- **Tokio** - Asynchronous runtime for Rust (Phase 2)
- **zmq-rs** - Rust bindings for ZeroMQ (Phase 2)

## 📊 Implementation Phases

### Phase 1: Basic Distributed System
The first phase implements the core architecture with:
- **Socket Communication**: Custom binary protocol for data exchange
- **Three-Tier Architecture**: Wavy, Aggregator, and Server components
- **Basic Reliability**: Acknowledgments, retries, and persistence
- **SQL Server Integration**: Data storage in relational database
- **Monitoring Features**: Connection tracking and data visualization

### Phase 2: Enhanced with Rust, ZeroMQ, and gRPC Analytics
The second phase builds upon Phase 1 by adding:
- **Multi-Language Architecture**: Integration of C#, Rust, and Python services
- **ZeroMQ Messaging**: High-performance asynchronous messaging between components
- **Rust-Powered Data Processing**: High-performance data validation and transformation
- **gRPC Analytics**: Python-based statistical service using Protocol Buffers
- **Advanced Analytics**: Statistical calculations and anomaly detection with z-score analysis
- **Enhanced Visualization**: Improved data presentation with Spectre.Console
- **Performance Optimizations**: Asynchronous operations and parallel processing

## 🔮 Future Enhancements

- [ ] **Web Dashboard**: Browser-based visualization of ocean data
- [ ] **Machine Learning Integration**: Predictive analytics for ocean trends
- [ ] **Containerization**: Docker deployment for all components
- [ ] **Cloud Deployment**: Azure/AWS integration for scalability
- [ ] **Kafka Integration**: Message broker for improved scalability
- [ ] **Authentication & Authorization**: Security layer for system access
- [ ] **Geographic Distribution**: Support for geographically distributed sensors
- [ ] **Mobile Client**: Monitoring application for iOS/Android
- [ ] **Real-time Alerting**: Notification system for anomalous conditions
- [ ] **Horizontal Scaling**: Load balancing for aggregator tier

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**OceanMonitor** - Distributed systems for scalable ocean data collection and analysis

Made with ❤️ using .NET, gRPC, Rust and Python

</div>
