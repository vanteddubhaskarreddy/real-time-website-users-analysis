# Real-Time Website Users Analysis

A sophisticated real-time analytics platform that processes website events, enriches them with geographical data, and visualizes user engagement patterns.

## Project Overview

This project implements a complete real-time event processing pipeline that ingests website user events and transforms them into actionable insights through geographical enrichment and advanced analytics.

### Key Features

- **Real-Time Event Processing**: Ingests 2,400+ website events/hour from Confluent Kafka to Apache Iceberg using PySpark on Databricks
- **Fault-Tolerant Architecture**: Implements offset-limiting (10,000 records/batch) to ensure reliable data processing
- **Medallion Architecture**:
  - **Bronze Layer**: Binary-decoded raw event data
  - **Silver Layer**: Schema-validated tables with nested structure normalization
  - **Gold Layer**: Enriched tables with geographical data ready for analysis
- **Advanced Streaming Analytics**:
  - Tumbling windows (1-minute with 30-second watermarks)
  - Session windows (5-minute inactivity threshold)
  - 99% data completeness for accurate analytics
- **IP-Based Location Enrichment**: Custom caching mechanism for ipinfo.io API calls, reducing latency by 6 seconds during peak periods
- **Interactive Visualization**: Databricks dashboard showing regional engagement patterns and infrastructure resource utilization

## Architecture

The system follows a medallion architecture pattern with progressive data transformations:

1. **Data Ingestion**: Raw events from website user interactions are captured and sent to Confluent Kafka
2. **Bronze Layer**: Events are consumed by PySpark streaming jobs and stored in binary-decoded format in Apache Iceberg tables
3. **Silver Layer**: Data is validated, normalized, and structured for further processing
4. **Gold Layer**: Data is enriched with geographical information and analytics-ready metrics
5. **Analytics & Visualization**: Real-time dashboards visualize user engagement patterns and system performance metrics

## Implementation Details

- **Technology Stack**: Confluent Kafka, Apache Spark, PySpark, Databricks, Apache Iceberg
- **Deployment**: Hosted on cloud infrastructure
- **Data Processing**: Streaming analytics with windowed aggregations
- **Enrichment**: IP-based geolocation with custom caching for optimized performance

## Dashboard

For dashboard metrics example, please refer to the [project documentation](./databricks/website-traffic-dashboard.pdf).



