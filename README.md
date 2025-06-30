# Kafka Client

A comprehensive Python3 tool for Apache Kafka administration, monitoring, and security assessment. Connect to Kafka clusters using SSL/TLS certificates and perform a wide range of operations including topic management, message production/consumption, consumer group administration, and security auditing.

## Author

**Garland Glessner** <gglessner@gmail.com>

## Overview

This tool provides a complete interface for Apache Kafka cluster management and security assessment. It supports both plaintext and SSL/TLS connections with client certificate authentication, making it suitable for both development and production environments.

## Key Features

### Connection & Security
- SSL/TLS client certificate authentication
- Support for separate client and CA certificates
- Broker-sticking configuration for targeted testing
- Server verification options for testing environments

### Topic Management
- Create, delete, and list topics
- Add partitions to existing topics
- Alter topic configurations
- Delete records from specific partitions
- Elect leaders for partitions
- View topic offsets and configurations

### Message Operations
- Produce messages with keys and values
- JSON message support
- Batch message production from files
- Real-time message consumption
- Wildcard topic subscription
- Batch message consumption with configurable limits

### Consumer Group Management
- List and describe consumer groups
- Create and delete consumer groups
- Browse messages without consuming them
- Alter consumer group offsets
- Monitor consumer lag
- Test consumer group creation

### Consumer Control
- Seek to specific offsets or timestamps
- Pause and resume partitions
- Get watermark offsets
- Configure isolation levels
- Set custom timeouts and message limits

### Cluster & Broker Management
- View cluster information and metadata
- List all brokers in the cluster
- Display broker configurations
- Monitor broker health status

### Security & Access Control
- View and manage Access Control Lists (ACLs)
- Manage user SCRAM credentials
- Test permissions and access rights
- Comprehensive security auditing
- Sensitive data enumeration
- Message injection testing

### Security Assessment
- CVE-based vulnerability scanning
- Deserialization vulnerability checks
- SASL authentication bypass detection
- Metadata disclosure testing
- Log4j vulnerability assessment
- Path traversal vulnerability checks
- Kafka Connect security analysis
- Denial of service vulnerability detection

### Monitoring & Metrics
- Consumer metrics display
- Producer metrics monitoring
- Comprehensive cluster monitoring

### Transaction Management
- Begin, commit, and abort transactions
- Transaction state management

## Requirements

- Python 3.6+
- `confluent-kafka` library
- OpenSSL (for certificate conversion if needed)

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd KafkaClient
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Argument Organization

All command line arguments are organized by logical function with consistent prefixes for improved usability and discoverability.

### Argument Groups Overview

| Group                | Prefix              | Example Argument                |
|----------------------|--------------------|---------------------------------|
| Connection/Security  | --connection-*     | --connection-tls                |
| Topic Management     | --topic-*          | --topic-list, --topic-create    |
| Message Production   | --produce-*        | --produce-message               |
| Message Consumption  | --consume-*        | --consume-subscribe             |
| Consumer Groups      | --group-*          | --group-list, --group-delete    |
| Consumer Control     | --consumer-*       | --consumer-seek-offset          |
| Cluster/Broker Mgmt  | --cluster-*        | --cluster-info                  |
| Security/ACL         | --security-*       | --security-acl-list             |
| Monitoring/Metrics   | --monitor-*        | --monitor-consumer-metrics      |
| Transactions         | --transaction-*    | --transaction-begin             |

### Basic Usage

Connect using PLAINTEXT (no TLS, default):

```bash
python3 KafkaClient.py <server:port>
```

Connect using SSL/TLS:

```bash
python3 KafkaClient.py <server:port> --connection-tls --connection-client-cert <path/to/cert.pem>
```

Example:
```bash
python3 KafkaClient.py localhost:9093 --connection-tls --connection-client-cert valid_ee.decrypted.pem
```

### Topic Management

```bash
# List all topics
python3 KafkaClient.py <server:port> --topic-list

# Create a topic
python3 KafkaClient.py <server:port> --topic-create mytopic:3:1

# Delete a topic
python3 KafkaClient.py <server:port> --topic-delete mytopic

# Add partitions to a topic
python3 KafkaClient.py <server:port> --topic-add-partitions mytopic:5

# Alter topic configuration
python3 KafkaClient.py <server:port> --topic-alter-config mytopic:retention.ms=86400000
```

### Message Production

```bash
# Produce a simple message
python3 KafkaClient.py <server:port> --produce-message mytopic --produce-value "hello world"

# Produce a message with key
python3 KafkaClient.py <server:port> --produce-message mytopic --produce-key "key1" --produce-value "value1"

# Produce JSON message
python3 KafkaClient.py <server:port> --produce-message mytopic --produce-json-value '{"user": "john", "action": "login"}'

# Produce from file
python3 KafkaClient.py <server:port> --produce-from-file mytopic:/path/to/message.txt
```

### Message Consumption

```bash
# Consume latest messages
python3 KafkaClient.py <server:port> --consume-subscribe mytopic

# Consume from beginning of topic
python3 KafkaClient.py <server:port> --consume-subscribe mytopic --consumer-from-beginning

# Limit number of messages
python3 KafkaClient.py <server:port> --consume-subscribe mytopic --consumer-max-messages 10

# Custom consumer group and timeout
python3 KafkaClient.py <server:port> --consume-subscribe mytopic --consumer-group-id my-group --consumer-timeout 2.0

# Subscribe to topics with wildcard
python3 KafkaClient.py <server:port> --topic-subscribe-wildcard test-
```

### Consumer Group Management

```bash
# List consumer groups
python3 KafkaClient.py <server:port> --group-list

# Get detailed group information
python3 KafkaClient.py <server:port> --group-list-detailed

# Describe specific group
python3 KafkaClient.py <server:port> --group-describe my-consumer-group

# Browse messages from group
python3 KafkaClient.py <server:port> --group-browse my-consumer-group

# Delete consumer group
python3 KafkaClient.py <server:port> --group-delete my-consumer-group

# Show consumer lag
python3 KafkaClient.py <server:port> --group-lag my-consumer-group
```

### Cluster Information

```bash
# Show cluster information
python3 KafkaClient.py <server:port> --cluster-info

# List all brokers
python3 KafkaClient.py <server:port> --cluster-list-brokers

# Show broker configurations
python3 KafkaClient.py <server:port> --cluster-broker-configs

# Check broker health
python3 KafkaClient.py <server:port> --cluster-broker-health
```

### Security & Access Control

```bash
# List ACLs
python3 KafkaClient.py <server:port> --security-acl-list

# Create ACL
python3 KafkaClient.py <server:port> --security-acl-create topic:mytopic:User:alice:read:allow

# Delete ACL
python3 KafkaClient.py <server:port> --security-acl-delete topic:mytopic:User:alice:read:allow

# Show user credentials
python3 KafkaClient.py <server:port> --security-user-credentials

# Alter user credentials
python3 KafkaClient.py <server:port> --security-user-credentials-alter alice:newpassword:SCRAM-SHA-256

# Test permissions
python3 KafkaClient.py <server:port> --security-test-permissions
```

### Security Assessment

```bash
# Run comprehensive security audit
python3 KafkaClient.py <server:port> --security-full-audit

# Check for specific vulnerabilities
python3 KafkaClient.py <server:port> --security-cve-deserialization
python3 KafkaClient.py <server:port> --security-cve-log4j
python3 KafkaClient.py <server:port> --security-cve-sasl-bypass

# Run all CVE checks
python3 KafkaClient.py <server:port> --security-cve-comprehensive
```

### Monitoring & Metrics

```bash
# Show consumer metrics
python3 KafkaClient.py <server:port> --monitor-consumer-metrics

# Show producer metrics
python3 KafkaClient.py <server:port> --monitor-producer-metrics

# Show all information
python3 KafkaClient.py <server:port> --monitor-all
```

### Advanced Configuration

Use separate files for client certificate and CA certificate:

```bash
python3 KafkaClient.py <server:port> --connection-client-cert <client.pem> --connection-ca-cert <ca.pem>
```

Stick to initial broker (disable broker discovery):

```bash
python3 KafkaClient.py <server:port> --connection-stick-to-broker
```

## Security Features

### CVE-Based Vulnerability Scanning

The tool includes comprehensive vulnerability scanning based on known Kafka CVEs:

- **CVE-2023-46663**: RCE via deserialization
- **CVE-2023-46662**: SASL authentication bypass
- **CVE-2023-46661**: Information disclosure in consumer group metadata
- **CVE-2021-44228**: Log4Shell vulnerability
- **CVE-2021-45046**: Log4j vulnerability
- **CVE-2022-23305**: Path traversal vulnerability
- **CVE-2024-3498**: Kafka Connect deserialization vulnerability
- **CVE-2023-46660**: Denial of service via crafted requests

### Security Assessment Capabilities

- **Permission Testing**: Test various permissions and access rights
- **Configuration Auditing**: Audit security configurations
- **Sensitive Data Enumeration**: Scan for sensitive data patterns
- **Message Injection Testing**: Test message injection capabilities
- **Comprehensive Auditing**: Run all security tests in sequence

## Error Handling

The tool includes comprehensive error handling for:

- Connection failures
- Authentication errors
- Permission denied scenarios
- Invalid topic configurations
- Consumer group conflicts
- Security policy violations

## Examples

### Basic Cluster Information

```bash
python3 KafkaClient.py localhost:9092 --topic-list --group-list --cluster-info
```

### Security Assessment

```bash
python3 KafkaClient.py localhost:9092 --connection-tls --connection-client-cert cert.pem --security-cve-comprehensive
```

### Message Production and Consumption

```bash
# Produce a message
python3 KafkaClient.py localhost:9092 --produce-message test-topic --produce-value "Hello Kafka!"

# Consume messages
python3 KafkaClient.py localhost:9092 --consume-subscribe test-topic --consumer-from-beginning --consumer-max-messages 5
```

### Consumer Group Management

```bash
# Create a test consumer group
python3 KafkaClient.py localhost:9092 --group-create-test my-test-group:test-topic

# Browse messages without consuming
python3 KafkaClient.py localhost:9092 --group-browse my-test-group --group-browse-max-messages 10
```

## Troubleshooting

### Common Issues

1. **Connection Refused**: Verify the Kafka server is running and accessible
2. **Authentication Failed**: Check certificate paths and permissions
3. **Permission Denied**: Verify ACLs and user credentials
4. **Topic Not Found**: Ensure the topic exists or create it first

### Debug Information

The tool provides detailed output including:
- Connection status and configuration
- Broker information
- Topic details and configurations
- Consumer group states
- Security assessment results

## License

This project is licensed under the terms specified in the LICENSE file.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests. 