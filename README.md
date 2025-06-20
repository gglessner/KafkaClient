# Kafka Client

A Python3 script to connect to Apache Kafka servers using SSL client certificates and list all available topics, brokers, and consumer groups. **NEW**: Now includes real-time message consumption capabilities and comprehensive penetration testing features!

## Author

**Garland Glessner** <gglessner@gmail.com>

## Description

This tool connects to an Apache Kafka server using TLS/SSL with client-side certificate authentication. It provides comprehensive information about the Kafka cluster including:

- Server version and connection information
- All brokers in the cluster
- All topics with partition details
- Consumer groups
- Sample topic configurations
- **NEW**: Detailed cluster information, ACLs, user credentials, and broker configurations (with individual flags)
- **NEW**: Real-time message consumption from topics
- **NEW**: Comprehensive penetration testing and security assessment capabilities

## Features

- SSL/TLS client certificate authentication
- Server verification disabled (for testing environments)
- Command-line argument support for flexible configuration
- Comprehensive cluster information display
- Support for separate client and CA certificates
- **NEW**: Selective server information gathering with individual flags
- **NEW**: Real-time message consumption with JSON parsing
- **NEW**: Configurable consumer options (group ID, offset reset, message limits)
- **NEW**: Consumer group browsing without consuming messages
- **NEW**: Safe consumer group deletion (only if no active consumers)
- **NEW**: Detailed consumer group information and analysis
- **NEW**: Test consumer group creation for development and testing
- **NEW**: Permission testing and privilege escalation detection
- **NEW**: Security configuration auditing
- **NEW**: Sensitive data enumeration and pattern matching
- **NEW**: Message injection testing and payload validation
- **NEW**: Scan all consumer groups and topics for available (unconsumed) messages without consuming them

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

### Basic Usage

Connect using a single PEM file containing both client certificate and private key:

```bash
python3 KafkaClient.py <server:port> --client-cert <path/to/cert.pem>
```

Example:
```bash
python3 KafkaClient.py localhost:9093 --client-cert valid_ee.decrypted.pem
```

### Topic Listing Options

List only topic names (clean output):
```bash
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --list-topics
```

List topics with partition details:
```bash
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --list-topics-partitions
```

List consumer groups:
```bash
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --list-consumer-groups
```

List broker information:
```bash
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --list-brokers
```

### Advanced Usage

Use separate files for client certificate and CA certificate:

```bash
python3 KafkaClient.py <server:port> --client-cert <client.pem> --ca-cert <ca.pem>
```

### Selective Information

Get specific types of server information:

```bash
# Show only cluster information
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --cluster-info

# Show ACLs and user credentials
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --acls --user-credentials

# Show all available information
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --all
```

### **NEW: Message Consumption**

Consume messages from a topic in real-time:

```bash
# Basic consumption (latest messages)
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --subscribe my-topic

# Consume from beginning of topic
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --subscribe my-topic --from-beginning

# Limit number of messages
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --subscribe my-topic --max-messages 10

# Custom consumer group and timeout
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --subscribe my-topic --consumer-group my-group --timeout 2.0
```

### **NEW: Consumer Group Browsing**

Browse messages from an existing consumer group without consuming them:

```bash
# Browse messages from a consumer group (default: 10 messages, 5 second timeout)
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --browse-group my-consumer-group

# Browse with custom limits
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --browse-group my-consumer-group --browse-max-messages 20 --browse-timeout 10.0
```

**Features:**
- **Safe Browsing**: Uses temporary consumer group, doesn't affect original group
- **No Offset Commits**: Reads messages without advancing group offsets
- **Group Information**: Shows group state, members, and protocol
- **Offset Details**: Displays current committed offsets for each partition
- **Message Preview**: Shows message content with JSON parsing
- **Configurable Limits**: Control number of messages and timeout

### **NEW: Consumer Group Management**

Safely delete consumer groups (only if no active consumers):

```bash
# Delete a consumer group safely
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --delete-consumer-group my-consumer-group
```

Get detailed information about a specific consumer group:

```bash
# Describe a consumer group in detail
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --describe-consumer-group my-consumer-group
```

Create a test consumer group for testing purposes:

```bash
# Create a test consumer group (format: group_name:topic_name)
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --create-test-group test-group:test-topic
```

**Safety Features:**
- **Pre-deletion Check**: Verifies group state and member count
- **Active Consumer Detection**: Refuses to delete if consumers are connected
- **Clear Error Messages**: Explains why deletion failed
- **Helpful Suggestions**: Tells users what to do if deletion fails

**Describe Consumer Group Features:**
- **Group Information**: State, protocol type, member count
- **Member Details**: Client ID, host, timeouts, partition assignments
- **Committed Offsets**: Current offset positions for all partitions
- **Group Health Summary**: Status assessment and recommendations

**Example Output for Describe:**
```
============================================================
CONSUMER GROUP DETAILS: my-consumer-group
============================================================

1. Getting consumer group information...
   ✓ Group ID: my-consumer-group
   ✓ State: Stable
   ✓ Protocol Type: consumer
   ✓ Protocol: range
   ✓ Members: 2

2. Member Details:
   Member 1:
     ✓ Member ID: consumer-1-1234567890
     ✓ Client ID: my-consumer-app
     ✓ Client Host: 192.168.1.100
     ✓ Session Timeout: 30000ms
     ✓ Partition Assignments:
       - my-topic[0]: partition 0
       - my-topic[1]: partition 1

3. Committed Offsets:
   ✓ Found 2 partition assignments with offsets:
     - my-topic[0]: offset 12345
     - my-topic[1]: offset 67890

4. Group Summary:
   ✓ Group is healthy and active
   ✓ 2 consumer(s) are processing messages
```

**Example Output for Delete:**
```
============================================================
SAFELY DELETING CONSUMER GROUP: my-consumer-group
============================================================

1. Checking consumer group status...
   ✓ Group State: Stable
   ✓ Members: 0
   ✓ Group is empty - safe to delete

2. Attempting to delete consumer group...
   ✓ SUCCESS: Consumer group 'my-consumer-group' deleted successfully
```

### **NEW: Scan for Available Messages**

Scan all consumer groups and topics to see where messages are available for consumption (without consuming or committing offsets):

```bash
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --scan-available-messages
```

- Lists all consumer groups and topics
- Compares each group's committed offset to the latest offset for every topic/partition
- Reports where messages are available for consumption
- Does not consume or commit any messages

**Sample Output:**
```
==============================
SCANNING FOR AVAILABLE MESSAGES
==============================

Group                          Topic                          Partition  Committed  Latest     Available 
----------------------------------------------------------------------------------------------------
my-group                       my-topic                       0          12345      12350      5         
my-group                       my-topic                       1          67890      67900      10        

✓ Scan complete. 2 topic-partitions have available messages for their groups.
```

**Note:**
- This feature requires a recent version of the `confluent_kafka` Python library. Some older versions may not support the necessary offset query APIs and will display an error or no results.
- If you see errors about `ConsumerGroupTopicPartitions` or argument signatures, upgrade with:
  ```
  pip install -U confluent-kafka
  ```
- This feature works with both Apache Kafka and Confluent Platform brokers.

### **NEW: Penetration Testing & Security Assessment**

#### Complete Security Audit
Run all security tests and assessments:

```bash
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --full-security-audit
```

#### Individual Security Tests

**Permission Testing:**
```bash
# Test topic creation, deletion, partition creation, configuration alteration, and consumer group permissions
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --test-permissions
```

**Security Configuration Audit:**
```bash
# Audit SSL/TLS, authentication, and authorization settings
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --audit-security
```

**Sensitive Data Enumeration:**
```bash
# Scan topics for potentially sensitive data patterns
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --enumerate-sensitive
```

**Message Injection Testing:**
```bash
# Test ability to inject messages into topics
python3 KafkaClient.py <server:port> --client-cert <cert.pem> --test-injection
```

### Command Line Arguments

#### Basic Arguments
- `server`: Kafka server address and port (positional argument)
  - Example: `localhost:9093`
- `--client-cert`: Path to client certificate PEM file (optional)
  - If provided, must contain both certificate and private key
  - If omitted, TLS will be used without client authentication (encryption only)
- `--ca-cert`: Path to CA certificate PEM file (optional)
  - If not provided, uses the client certificate file if available
  - If neither is provided, system CA certificates will be used
- `--no-tls`: Connect without TLS/SSL encryption (plaintext)

#### Topic Listing Arguments
- `--list-topics`: List only topic names (no partition details)
- `--list-topics-partitions`: List topics with partition details (leader, replicas, ISRs)
- `--list-consumer-groups`: List consumer groups
- `--list-brokers`: List broker information

#### Information Flags
- `--cluster-info`: Show cluster information (ID, controller, etc.)
- `--acls`: Show Access Control Lists (ACLs)
- `--detailed-consumer-groups`: Show detailed consumer group information
- `--user-credentials`: Show user SCRAM credentials
- `--broker-configs`: Show broker configurations
- `--topic-offsets`: Show topic offsets and message positions
- `--topic-configs`: Show topic configurations
- `--all`: Show all available information

#### Consumer Arguments
- `--subscribe`: Topic to subscribe to and read messages from
- `--consumer-group`: Consumer group ID (default: kafka-client-consumer)
- `--max-messages`: Maximum number of messages to read (default: unlimited)
- `--from-beginning`: Start reading from the beginning of the topic
- `--timeout`: Consumer poll timeout in seconds (default: 1.0)
- `--browse-group`: Browse messages from an existing consumer group (without consuming)
- `--browse-max-messages`: Maximum messages to browse from group (default: 10)
- `--browse-timeout`: Browse timeout in seconds (default: 5.0)
- `--delete-consumer-group`: Safely delete a consumer group (only if no active consumers)
- `--describe-consumer-group`: Get detailed information about a specific consumer group
- `--create-test-group`: Create a test consumer group for testing purposes (format: group_name:topic_name)

#### **NEW: Penetration Testing Arguments**
- `--test-permissions`: Test various permissions (topic creation, deletion, etc.)
- `--audit-security`: Audit security configurations (SSL/TLS, auth, etc.)
- `--enumerate-sensitive`: Look for potentially sensitive data in topics
- `--test-injection`: Test ability to inject messages into topics
- `--full-security-audit`: Run all security tests and audits

#### **NEW: Scan for Available Messages**
- `--scan-available-messages`: Scan all consumer groups and topics for available (unconsumed) messages without consuming them

### Help

```bash
python3 KafkaClient.py --help
```

## Certificate Formats

### PEM Files

The script expects PEM format certificates. If you have a PFX/PKCS#12 file, convert it using OpenSSL:

```bash
# Extract all certificates from PFX to PEM
openssl pkcs12 -in your_cert.pfx -out extracted_certs.pem -nodes

# Extract only client certificate
openssl pkcs12 -in your_cert.pfx -out client_cert.pem -clcerts -nokeys

# Extract only private key
openssl pkcs12 -in your_cert.pfx -out private_key.pem -nocerts -nodes

# Extract CA certificate
openssl pkcs12 -in your_cert.pfx -out ca_cert.pem -cacerts -nokeys
```

## Output

### Basic Output

The script displays:

1. **Kafka Server Info**: Connection details and broker count
2. **Topics**: All topics (topic names only by default)
3. **Controller ID**: The controller broker ID

**Note**: By default, topics are shown without partition details. Use `--list-topics-partitions` to see detailed partition information including leaders, replicas, and ISRs. Use `--list-consumer-groups` to see consumer groups. Use `--list-brokers` to see broker information.

### Selective Information Output

Additional information based on flags:

1. **Cluster Information**: Cluster ID, controller details, authorized operations
2. **Access Control Lists**: All configured ACLs (if enabled)
3. **Detailed Consumer Groups**: Group states, member counts, protocols
4. **User SCRAM Credentials**: User authentication details (if using SASL/SCRAM)
5. **Broker Configurations**: Key broker settings like listeners, log directories
6. **Topic Offsets**: Current message positions for topics
7. **Topic Configurations**: Topic-specific settings

### Message Subscription Output

When subscribing to a topic, the script displays:

```
Subscribing to topic: my-topic
Press Ctrl+C to stop reading
--------------------------------------------------

Message #1
  Topic: my-topic
  Partition: 0
  Offset: 12345
  Key: user-123
  Value (JSON): {
    "user_id": "user-123",
    "action": "login",
    "timestamp": "2024-01-15T10:30:00Z"
  }
  Timestamp: 1705312200000
------------------------------
```

**Features:**
- **JSON Parsing**: Automatically detects and pretty-prints JSON messages
- **Fallback Handling**: Shows raw text for non-JSON messages
- **Binary Data Support**: Handles binary message content
- **Message Metadata**: Shows topic, partition, offset, key, and timestamp
- **Graceful Shutdown**: Ctrl+C to stop reading

### Consumer Group Browsing Output

When browsing a consumer group, the script displays:

```
============================================================
BROWSING CONSUMER GROUP: my-consumer-group
============================================================

1. Getting group information...
   ✓ Group State: Stable
   ✓ Members: 2
   ✓ Protocol: range
   ⚠ WARNING: Group has active members. Browsing may interfere with consumption.

2. Getting committed offsets...
   ✓ Found 3 partition assignments
   - my-topic[0]: offset 12345
   - my-topic[1]: offset 67890
   - my-topic[2]: offset 11111

3. Creating temporary browser consumer...

4. Browsing messages (max: 10)...
Press Ctrl+C to stop browsing
--------------------------------------------------

Message #1
  Topic: my-topic
  Partition: 0
  Offset: 12345
  Key: user-123
  Value (JSON): {
    "user_id": "user-123",
    "action": "login",
    "timestamp": "2024-01-15T10:30:00Z"
  }
  Timestamp: 1705312200000
------------------------------

5. Browse Summary:
   ✓ Browsed 3 messages from group 'my-consumer-group'
   ✓ No offsets were committed (safe browsing)
   ✓ Original group 'my-consumer-group' was not affected
```

**Features:**
- **Group Analysis**: Shows group state, member count, and protocol
- **Offset Mapping**: Displays current committed offsets for each partition
- **Safe Operation**: Uses temporary consumer group to avoid interference
- **Message Preview**: Shows message content with JSON parsing
- **Browse Summary**: Reports results and confirms no offset commits

### **NEW: Security Assessment Output**

#### Permission Testing Output
```
============================================================
PERMISSION TESTING
============================================================

1. Testing topic creation permission...
   ✓ SUCCESS: Can create topic 'security-test-1705312200'
   ✓ SUCCESS: Can delete topic 'security-test-1705312200'

2. Testing partition creation permission...
   ✓ Created temporary topic 'partition-test-1705312200' with 1 partition
   ✓ SUCCESS: Can create partitions for topic 'partition-test-1705312200' (increased from 1 to 2)
   ✓ Cleaned up temporary topic 'partition-test-1705312200'

3. Testing configuration alteration permission...
   ✓ Created temporary topic 'config-test-1705312200' for config testing
   ✓ SUCCESS: Can alter topic configuration for 'config-test-1705312200'
   ✓ Cleaned up temporary topic 'config-test-1705312200'

4. Testing consumer group permissions...
   ✓ Created temporary topic 'consumer-test-1705312200' for consumer group testing
   ✓ No messages in topic, but consumer group 'test-group-1705312200' was created
   ✓ SUCCESS: Can commit offsets for consumer group 'test-group-1705312200'
   ✓ Closed consumer to leave group 'test-group-1705312200'
   ✓ SUCCESS: Can delete consumer group 'test-group-1705312200'
   ✓ Cleaned up temporary topic 'consumer-test-1705312200'
```

#### Security Configuration Audit Output
```
============================================================
SECURITY CONFIGURATION AUDIT
============================================================

1. Broker Security Configurations:
   Broker 1:
     ✓ SSL/TLS enabled
     ✓ SASL authentication enabled
     ✓ Authorization enabled: kafka.security.authorizer.AclAuthorizer

2. Topic Security Configurations:
   Topic: sensitive-data:
     ✓ Retention configured: 86400000ms
     ✓ Cleanup policy: delete
```

#### Sensitive Data Enumeration Output
```
============================================================
SENSITIVE DATA ENUMERATION
============================================================

Scanning 15 topics for sensitive data patterns...

⚠ POTENTIALLY SENSITIVE TOPICS FOUND:
   - user-passwords (matches pattern: 'password')
   - admin-credentials (matches pattern: 'credential')
   - financial-transactions (matches pattern: 'financial')
```

#### Message Injection Testing Output
```
============================================================
MESSAGE INJECTION TESTING
============================================================

Testing message injection into topic: test-topic
   ✓ Message delivered to test-topic [0] at offset 12345
   ✓ SUCCESS: Can inject messages into topic 'test-topic'
```

## Security Notes

- Server verification is disabled by default (`ssl.endpoint.identification.algorithm: none`)
- This is suitable for testing environments but should be enabled for production
- The script uses client certificate authentication for secure connections
- Individual information flags may expose sensitive configuration information
- **Consumer groups**: Uses a default group ID that can be customized
- **Security testing**: May create temporary test topics and send test messages
- **Permission testing**: Automatically cleans up test resources when possible

## Troubleshooting

### Connection Issues

1. **"SSL handshake failed"**: Server may not be configured for SSL
2. **"Connect failed"**: Check server address, port, and firewall settings
3. **"No such file"**: Verify certificate file paths are correct

### Certificate Issues

1. **"BIO routines::no such file"**: Certificate file not found
2. **"x509 certificate routines"**: Invalid certificate format
3. **"SSL routines"**: Certificate/private key mismatch

### Information Issues

1. **"Could not fetch ACLs"**: ACLs may not be enabled on the server
2. **"Could not fetch user credentials"**: SCRAM authentication may not be configured
3. **"Could not fetch broker configurations"**: Insufficient permissions or API not supported

### Consumer Issues

1. **"Topic not found"**: Verify the topic name exists
2. **"No messages received"**: Check if messages are being produced to the topic
3. **"Consumer group errors"**: May indicate permission issues or group conflicts
4. **"JSON decode errors"**: Normal for non-JSON messages, will show as raw text

### **NEW: Security Testing Issues**

1. **"Permission denied"**: Expected for properly secured environments
2. **"Topic creation failed"**: May indicate proper access controls
3. **"Configuration alteration failed"**: May indicate proper security restrictions
4. **"Message injection failed"**: May indicate proper write permissions

## Examples

### Server Information Examples

```bash
# Get basic cluster info
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --cluster-info

# Check ACLs and user access
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --acls --user-credentials

# Monitor topic offsets
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --topic-offsets
```

### Message Consumption Examples

```bash
# Subscribe to a topic for new messages
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --subscribe events

# Read all historical messages from a topic
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --subscribe events --from-beginning

# Get last 5 messages from a topic
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --subscribe events --max-messages 5

# Use custom consumer group for testing
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --subscribe events --consumer-group test-group
```

### **NEW: Consumer Group Management Examples**

```bash
# Safely delete an empty consumer group
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --delete-consumer-group old-consumer-group

# Get detailed information about a consumer group
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --describe-consumer-group my-consumer-group

# Create a test consumer group for testing
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --create-test-group test-group:test-topic

# Browse messages from a consumer group without consuming
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --browse-group my-consumer-group --browse-max-messages 20
```

### **NEW: Penetration Testing Examples**

```bash
# Run complete security assessment
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --full-security-audit

# Test specific permissions
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --test-permissions

# Audit security configurations
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --audit-security

# Look for sensitive data patterns
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --enumerate-sensitive

# Test message injection capabilities
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --test-injection
```

### Scan for Available Messages Example
```bash
python3 KafkaClient.py localhost:9093 --client-cert cert.pem --scan-available-messages
```

## Penetration Testing Use Cases

### Security Assessment
- **Privilege Escalation Detection**: Identify excessive permissions
- **Access Control Validation**: Verify proper authorization implementation
- **Configuration Weaknesses**: Find security misconfigurations
- **Data Exposure Assessment**: Locate potentially sensitive topics

### Compliance Testing
- **Security Standards**: Assess against security frameworks
- **Audit Requirements**: Generate security assessment reports
- **Risk Assessment**: Identify security vulnerabilities
- **Remediation Planning**: Prioritize security fixes

### Red Team Operations
- **Initial Reconnaissance**: Understand Kafka infrastructure
- **Permission Mapping**: Map available capabilities
- **Data Discovery**: Find sensitive information
- **Persistence Testing**: Validate security controls

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please contact:
- **Author**: Garland Glessner <gglessner@gmail.com>
- **GitHub Issues**: [Create an issue](https://github.com/yourusername/KafkaClient/issues) 

## TLS/SSL Usage

You can connect to Kafka using TLS/SSL with or without a client certificate:

- **With client certificate (mutual TLS):**
  - Use `--client-cert <path/to/cert.pem>` to provide your client certificate (must contain both certificate and private key).
  - Optionally, use `--ca-cert <path/to/ca.pem>` to specify a custom CA certificate for server verification.
- **Without client certificate (encryption only):**
  - Omit `--client-cert`. The connection will be encrypted, but the client will not authenticate with a certificate. The broker must be configured with `ssl.client.auth=none`.
  - You may still use `--ca-cert` to specify a custom CA for server verification.

**Example: TLS with client certificate**
```bash
python3 KafkaClient.py <server:port> --client-cert <path/to/cert.pem>
```

**Example: TLS without client certificate**
```bash
python3 KafkaClient.py <server:port>
```

**Example: TLS with custom CA only**
```bash
python3 KafkaClient.py <server:port> --ca-cert <path/to/ca.pem>
``` 