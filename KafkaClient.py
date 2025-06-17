#!/usr/bin/env python3
"""
Kafka Client - Connect to Apache Kafka using SSL client certificates

Author: Garland Glessner <gglessner@gmail.com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

import argparse
import json
import signal
import sys
import time
from confluent_kafka.admin import AdminClient, ConfigResource
from confluent_kafka import Consumer, KafkaError, Producer

def signal_handler(sig, frame):
    print('\nShutting down gracefully...')
    sys.exit(0)

def consume_messages(consumer, topic, max_messages=None, timeout=1.0):
    """Consume messages from a topic"""
    print(f"\nConsuming messages from topic: {topic}")
    print("Press Ctrl+C to stop consuming")
    print("-" * 50)
    
    message_count = 0
    
    try:
        while True:
            if max_messages and message_count >= max_messages:
                print(f"\nReached maximum message count ({max_messages})")
                break
                
            msg = consumer.poll(timeout)
            
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of partition {msg.partition()}")
                else:
                    print(f"Consumer error: {msg.error()}")
            else:
                message_count += 1
                print(f"\nMessage #{message_count}")
                print(f"  Topic: {msg.topic()}")
                print(f"  Partition: {msg.partition()}")
                print(f"  Offset: {msg.offset()}")
                print(f"  Key: {msg.key().decode('utf-8') if msg.key() else 'None'}")
                
                # Try to decode as JSON, fallback to string
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    print(f"  Value (JSON): {json.dumps(value, indent=2)}")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    try:
                        value = msg.value().decode('utf-8')
                        print(f"  Value: {value}")
                    except UnicodeDecodeError:
                        print(f"  Value: {msg.value()} (binary data)")
                
                print(f"  Timestamp: {msg.timestamp()}")
                print("-" * 30)
                
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()

def test_permissions(admin_client, metadata):
    """Test various permissions to identify security misconfigurations"""
    print("\n" + "="*60)
    print("PERMISSION TESTING")
    print("="*60)
    
    # Test topic creation permission
    test_topic = f"security-test-{int(time.time())}"
    print(f"\n1. Testing topic creation permission...")
    try:
        from confluent_kafka.admin import NewTopic
        topic_future = admin_client.create_topics([NewTopic(test_topic, num_partitions=1, replication_factor=1)])
        topic_future[test_topic].result(timeout=10)
        print(f"   ✓ SUCCESS: Can create topic '{test_topic}'")
        
        # Clean up - delete the test topic
        try:
            delete_future = admin_client.delete_topics([test_topic])
            delete_future[test_topic].result(timeout=10)
            print(f"   ✓ SUCCESS: Can delete topic '{test_topic}'")
        except Exception as e:
            print(f"   ⚠ WARNING: Can create but cannot delete topic '{test_topic}': {e}")
            
    except Exception as e:
        print(f"   ✗ FAILED: Cannot create topic '{test_topic}': {e}")
    
    # Test partition creation permission
    print(f"\n2. Testing partition creation permission...")
    try:
        if metadata.topics:
            test_topic_name = list(metadata.topics.keys())[0]
            from confluent_kafka.admin import NewPartitions
            partition_future = admin_client.create_partitions([NewPartitions(test_topic_name, 2)])
            partition_future[test_topic_name].result(timeout=10)
            print(f"   ✓ SUCCESS: Can create partitions for topic '{test_topic_name}'")
        else:
            print("   ⚠ SKIPPED: No topics available for partition testing")
    except Exception as e:
        print(f"   ✗ FAILED: Cannot create partitions: {e}")
    
    # Test configuration alteration permission
    print(f"\n3. Testing configuration alteration permission...")
    try:
        if metadata.topics:
            test_topic_name = list(metadata.topics.keys())[0]
            config_resource = ConfigResource('topic', test_topic_name)
            config_resource.set_config('retention.ms', '3600000')  # 1 hour
            config_future = admin_client.alter_configs([config_resource])
            config_future[config_resource].result(timeout=10)
            print(f"   ✓ SUCCESS: Can alter topic configuration for '{test_topic_name}'")
        else:
            print("   ⚠ SKIPPED: No topics available for config testing")
    except Exception as e:
        print(f"   ✗ FAILED: Cannot alter topic configuration: {e}")

def audit_security_configs(admin_client, metadata):
    """Audit security-related configurations"""
    print("\n" + "="*60)
    print("SECURITY CONFIGURATION AUDIT")
    print("="*60)
    
    # Check broker security configurations
    print(f"\n1. Broker Security Configurations:")
    try:
        broker_configs = [ConfigResource('broker', str(broker.id)) for broker in metadata.brokers.values()]
        broker_configs_future = admin_client.describe_configs(broker_configs)
        broker_configs_result = broker_configs_future.result()
        
        security_issues = []
        for broker_id, config in broker_configs_result.items():
            config_dict = config.result()
            print(f"   Broker {broker_id}:")
            
            # Check SSL/TLS settings
            listeners = config_dict.get('listeners', '')
            if 'SSL' in listeners or 'SASL_SSL' in listeners:
                print(f"     ✓ SSL/TLS enabled")
            else:
                print(f"     ⚠ SSL/TLS not enabled in listeners")
                security_issues.append(f"Broker {broker_id}: No SSL/TLS")
            
            # Check authentication settings
            if 'SASL' in listeners:
                print(f"     ✓ SASL authentication enabled")
            else:
                print(f"     ⚠ SASL authentication not enabled")
                security_issues.append(f"Broker {broker_id}: No SASL")
            
            # Check authorization settings
            authorizer = config_dict.get('authorizer.class.name', '')
            if authorizer:
                print(f"     ✓ Authorization enabled: {authorizer}")
            else:
                print(f"     ⚠ No authorization configured")
                security_issues.append(f"Broker {broker_id}: No authorization")
                
    except Exception as e:
        print(f"   ✗ ERROR: Could not fetch broker configurations: {e}")
    
    # Check topic security configurations
    print(f"\n2. Topic Security Configurations:")
    try:
        if metadata.topics:
            sample_topics = list(metadata.topics.keys())[:3]
            topic_configs = [ConfigResource('topic', topic) for topic in sample_topics]
            topic_configs_future = admin_client.describe_configs(topic_configs)
            topic_configs_result = topic_configs_future.result()
            
            for topic_name, config in topic_configs_result.items():
                config_dict = config.result()
                print(f"   Topic: {topic_name}")
                
                # Check retention settings
                retention_ms = config_dict.get('retention.ms', '')
                if retention_ms and retention_ms != '-1':
                    print(f"     ✓ Retention configured: {retention_ms}ms")
                else:
                    print(f"     ⚠ No retention limit configured")
                
                # Check cleanup policy
                cleanup_policy = config_dict.get('cleanup.policy', '')
                if cleanup_policy:
                    print(f"     ✓ Cleanup policy: {cleanup_policy}")
                else:
                    print(f"     ⚠ No cleanup policy configured")
                    
    except Exception as e:
        print(f"   ✗ ERROR: Could not fetch topic configurations: {e}")

def enumerate_sensitive_data(admin_client, metadata):
    """Look for potentially sensitive data in topics"""
    print("\n" + "="*60)
    print("SENSITIVE DATA ENUMERATION")
    print("="*60)
    
    sensitive_patterns = [
        'password', 'secret', 'key', 'token', 'credential', 'auth',
        'user', 'admin', 'root', 'login', 'session', 'cookie',
        'ssn', 'credit', 'card', 'account', 'bank', 'financial',
        'personal', 'private', 'confidential', 'internal'
    ]
    
    print(f"\nScanning {len(metadata.topics)} topics for sensitive data patterns...")
    
    sensitive_topics = []
    for topic_name in metadata.topics.keys():
        topic_lower = topic_name.lower()
        for pattern in sensitive_patterns:
            if pattern in topic_lower:
                sensitive_topics.append((topic_name, pattern))
                break
    
    if sensitive_topics:
        print(f"\n⚠ POTENTIALLY SENSITIVE TOPICS FOUND:")
        for topic_name, pattern in sensitive_topics:
            print(f"   - {topic_name} (matches pattern: '{pattern}')")
    else:
        print(f"\n✓ No obviously sensitive topic names found")

def test_message_injection(admin_client, metadata):
    """Test ability to inject messages into topics"""
    print("\n" + "="*60)
    print("MESSAGE INJECTION TESTING")
    print("="*60)
    
    # Configuration for producer
    conf = {
        'bootstrap.servers': admin_client._impl._rd_kafka.conf.get('bootstrap.servers'),
        'security.protocol': 'SSL',
        'ssl.certificate.location': admin_client._impl._rd_kafka.conf.get('ssl.certificate.location'),
        'ssl.key.location': admin_client._impl._rd_kafka.conf.get('ssl.key.location'),
        'ssl.ca.location': admin_client._impl._rd_kafka.conf.get('ssl.ca.location'),
        'ssl.endpoint.identification.algorithm': 'none',
    }
    
    if metadata.topics:
        test_topic = list(metadata.topics.keys())[0]
        print(f"\nTesting message injection into topic: {test_topic}")
        
        try:
            producer = Producer(conf)
            
            # Test message
            test_message = {
                "test": "security_audit",
                "timestamp": time.time(),
                "source": "kafka_client_pen_test"
            }
            
            producer.produce(
                topic=test_topic,
                key='security-test',
                value=json.dumps(test_message),
                callback=lambda err, msg: print(f"   ✓ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}") if not err else print(f"   ✗ Message delivery failed: {err}")
            )
            
            producer.flush(timeout=10)
            print(f"   ✓ SUCCESS: Can inject messages into topic '{test_topic}'")
            
        except Exception as e:
            print(f"   ✗ FAILED: Cannot inject messages into topic '{test_topic}': {e}")
    else:
        print("\n⚠ SKIPPED: No topics available for message injection testing")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Connect to Kafka server using SSL client certificates')
    parser.add_argument('server', help='Kafka server address and port (e.g., localhost:9093)')
    parser.add_argument('--client-cert', required=True, help='Path to client certificate PEM file')
    parser.add_argument('--ca-cert', help='Path to CA certificate PEM file (optional, uses client cert if not provided)')
    
    # Individual data type flags
    parser.add_argument('--cluster-info', action='store_true', help='Show cluster information (ID, controller, etc.)')
    parser.add_argument('--acls', action='store_true', help='Show Access Control Lists (ACLs)')
    parser.add_argument('--detailed-consumer-groups', action='store_true', help='Show detailed consumer group information')
    parser.add_argument('--user-credentials', action='store_true', help='Show user SCRAM credentials')
    parser.add_argument('--broker-configs', action='store_true', help='Show broker configurations')
    parser.add_argument('--topic-offsets', action='store_true', help='Show topic offsets and message positions')
    parser.add_argument('--topic-configs', action='store_true', help='Show topic configurations')
    parser.add_argument('--all', action='store_true', help='Show all available information')
    
    # Consumer options
    parser.add_argument('--consume', help='Topic to consume messages from')
    parser.add_argument('--consumer-group', default='kafka-client-consumer', help='Consumer group ID (default: kafka-client-consumer)')
    parser.add_argument('--max-messages', type=int, help='Maximum number of messages to consume (default: unlimited)')
    parser.add_argument('--from-beginning', action='store_true', help='Start consuming from the beginning of the topic')
    parser.add_argument('--timeout', type=float, default=1.0, help='Consumer poll timeout in seconds (default: 1.0)')
    
    # Penetration testing specific flags
    parser.add_argument('--test-permissions', action='store_true', help='Test various permissions (topic creation, deletion, etc.)')
    parser.add_argument('--audit-security', action='store_true', help='Audit security configurations')
    parser.add_argument('--enumerate-sensitive', action='store_true', help='Look for potentially sensitive data in topics')
    parser.add_argument('--test-injection', action='store_true', help='Test ability to inject messages into topics')
    parser.add_argument('--full-security-audit', action='store_true', help='Run all security tests and audits')
    
    args = parser.parse_args()
    
    # Configuration
    bootstrap_servers = args.server
    client_cert = args.client_cert
    ca_cert = args.ca_cert if args.ca_cert else client_cert

    conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SSL',
        'ssl.certificate.location': client_cert,
        'ssl.key.location': client_cert,
        'ssl.ca.location': ca_cert,  # Use the same PEM if you don't care about CA
        'ssl.endpoint.identification.algorithm': 'none',  # Disable hostname verification
    }

    # If consuming messages, set up consumer
    if args.consume:
        consumer_conf = conf.copy()
        consumer_conf.update({
            'group.id': args.consumer_group,
            'auto.offset.reset': 'earliest' if args.from_beginning else 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
        })
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([args.consume])
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        
        # Start consuming
        consume_messages(consumer, args.consume, args.max_messages, args.timeout)
        return

    # Admin client for server information
    admin_client = AdminClient(conf)

    # Get server version and basic metadata
    try:
        # Get API versions to determine Kafka version
        api_versions_future = admin_client.list_topics(timeout=10)
        metadata = api_versions_future
        
        print(f"\nKafka Server Info:")
        print(f"  Connected to: {bootstrap_servers}")
        print(f"  Number of brokers: {len(metadata.brokers)}")
        
        # Try to get more detailed version info
        try:
            # This will give us API version information
            api_versions = admin_client._impl._rd_kafka.api_versions()
            if api_versions:
                print(f"  API Versions supported: {len(api_versions)} APIs")
        except:
            pass
            
        for broker_id, broker in metadata.brokers.items():
            print(f"  Broker {broker_id}: {broker.host}:{broker.port}")
    except Exception as e:
        print(f"\nCould not get server version info: {e}")
        return

    # Always show basic information
    print("\nBrokers:")
    for broker in metadata.brokers.values():
        print(f" - id: {broker.id}, host: {broker.host}, port: {broker.port}")

    print("\nTopics:")
    for topic_name, topic in metadata.topics.items():
        print(f"\nTopic: {topic_name}")
        print(f"  Partitions: {len(topic.partitions)}")
        for partition_id, partition in topic.partitions.items():
            print(f"    Partition {partition_id}: leader={partition.leader}, replicas={partition.replicas}, isrs={partition.isrs}")
        if topic.error is not None:
            print(f"  Error: {topic.error}")

    print("\nController ID:", metadata.controller_id)

    # List consumer groups (basic info - always shown)
    try:
        groups_future = admin_client.list_consumer_groups()
        groups_result = groups_future.result()
        print("\nConsumer Groups:")
        # Use the 'valid' attribute and access group_id
        for group in groups_result.valid:
            print(f" - {group.group_id}")
    except Exception as e:
        print("\nCould not fetch consumer groups:", e)

    # Run security tests if requested
    if args.full_security_audit:
        test_permissions(admin_client, metadata)
        audit_security_configs(admin_client, metadata)
        enumerate_sensitive_data(admin_client, metadata)
        test_message_injection(admin_client, metadata)
    else:
        if args.test_permissions:
            test_permissions(admin_client, metadata)
        if args.audit_security:
            audit_security_configs(admin_client, metadata)
        if args.enumerate_sensitive:
            enumerate_sensitive_data(admin_client, metadata)
        if args.test_injection:
            test_message_injection(admin_client, metadata)

    # Show topic configurations if requested
    if args.topic_configs or args.all:
        try:
            resource_list = [("topic", t) for t in metadata.topics.keys()]
            # Only fetch configs for a few topics to avoid overwhelming output
            resource_list = resource_list[:5]
            config_resources = [ConfigResource('topic', t) for _, t in resource_list]
            configs = admin_client.describe_configs(config_resources)
            print("\nSample Topic Configs:")
            for resource, f in configs.items():
                print(f"Config for {resource}: {f.result()}")
        except Exception as e:
            print("\nCould not fetch topic configs:", e)

    # Show cluster information if requested
    if args.cluster_info or args.all:
        try:
            cluster_future = admin_client.describe_cluster()
            cluster_info = cluster_future.result()
            print(f"\nCluster Information:")
            print(f"  Cluster ID: {cluster_info.cluster_id}")
            print(f"  Controller: {cluster_info.controller}")
            print(f"  Authorized Operations: {cluster_info.authorized_operations}")
        except Exception as e:
            print(f"\nCould not fetch cluster information: {e}")

    # Show ACLs if requested
    if args.acls or args.all:
        try:
            acls_future = admin_client.describe_acls()
            acls_result = acls_future.result()
            print(f"\nAccess Control Lists (ACLs):")
            if acls_result:
                for acl in acls_result:
                    print(f"  - {acl}")
            else:
                print("  No ACLs found or ACLs not enabled")
        except Exception as e:
            print(f"\nCould not fetch ACLs: {e}")

    # Show detailed consumer groups if requested
    if args.detailed_consumer_groups or args.all:
        try:
            if groups_result.valid:
                group_ids = [group.group_id for group in groups_result.valid[:5]]  # Limit to 5 groups
                detailed_groups_future = admin_client.describe_consumer_groups(group_ids)
                detailed_groups = detailed_groups_future.result()
                print(f"\nDetailed Consumer Group Information:")
                for group_id, group_info in detailed_groups.items():
                    print(f"  Group: {group_id}")
                    print(f"    State: {group_info.state}")
                    print(f"    Members: {len(group_info.members)}")
                    print(f"    Protocol: {group_info.protocol}")
                    print(f"    Protocol Type: {group_info.protocol_type}")
        except Exception as e:
            print(f"\nCould not fetch detailed consumer groups: {e}")

    # Show user credentials if requested
    if args.user_credentials or args.all:
        try:
            users_future = admin_client.describe_user_scram_credentials()
            users_result = users_future.result()
            print(f"\nUser SCRAM Credentials:")
            if users_result:
                for user in users_result:
                    print(f"  - {user}")
            else:
                print("  No SCRAM users found or SCRAM not enabled")
        except Exception as e:
            print(f"\nCould not fetch user credentials: {e}")

    # Show broker configurations if requested
    if args.broker_configs or args.all:
        try:
            broker_configs = [ConfigResource('broker', str(broker.id)) for broker in metadata.brokers.values()]
            broker_configs_future = admin_client.describe_configs(broker_configs)
            broker_configs_result = broker_configs_future.result()
            print(f"\nBroker Configurations:")
            for broker_id, config in broker_configs_result.items():
                print(f"  Broker {broker_id}:")
                config_dict = config.result()
                for key, value in config_dict.items():
                    if key in ['listeners', 'advertised.listeners', 'log.dirs', 'zookeeper.connect']:
                        print(f"    {key}: {value}")
        except Exception as e:
            print(f"\nCould not fetch broker configurations: {e}")

    # Show topic offsets if requested
    if args.topic_offsets or args.all:
        try:
            print(f"\nTopic Offsets (Latest):")
            for topic_name in list(metadata.topics.keys())[:3]:  # Limit to 3 topics
                offsets_future = admin_client.list_offsets([(topic_name, partition_id) for partition_id in metadata.topics[topic_name].partitions.keys()])
                offsets_result = offsets_future.result()
                print(f"  Topic: {topic_name}")
                for (topic, partition), offset_info in offsets_result.items():
                    print(f"    Partition {partition}: offset={offset_info.offset}, timestamp={offset_info.timestamp}")
        except Exception as e:
            print(f"\nCould not fetch topic offsets: {e}")

if __name__ == "__main__":
    main() 