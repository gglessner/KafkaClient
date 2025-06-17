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
from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

def signal_handler(sig, frame):
    print('\nShutting down gracefully...')
    sys.exit(0)

def subscribe_messages(consumer, topic, max_messages=None, timeout=1.0):
    """Subscribe to and read messages from a topic"""
    print(f"\nSubscribing to topic: {topic}")
    print("Press Ctrl+C to stop reading")
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
        print("\nStopping subscription...")
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
    
    # Test message injection into a topic
    print(f"\n1. Testing message injection...")
    try:
        if metadata.topics:
            test_topic = list(metadata.topics.keys())[0]
            producer = Producer({
                'bootstrap.servers': admin_client._impl._rd_kafka.conf_get('bootstrap.servers'),
                'security.protocol': 'SSL',
                'ssl.certificate.location': admin_client._impl._rd_kafka.conf_get('ssl.certificate.location'),
                'ssl.key.location': admin_client._impl._rd_kafka.conf_get('ssl.key.location'),
                'ssl.ca.location': admin_client._impl._rd_kafka.conf_get('ssl.ca.location'),
                'ssl.endpoint.identification.algorithm': 'none',
            })
            
            # Send a test message
            test_message = {
                'test_type': 'security_audit',
                'timestamp': int(time.time()),
                'source': 'kafka-client-tool',
                'message': 'Test message for security assessment'
            }
            
            producer.produce(
                topic=test_topic,
                key='security-test',
                value=json.dumps(test_message).encode('utf-8'),
                callback=lambda err, msg: None
            )
            producer.flush(timeout=10)
            print(f"   ✓ SUCCESS: Can inject messages into topic '{test_topic}'")
            
        else:
            print("   ⚠ SKIPPED: No topics available for injection testing")
    except Exception as e:
        print(f"   ✗ FAILED: Cannot inject messages: {e}")

def browse_group(admin_client, group_name, max_messages=10, timeout=5.0):
    """Safely browse messages from an existing consumer group without consuming them"""
    print(f"\n" + "="*60)
    print(f"BROWSING CONSUMER GROUP: {group_name}")
    print("="*60)
    
    try:
        # Get group information
        print(f"\n1. Getting group information...")
        group_future = admin_client.describe_consumer_groups([group_name])
        group_info = group_future.result()
        
        if group_name not in group_info:
            print(f"   ✗ ERROR: Consumer group '{group_name}' not found")
            return
        
        group = group_info[group_name]
        print(f"   ✓ Group State: {group.state}")
        print(f"   ✓ Members: {len(group.members)}")
        print(f"   ✓ Protocol: {group.protocol}")
        
        if group.state == 'Stable' and len(group.members) > 0:
            print(f"   ⚠ WARNING: Group has active members. Browsing may interfere with consumption.")
        
        # Get committed offsets for the group
        print(f"\n2. Getting committed offsets...")
        offsets_future = admin_client.list_consumer_group_offsets([group_name])
        offsets_result = offsets_future.result()
        
        if group_name not in offsets_result:
            print(f"   ✗ ERROR: No committed offsets found for group '{group_name}'")
            return
        
        group_offsets = offsets_result[group_name]
        if not group_offsets:
            print(f"   ⚠ WARNING: No committed offsets found for group '{group_name}'")
            return
        
        print(f"   ✓ Found {len(group_offsets)} partition assignments")
        
        # Create a temporary consumer for browsing
        print(f"\n3. Creating temporary browser consumer...")
        browser_conf = {
            'bootstrap.servers': admin_client._impl._rd_kafka.conf_get('bootstrap.servers'),
            'security.protocol': 'SSL',
            'ssl.certificate.location': admin_client._impl._rd_kafka.conf_get('ssl.certificate.location'),
            'ssl.key.location': admin_client._impl._rd_kafka.conf_get('ssl.key.location'),
            'ssl.ca.location': admin_client._impl._rd_kafka.conf_get('ssl.ca.location'),
            'ssl.endpoint.identification.algorithm': 'none',
            'group.id': f'browse-{group_name}-{int(time.time())}',  # Temporary group
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Don't commit offsets
            'enable.partition.eof': True,
        }
        
        browser_consumer = Consumer(browser_conf)
        
        # Manually assign partitions and set offsets
        topic_partitions = []
        for (topic, partition), offset_info in group_offsets.items():
            tp = TopicPartition(topic, partition, offset_info.offset)
            topic_partitions.append(tp)
            print(f"   - {topic}[{partition}]: offset {offset_info.offset}")
        
        browser_consumer.assign(topic_partitions)
        
        # Browse messages
        print(f"\n4. Browsing messages (max: {max_messages})...")
        print("Press Ctrl+C to stop browsing")
        print("-" * 50)
        
        message_count = 0
        start_time = time.time()
        
        try:
            while message_count < max_messages and (time.time() - start_time) < timeout:
                msg = browser_consumer.poll(1.0)
                
                if msg is None:
                    continue
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Reached end of partition {msg.partition()}")
                        break
                    else:
                        print(f"Consumer error: {msg.error()}")
                        break
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
            print("\nStopping browser...")
        finally:
            browser_consumer.close()
        
        print(f"\n5. Browse Summary:")
        print(f"   ✓ Browsed {message_count} messages from group '{group_name}'")
        print(f"   ✓ No offsets were committed (safe browsing)")
        print(f"   ✓ Original group '{group_name}' was not affected")
        
    except Exception as e:
        print(f"\n✗ ERROR browsing group '{group_name}': {e}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Connect to Kafka server using SSL client certificates')
    parser.add_argument('server', help='Kafka server address and port (e.g., localhost:9093)')
    parser.add_argument('--client-cert', required=True, help='Path to client certificate PEM file')
    parser.add_argument('--ca-cert', help='Path to CA certificate PEM file (optional, uses client cert if not provided)')
    
    # Topic listing options
    parser.add_argument('--list-topics', action='store_true', help='List only topic names (no partitions)')
    parser.add_argument('--list-topics-partitions', action='store_true', help='List topics with partition details')
    
    # Consumer group listing option
    parser.add_argument('--list-consumer-groups', action='store_true', help='List consumer groups')
    
    # Broker listing option
    parser.add_argument('--list-brokers', action='store_true', help='List broker information')
    
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
    parser.add_argument('--subscribe', help='Topic to subscribe to and read messages from')
    parser.add_argument('--consumer-group', default='kafka-client-consumer', help='Consumer group ID (default: kafka-client-consumer)')
    parser.add_argument('--max-messages', type=int, help='Maximum number of messages to read (default: unlimited)')
    parser.add_argument('--from-beginning', action='store_true', help='Start reading from the beginning of the topic')
    parser.add_argument('--timeout', type=float, default=1.0, help='Consumer poll timeout in seconds (default: 1.0)')
    
    # Consumer group browsing option
    parser.add_argument('--browse-group', help='Browse messages from an existing consumer group (without consuming)')
    parser.add_argument('--browse-max-messages', type=int, default=10, help='Maximum messages to browse from group (default: 10)')
    parser.add_argument('--browse-timeout', type=float, default=5.0, help='Browse timeout in seconds (default: 5.0)')
    
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

    # If subscribing to a topic, set up consumer
    if args.subscribe:
        consumer_conf = conf.copy()
        consumer_conf.update({
            'group.id': args.consumer_group,
            'auto.offset.reset': 'earliest' if args.from_beginning else 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
        })
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([args.subscribe])
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        
        # Start consuming
        subscribe_messages(consumer, args.subscribe, args.max_messages, args.timeout)
        return

    # If browsing a consumer group, set up browser
    if args.browse_group:
        # Admin client for group browsing
        admin_client = AdminClient(conf)
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        
        # Start browsing
        browse_group(admin_client, args.browse_group, args.browse_max_messages, args.browse_timeout)
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

    # Handle topic listing based on options
    if args.list_topics:
        print("\nTopics:")
        for topic_name in metadata.topics.keys():
            print(f" - {topic_name}")
    elif args.list_topics_partitions:
        print("\nTopics:")
        for topic_name, topic in metadata.topics.items():
            print(f"\nTopic: {topic_name}")
            print(f"  Partitions: {len(topic.partitions)}")
            for partition_id, partition in topic.partitions.items():
                print(f"    Partition {partition_id}: leader={partition.leader}, replicas={partition.replicas}, isrs={partition.isrs}")
            if topic.error is not None:
                print(f"  Error: {topic.error}")
    else:
        # Default behavior - show topics without partitions unless other flags are specified
        print("\nTopics:")
        for topic_name in metadata.topics.keys():
            print(f" - {topic_name}")

    print("\nController ID:", metadata.controller_id)

    # List brokers if requested
    if args.list_brokers:
        print("\nBrokers:")
        for broker in metadata.brokers.values():
            print(f" - id: {broker.id}, host: {broker.host}, port: {broker.port}")

    # List consumer groups if requested
    groups_result = None
    if args.list_consumer_groups:
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
            # Get consumer groups if not already fetched
            if groups_result is None:
                groups_future = admin_client.list_consumer_groups()
                groups_result = groups_future.result()
            
            if groups_result.valid:
                group_ids = [group.group_id for group in groups_result.valid[:5]]  # Limit to 5 groups
                detailed_groups = admin_client.describe_consumer_groups(group_ids)
                print(f"\nDetailed Consumer Group Information:")
                for group_id, group_info_future in detailed_groups.items():
                    group_info = group_info_future.result()
                    print(f"  Group: {group_id}")
                    print(f"    State: {getattr(group_info, 'state', 'Unknown')}")
                    print(f"    Members: {len(getattr(group_info, 'members', []))}")
                    if hasattr(group_info, 'protocol'):
                        print(f"    Protocol: {group_info.protocol}")
                    if hasattr(group_info, 'protocol_type'):
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