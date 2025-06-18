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

def find_topics_by_prefix(admin_client, prefix):
    """Find all topics that start with the given prefix"""
    try:
        metadata_future = admin_client.list_topics(timeout=10)
        metadata = metadata_future
        matching_topics = [topic_name for topic_name in metadata.topics.keys() if topic_name.startswith(prefix)]
        return matching_topics
    except Exception as e:
        print(f"Error finding topics with prefix '{prefix}': {e}")
        return []

def subscribe_messages(consumer, topics, max_messages=None, timeout=1.0):
    """Subscribe to and read messages from one or more topics"""
    if isinstance(topics, str):
        topics = [topics]
    
    print(f"\nSubscribing to topics: {', '.join(topics)}")
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
                # Robust key handling
                if msg.key():
                    try:
                        key_str = msg.key().decode('utf-8')
                    except UnicodeDecodeError:
                        key_str = repr(msg.key())
                else:
                    key_str = 'None'
                print(f"  Key: {key_str}")
                
                # Robust value handling
                try:
                    if msg.value() is None:
                        print("  Value: None")
                    else:
                        try:
                            value = json.loads(msg.value().decode('utf-8'))
                            print(f"  Value (JSON): {json.dumps(value, indent=2)}")
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            try:
                                value = msg.value().decode('utf-8')
                                print(f"  Value: {value}")
                            except UnicodeDecodeError:
                                print(f"  Value: {repr(msg.value())} (binary data)")
                except Exception as e:
                    print(f"  Error reading value: {e}")
                
                print(f"  Timestamp: {msg.timestamp()}")
                print("-" * 30)
                
    except KeyboardInterrupt:
        print("\nStopping subscription...")
    finally:
        consumer.close()

def test_permissions(admin_client, metadata, conf):
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
        # Create a temporary topic for partition testing
        temp_topic = f"partition-test-{int(time.time())}"
        from confluent_kafka.admin import NewTopic
        
        # Create topic with 1 partition initially
        topic_future = admin_client.create_topics([NewTopic(temp_topic, num_partitions=1, replication_factor=1)])
        topic_future[temp_topic].result(timeout=10)
        print(f"   ✓ Created temporary topic '{temp_topic}' with 1 partition")
        
        # Now test adding a partition
        from confluent_kafka.admin import NewPartitions
        partition_future = admin_client.create_partitions([NewPartitions(temp_topic, 2)])
        partition_future[temp_topic].result(timeout=10)
        print(f"   ✓ SUCCESS: Can create partitions for topic '{temp_topic}' (increased from 1 to 2)")
        
        # Clean up - delete the temporary topic
        try:
            delete_future = admin_client.delete_topics([temp_topic])
            delete_future[temp_topic].result(timeout=10)
            print(f"   ✓ Cleaned up temporary topic '{temp_topic}'")
        except Exception as e:
            print(f"   ⚠ WARNING: Could not delete temporary topic '{temp_topic}': {e}")
            
    except Exception as e:
        print(f"   ✗ FAILED: Cannot create partitions: {e}")
        # Try to clean up if topic was created but partition creation failed
        try:
            if 'temp_topic' in locals():
                delete_future = admin_client.delete_topics([temp_topic])
                delete_future[temp_topic].result(timeout=10)
                print(f"   ✓ Cleaned up temporary topic '{temp_topic}' after failure")
        except:
            pass
    
    # Test configuration alteration permission
    print(f"\n3. Testing configuration alteration permission...")
    try:
        import warnings
        # Create a temporary topic for config testing
        temp_topic = f"config-test-{int(time.time())}"
        from confluent_kafka.admin import NewTopic, ConfigResource
        
        # Create topic initially
        topic_future = admin_client.create_topics([NewTopic(temp_topic, num_partitions=1, replication_factor=1)])
        topic_future[temp_topic].result(timeout=10)
        print(f"   ✓ Created temporary topic '{temp_topic}' for config testing")

        # Suppress deprecation warnings for alter_configs
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=DeprecationWarning)
            config_resource = ConfigResource('topic', temp_topic)
            config_resource.set_config('retention.ms', '3600000')  # 1 hour
            config_future = admin_client.alter_configs([config_resource])
            config_future[config_resource].result(timeout=10)
        print(f"   ✓ SUCCESS: Can alter topic configuration for '{temp_topic}'")
        
        # Clean up - delete the temporary topic
        try:
            delete_future = admin_client.delete_topics([temp_topic])
            delete_future[temp_topic].result(timeout=10)
            print(f"   ✓ Cleaned up temporary topic '{temp_topic}'")
        except Exception as e:
            print(f"   ⚠ WARNING: Could not delete temporary topic '{temp_topic}': {e}")
            
    except Exception as e:
        print(f"   ✗ FAILED: Cannot alter topic configuration: {e}")
        # Try to clean up if topic was created but config alteration failed
        try:
            if 'temp_topic' in locals():
                delete_future = admin_client.delete_topics([temp_topic])
                delete_future[temp_topic].result(timeout=10)
                print(f"   ✓ Cleaned up temporary topic '{temp_topic}' after failure")
        except:
            pass

    # Test consumer group permissions
    print(f"\n4. Testing consumer group permissions...")
    try:
        # Create a temporary topic for consumer group testing
        temp_topic = f"consumer-test-{int(time.time())}"
        temp_group = f"test-group-{int(time.time())}"
        from confluent_kafka.admin import NewTopic
        
        # Create topic initially
        topic_future = admin_client.create_topics([NewTopic(temp_topic, num_partitions=1, replication_factor=1)])
        topic_future[temp_topic].result(timeout=10)
        print(f"   ✓ Created temporary topic '{temp_topic}' for consumer group testing")
        
        # Test consumer group creation by creating a consumer and committing offsets
        from confluent_kafka import Consumer, TopicPartition
        
        # Use the same configuration as the admin client
        consumer_conf = conf.copy()
        consumer_conf.update({
            'group.id': temp_group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Manual commit for testing
        })
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([temp_topic])
        
        # Poll for a message to create the group
        msg = consumer.poll(5.0)
        if msg is None:
            print(f"   ✓ No messages in topic, but consumer group '{temp_group}' was created")
        
        # Test committing offsets
        try:
            consumer.commit()
            print(f"   ✓ SUCCESS: Can commit offsets for consumer group '{temp_group}'")
        except Exception as e:
            print(f"   ⚠ WARNING: Cannot commit offsets: {e}")
        
        # Test offset manipulation via admin client
        try:
            # Get current offsets
            offsets_result = admin_client.list_consumer_group_offsets([temp_group])
            if temp_group in offsets_result:
                print(f"   ✓ SUCCESS: Can list offsets for consumer group '{temp_group}'")
            else:
                print(f"   ⚠ WARNING: No offsets found for consumer group '{temp_group}'")
        except Exception as e:
            print(f"   ⚠ WARNING: Cannot list consumer group offsets: {e}")
        
        # Properly close the consumer to leave the group
        consumer.close()
        print(f"   ✓ Closed consumer to leave group '{temp_group}'")
        
        # Wait a moment for the consumer to fully leave the group
        time.sleep(2)
        
        # Test consumer group deletion (if supported)
        try:
            delete_groups_future = admin_client.delete_consumer_groups([temp_group])
            delete_groups_future[temp_group].result(timeout=10)
            print(f"   ✓ SUCCESS: Can delete consumer group '{temp_group}'")
        except Exception as e:
            print(f"   ⚠ WARNING: Cannot delete consumer group '{temp_group}': {e}")
            # This is often not supported in older Kafka versions
        
        # Clean up - delete the temporary topic
        try:
            delete_future = admin_client.delete_topics([temp_topic])
            delete_future[temp_topic].result(timeout=10)
            print(f"   ✓ Cleaned up temporary topic '{temp_topic}'")
        except Exception as e:
            print(f"   ⚠ WARNING: Could not delete temporary topic '{temp_topic}': {e}")
            
    except Exception as e:
        print(f"   ✗ FAILED: Cannot test consumer group permissions: {e}")
        # Try to clean up if topic was created but consumer group testing failed
        try:
            if 'temp_topic' in locals():
                delete_future = admin_client.delete_topics([temp_topic])
                delete_future[temp_topic].result(timeout=10)
                print(f"   ✓ Cleaned up temporary topic '{temp_topic}' after failure")
        except:
            pass

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
        print(f"DEBUG: Called admin_client.describe_configs()")
        print(f"DEBUG: broker_configs_future type: {type(broker_configs_future)}")
        broker_configs_result = broker_configs_future.result()
        print(f"DEBUG: Got broker_configs_result, type: {type(broker_configs_result)}")
        print(f"DEBUG: broker_configs_result content: {broker_configs_result}")
        
        security_issues = []
        for broker_id, config in broker_configs_result.items():
            print(f"DEBUG: broker_id={broker_id}, config type={type(config)}")
            try:
                config_dict = config.result()
            except Exception as e:
                print(f"DEBUG: Exception calling config.result(): {e} (type: {type(e)})")
                config_dict = config
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
                config_dict = config.result() if hasattr(config, 'result') else config
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

def test_message_injection(admin_client, metadata, conf):
    """Test ability to inject messages into topics"""
    print("\n" + "="*60)
    print("MESSAGE INJECTION TESTING")
    print("="*60)
    
    # Test message injection into a topic
    print(f"\n1. Testing message injection...")
    try:
        if metadata.topics:
            test_topic = list(metadata.topics.keys())[0]
            
            # Use the same configuration as the admin client
            producer_conf = conf.copy()
            producer = Producer(producer_conf)
            
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
        group_info = admin_client.describe_consumber_groups([group_name])
        
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
        offsets_result = admin_client.list_consumer_group_offsets([group_name])
        
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
                    
                    # Robust key handling
                    if msg.key():
                        try:
                            key_str = msg.key().decode('utf-8')
                        except UnicodeDecodeError:
                            key_str = repr(msg.key())
                    else:
                        key_str = 'None'
                    print(f"  Key: {key_str}")
                    
                    # Robust value handling
                    try:
                        if msg.value() is None:
                            print("  Value: None")
                        else:
                            try:
                                value = json.loads(msg.value().decode('utf-8'))
                                print(f"  Value (JSON): {json.dumps(value, indent=2)}")
                            except (json.JSONDecodeError, UnicodeDecodeError):
                                try:
                                    value = msg.value().decode('utf-8')
                                    print(f"  Value: {value}")
                                except UnicodeDecodeError:
                                    print(f"  Value: {repr(msg.value())} (binary data)")
                    except Exception as e:
                        print(f"  Error reading value: {e}")
                    
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

def describe_consumer_group_details(admin_client, group_name):
    """Get detailed information about a specific consumer group"""
    print(f"\n" + "="*60)
    print(f"CONSUMER GROUP DETAILS: {group_name}")
    print("="*60)
    
    try:
        # Get detailed consumer group information
        print(f"\n1. Getting consumer group information...")
        detailed_groups = admin_client.describe_consumer_groups([group_name])
        group_info = detailed_groups[group_name].result(timeout=10)
        
        print(f"   ✓ Group ID: {group_name}")
        print(f"   ✓ State: {group_info.state}")
        print(f"   ✓ Protocol Type: {getattr(group_info, 'protocol_type', 'Unknown')}")
        print(f"   ✓ Protocol: {getattr(group_info, 'protocol', 'Unknown')}")
        print(f"   ✓ Members: {len(group_info.members)}")
        
        # Show member details
        if group_info.members:
            print(f"\n2. Member Details:")
            for i, member in enumerate(group_info.members, 1):
                print(f"   Member {i}:")
                print(f"     ✓ Member ID: {member.member_id}")
                print(f"     ✓ Client ID: {getattr(member, 'client_id', 'Unknown')}")
                print(f"     ✓ Client Host: {getattr(member, 'client_host', 'Unknown')}")
                print(f"     ✓ Session Timeout: {getattr(member, 'session_timeout_ms', 'Unknown')}ms")
                print(f"     ✓ Rebalance Timeout: {getattr(member, 'rebalance_timeout_ms', 'Unknown')}ms")
                
                # Show partition assignments
                if hasattr(member, 'member_metadata') and member.member_metadata:
                    print(f"     ✓ Member Metadata: {len(member.member_metadata)} bytes")
                
                if hasattr(member, 'member_assignment') and member.member_assignment:
                    print(f"     ✓ Partition Assignments:")
                    try:
                        # Try to decode assignment if possible
                        assignments = member.member_assignment
                        if hasattr(assignments, 'partition_assignments'):
                            for assignment in assignments.partition_assignments:
                                print(f"       - {assignment.topic}:{assignment.partition}")
                        else:
                            print(f"       - {len(member.member_assignment)} bytes of assignment data")
                    except Exception as e:
                        print(f"       - Assignment data (could not decode): {e}")
                else:
                    print(f"     ✓ No partition assignments")
                print()
        else:
            print(f"\n2. Member Details:")
            print(f"   ⚠ No active members in this consumer group")
        
        # Get committed offsets
        print(f"3. Committed Offsets:")
        try:
            offsets_result = admin_client.list_consumer_group_offsets([group_name])
            if group_name in offsets_result:
                group_offsets = offsets_result[group_name]
                if group_offsets:
                    print(f"   ✓ Found {len(group_offsets)} partition assignments with offsets:")
                    for (topic, partition), offset_info in group_offsets.items():
                        print(f"     - {topic}[{partition}]: offset {offset_info.offset}")
                        if hasattr(offset_info, 'metadata') and offset_info.metadata:
                            print(f"       Metadata: {offset_info.metadata}")
                else:
                    print(f"   ⚠ No committed offsets found")
            else:
                print(f"   ⚠ No offset information available")
        except Exception as e:
            print(f"   ⚠ Could not retrieve offset information: {e}")
        
        # Show group summary
        print(f"\n4. Group Summary:")
        if group_info.state == 'Stable' and len(group_info.members) > 0:
            print(f"   ✓ Group is healthy and active")
            print(f"   ✓ {len(group_info.members)} consumer(s) are processing messages")
        elif group_info.state == 'Empty':
            print(f"   ⚠ Group is empty (no active consumers)")
        elif group_info.state == 'PreparingRebalance':
            print(f"   ⚠ Group is rebalancing (preparing)")
        elif group_info.state == 'CompletingRebalance':
            print(f"   ⚠ Group is rebalancing (completing)")
        elif group_info.state == 'Dead':
            print(f"   ✗ Group is dead (no members)")
        else:
            print(f"   ⚠ Group state: {group_info.state}")
            
    except Exception as e:
        if "GROUP_ID_NOT_FOUND" in str(e) or "not found" in str(e).lower():
            print(f"\n✗ ERROR: Consumer group '{group_name}' not found")
            print(f"   💡 Use --list-consumer-groups to see available groups")
        else:
            print(f"\n✗ ERROR: Could not describe consumer group '{group_name}': {e}")

def safely_delete_consumer_group(admin_client, group_name):
    """Safely delete a consumer group only if it's empty (no active consumers)"""
    print(f"\n" + "="*60)
    print(f"SAFELY DELETING CONSUMER GROUP: {group_name}")
    print("="*60)
    
    try:
        # First, check if the group exists and get its details
        print(f"\n1. Checking consumer group status...")
        try:
            detailed_groups = admin_client.describe_consumer_groups([group_name])
            group_info = detailed_groups[group_name].result(timeout=10)
            
            print(f"   ✓ Group State: {group_info.state}")
            print(f"   ✓ Members: {len(group_info.members)}")
            
            if len(group_info.members) > 0:
                print(f"   ✗ CANNOT DELETE: Group has {len(group_info.members)} active member(s)")
                print(f"   ⚠ WARNING: Deleting a group with active consumers can cause data loss")
                print(f"   💡 Suggestion: Stop all consumers in the group first, then try again")
                return False
            else:
                print(f"   ✓ Group is empty - safe to delete")
                
        except Exception as e:
            print(f"   ⚠ WARNING: Could not get detailed group info: {e}")
            print(f"   💡 Proceeding with deletion attempt...")
        
        # Attempt to delete the group
        print(f"\n2. Attempting to delete consumer group...")
        try:
            delete_groups_future = admin_client.delete_consumer_groups([group_name])
            delete_groups_future[group_name].result(timeout=10)
            print(f"   ✓ SUCCESS: Consumer group '{group_name}' deleted successfully")
            return True
            
        except Exception as e:
            if "NON_EMPTY_GROUP" in str(e):
                print(f"   ✗ FAILED: Group is not empty - cannot delete")
                print(f"   💡 Suggestion: Stop all consumers in the group first")
            elif "GROUP_ID_NOT_FOUND" in str(e):
                print(f"   ✗ FAILED: Consumer group '{group_name}' not found")
            else:
                print(f"   ✗ FAILED: Could not delete consumer group: {e}")
            return False
            
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        return False

def produce_message(producer, topic, key=None, value=None, json_value=None):
    """Produce a message to a topic"""
    try:
        # Handle JSON value
        if json_value is not None:
            try:
                value = json.dumps(json_value).encode('utf-8')
            except json.JSONDecodeError as e:
                print(f"Error encoding JSON value: {e}")
                return False
        elif value is not None:
            value = str(value).encode('utf-8')
        
        # Handle key
        if key is not None:
            key = str(key).encode('utf-8')
        
        # Produce the message
        producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=lambda err, msg: print(f"✓ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
            if err is None else print(f"✗ Failed to deliver message: {err}")
        )
        producer.flush(timeout=10)
        return True
    except Exception as e:
        print(f"✗ Error producing message: {e}")
        return False

def create_test_consumer_group(admin_client, conf, group_name, topic_name):
    """Create a simple consumer group for testing purposes"""
    print(f"\n" + "="*60)
    print(f"CREATING TEST CONSUMER GROUP: {group_name}")
    print("="*60)
    
    try:
        # Create a topic if it doesn't exist
        from confluent_kafka.admin import NewTopic
        topic_future = admin_client.create_topics([NewTopic(topic_name, num_partitions=1, replication_factor=1)])
        topic_future[topic_name].result(timeout=10)
        print(f"   ✓ Created topic '{topic_name}' for testing")
        
        # Create a consumer and commit some offsets to establish the group
        from confluent_kafka import Consumer
        
        consumer_conf = conf.copy()
        consumer_conf.update({
            'group.id': group_name,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([topic_name])
        
        # Poll briefly to create the group
        msg = consumer.poll(2.0)
        if msg is None:
            print(f"   ✓ No messages in topic, but consumer group '{group_name}' was created")
        
        # Commit offsets to establish the group
        consumer.commit()
        print(f"   ✓ Committed offsets for group '{group_name}'")
        
        consumer.close()
        print(f"   ✓ Consumer group '{group_name}' is now ready for testing")
        print(f"   💡 Use --describe-consumer-group {group_name} to see details")
        
    except Exception as e:
        print(f"   ✗ ERROR creating test consumer group: {e}")

def scan_available_messages(conf):
    """Scan all consumer groups and topics for available messages (unconsumed) without consuming them."""
    from confluent_kafka.admin import AdminClient
    from confluent_kafka import Consumer, TopicPartition
    import time

    print("\n==============================")
    print("SCANNING FOR AVAILABLE MESSAGES")
    print("==============================\n")

    admin = AdminClient(conf)
    # List all topics
    metadata = admin.list_topics(timeout=10)
    topics = list(metadata.topics.keys())
    # List all consumer groups (get result from Future)
    group_future = admin.list_consumer_groups()
    groups_result = group_future.result()  # ListConsumerGroupsResult object
    groups = [group.group_id for group in getattr(groups_result, 'valid', [])]
    if not groups:
        print("No consumer groups found.")
        return
    if not topics:
        print("No topics found.")
        return

    # Prepare a summary
    summary = []
    for group in groups:
        try:
            offsets = admin.list_consumer_group_offsets(group)
        except Exception as e:
            print(f"  ✗ Could not get offsets for group {group}: {e}")
            continue
        for topic in topics:
            partitions = list(metadata.topics[topic].partitions.keys())
            for partition in partitions:
                tp = TopicPartition(topic, partition)
                committed = offsets.get(tp, None)
                if committed is None or committed.offset is None or committed.offset < 0:
                    continue
                # Get latest offset for this topic/partition
                consumer_conf = conf.copy()
                consumer_conf['group.id'] = f'scan-{int(time.time())}'
                consumer_conf['enable.auto.commit'] = False
                consumer = Consumer(consumer_conf)
                try:
                    low, high = consumer.get_watermark_offsets(tp, timeout=5)
                except Exception as e:
                    consumer.close()
                    continue
                consumer.close()
                if high > committed.offset:
                    summary.append({
                        'group': group,
                        'topic': topic,
                        'partition': partition,
                        'committed': committed.offset,
                        'latest': high,
                        'available': high - committed.offset
                    })

    if not summary:
        print("No available messages found for any group.")
        return

    print(f"{'Group':<30} {'Topic':<30} {'Partition':<10} {'Committed':<10} {'Latest':<10} {'Available':<10}")
    print("-"*100)
    for row in summary:
        print(f"{row['group']:<30} {row['topic']:<30} {row['partition']:<10} {row['committed']:<10} {row['latest']:<10} {row['available']:<10}")
    print(f"\n✓ Scan complete. {len(summary)} topic-partitions have available messages for their groups.")

def parse_args():
    parser = argparse.ArgumentParser(description='Kafka Client Tool')
    parser.add_argument('server', help='Kafka server address and port (e.g., localhost:9093)')
    parser.add_argument('--client-cert', help='Path to client certificate PEM file (required when using TLS)')
    parser.add_argument('--ca-cert', help='Path to CA certificate PEM file (optional, uses client cert if not provided)')
    parser.add_argument('--no-tls', action='store_true', help='Connect without TLS/SSL encryption (plaintext)')
    
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
    parser.add_argument('--subscribe-wildcard', help='Subscribe to all topics that start with the provided prefix')
    parser.add_argument('--consumer-group', default='kafka-client-consumer', help='Consumer group ID (default: kafka-client-consumer)')
    parser.add_argument('--max-messages', type=int, help='Maximum number of messages to read (default: unlimited)')
    parser.add_argument('--from-beginning', action='store_true', help='Start reading from the beginning of the topic')
    parser.add_argument('--timeout', type=float, default=1.0, help='Consumer poll timeout in seconds (default: 1.0)')
    
    # Consumer group browsing option
    parser.add_argument('--browse-group', help='Browse messages from an existing consumer group (without consuming)')
    parser.add_argument('--browse-max-messages', type=int, default=10, help='Maximum messages to browse from group (default: 10)')
    parser.add_argument('--browse-timeout', type=float, default=5.0, help='Browse timeout in seconds (default: 5.0)')
    
    # Consumer group management options
    parser.add_argument('--delete-consumer-group', help='Safely delete a consumer group (only if no active consumers)')
    parser.add_argument('--describe-consumer-group', help='Get detailed information about a specific consumer group')
    parser.add_argument('--create-test-group', help='Create a test consumer group for testing purposes (format: group_name:topic_name)')
    
    # Penetration testing specific flags
    parser.add_argument('--test-permissions', action='store_true', help='Test various permissions (topic creation, deletion, etc.)')
    parser.add_argument('--audit-security', action='store_true', help='Audit security configurations')
    parser.add_argument('--enumerate-sensitive', action='store_true', help='Look for potentially sensitive data in topics')
    parser.add_argument('--test-injection', action='store_true', help='Test ability to inject messages into topics')
    parser.add_argument('--full-security-audit', action='store_true', help='Run all security tests and audits')
    
    # Producer options
    parser.add_argument('--produce', help='Topic to produce messages to')
    parser.add_argument('--key', help='Key for the message (optional)')
    parser.add_argument('--value', help='Value for the message (optional)')
    parser.add_argument('--json-value', help='JSON string to use as message value (optional)')
    
    # New flag
    parser.add_argument('--scan-available-messages', action='store_true', help='Scan all consumer groups and topics for available (unconsumed) messages without consuming them')
    
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Configuration
    bootstrap_servers = args.server
    
    # Validate arguments
    if not args.no_tls and not args.client_cert:
        print("Error: --client-cert is required when not using --no-tls")
        return
    
    # Build configuration based on TLS settings
    if args.no_tls:
        # Plaintext connection without TLS
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'PLAINTEXT',
        }
        print(f"Connecting to {bootstrap_servers} using PLAINTEXT (no TLS)")
    else:
        # TLS/SSL connection
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
        print(f"Connecting to {bootstrap_servers} using SSL/TLS")

    # If subscribing to a topic, set up consumer
    if args.subscribe:
        consumer_conf = conf.copy()
        consumer_conf.update({
            'group.id': args.consumer_group,
            'auto.offset.reset': 'earliest' if args.from_beginning else 'latest',
        })
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([args.subscribe])
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        
        # Start consuming
        subscribe_messages(consumer, args.subscribe, args.max_messages, args.timeout)
        return

    # If subscribing to topics with wildcard, set up consumer
    if args.subscribe_wildcard:
        # Admin client for topic discovery
        admin_client = AdminClient(conf)
        
        # Find topics matching the prefix
        matching_topics = find_topics_by_prefix(admin_client, args.subscribe_wildcard)
        
        if not matching_topics:
            print(f"No topics found starting with prefix '{args.subscribe_wildcard}'")
            return
        
        print(f"Found {len(matching_topics)} topics matching prefix '{args.subscribe_wildcard}':")
        for topic in matching_topics:
            print(f"  - {topic}")
        
        consumer_conf = conf.copy()
        consumer_conf.update({
            'group.id': args.consumer_group,
            'auto.offset.reset': 'earliest' if args.from_beginning else 'latest',
        })
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe(matching_topics)
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        
        # Start consuming
        subscribe_messages(consumer, matching_topics, args.max_messages, args.timeout)
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

    # If producing to a topic, set up producer
    if args.produce:
        producer_conf = conf.copy()
        producer = Producer(producer_conf)
        
        if not (args.value or args.json_value):
            print("Error: Either --value or --json-value must be provided with --produce")
            return
            
        if args.json_value:
            try:
                json_data = json.loads(args.json_value)
            except json.JSONDecodeError as e:
                print(f"Error: Invalid JSON value: {e}")
                return
            success = produce_message(producer, args.produce, args.key, json_value=json_data)
        else:
            success = produce_message(producer, args.produce, args.key, value=args.value)
            
        if success:
            print(f"✓ Successfully produced message to topic '{args.produce}'")
        return

    # Admin client for server information
    admin_client = AdminClient(conf)

    # Get server version and basic metadata
    metadata = None
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
        print("  Some commands may not work due to metadata connection issues")
        print("  This may be due to broker configuration (advertised.listeners)")
        print("  Try fixing broker's server.properties:")
        print("    advertised.listeners=PLAINTEXT://10.0.0.181:9092")

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

    print("\nController ID:", metadata.controller_id if metadata else "Unknown")

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
        test_permissions(admin_client, metadata, conf)
        audit_security_configs(admin_client, metadata)
        enumerate_sensitive_data(admin_client, metadata)
        test_message_injection(admin_client, metadata, conf)
    else:
        if args.test_permissions:
            test_permissions(admin_client, metadata, conf)
        if args.audit_security:
            audit_security_configs(admin_client, metadata)
        if args.enumerate_sensitive:
            enumerate_sensitive_data(admin_client, metadata)
        if args.test_injection:
            test_message_injection(admin_client, metadata, conf)

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
            # Check if we have metadata (needed for ACL operations)
            if metadata is None:
                print(f"\nAccess Control Lists (ACLs):")
                print("  Cannot retrieve ACLs: Metadata connection failed")
                print("  This may be due to broker configuration (advertised.listeners)")
                print("  Try fixing broker's server.properties:")
                print("    advertised.listeners=PLAINTEXT://10.0.0.181:9092")
                return
                
            from confluent_kafka.admin import AclBindingFilter, ResourceType, ResourcePatternType, AclOperation, AclPermissionType
            
            # Create a filter that matches all ACLs
            acl_filter = AclBindingFilter(
                resource_pattern_type=ResourcePatternType.ANY,
                restype=ResourceType.ANY,
                name=None,
                principal=None,
                host=None,
                operation=AclOperation.ANY,
                permission_type=AclPermissionType.ANY
            )
            
            acls_future = admin_client.describe_acls(acl_filter)
            acls_result = acls_future.result()
            print(f"\nAccess Control Lists (ACLs):")
            if acls_result:
                for acl in acls_result:
                    print(f"  - {acl}")
            else:
                print("  No ACLs found or ACLs not enabled")
        except Exception as e:
            print(f"\nCould not fetch ACLs: {e}")
            if "SecurityDisabledException" in str(e):
                print("  Security/Authentication is not enabled on this cluster")
            elif "AuthorizationException" in str(e):
                print("  Not authorized to view ACLs")
            elif "Transport" in str(e) or "Timed out" in str(e):
                print("  Connection issue - check broker configuration")
                print("  Try fixing broker's server.properties:")
                print("    advertised.listeners=PLAINTEXT://10.0.0.181:9092")
            else:
                import traceback
                print(f"Full traceback:")
                traceback.print_exc()

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
            # We can check for SCRAM credentials using describe_acls with a filter
            from confluent_kafka.admin import AclBindingFilter, ResourceType, ResourcePatternType, AclOperation, AclPermissionType
            
            print("\nUser Credentials:")
            try:
                # Create a filter for User resources
                user_filter = AclBindingFilter(
                    ResourceType.USER,  # resource_type
                    None,               # resource_name
                    ResourcePatternType.ANY,  # resource_pattern_type
                    None,               # principal
                    None,               # host
                    AclOperation.ANY,   # operation
                    AclPermissionType.ANY  # permission_type
                )
                
                users_future = admin_client.describe_acls(user_filter)
                users_result = users_future.result()
                if users_result:
                    print("\nAuthorized Users:")
                    # Extract unique principals from ACLs
                    principals = set()
                    for acl in users_result:
                        if acl.principal:
                            principals.add(acl.principal)
                    for principal in sorted(principals):
                        print(f" - {principal}")
                else:
                    print("No user credentials found or access not authorized")
            except Exception as e:
                if "SecurityDisabledException" in str(e) or "USER" in str(e):
                    print("User credentials enumeration is only available when authentication is enabled on the Kafka cluster.")
                elif "AuthorizationException" in str(e):
                    print("Not authorized to view user credentials")
                else:
                    print(f"Error fetching user credentials: {e}")
                    
        except Exception as e:
            print(f"Error checking user credentials: {e}")

    # Show broker configurations if requested
    if args.broker_configs or args.all:
        try:
            broker_configs = [ConfigResource('broker', str(broker.id)) for broker in metadata.brokers.values()]
            broker_configs_result = admin_client.describe_configs(broker_configs)
            print(f"\nBroker Configurations:")
            for broker_id, config in broker_configs_result.items():
                config_dict = config.result() if hasattr(config, 'result') else config
                print(f"  Broker {broker_id}:")
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

    # Consumer group management
    if args.delete_consumer_group:
        safely_delete_consumer_group(admin_client, args.delete_consumer_group)
        return
    
    if args.describe_consumer_group:
        describe_consumer_group_details(admin_client, args.describe_consumer_group)
        return

    if args.create_test_group:
        group_name, topic_name = args.create_test_group.split(':')
        create_test_consumer_group(admin_client, conf, group_name, topic_name)
        return

    if args.scan_available_messages:
        scan_available_messages(conf)
        return

if __name__ == "__main__":
    main() 