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
import datetime
from confluent_kafka.admin import (
    AdminClient, ConfigResource, NewTopic, ConfigEntry, ResourceType,
    NewPartitions, AclBinding, AclBindingFilter, AclOperation, AclPermissionType,
    ResourcePatternType, AlterConfigOpType, OffsetSpec, UserScramCredentialUpsertion,
    UserScramCredentialDeletion, ScramMechanism
)
from confluent_kafka import (
    Consumer, KafkaError, Producer, TopicPartition, SerializingProducer,
    DeserializingConsumer, IsolationLevel, KafkaException
)

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
    """Test various permissions on the Kafka cluster"""
    print("=" * 60)
    print("PERMISSION TESTING")
    print("=" * 60)
    
    # Test topic creation permission
    print("\n1. Testing topic creation permission...")
    test_topic = f"security-test-{int(time.time())}"
    
    try:
        # Try to create a topic
        new_topic = NewTopic(test_topic, num_partitions=1, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        
        for topic, f in fs.items():
            f.result()  # Wait for topic creation
        
        print(f"   [+] SUCCESS: Can create topic '{test_topic}'")
        
        # Try to delete the topic
        try:
            fs = admin_client.delete_topics([test_topic])
            for topic, f in fs.items():
                f.result()  # Wait for topic deletion
            print(f"   [+] SUCCESS: Can delete topic '{test_topic}'")
        except Exception as e:
            print(f"   [!] WARNING: Can create but cannot delete topic '{test_topic}': {e}")
            
    except Exception as e:
        print(f"   [-] FAILED: Cannot create topic '{test_topic}': {e}")
    
    # Test partition management permission
    print("\n2. Testing partition management permission...")
    temp_topic = f"partition-test-{int(time.time())}"
    
    try:
        # Create a topic with 1 partition
        new_topic = NewTopic(temp_topic, num_partitions=1, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            f.result()
        print(f"   [+] Created temporary topic '{temp_topic}' with 1 partition")
        
        # Try to increase partitions
        try:
            fs = admin_client.create_partitions([(temp_topic, 2)])
            for topic, f in fs.items():
                f.result()
            print(f"   [+] SUCCESS: Can create partitions for topic '{temp_topic}' (increased from 1 to 2)")
        except Exception as e:
            print(f"   [-] FAILED: Cannot create partitions: {e}")
        
        # Clean up
        try:
            fs = admin_client.delete_topics([temp_topic])
            for topic, f in fs.items():
                f.result()
            print(f"   [+] Cleaned up temporary topic '{temp_topic}'")
        except Exception as e:
            print(f"   [!] WARNING: Could not delete temporary topic '{temp_topic}': {e}")
            
    except Exception as e:
        print(f"   [-] FAILED: Cannot create partitions: {e}")
        # Try to clean up
        try:
            fs = admin_client.delete_topics([temp_topic])
            for topic, f in fs.items():
                f.result()
            print(f"   [+] Cleaned up temporary topic '{temp_topic}' after failure")
        except:
            pass
    
    # Test configuration management permission
    print("\n3. Testing configuration management permission...")
    temp_topic = f"config-test-{int(time.time())}"
    
    try:
        # Create a topic
        new_topic = NewTopic(temp_topic, num_partitions=1, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            f.result()
        print(f"   [+] Created temporary topic '{temp_topic}' for config testing")
        
        # Try to alter configuration
        try:
            configs = [ConfigEntry("retention.ms", "3600000")]  # 1 hour retention
            fs = admin_client.alter_configs([(temp_topic, configs)])
            for topic, f in fs.items():
                f.result()
            print(f"   [+] SUCCESS: Can alter topic configuration for '{temp_topic}'")
        except Exception as e:
            print(f"   [-] FAILED: Cannot alter topic configuration: {e}")
        
        # Clean up
        try:
            fs = admin_client.delete_topics([temp_topic])
            for topic, f in fs.items():
                f.result()
            print(f"   [+] Cleaned up temporary topic '{temp_topic}'")
        except Exception as e:
            print(f"   [!] WARNING: Could not delete temporary topic '{temp_topic}': {e}")
            
    except Exception as e:
        print(f"   [-] FAILED: Cannot alter topic configuration: {e}")
        # Try to clean up
        try:
            fs = admin_client.delete_topics([temp_topic])
            for topic, f in fs.items():
                f.result()
            print(f"   [+] Cleaned up temporary topic '{temp_topic}' after failure")
        except:
            pass
    
    # Test consumer group management permission
    print("\n4. Testing consumer group management permission...")
    temp_topic = f"consumer-test-{int(time.time())}"
    temp_group = f"test-group-{int(time.time())}"
    
    try:
        # Create a topic
        new_topic = NewTopic(temp_topic, num_partitions=1, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            f.result()
        print(f"   [+] Created temporary topic '{temp_topic}' for consumer group testing")
        
        # Create a consumer and join the group
        consumer_conf = conf.copy()
        consumer_conf.update({
            'group.id': temp_group,
            'auto.offset.reset': 'earliest'
        })
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([temp_topic])
        
        # Wait a bit for group to form
        time.sleep(2)
        
        # Check if group exists
        try:
            group_info = admin_client.list_consumer_groups()
            group_exists = any(g.group_id == temp_group for g in group_info.valid)
            if group_exists:
                print(f"   [+] No messages in topic, but consumer group '{temp_group}' was created")
            else:
                print(f"   [!] WARNING: Consumer group '{temp_group}' was not created")
        except Exception as e:
            print(f"   [!] WARNING: Could not check consumer group: {e}")
        
        # Try to commit offsets
        try:
            consumer.commit()
            print(f"   [+] SUCCESS: Can commit offsets for consumer group '{temp_group}'")
        except Exception as e:
            print(f"   [!] WARNING: Cannot commit offsets: {e}")
        
        # Try to list offsets
        try:
            offsets = admin_client.list_consumer_group_offsets([temp_group])
            if offsets and temp_group in offsets:
                print(f"   [+] SUCCESS: Can list offsets for consumer group '{temp_group}'")
            else:
                print(f"   [!] WARNING: No offsets found for consumer group '{temp_group}'")
        except Exception as e:
            print(f"   [!] WARNING: Cannot list consumer group offsets: {e}")
        
        # Close consumer to leave group
        consumer.close()
        print(f"   [+] Closed consumer to leave group '{temp_group}'")
        
        # Wait for group to become empty
        time.sleep(2)
        
        # Try to delete the consumer group
        try:
            fs = admin_client.delete_consumer_groups([temp_group])
            for group, f in fs.items():
                f.result()
            print(f"   [+] SUCCESS: Can delete consumer group '{temp_group}'")
        except Exception as e:
            print(f"   [!] WARNING: Cannot delete consumer group '{temp_group}': {e}")
        
        # Clean up topic
        try:
            fs = admin_client.delete_topics([temp_topic])
            for topic, f in fs.items():
                f.result()
            print(f"   [+] Cleaned up temporary topic '{temp_topic}'")
        except Exception as e:
            print(f"   [!] WARNING: Could not delete temporary topic '{temp_topic}': {e}")
            
    except Exception as e:
        print(f"   [-] FAILED: Cannot test consumer group permissions: {e}")
        # Try to clean up
        try:
            consumer.close()
        except:
            pass
        try:
            fs = admin_client.delete_topics([temp_topic])
            for topic, f in fs.items():
                f.result()
            print(f"   [+] Cleaned up temporary topic '{temp_topic}' after failure")
        except:
            pass

def audit_security_configs(admin_client, metadata):
    """Audit security configurations of the Kafka cluster"""
    print("=" * 60)
    print("SECURITY CONFIGURATION AUDIT")
    print("=" * 60)
    
    # Check broker security configurations
    print("\n1. Broker Security Configurations:")
    try:
        broker_configs_future = admin_client.describe_configs([ConfigResource(ResourceType.BROKER, 1)])
        print("DEBUG: Called admin_client.describe_configs()")
        print(f"DEBUG: broker_configs_future type: {type(broker_configs_future)}")
        
        broker_configs_result = broker_configs_future[ConfigResource(ResourceType.BROKER, 1)]
        print(f"DEBUG: Got broker_configs_result, type: {type(broker_configs_result)}")
        print(f"DEBUG: broker_configs_result content: {broker_configs_result}")
        
        for broker_id, config in broker_configs_result.items():
            print(f"DEBUG: broker_id={broker_id}, config type={type(config)}")
            print(f"   Broker {broker_id}:")
            
            # Check for SSL/TLS configuration
            listeners = config.get('listeners', None)
            if listeners and 'SSL' in str(listeners.value):
                print(f"     [+] SSL/TLS enabled")
            else:
                print(f"     [!] SSL/TLS not enabled in listeners")
            
            # Check for SASL authentication
            sasl_enabled = config.get('sasl.enabled.mechanisms', None)
            if sasl_enabled and sasl_enabled.value:
                print(f"     [+] SASL authentication enabled")
            else:
                print(f"     [!] SASL authentication not enabled")
            
            # Check for authorization
            authorizer = config.get('authorizer.class.name', None)
            if authorizer and authorizer.value:
                print(f"     [+] Authorization enabled: {authorizer.value}")
            else:
                print(f"     [!] No authorization configured")
                
    except Exception as e:
        print(f"   [-] ERROR: Could not fetch broker configurations: {e}")
    
    # Check topic security configurations
    print("\n2. Topic Security Configurations:")
    try:
        topics = list(metadata.topics.keys())
        if topics:
            sample_topic = topics[0]
            topic_configs = admin_client.describe_configs([ConfigResource(ResourceType.TOPIC, sample_topic)])
            
            for topic, config in topic_configs[sample_topic].items():
                print(f"   Topic: {sample_topic}")
                
                # Check retention settings
                retention_ms = config.get('retention.ms', None)
                if retention_ms and retention_ms.value != '-1':
                    print(f"     [+] Retention configured: {retention_ms.value}ms")
                else:
                    print(f"     [!] No retention limit configured")
                
                # Check cleanup policy
                cleanup_policy = config.get('cleanup.policy', None)
                if cleanup_policy:
                    print(f"     [+] Cleanup policy: {cleanup_policy.value}")
                else:
                    print(f"     [!] No cleanup policy configured")
                break
        else:
            print("   [!] No topics available for security audit")
            
    except Exception as e:
        print(f"   [-] ERROR: Could not fetch topic configurations: {e}")

def enumerate_sensitive_data(admin_client, metadata):
    """Enumerate potentially sensitive data in topics"""
    print("=" * 60)
    print("SENSITIVE DATA ENUMERATION")
    print("=" * 60)
    
    topics = list(metadata.topics.keys())
    print(f"\nScanning {len(topics)} topics for sensitive data patterns...")
    
    sensitive_patterns = [
        'password', 'secret', 'key', 'token', 'credential', 'auth',
        'user', 'admin', 'root', 'private', 'confidential', 'internal'
    ]
    
    sensitive_topics = []
    
    for topic in topics:
        topic_lower = topic.lower()
        for pattern in sensitive_patterns:
            if pattern in topic_lower:
                sensitive_topics.append((topic, pattern))
                break
    
    if sensitive_topics:
        print(f"\n[!] POTENTIALLY SENSITIVE TOPICS FOUND:")
        for topic, pattern in sensitive_topics:
            print(f"   - {topic} (matches pattern: {pattern})")
    else:
        print(f"\n[+] No obviously sensitive topic names found")

def test_message_injection(admin_client, metadata, conf):
    """Test message injection capabilities"""
    print("=" * 60)
    print("MESSAGE INJECTION TESTING")
    print("=" * 60)
    
    # Find a topic to test with
    topics = list(metadata.topics.keys())
    test_topic = None
    
    for topic in topics:
        if not topic.startswith('__'):  # Skip internal topics
            test_topic = topic
            break
    
    if test_topic:
        print(f"\n1. Testing message injection...")
        try:
            # Create a producer
            producer = Producer(conf)
            
            # Try to produce a test message
            test_message = f"Security test message - {int(time.time())}"
            producer.produce(test_topic, test_message.encode('utf-8'))
            producer.flush(timeout=10)
            
            print(f"   [+] SUCCESS: Can inject messages into topic '{test_topic}'")
            
        except Exception as e:
            print(f"   [-] FAILED: Cannot inject messages: {e}")
    else:
        print("   [!] SKIPPED: No topics available for injection testing")

def browse_group(admin_client, group_name, max_messages=10, timeout=30):
    """Browse messages from a consumer group without affecting consumption"""
    print("=" * 60)
    print(f"BROWSING CONSUMER GROUP: {group_name}")
    print("=" * 60)
    
    try:
        # Get group information
        print("\n1. Getting group information...")
        group_info = admin_client.describe_consumer_groups([group_name])
        
        if group_name not in group_info:
            print(f"   [-] ERROR: Consumer group '{group_name}' not found")
            return
        
        group = group_info[group_name]
        print(f"   [+] Group State: {group.state}")
        print(f"   [+] Members: {len(group.members)}")
        print(f"   [+] Protocol: {group.protocol}")
        
        if len(group.members) > 0:
            print(f"   [!] WARNING: Group has active members. Browsing may interfere with consumption.")
        
        # Get committed offsets
        print("\n2. Getting committed offsets...")
        try:
            group_offsets = admin_client.list_consumer_group_offsets([group_name])
            
            if not group_offsets or group_name not in group_offsets:
                print(f"   [-] ERROR: No committed offsets found for group '{group_name}'")
                return
            
            offsets = group_offsets[group_name]
            
            if not offsets:
                print(f"   [!] WARNING: No committed offsets found for group '{group_name}'")
                return
            
            print(f"   [+] Found {len(offsets)} partition assignments")
            
            # Browse messages from each partition
            print(f"\n3. Browsing up to {max_messages} messages per partition...")
            message_count = 0
            
            for tp, offset in offsets.items():
                if message_count >= max_messages:
                    break
                    
                print(f"   Partition {tp.partition}: offset {offset}")
                
                # Create a consumer to read from this specific offset
                consumer_conf = {
                    'bootstrap.servers': admin_client._conf['bootstrap.servers'],
                    'group.id': f"{group_name}-browser-{int(time.time())}",
                    'auto.offset.reset': 'earliest',
                    'enable.auto.commit': False
                }
                
                consumer = Consumer(consumer_conf)
                consumer.assign([tp])
                consumer.seek(tp, offset)
                
                # Read a few messages
                messages_read = 0
                start_time = time.time()
                
                while messages_read < 3 and (time.time() - start_time) < timeout:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        break
                    if msg.error():
                        break
                    
                    print(f"     Message {messages_read + 1}: {msg.value().decode('utf-8', errors='ignore')[:100]}...")
                    messages_read += 1
                    message_count += 1
                
                consumer.close()
            
            print(f"\n   [+] Browsed {message_count} messages from group '{group_name}'")
            print(f"   [+] No offsets were committed (safe browsing)")
            print(f"   [+] Original group '{group_name}' was not affected")
            
        except Exception as e:
            print(f"\n   [-] ERROR browsing group '{group_name}': {e}")
            
    except Exception as e:
        print(f"\n   [-] ERROR browsing group '{group_name}': {e}")

def describe_consumer_group_details(admin_client, group_name):
    """Get detailed information about a specific consumer group"""
    print("=" * 60)
    print(f"CONSUMER GROUP DETAILS: {group_name}")
    print("=" * 60)
    
    try:
        print("\n1. Getting group information...")
        group_info = admin_client.describe_consumer_groups([group_name])
        
        if group_name not in group_info:
            print(f"   [-] ERROR: Consumer group '{group_name}' not found")
            return
        
        group = group_info[group_name]
        print(f"   [+] Group ID: {group_name}")
        print(f"   [+] State: {group.state}")
        print(f"   [+] Protocol Type: {getattr(group, 'protocol_type', 'Unknown')}")
        print(f"   [+] Protocol: {getattr(group, 'protocol', 'Unknown')}")
        print(f"   [+] Members: {len(group.members)}")
        
        if group.members:
            print(f"\n2. Member Details:")
            for i, member in enumerate(group.members, 1):
                print(f"   Member {i}:")
                print(f"     [+] Member ID: {member.member_id}")
                print(f"     [+] Client ID: {getattr(member, 'client_id', 'Unknown')}")
                print(f"     [+] Client Host: {getattr(member, 'client_host', 'Unknown')}")
                print(f"     [+] Session Timeout: {getattr(member, 'session_timeout_ms', 'Unknown')}ms")
                print(f"     [+] Rebalance Timeout: {getattr(member, 'rebalance_timeout_ms', 'Unknown')}ms")
                
                if hasattr(member, 'member_metadata') and member.member_metadata:
                    print(f"     [+] Member Metadata: {len(member.member_metadata)} bytes")
                
                if hasattr(member, 'member_assignment') and member.member_assignment:
                    print(f"     [+] Partition Assignments:")
                    # Parse assignment data (this is a simplified version)
                    print(f"       - Assignment data: {len(member.member_assignment)} bytes")
                else:
                    print(f"     [+] No partition assignments")
        else:
            print(f"   [!] No active members in this consumer group")
        
        # Get offset information
        print(f"\n3. Offset Information:")
        try:
            group_offsets = admin_client.list_consumer_group_offsets([group_name])
            
            if group_offsets and group_name in group_offsets:
                offsets = group_offsets[group_name]
                if offsets:
                    print(f"   [+] Found {len(offsets)} partition assignments with offsets:")
                    for tp, offset in list(offsets.items())[:5]:  # Show first 5
                        print(f"     - {tp.topic}[{tp.partition}]: offset {offset}")
                    if len(offsets) > 5:
                        print(f"     ... and {len(offsets) - 5} more")
                else:
                    print(f"   [!] No committed offsets found")
            else:
                print(f"   [!] No offset information available")
                
        except Exception as e:
            print(f"   [!] Could not retrieve offset information: {e}")
        
        # Group health assessment
        print(f"\n4. Group Health Assessment:")
        if group.state == 'Stable' and group.members:
            print(f"   [+] Group is healthy and active")
            print(f"   [+] {len(group.members)} consumer(s) are processing messages")
        elif group.state == 'Empty':
            print(f"   [!] Group is empty (no active consumers)")
        elif group.state == 'PreparingRebalance':
            print(f"   [!] Group is rebalancing (preparing)")
        elif group.state == 'CompletingRebalance':
            print(f"   [!] Group is rebalancing (completing)")
        elif group.state == 'Dead':
            print(f"   [-] Group is dead (no members)")
        else:
            print(f"   [!] Group state: {group.state}")
            
    except Exception as e:
        print(f"\n   [-] ERROR: Consumer group '{group_name}' not found")
    except Exception as e:
        print(f"\n   [-] ERROR: Could not describe consumer group '{group_name}': {e}")

def delete_consumer_group(admin_client, group_name):
    """Delete a consumer group"""
    print("=" * 60)
    print(f"DELETING CONSUMER GROUP: {group_name}")
    print("=" * 60)
    
    try:
        # Get detailed group info first
        print("\n1. Checking group status...")
        try:
            group_info = admin_client.describe_consumer_groups([group_name])
            if group_name in group_info:
                group = group_info[group_name]
                print(f"   [+] Group State: {group.state}")
                print(f"   [+] Members: {len(group.members)}")
                
                if len(group.members) > 0:
                    print(f"   [-] CANNOT DELETE: Group has {len(group.members)} active member(s)")
                    print(f"   [!] WARNING: Deleting a group with active consumers can cause data loss")
                    return False
                else:
                    print(f"   [+] Group is empty - safe to delete")
            else:
                print(f"   [!] WARNING: Could not get detailed group info: {e}")
        except Exception as e:
            print(f"   [!] WARNING: Could not get detailed group info: {e}")
        
        # Delete the group
        print("\n2. Deleting consumer group...")
        fs = admin_client.delete_consumer_groups([group_name])
        
        for group, f in fs.items():
            f.result()  # Wait for deletion
        
        print(f"   [+] SUCCESS: Consumer group '{group_name}' deleted successfully")
        return True
        
    except Exception as e:
        if "not empty" in str(e).lower():
            print(f"   [-] FAILED: Group is not empty - cannot delete")
        elif "not found" in str(e).lower():
            print(f"   [-] FAILED: Consumer group '{group_name}' not found")
        else:
            print(f"   [-] FAILED: Could not delete consumer group: {e}")
        return False

def create_topic(admin_client, topic_spec):
    """Create a new topic"""
    print("=" * 60)
    print("CREATING TOPIC")
    print("=" * 60)
    
    try:
        # Parse topic specification
        parts = topic_spec.split(':')
        if len(parts) != 3:
            print("   [-] ERROR: Topic specification must be in format: topic_name:partitions:replication_factor")
            return False
        
        topic_name, partitions_str, replication_factor_str = parts
        
        try:
            partitions = int(partitions_str)
            replication_factor = int(replication_factor_str)
        except ValueError:
            print("   [-] ERROR: Partitions and replication factor must be integers")
            return False
        
        print(f"   Topic Name: {topic_name}")
        print(f"   Partitions: {partitions}")
        print(f"   Replication Factor: {replication_factor}")
        
        # Create the topic
        new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=replication_factor)
        fs = admin_client.create_topics([new_topic])
        
        for topic, f in fs.items():
            f.result()  # Wait for topic creation
        
        print(f"   [+] SUCCESS: Topic '{topic_name}' created successfully")
        return True
        
    except Exception as e:
        if "TOPIC_ALREADY_EXISTS" in str(e):
            print(f"   [-] ERROR: Topic '{topic_name}' already exists")
        else:
            print(f"   [-] ERROR: Could not create topic: {e}")
        return False

def delete_topic(admin_client, topic_name):
    """Delete a topic"""
    print("=" * 60)
    print("DELETING TOPIC")
    print("=" * 60)
    
    try:
        print(f"   Topic Name: {topic_name}")
        
        # Check if topic exists first
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            print(f"   [-] ERROR: Topic '{topic_name}' does not exist")
            return False
        
        # Delete the topic
        fs = admin_client.delete_topics([topic_name])
        
        for topic, f in fs.items():
            f.result()  # Wait for topic deletion
        
        print(f"   [+] SUCCESS: Topic '{topic_name}' deleted successfully")
        return True
        
    except Exception as e:
        if "TOPIC_ALREADY_EXISTS" in str(e):
            print(f"   [-] ERROR: Topic '{topic_name}' does not exist")
        elif "NON_EMPTY_TOPIC" in str(e):
            print(f"   [-] ERROR: Topic '{topic_name}' is not empty and cannot be deleted")
        else:
            print(f"   [-] ERROR: Could not delete topic: {e}")
        return False

def create_consumer_group(admin_client, conf, group_spec):
    """Create a consumer group by subscribing to a topic"""
    print("=" * 60)
    print("CREATING CONSUMER GROUP")
    print("=" * 60)
    
    try:
        # Parse group specification: group_name:topic_name
        parts = group_spec.split(':')
        if len(parts) != 2:
            print(f"   [-] ERROR: Invalid group specification. Use format: group_name:topic_name")
            return False
        
        group_name, topic_name = parts
        
        print(f"   Group Name: {group_name}")
        print(f"   Topic Name: {topic_name}")
        
        # Create consumer configuration
        consumer_conf = conf.copy()
        consumer_conf.update({
            'group.id': group_name,
            'auto.offset.reset': 'earliest'
        })
        
        # Create consumer and subscribe
        consumer = Consumer(consumer_conf)
        consumer.subscribe([topic_name])
        
        # Wait a bit for group to form
        time.sleep(2)
        
        # Check if group was created
        try:
            groups_future = admin_client.list_consumer_groups()
            groups_result = groups_future.result()
            group_exists = any(g.group_id == group_name for g in groups_result.valid)
            
            if group_exists:
                print(f"   [+] No messages in topic, but consumer group '{group_name}' was created")
            else:
                print(f"   [!] WARNING: Consumer group '{group_name}' was not created")
                
        except Exception as e:
            print(f"   [!] WARNING: Could not verify group creation: {e}")
        
        # Close consumer
        consumer.close()
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not create consumer group: {e}")
        return False

def produce_message(producer, topic, key=None, value=None, json_value=None):
    """Produce a message to a topic"""
    try:
        if json_value:
            # Send JSON message
            producer.produce(topic, json_value.encode('utf-8'), key=key.encode('utf-8') if key else None,
                           callback=lambda err, msg: print(f"[+] Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                           if err is None else print(f"[-] Failed to deliver message: {err}"))
        else:
            # Send regular message
            producer.produce(topic, value.encode('utf-8'), key=key.encode('utf-8') if key else None,
                           callback=lambda err, msg: print(f"[+] Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                           if err is None else print(f"[-] Failed to deliver message: {err}"))
        
        producer.flush(timeout=10)
        return True
        
    except Exception as e:
        print(f"[-] Error producing message: {e}")
        return False

def scan_available_messages(conf):
    """Scan for available messages in topics"""
    print("=" * 60)
    print("SCANNING FOR AVAILABLE MESSAGES")
    print("=" * 60)
    
    try:
        # Get list of consumer groups
        admin_client = AdminClient(conf)
        groups_future = admin_client.list_consumer_groups()
        groups_result = groups_future.result()
        
        print(f"\nFound {len(groups_result.valid)} consumer groups:")
        for group in groups_result.valid:
            print(f"  - {group.group_id}")
        
        # Check each group for available messages
        print(f"\nChecking for available messages...")
        available_count = 0
        
        for group in groups_result.valid:
            try:
                offsets = admin_client.list_consumer_group_offsets([group.group_id])
                if offsets and group.group_id in offsets:
                    group_offsets = offsets[group.group_id]
                    if group_offsets:
                        print(f"  [+] Group '{group.group_id}' has {len(group_offsets)} partition assignments")
                        available_count += len(group_offsets)
                    else:
                        print(f"  [!] Group '{group.group_id}' has no committed offsets")
                else:
                    print(f"  [!] Group '{group.group_id}' has no offset information")
                    
            except Exception as e:
                print(f"  [-] Could not get offsets for group {group.group_id}: {e}")
        
        print(f"\n[+] Scan complete. {available_count} topic-partitions have available messages for their groups.")
        
    except Exception as e:
        print(f"[-] ERROR: {e}")

def write_message_to_topic(admin_client, conf, topic_spec):
    """Write a message from file to a topic"""
    print("=" * 60)
    print("WRITING MESSAGE TO TOPIC")
    print("=" * 60)
    
    try:
        # Parse topic specification: topic_name:file_path
        parts = topic_spec.split(':')
        if len(parts) != 2:
            print(f"   [-] ERROR: Invalid topic specification. Use format: topic_name:file_path")
            return False
            
        topic_name, file_path = parts
        
        print(f"   Topic Name: {topic_name}")
        print(f"   File Path: {file_path}")
        
        # Read message from file
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                message_content = f.read().strip()
            print(f"   Message Length: {len(message_content)} characters")
        except FileNotFoundError:
            print(f"   [-] ERROR: File '{file_path}' not found")
            return False
        except Exception as e:
            print(f"   [-] ERROR: Could not read file '{file_path}': {e}")
            return False
        
        # Create producer and send message
        producer = Producer(conf)
        
        # Send the message
        producer.produce(topic_name, value=message_content.encode('utf-8'))
        producer.flush(timeout=10)
        
        print(f"   [+] SUCCESS: Message written to topic '{topic_name}'")
        print(f"   [TIP] Use --subscribe {topic_name} to read the message")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not write message to topic: {e}")
        return False

def write_message_to_group(admin_client, conf, group_spec):
    """Write a message from file to a consumer group's topic"""
    print("=" * 60)
    print("WRITING MESSAGE TO CONSUMER GROUP")
    print("=" * 60)
    
    try:
        # Parse group specification: group_name:topic_name:file_path
        parts = group_spec.split(':')
        if len(parts) != 3:
            print(f"   [-] ERROR: Invalid group specification. Use format: group_name:topic_name:file_path")
            return False
            
        group_name, topic_name, file_path = parts
        
        print(f"   Group Name: {group_name}")
        print(f"   Topic Name: {topic_name}")
        print(f"   File Path: {file_path}")
        
        # Read message from file
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                message_content = f.read().strip()
            print(f"   Message Length: {len(message_content)} characters")
        except FileNotFoundError:
            print(f"   [-] ERROR: File '{file_path}' not found")
            return False
        except Exception as e:
            print(f"   [-] ERROR: Could not read file '{file_path}': {e}")
            return False
        
        # Create producer and send message
        producer = Producer(conf)
        
        # Send the message
        producer.produce(topic_name, value=message_content.encode('utf-8'))
        producer.flush(timeout=10)
        
        print(f"   [+] SUCCESS: Message written to topic '{topic_name}' for group '{group_name}'")
        print(f"   [TIP] Use --browse-group {group_name} to read the message")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not write message to group: {e}")
        return False

def create_acl(admin_client, acl_spec):
    """Create an ACL (Access Control List) entry"""
    print("=" * 60)
    print("CREATING ACL")
    print("=" * 60)
    
    try:
        # Parse ACL specification: resource_type:resource_name:principal:operation:permission
        parts = acl_spec.split(':')
        if len(parts) != 5:
            print("   [-] ERROR: ACL specification must be in format: resource_type:resource_name:principal:operation:permission")
            print("   Examples:")
            print("     topic:my-topic:User:alice:ALLOW:READ")
            print("     group:my-group:User:bob:ALLOW:DESCRIBE")
            return False
        
        resource_type_str, resource_name, principal, operation_str, permission_str = parts
        
        # Map resource type
        resource_type_map = {
            'topic': ResourceType.TOPIC,
            'group': ResourceType.GROUP,
            'broker': ResourceType.BROKER,
            'cluster': ResourceType.CLUSTER,
            'transactional_id': ResourceType.TRANSACTIONAL_ID
        }
        
        if resource_type_str not in resource_type_map:
            print(f"   [-] ERROR: Invalid resource type '{resource_type_str}'. Valid types: {list(resource_type_map.keys())}")
            return False
        
        resource_type = resource_type_map[resource_type_str]
        
        # Map operation
        operation_map = {
            'ALL': AclOperation.ALL,
            'READ': AclOperation.READ,
            'WRITE': AclOperation.WRITE,
            'CREATE': AclOperation.CREATE,
            'DELETE': AclOperation.DELETE,
            'ALTER': AclOperation.ALTER,
            'DESCRIBE': AclOperation.DESCRIBE,
            'CLUSTER_ACTION': AclOperation.CLUSTER_ACTION,
            'DESCRIBE_CONFIGS': AclOperation.DESCRIBE_CONFIGS,
            'ALTER_CONFIGS': AclOperation.ALTER_CONFIGS,
            'IDEMPOTENT_WRITE': AclOperation.IDEMPOTENT_WRITE
        }
        
        if operation_str not in operation_map:
            print(f"   [-] ERROR: Invalid operation '{operation_str}'. Valid operations: {list(operation_map.keys())}")
            return False
        
        operation = operation_map[operation_str]
        
        # Map permission
        permission_map = {
            'ALLOW': AclPermissionType.ALLOW,
            'DENY': AclPermissionType.DENY
        }
        
        if permission_str not in permission_map:
            print(f"   [-] ERROR: Invalid permission '{permission_str}'. Valid permissions: {list(permission_map.keys())}")
            return False
        
        permission = permission_map[permission_str]
        
        print(f"   Resource Type: {resource_type_str}")
        print(f"   Resource Name: {resource_name}")
        print(f"   Principal: {principal}")
        print(f"   Operation: {operation_str}")
        print(f"   Permission: {permission_str}")
        
        # Create ACL binding
        acl_binding = AclBinding(
            restype=resource_type,
            name=resource_name,
            resource_pattern_type=ResourcePatternType.LITERAL,
            principal=principal,
            host='*',
            operation=operation,
            permission_type=permission
        )
        
        # Create ACL
        fs = admin_client.create_acls([acl_binding])
        
        for acl, f in fs.items():
            f.result()  # Wait for ACL creation
        
        print(f"   [+] SUCCESS: ACL created successfully")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not create ACL: {e}")
        return False

def delete_acl(admin_client, acl_spec):
    """Delete an ACL (Access Control List) entry"""
    print("=" * 60)
    print("DELETING ACL")
    print("=" * 60)
    
    try:
        # Parse ACL specification: resource_type:resource_name:principal:operation:permission
        parts = acl_spec.split(':')
        if len(parts) != 5:
            print("   [-] ERROR: ACL specification must be in format: resource_type:resource_name:principal:operation:permission")
            return False
        
        resource_type_str, resource_name, principal, operation_str, permission_str = parts
        
        # Map resource type
        resource_type_map = {
            'topic': ResourceType.TOPIC,
            'group': ResourceType.GROUP,
            'broker': ResourceType.BROKER,
            'cluster': ResourceType.CLUSTER,
            'transactional_id': ResourceType.TRANSACTIONAL_ID
        }
        
        if resource_type_str not in resource_type_map:
            print(f"   [-] ERROR: Invalid resource type '{resource_type_str}'")
            return False
        
        resource_type = resource_type_map[resource_type_str]
        
        # Map operation
        operation_map = {
            'ALL': AclOperation.ALL,
            'READ': AclOperation.READ,
            'WRITE': AclOperation.WRITE,
            'CREATE': AclOperation.CREATE,
            'DELETE': AclOperation.DELETE,
            'ALTER': AclOperation.ALTER,
            'DESCRIBE': AclOperation.DESCRIBE,
            'CLUSTER_ACTION': AclOperation.CLUSTER_ACTION,
            'DESCRIBE_CONFIGS': AclOperation.DESCRIBE_CONFIGS,
            'ALTER_CONFIGS': AclOperation.ALTER_CONFIGS,
            'IDEMPOTENT_WRITE': AclOperation.IDEMPOTENT_WRITE
        }
        
        if operation_str not in operation_map:
            print(f"   [-] ERROR: Invalid operation '{operation_str}'")
            return False
        
        operation = operation_map[operation_str]
        
        # Map permission
        permission_map = {
            'ALLOW': AclPermissionType.ALLOW,
            'DENY': AclPermissionType.DENY
        }
        
        if permission_str not in permission_map:
            print(f"   [-] ERROR: Invalid permission '{permission_str}'")
            return False
        
        permission = permission_map[permission_str]
        
        print(f"   Resource Type: {resource_type_str}")
        print(f"   Resource Name: {resource_name}")
        print(f"   Principal: {principal}")
        print(f"   Operation: {operation_str}")
        print(f"   Permission: {permission_str}")
        
        # Create ACL filter
        acl_filter = AclBindingFilter(
            restype=resource_type,
            name=resource_name,
            resource_pattern_type=ResourcePatternType.LITERAL,
            principal=principal,
            host='*',
            operation=operation,
            permission_type=permission
        )
        
        # Delete ACL
        fs = admin_client.delete_acls([acl_filter])
        
        for acl, f in fs.items():
            f.result()  # Wait for ACL deletion
        
        print(f"   [+] SUCCESS: ACL deleted successfully")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not delete ACL: {e}")
        return False

def add_partitions(admin_client, topic_spec):
    """Add partitions to an existing topic"""
    print("=" * 60)
    print("ADDING PARTITIONS")
    print("=" * 60)
    
    try:
        # Parse topic specification: topic_name:new_partition_count
        parts = topic_spec.split(':')
        if len(parts) != 2:
            print("   [-] ERROR: Topic specification must be in format: topic_name:new_partition_count")
            return False
        
        topic_name, new_partitions_str = parts
        
        try:
            new_partitions = int(new_partitions_str)
        except ValueError:
            print("   [-] ERROR: New partition count must be an integer")
            return False
        
        print(f"   Topic Name: {topic_name}")
        print(f"   New Partition Count: {new_partitions}")
        
        # Check if topic exists and get current partition count
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            print(f"   [-] ERROR: Topic '{topic_name}' does not exist")
            return False
        
        current_partitions = len(metadata.topics[topic_name].partitions)
        print(f"   Current Partitions: {current_partitions}")
        
        if new_partitions <= current_partitions:
            print(f"   [-] ERROR: New partition count ({new_partitions}) must be greater than current count ({current_partitions})")
            return False
        
        # Create new partitions
        new_partitions_obj = NewPartitions(topic_name, new_partitions)
        fs = admin_client.create_partitions([new_partitions_obj])
        
        for topic, f in fs.items():
            f.result()  # Wait for partition creation
        
        print(f"   [+] SUCCESS: Added partitions to topic '{topic_name}' (now {new_partitions} partitions)")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not add partitions: {e}")
        return False

def alter_topic_config(admin_client, config_spec):
    """Alter topic configuration"""
    print("=" * 60)
    print("ALTERING TOPIC CONFIGURATION")
    print("=" * 60)
    
    try:
        # Parse config specification: topic_name:config_key=value
        if ':' not in config_spec or '=' not in config_spec:
            print("   [-] ERROR: Config specification must be in format: topic_name:config_key=value")
            return False
        
        topic_part, config_part = config_spec.split(':', 1)
        if '=' not in config_part:
            print("   [-] ERROR: Config specification must be in format: topic_name:config_key=value")
            return False
        
        topic_name, config_key_value = topic_part, config_part
        config_key, config_value = config_key_value.split('=', 1)
        
        print(f"   Topic Name: {topic_name}")
        print(f"   Config Key: {config_key}")
        print(f"   Config Value: {config_value}")
        
        # Check if topic exists
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            print(f"   [-] ERROR: Topic '{topic_name}' does not exist")
            return False
        
        # Create config resource
        config_resource = ConfigResource(ResourceType.TOPIC, topic_name)
        
        # Create config entry
        config_entry = ConfigEntry(config_key, config_value)
        
        # Alter config
        fs = admin_client.alter_configs([config_resource])
        
        for resource, f in fs.items():
            f.result()  # Wait for config alteration
        
        print(f"   [+] SUCCESS: Configuration altered for topic '{topic_name}'")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not alter topic configuration: {e}")
        return False

def delete_records(admin_client, delete_spec):
    """Delete records from a topic (truncate to specific offsets)"""
    print("=" * 60)
    print("DELETING RECORDS")
    print("=" * 60)
    
    try:
        # Parse delete specification: topic_name:partition:offset
        parts = delete_spec.split(':')
        if len(parts) != 3:
            print("   [-] ERROR: Delete specification must be in format: topic_name:partition:offset")
            return False
        
        topic_name, partition_str, offset_str = parts
        
        try:
            partition = int(partition_str)
            offset = int(offset_str)
        except ValueError:
            print("   [-] ERROR: Partition and offset must be integers")
            return False
        
        print(f"   Topic Name: {topic_name}")
        print(f"   Partition: {partition}")
        print(f"   Offset: {offset}")
        
        # Check if topic exists
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            print(f"   [-] ERROR: Topic '{topic_name}' does not exist")
            return False
        
        if partition not in metadata.topics[topic_name].partitions:
            print(f"   [-] ERROR: Partition {partition} does not exist in topic '{topic_name}'")
            return False
        
        # Create topic partition with offset
        tp = TopicPartition(topic_name, partition, offset)
        
        # Delete records
        fs = admin_client.delete_records([tp])
        
        for tp, f in fs.items():
            f.result()  # Wait for record deletion
        
        print(f"   [+] SUCCESS: Records deleted from topic '{topic_name}' partition {partition} up to offset {offset}")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not delete records: {e}")
        return False

def elect_leaders(admin_client, election_spec):
    """Trigger leader election for specific partitions"""
    print("=" * 60)
    print("ELECTING LEADERS")
    print("=" * 60)
    
    try:
        # Parse election specification: topic_name:partition
        parts = election_spec.split(':')
        if len(parts) != 2:
            print("   [-] ERROR: Election specification must be in format: topic_name:partition")
            return False
        
        topic_name, partition_str = parts
        
        try:
            partition = int(partition_str)
        except ValueError:
            print("   [-] ERROR: Partition must be an integer")
            return False
        
        print(f"   Topic Name: {topic_name}")
        print(f"   Partition: {partition}")
        
        # Check if topic exists
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            print(f"   [-] ERROR: Topic '{topic_name}' does not exist")
            return False
        
        if partition not in metadata.topics[topic_name].partitions:
            print(f"   [-] ERROR: Partition {partition} does not exist in topic '{topic_name}'")
            return False
        
        # Create topic partition
        tp = TopicPartition(topic_name, partition)
        
        # Elect leader
        fs = admin_client.elect_leaders([tp])
        
        for tp, f in fs.items():
            f.result()  # Wait for leader election
        
        print(f"   [+] SUCCESS: Leader election triggered for topic '{topic_name}' partition {partition}")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not elect leader: {e}")
        return False

def alter_consumer_group_offsets(admin_client, offset_spec):
    """Alter consumer group offsets"""
    print("=" * 60)
    print("ALTERING CONSUMER GROUP OFFSETS")
    print("=" * 60)
    
    try:
        # Parse offset specification: group_name:topic:partition:offset
        parts = offset_spec.split(':')
        if len(parts) != 4:
            print("   [-] ERROR: Offset specification must be in format: group_name:topic:partition:offset")
            return False
        
        group_name, topic_name, partition_str, offset_str = parts
        
        try:
            partition = int(partition_str)
            offset = int(offset_str)
        except ValueError:
            print("   [-] ERROR: Partition and offset must be integers")
            return False
        
        print(f"   Group Name: {group_name}")
        print(f"   Topic Name: {topic_name}")
        print(f"   Partition: {partition}")
        print(f"   New Offset: {offset}")
        
        # Create topic partition with offset
        tp = TopicPartition(topic_name, partition, offset)
        
        # Alter offsets
        fs = admin_client.alter_consumer_group_offsets([group_name], [tp])
        
        for group, f in fs.items():
            f.result()  # Wait for offset alteration
        
        print(f"   [+] SUCCESS: Consumer group '{group_name}' offset altered for topic '{topic_name}' partition {partition}")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not alter consumer group offsets: {e}")
        return False

def alter_user_scram_credentials(admin_client, credential_spec):
    """Alter user SCRAM credentials"""
    print("=" * 60)
    print("ALTERING USER SCRAM CREDENTIALS")
    print("=" * 60)
    
    try:
        # Parse credential specification: username:password:mechanism
        parts = credential_spec.split(':')
        if len(parts) != 3:
            print("   [-] ERROR: Credential specification must be in format: username:password:mechanism")
            print("   Valid mechanisms: SCRAM-SHA-256, SCRAM-SHA-512")
            return False
        
        username, password, mechanism_str = parts
        
        # Map mechanism
        mechanism_map = {
            'SCRAM-SHA-256': ScramMechanism.SCRAM_SHA_256,
            'SCRAM-SHA-512': ScramMechanism.SCRAM_SHA_512
        }
        
        if mechanism_str not in mechanism_map:
            print(f"   [-] ERROR: Invalid mechanism '{mechanism_str}'. Valid mechanisms: {list(mechanism_map.keys())}")
            return False
        
        mechanism = mechanism_map[mechanism_str]
        
        print(f"   Username: {username}")
        print(f"   Mechanism: {mechanism_str}")
        print(f"   Password: {'*' * len(password)}")
        
        # Create credential upsertion
        credential_upsertion = UserScramCredentialUpsertion(
            username=username,
            password=password,
            mechanism=mechanism
        )
        
        # Alter credentials
        fs = admin_client.alter_user_scram_credentials([credential_upsertion])
        
        for credential, f in fs.items():
            f.result()  # Wait for credential alteration
        
        print(f"   [+] SUCCESS: SCRAM credentials altered for user '{username}'")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not alter user SCRAM credentials: {e}")
        return False

def delete_user_scram_credentials(admin_client, credential_spec):
    """Delete user SCRAM credentials"""
    print("=" * 60)
    print("DELETING USER SCRAM CREDENTIALS")
    print("=" * 60)
    
    try:
        # Parse credential specification: username:mechanism
        parts = credential_spec.split(':')
        if len(parts) != 2:
            print("   [-] ERROR: Credential specification must be in format: username:mechanism")
            print("   Valid mechanisms: SCRAM-SHA-256, SCRAM-SHA-512")
            return False
        
        username, mechanism_str = parts
        
        # Map mechanism
        mechanism_map = {
            'SCRAM-SHA-256': ScramMechanism.SCRAM_SHA_256,
            'SCRAM-SHA-512': ScramMechanism.SCRAM_SHA_512
        }
        
        if mechanism_str not in mechanism_map:
            print(f"   [-] ERROR: Invalid mechanism '{mechanism_str}'. Valid mechanisms: {list(mechanism_map.keys())}")
            return False
        
        mechanism = mechanism_map[mechanism_str]
        
        print(f"   Username: {username}")
        print(f"   Mechanism: {mechanism_str}")
        
        # Create credential deletion
        credential_deletion = UserScramCredentialDeletion(
            username=username,
            mechanism=mechanism
        )
        
        # Delete credentials
        fs = admin_client.alter_user_scram_credentials([credential_deletion])
        
        for credential, f in fs.items():
            f.result()  # Wait for credential deletion
        
        print(f"   [+] SUCCESS: SCRAM credentials deleted for user '{username}'")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not delete user SCRAM credentials: {e}")
        return False

def describe_user_scram_credentials(admin_client, username=None):
    """Describe user SCRAM credentials"""
    print("=" * 60)
    print("DESCRIBING USER SCRAM CREDENTIALS")
    print("=" * 60)
    
    try:
        if username:
            print(f"   Username: {username}")
        else:
            print("   All Users")
        
        # Describe credentials
        fs = admin_client.describe_user_scram_credentials([username] if username else [])
        
        for user, f in fs.items():
            result = f.result()
            print(f"\n   User: {user}")
            if hasattr(result, 'mechanisms'):
                for mechanism in result.mechanisms:
                    print(f"     Mechanism: {mechanism}")
            else:
                print("     No credentials found")
        
        print(f"\n   [+] SUCCESS: SCRAM credentials described")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not describe user SCRAM credentials: {e}")
        return False

def seek_to_offset(consumer, seek_spec):
    """Seek to specific offset in a topic partition"""
    print("=" * 60)
    print("SEEKING TO OFFSET")
    print("=" * 60)
    
    try:
        # Parse seek specification: topic:partition:offset
        parts = seek_spec.split(':')
        if len(parts) != 3:
            print("   [-] ERROR: Seek specification must be in format: topic:partition:offset")
            return False
        
        topic_name, partition_str, offset_str = parts
        
        try:
            partition = int(partition_str)
            offset = int(offset_str)
        except ValueError:
            print("   [-] ERROR: Partition and offset must be integers")
            return False
        
        print(f"   Topic Name: {topic_name}")
        print(f"   Partition: {partition}")
        print(f"   Offset: {offset}")
        
        # Create topic partition
        tp = TopicPartition(topic_name, partition, offset)
        
        # Seek to offset
        consumer.seek(tp)
        
        print(f"   [+] SUCCESS: Consumer seeked to offset {offset} in topic '{topic_name}' partition {partition}")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not seek to offset: {e}")
        return False

def seek_to_timestamp(consumer, seek_spec):
    """Seek to specific timestamp in a topic partition"""
    print("=" * 60)
    print("SEEKING TO TIMESTAMP")
    print("=" * 60)
    
    try:
        # Parse seek specification: topic:partition:timestamp
        parts = seek_spec.split(':')
        if len(parts) != 3:
            print("   [-] ERROR: Seek specification must be in format: topic:partition:timestamp")
            return False
        
        topic_name, partition_str, timestamp_str = parts
        
        try:
            partition = int(partition_str)
            timestamp = int(timestamp_str)
        except ValueError:
            print("   [-] ERROR: Partition and timestamp must be integers")
            return False
        
        print(f"   Topic Name: {topic_name}")
        print(f"   Partition: {partition}")
        print(f"   Timestamp: {timestamp}")
        
        # Create topic partition
        tp = TopicPartition(topic_name, partition)
        
        # Get offsets for timestamp
        offsets = consumer.offsets_for_times([tp])
        
        if tp in offsets and offsets[tp].offset >= 0:
            offset = offsets[tp].offset
            consumer.seek(tp)
            print(f"   [+] SUCCESS: Consumer seeked to timestamp {timestamp} (offset {offset}) in topic '{topic_name}' partition {partition}")
            return True
        else:
            print(f"   [-] ERROR: No offset found for timestamp {timestamp}")
            return False
        
    except Exception as e:
        print(f"   [-] ERROR: Could not seek to timestamp: {e}")
        return False

def pause_partitions(consumer, partition_spec):
    """Pause consumption from specific partitions"""
    print("=" * 60)
    print("PAUSING PARTITIONS")
    print("=" * 60)
    
    try:
        # Parse partition specification: topic:partition_list (comma-separated)
        parts = partition_spec.split(':')
        if len(parts) != 2:
            print("   [-] ERROR: Partition specification must be in format: topic:partition_list")
            print("   Example: my-topic:0,1,2")
            return False
        
        topic_name, partition_list_str = parts
        
        try:
            partition_list = [int(p.strip()) for p in partition_list_str.split(',')]
        except ValueError:
            print("   [-] ERROR: Partition list must contain integers separated by commas")
            return False
        
        print(f"   Topic Name: {topic_name}")
        print(f"   Partitions: {partition_list}")
        
        # Create topic partitions
        tps = [TopicPartition(topic_name, p) for p in partition_list]
        
        # Pause partitions
        consumer.pause(tps)
        
        print(f"   [+] SUCCESS: Partitions {partition_list} paused for topic '{topic_name}'")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not pause partitions: {e}")
        return False

def resume_partitions(consumer, partition_spec):
    """Resume consumption from specific partitions"""
    print("=" * 60)
    print("RESUMING PARTITIONS")
    print("=" * 60)
    
    try:
        # Parse partition specification: topic:partition_list (comma-separated)
        parts = partition_spec.split(':')
        if len(parts) != 2:
            print("   [-] ERROR: Partition specification must be in format: topic:partition_list")
            print("   Example: my-topic:0,1,2")
            return False
        
        topic_name, partition_list_str = parts
        
        try:
            partition_list = [int(p.strip()) for p in partition_list_str.split(',')]
        except ValueError:
            print("   [-] ERROR: Partition list must contain integers separated by commas")
            return False
        
        print(f"   Topic Name: {topic_name}")
        print(f"   Partitions: {partition_list}")
        
        # Create topic partitions
        tps = [TopicPartition(topic_name, p) for p in partition_list]
        
        # Resume partitions
        consumer.resume(tps)
        
        print(f"   [+] SUCCESS: Partitions {partition_list} resumed for topic '{topic_name}'")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not resume partitions: {e}")
        return False

def get_watermark_offsets(consumer, topic_partition_spec):
    """Get low and high watermark offsets for a topic partition"""
    print("=" * 60)
    print("GETTING WATERMARK OFFSETS")
    print("=" * 60)
    
    try:
        # Parse specification: topic:partition
        parts = topic_partition_spec.split(':')
        if len(parts) != 2:
            print("   [-] ERROR: Specification must be in format: topic:partition")
            return False
        
        topic_name, partition_str = parts
        
        try:
            partition = int(partition_str)
        except ValueError:
            print("   [-] ERROR: Partition must be an integer")
            return False
        
        print(f"   Topic Name: {topic_name}")
        print(f"   Partition: {partition}")
        
        # Create topic partition
        tp = TopicPartition(topic_name, partition)
        
        # Get watermark offsets
        low, high = consumer.get_watermark_offsets(tp)
        
        print(f"   Low Watermark: {low}")
        print(f"   High Watermark: {high}")
        print(f"   Available Messages: {high - low}")
        
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not get watermark offsets: {e}")
        return False

def batch_produce_messages(producer, batch_spec):
    """Batch produce messages from a file"""
    print("=" * 60)
    print("BATCH PRODUCING MESSAGES")
    print("=" * 60)
    
    try:
        # Parse batch specification: topic:file_path
        parts = batch_spec.split(':')
        if len(parts) != 2:
            print("   [-] ERROR: Batch specification must be in format: topic:file_path")
            return False
        
        topic_name, file_path = parts
        
        print(f"   Topic Name: {topic_name}")
        print(f"   File Path: {file_path}")
        
        # Read messages from file
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                messages = [line.strip() for line in f if line.strip()]
            print(f"   Message Count: {len(messages)}")
        except FileNotFoundError:
            print(f"   [-] ERROR: File '{file_path}' not found")
            return False
        except Exception as e:
            print(f"   [-] ERROR: Could not read file '{file_path}': {e}")
            return False
        
        # Produce messages
        success_count = 0
        for i, message in enumerate(messages, 1):
            try:
                producer.produce(topic_name, value=message.encode('utf-8'))
                success_count += 1
                if i % 100 == 0:  # Progress indicator
                    print(f"   Progress: {i}/{len(messages)} messages")
            except Exception as e:
                print(f"   [-] ERROR: Could not produce message {i}: {e}")
        
        # Flush remaining messages
        producer.flush(timeout=30)
        
        print(f"   [+] SUCCESS: {success_count}/{len(messages)} messages produced to topic '{topic_name}'")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not batch produce messages: {e}")
        return False

def batch_consume_messages(consumer, batch_spec):
    """Batch consume messages from a topic"""
    print("=" * 60)
    print("BATCH CONSUMING MESSAGES")
    print("=" * 60)
    
    try:
        # Parse batch specification: topic:max_messages:timeout
        parts = batch_spec.split(':')
        if len(parts) != 3:
            print("   [-] ERROR: Batch specification must be in format: topic:max_messages:timeout")
            return False
        
        topic_name, max_messages_str, timeout_str = parts
        
        try:
            max_messages = int(max_messages_str)
            timeout = float(timeout_str)
        except ValueError:
            print("   [-] ERROR: Max messages and timeout must be numbers")
            return False
        
        print(f"   Topic Name: {topic_name}")
        print(f"   Max Messages: {max_messages}")
        print(f"   Timeout: {timeout} seconds")
        
        # Subscribe to topic
        consumer.subscribe([topic_name])
        
        # Consume messages
        messages = []
        start_time = time.time()
        
        while len(messages) < max_messages and (time.time() - start_time) < timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"   Reached end of partition {msg.partition()}")
                    break
                else:
                    print(f"   Consumer error: {msg.error()}")
                    break
            else:
                messages.append(msg)
                if len(messages) % 100 == 0:  # Progress indicator
                    print(f"   Progress: {len(messages)}/{max_messages} messages")
        
        # Close consumer
        consumer.close()
        
        print(f"   [+] SUCCESS: Consumed {len(messages)} messages from topic '{topic_name}'")
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not batch consume messages: {e}")
        return False

def show_consumer_metrics(consumer):
    """Show consumer metrics"""
    print("=" * 60)
    print("CONSUMER METRICS")
    print("=" * 60)
    
    try:
        # Get metrics
        metrics = consumer.list_topics()
        
        print("   Consumer Metrics:")
        print(f"     Assignment: {consumer.assignment()}")
        print(f"     Member ID: {consumer.memberid()}")
        
        # Get position for assigned partitions
        assignment = consumer.assignment()
        if assignment:
            positions = consumer.position(assignment)
            print("     Partition Positions:")
            for tp in positions:
                print(f"       {tp.topic}[{tp.partition}]: offset {tp.offset}")
        
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not get consumer metrics: {e}")
        return False

def show_producer_metrics(producer):
    """Show producer metrics"""
    print("=" * 60)
    print("PRODUCER METRICS")
    print("=" * 60)
    
    try:
        # Get metrics
        metrics = producer.list_topics()
        
        print("   Producer Metrics:")
        print(f"     Topics: {len(metrics.topics) if hasattr(metrics, 'topics') else 'Unknown'}")
        
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not get producer metrics: {e}")
        return False

def show_broker_health(admin_client):
    """Show broker health information"""
    print("=" * 60)
    print("BROKER HEALTH")
    print("=" * 60)
    
    try:
        # Get cluster metadata
        metadata = admin_client.list_topics(timeout=10)
        
        print("   Broker Health:")
        for broker_id, broker in metadata.brokers.items():
            print(f"     Broker {broker_id}: {broker.host}:{broker.port}")
            print(f"       Rack: {getattr(broker, 'rack', 'Unknown')}")
        
        # Get controller information
        print(f"   Controller ID: {metadata.controller_id}")
        
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not get broker health: {e}")
        return False

def show_consumer_lag(admin_client, group_name):
    """Show consumer lag for a specific group"""
    print("=" * 60)
    print("CONSUMER LAG")
    print("=" * 60)
    
    try:
        print(f"   Group Name: {group_name}")
        
        # Get consumer group offsets
        offsets = admin_client.list_consumer_group_offsets([group_name])
        
        if group_name in offsets:
            group_offsets = offsets[group_name]
            print(f"   Partition Assignments: {len(group_offsets)}")
            
            for tp, offset_info in group_offsets.items():
                print(f"     {tp.topic}[{tp.partition}]: offset {offset_info.offset}")
        else:
            print("   No offset information found for this group")
        
        return True
        
    except Exception as e:
        print(f"   [-] ERROR: Could not get consumer lag: {e}")
        return False

def parse_args():
    parser = argparse.ArgumentParser(description='Kafka Client - Connect to Apache Kafka using SSL client certificates')
    
    # =============================================================================
    # CONNECTION & SECURITY GROUP (--connection-*)
    # =============================================================================
    connection_group = parser.add_argument_group('Connection & Security')
    connection_group.add_argument('server', help='Kafka server (host:port)')
    
    # New prefixed arguments
    connection_group.add_argument('--connection-tls', action='store_true', help='Use SSL/TLS connection (default is plaintext)')
    connection_group.add_argument('--connection-ca-cert', help='CA certificate file for SSL/TLS')
    connection_group.add_argument('--connection-client-cert', help='Client certificate file for SSL/TLS (PEM format)')
    connection_group.add_argument('--connection-stick-to-broker', action='store_true', 
                                 help='Stay connected to the initial broker only (disable broker discovery and load balancing)')
    
    # Legacy arguments (deprecated)
    connection_group.add_argument('--tls', action='store_true', help='[DEPRECATED] Use --connection-tls instead')
    connection_group.add_argument('--ca-cert', help='[DEPRECATED] Use --connection-ca-cert instead')
    connection_group.add_argument('--client-cert', help='[DEPRECATED] Use --connection-client-cert instead')
    connection_group.add_argument('--stick-to-broker', action='store_true', 
                                 help='[DEPRECATED] Use --connection-stick-to-broker instead')
    
    # =============================================================================
    # TOPIC MANAGEMENT GROUP (--topic-*)
    # =============================================================================
    topic_group = parser.add_argument_group('Topic Management')
    
    # New prefixed arguments
    topic_group.add_argument('--topic-list', action='store_true', help='List all topics')
    topic_group.add_argument('--topic-list-partitions', action='store_true', help='List topics with partition details')
    topic_group.add_argument('--topic-create', help='Create a new topic (format: topic_name:partitions:replication_factor)')
    topic_group.add_argument('--topic-delete', help='Delete a topic')
    topic_group.add_argument('--topic-add-partitions', help='Add partitions to topic (format: topic_name:new_partition_count)')
    topic_group.add_argument('--topic-alter-config', help='Alter topic configuration (format: topic_name:config_key=value)')
    topic_group.add_argument('--topic-delete-records', help='Delete records from topic (format: topic_name:partition:offset)')
    topic_group.add_argument('--topic-elect-leaders', help='Elect leaders for partitions (format: topic_name:partition)')
    topic_group.add_argument('--topic-configs', action='store_true', help='Show topic configurations')
    topic_group.add_argument('--topic-offsets', action='store_true', help='Show topic offsets')
    
    # Legacy arguments (deprecated)
    topic_group.add_argument('--list-topics', action='store_true', help='[DEPRECATED] Use --topic-list instead')
    topic_group.add_argument('--list-topics-partitions', action='store_true', help='[DEPRECATED] Use --topic-list-partitions instead')
    topic_group.add_argument('--add-topic', help='[DEPRECATED] Use --topic-create instead')
    topic_group.add_argument('--delete-topic', help='[DEPRECATED] Use --topic-delete instead')
    topic_group.add_argument('--add-partitions', help='[DEPRECATED] Use --topic-add-partitions instead')
    topic_group.add_argument('--alter-topic-config', help='[DEPRECATED] Use --topic-alter-config instead')
    topic_group.add_argument('--delete-records', help='[DEPRECATED] Use --topic-delete-records instead')
    topic_group.add_argument('--elect-leaders', help='[DEPRECATED] Use --topic-elect-leaders instead')
    
    # =============================================================================
    # MESSAGE PRODUCTION GROUP (--produce-*)
    # =============================================================================
    produce_group = parser.add_argument_group('Message Production')
    
    # New prefixed arguments
    produce_group.add_argument('--produce-message', help='Produce a message to a topic')
    produce_group.add_argument('--produce-key', help='Message key for producing')
    produce_group.add_argument('--produce-value', help='Message value for producing')
    produce_group.add_argument('--produce-json-value', help='JSON message value for producing')
    produce_group.add_argument('--produce-from-file', help='Write message from file to topic (format: topic_name:file_path)')
    produce_group.add_argument('--produce-batch', help='Batch produce messages (format: topic:file_path)')
    
    # Legacy arguments (deprecated)
    produce_group.add_argument('--produce', help='[DEPRECATED] Use --produce-message instead')
    produce_group.add_argument('--key', help='[DEPRECATED] Use --produce-key instead')
    produce_group.add_argument('--value', help='[DEPRECATED] Use --produce-value instead')
    produce_group.add_argument('--json-value', help='[DEPRECATED] Use --produce-json-value instead')
    produce_group.add_argument('--write-to-topic', help='[DEPRECATED] Use --produce-from-file instead')
    produce_group.add_argument('--batch-produce', help='[DEPRECATED] Use --produce-batch instead')
    
    # =============================================================================
    # MESSAGE CONSUMPTION GROUP (--consume-*)
    # =============================================================================
    consume_group = parser.add_argument_group('Message Consumption')
    
    # New prefixed arguments
    consume_group.add_argument('--consume-subscribe', help='Subscribe to a topic and read messages')
    consume_group.add_argument('--consume-subscribe-wildcard', help='Subscribe to topics matching a prefix (wildcard)')
    consume_group.add_argument('--consume-batch', help='Batch consume messages (format: topic:max_messages:timeout)')
    consume_group.add_argument('--consume-scan-available', action='store_true', help='Scan for available messages in topics')
    
    # Legacy arguments (deprecated)
    consume_group.add_argument('--subscribe', help='[DEPRECATED] Use --consume-subscribe instead')
    consume_group.add_argument('--subscribe-wildcard', help='[DEPRECATED] Use --consume-subscribe-wildcard instead')
    consume_group.add_argument('--batch-consume', help='[DEPRECATED] Use --consume-batch instead')
    consume_group.add_argument('--scan-available-messages', action='store_true', help='[DEPRECATED] Use --consume-scan-available instead')
    
    # =============================================================================
    # CONSUMER GROUP MANAGEMENT GROUP (--group-*)
    # =============================================================================
    group_group = parser.add_argument_group('Consumer Group Management')
    
    # New prefixed arguments
    group_group.add_argument('--group-list', action='store_true', help='List consumer groups')
    group_group.add_argument('--group-list-detailed', action='store_true', help='Show detailed consumer group information')
    group_group.add_argument('--group-create', help='Create a new consumer group (format: group_name:topic_name)')
    group_group.add_argument('--group-delete', help='Delete a consumer group')
    group_group.add_argument('--group-describe', help='Describe a specific consumer group')
    group_group.add_argument('--group-browse', help='Browse messages from a consumer group')
    group_group.add_argument('--group-browse-max-messages', type=int, default=10, help='Maximum messages to browse from group')
    group_group.add_argument('--group-browse-timeout', type=float, default=5.0, help='Browse timeout in seconds')
    group_group.add_argument('--group-create-test', help='Create a test consumer group (format: group_name:topic_name)')
    group_group.add_argument('--group-alter-offsets', help='Alter consumer group offsets (format: group_name:topic:partition:offset)')
    group_group.add_argument('--group-write-from-file', help='Write message from file to consumer group (format: group_name:topic_name:file_path)')
    group_group.add_argument('--group-lag', help='Show consumer lag for group')
    
    # Legacy arguments (deprecated)
    group_group.add_argument('--list-consumer-groups', action='store_true', help='[DEPRECATED] Use --group-list instead')
    group_group.add_argument('--detailed-consumer-groups', action='store_true', help='[DEPRECATED] Use --group-list-detailed instead')
    group_group.add_argument('--add-consumer-group', help='[DEPRECATED] Use --group-create instead')
    group_group.add_argument('--delete-consumer-group', help='[DEPRECATED] Use --group-delete instead')
    group_group.add_argument('--describe-consumer-group', help='[DEPRECATED] Use --group-describe instead')
    group_group.add_argument('--browse-group', help='[DEPRECATED] Use --group-browse instead')
    group_group.add_argument('--browse-max-messages', type=int, default=10, help='[DEPRECATED] Use --group-browse-max-messages instead')
    group_group.add_argument('--browse-timeout', type=float, default=5.0, help='[DEPRECATED] Use --group-browse-timeout instead')
    group_group.add_argument('--create-test-group', help='[DEPRECATED] Use --group-create-test instead')
    group_group.add_argument('--alter-group-offsets', help='[DEPRECATED] Use --group-alter-offsets instead')
    group_group.add_argument('--write-to-group', help='[DEPRECATED] Use --group-write-from-file instead')
    group_group.add_argument('--show-consumer-lag', help='[DEPRECATED] Use --group-lag instead')
    
    # =============================================================================
    # CONSUMER CONTROL GROUP (--consumer-*)
    # =============================================================================
    consumer_group = parser.add_argument_group('Consumer Control')
    
    # New prefixed arguments
    consumer_group.add_argument('--consumer-seek-offset', help='Seek to specific offset (format: topic:partition:offset)')
    consumer_group.add_argument('--consumer-seek-timestamp', help='Seek to specific timestamp (format: topic:partition:timestamp)')
    consumer_group.add_argument('--consumer-pause-partitions', help='Pause partitions (format: topic:partition_list)')
    consumer_group.add_argument('--consumer-resume-partitions', help='Resume partitions (format: topic:partition_list)')
    consumer_group.add_argument('--consumer-get-watermarks', help='Get watermark offsets (format: topic:partition)')
    consumer_group.add_argument('--consumer-group-id', default='kafka-client-group', help='Consumer group ID')
    consumer_group.add_argument('--consumer-from-beginning', action='store_true', help='Start reading from the beginning of the topic')
    consumer_group.add_argument('--consumer-max-messages', type=int, help='Maximum number of messages to read')
    consumer_group.add_argument('--consumer-timeout', type=float, default=1.0, help='Poll timeout in seconds')
    consumer_group.add_argument('--consumer-isolation-level', choices=['read_committed', 'read_uncommitted'], 
                               help='Set consumer isolation level')
    
    # Legacy arguments (deprecated)
    consumer_group.add_argument('--seek-to-offset', help='[DEPRECATED] Use --consumer-seek-offset instead')
    consumer_group.add_argument('--seek-to-timestamp', help='[DEPRECATED] Use --consumer-seek-timestamp instead')
    consumer_group.add_argument('--pause-partitions', help='[DEPRECATED] Use --consumer-pause-partitions instead')
    consumer_group.add_argument('--resume-partitions', help='[DEPRECATED] Use --consumer-resume-partitions instead')
    consumer_group.add_argument('--get-watermarks', help='[DEPRECATED] Use --consumer-get-watermarks instead')
    consumer_group.add_argument('--consumer-group', default='kafka-client-group', help='[DEPRECATED] Use --consumer-group-id instead')
    consumer_group.add_argument('--from-beginning', action='store_true', help='[DEPRECATED] Use --consumer-from-beginning instead')
    consumer_group.add_argument('--max-messages', type=int, help='[DEPRECATED] Use --consumer-max-messages instead')
    consumer_group.add_argument('--timeout', type=float, default=1.0, help='[DEPRECATED] Use --consumer-timeout instead')
    consumer_group.add_argument('--isolation-level', choices=['read_committed', 'read_uncommitted'], 
                               help='[DEPRECATED] Use --consumer-isolation-level instead')
    
    # =============================================================================
    # CLUSTER & BROKER MANAGEMENT GROUP (--cluster-*)
    # =============================================================================
    cluster_group = parser.add_argument_group('Cluster & Broker Management')
    
    # New prefixed arguments
    cluster_group.add_argument('--cluster-info', action='store_true', help='Show cluster information')
    cluster_group.add_argument('--cluster-list-brokers', action='store_true', help='List brokers')
    cluster_group.add_argument('--cluster-broker-configs', action='store_true', help='Show broker configurations')
    cluster_group.add_argument('--cluster-broker-health', action='store_true', help='Show broker health information')
    
    # Legacy arguments (deprecated)
    cluster_group.add_argument('--list-brokers', action='store_true', help='[DEPRECATED] Use --cluster-list-brokers instead')
    cluster_group.add_argument('--broker-configs', action='store_true', help='[DEPRECATED] Use --cluster-broker-configs instead')
    cluster_group.add_argument('--show-broker-health', action='store_true', help='[DEPRECATED] Use --cluster-broker-health instead')
    
    # =============================================================================
    # SECURITY & ACCESS CONTROL GROUP (--security-*)
    # =============================================================================
    security_group = parser.add_argument_group('Security & Access Control')
    
    # New prefixed arguments
    security_group.add_argument('--security-acl-list', action='store_true', help='Show Access Control Lists')
    security_group.add_argument('--security-acl-create', help='Create an ACL (format: resource_type:resource_name:principal:operation:permission)')
    security_group.add_argument('--security-acl-delete', help='Delete an ACL (format: resource_type:resource_name:principal:operation:permission)')
    security_group.add_argument('--security-user-credentials', action='store_true', help='Show user credentials')
    security_group.add_argument('--security-user-credentials-alter', help='Alter user SCRAM credentials (format: username:password:mechanism)')
    security_group.add_argument('--security-user-credentials-delete', help='Delete user SCRAM credentials (format: username:mechanism)')
    security_group.add_argument('--security-user-credentials-describe', nargs='?', const=None, metavar='USERNAME',
                               help='Describe user SCRAM credentials (username optional)')
    security_group.add_argument('--security-test-permissions', action='store_true', help='Test various permissions')
    security_group.add_argument('--security-audit', action='store_true', help='Audit security configurations')
    security_group.add_argument('--security-enumerate-sensitive', action='store_true', help='Enumerate sensitive data')
    security_group.add_argument('--security-test-injection', action='store_true', help='Test message injection')
    security_group.add_argument('--security-full-audit', action='store_true', help='Run all security tests')
    
    # Legacy arguments (deprecated)
    security_group.add_argument('--acls', action='store_true', help='[DEPRECATED] Use --security-acl-list instead')
    security_group.add_argument('--create-acl', help='[DEPRECATED] Use --security-acl-create instead')
    security_group.add_argument('--delete-acl', help='[DEPRECATED] Use --security-acl-delete instead')
    security_group.add_argument('--user-credentials', action='store_true', help='[DEPRECATED] Use --security-user-credentials instead')
    security_group.add_argument('--alter-user-credentials', help='[DEPRECATED] Use --security-user-credentials-alter instead')
    security_group.add_argument('--delete-user-credentials', help='[DEPRECATED] Use --security-user-credentials-delete instead')
    security_group.add_argument('--describe-user-credentials', nargs='?', const=None, metavar='USERNAME',
                               help='[DEPRECATED] Use --security-user-credentials-describe instead')
    security_group.add_argument('--test-permissions', action='store_true', help='[DEPRECATED] Use --security-test-permissions instead')
    security_group.add_argument('--audit-security', action='store_true', help='[DEPRECATED] Use --security-audit instead')
    security_group.add_argument('--enumerate-sensitive', action='store_true', help='[DEPRECATED] Use --security-enumerate-sensitive instead')
    security_group.add_argument('--test-injection', action='store_true', help='[DEPRECATED] Use --security-test-injection instead')
    security_group.add_argument('--full-security-audit', action='store_true', help='[DEPRECATED] Use --security-full-audit instead')
    
    # =============================================================================
    # MONITORING & METRICS GROUP (--monitor-*)
    # =============================================================================
    monitor_group = parser.add_argument_group('Monitoring & Metrics')
    
    # New prefixed arguments
    monitor_group.add_argument('--monitor-consumer-metrics', action='store_true', help='Show consumer metrics')
    monitor_group.add_argument('--monitor-producer-metrics', action='store_true', help='Show producer metrics')
    monitor_group.add_argument('--monitor-all', action='store_true', help='Show all information')
    
    # Legacy arguments (deprecated)
    monitor_group.add_argument('--show-consumer-metrics', action='store_true', help='[DEPRECATED] Use --monitor-consumer-metrics instead')
    monitor_group.add_argument('--show-producer-metrics', action='store_true', help='[DEPRECATED] Use --monitor-producer-metrics instead')
    monitor_group.add_argument('--all', action='store_true', help='[DEPRECATED] Use --monitor-all instead')
    
    # =============================================================================
    # TRANSACTION MANAGEMENT GROUP (--transaction-*)
    # =============================================================================
    transaction_group = parser.add_argument_group('Transaction Management')
    
    # New prefixed arguments
    transaction_group.add_argument('--transaction-begin', action='store_true', help='Begin a transaction')
    transaction_group.add_argument('--transaction-commit', action='store_true', help='Commit current transaction')
    transaction_group.add_argument('--transaction-abort', action='store_true', help='Abort current transaction')
    
    # Legacy arguments (deprecated)
    transaction_group.add_argument('--begin-transaction', action='store_true', help='[DEPRECATED] Use --transaction-begin instead')
    transaction_group.add_argument('--commit-transaction', action='store_true', help='[DEPRECATED] Use --transaction-commit instead')
    transaction_group.add_argument('--abort-transaction', action='store_true', help='[DEPRECATED] Use --transaction-abort instead')
    
    return parser.parse_args()

def handle_legacy_args(args):
    """Handle legacy arguments for backward compatibility"""
    import warnings
    
    # Connection & Security legacy arguments
    if args.tls:
        warnings.warn("--tls is deprecated, use --connection-tls instead", DeprecationWarning, stacklevel=2)
        args.connection_tls = True
    if args.ca_cert:
        warnings.warn("--ca-cert is deprecated, use --connection-ca-cert instead", DeprecationWarning, stacklevel=2)
        args.connection_ca_cert = args.ca_cert
    if args.client_cert:
        warnings.warn("--client-cert is deprecated, use --connection-client-cert instead", DeprecationWarning, stacklevel=2)
        args.connection_client_cert = args.client_cert
    if args.stick_to_broker:
        warnings.warn("--stick-to-broker is deprecated, use --connection-stick-to-broker instead", DeprecationWarning, stacklevel=2)
        args.connection_stick_to_broker = True
    
    # Topic Management legacy arguments
    if args.list_topics:
        warnings.warn("--list-topics is deprecated, use --topic-list instead", DeprecationWarning, stacklevel=2)
        args.topic_list = True
    if args.list_topics_partitions:
        warnings.warn("--list-topics-partitions is deprecated, use --topic-list-partitions instead", DeprecationWarning, stacklevel=2)
        args.topic_list_partitions = True
    if args.add_topic:
        warnings.warn("--add-topic is deprecated, use --topic-create instead", DeprecationWarning, stacklevel=2)
        args.topic_create = args.add_topic
    if args.delete_topic:
        warnings.warn("--delete-topic is deprecated, use --topic-delete instead", DeprecationWarning, stacklevel=2)
        args.topic_delete = args.delete_topic
    if args.add_partitions:
        warnings.warn("--add-partitions is deprecated, use --topic-add-partitions instead", DeprecationWarning, stacklevel=2)
        args.topic_add_partitions = args.add_partitions
    if args.alter_topic_config:
        warnings.warn("--alter-topic-config is deprecated, use --topic-alter-config instead", DeprecationWarning, stacklevel=2)
        args.topic_alter_config = args.alter_topic_config
    if args.delete_records:
        warnings.warn("--delete-records is deprecated, use --topic-delete-records instead", DeprecationWarning, stacklevel=2)
        args.topic_delete_records = args.delete_records
    if args.elect_leaders:
        warnings.warn("--elect-leaders is deprecated, use --topic-elect-leaders instead", DeprecationWarning, stacklevel=2)
        args.topic_elect_leaders = args.elect_leaders
    
    # Message Production legacy arguments
    if args.produce:
        warnings.warn("--produce is deprecated, use --produce-message instead", DeprecationWarning, stacklevel=2)
        args.produce_message = args.produce
    if args.key:
        warnings.warn("--key is deprecated, use --produce-key instead", DeprecationWarning, stacklevel=2)
        args.produce_key = args.key
    if args.value:
        warnings.warn("--value is deprecated, use --produce-value instead", DeprecationWarning, stacklevel=2)
        args.produce_value = args.value
    if args.json_value:
        warnings.warn("--json-value is deprecated, use --produce-json-value instead", DeprecationWarning, stacklevel=2)
        args.produce_json_value = args.json_value
    if args.write_to_topic:
        warnings.warn("--write-to-topic is deprecated, use --produce-from-file instead", DeprecationWarning, stacklevel=2)
        args.produce_from_file = args.write_to_topic
    if args.batch_produce:
        warnings.warn("--batch-produce is deprecated, use --produce-batch instead", DeprecationWarning, stacklevel=2)
        args.produce_batch = args.batch_produce
    
    # Message Consumption legacy arguments
    if args.subscribe:
        warnings.warn("--subscribe is deprecated, use --consume-subscribe instead", DeprecationWarning, stacklevel=2)
        args.consume_subscribe = args.subscribe
    if args.subscribe_wildcard:
        warnings.warn("--subscribe-wildcard is deprecated, use --consume-subscribe-wildcard instead", DeprecationWarning, stacklevel=2)
        args.consume_subscribe_wildcard = args.subscribe_wildcard
    if args.batch_consume:
        warnings.warn("--batch-consume is deprecated, use --consume-batch instead", DeprecationWarning, stacklevel=2)
        args.consume_batch = args.batch_consume
    if args.scan_available_messages:
        warnings.warn("--scan-available-messages is deprecated, use --consume-scan-available instead", DeprecationWarning, stacklevel=2)
        args.consume_scan_available = True
    
    # Consumer Group Management legacy arguments
    if args.list_consumer_groups:
        warnings.warn("--list-consumer-groups is deprecated, use --group-list instead", DeprecationWarning, stacklevel=2)
        args.group_list = True
    if args.detailed_consumer_groups:
        warnings.warn("--detailed-consumer-groups is deprecated, use --group-list-detailed instead", DeprecationWarning, stacklevel=2)
        args.group_list_detailed = True
    if args.add_consumer_group:
        warnings.warn("--add-consumer-group is deprecated, use --group-create instead", DeprecationWarning, stacklevel=2)
        args.group_create = args.add_consumer_group
    if args.delete_consumer_group:
        warnings.warn("--delete-consumer-group is deprecated, use --group-delete instead", DeprecationWarning, stacklevel=2)
        args.group_delete = args.delete_consumer_group
    if args.describe_consumer_group:
        warnings.warn("--describe-consumer-group is deprecated, use --group-describe instead", DeprecationWarning, stacklevel=2)
        args.group_describe = args.describe_consumer_group
    if args.browse_group:
        warnings.warn("--browse-group is deprecated, use --group-browse instead", DeprecationWarning, stacklevel=2)
        args.group_browse = args.browse_group
    if args.browse_max_messages:
        warnings.warn("--browse-max-messages is deprecated, use --group-browse-max-messages instead", DeprecationWarning, stacklevel=2)
        args.group_browse_max_messages = args.browse_max_messages
    if args.browse_timeout:
        warnings.warn("--browse-timeout is deprecated, use --group-browse-timeout instead", DeprecationWarning, stacklevel=2)
        args.group_browse_timeout = args.browse_timeout
    if args.create_test_group:
        warnings.warn("--create-test-group is deprecated, use --group-create-test instead", DeprecationWarning, stacklevel=2)
        args.group_create_test = args.create_test_group
    if args.alter_group_offsets:
        warnings.warn("--alter-group-offsets is deprecated, use --group-alter-offsets instead", DeprecationWarning, stacklevel=2)
        args.group_alter_offsets = args.alter_group_offsets
    if args.write_to_group:
        warnings.warn("--write-to-group is deprecated, use --group-write-from-file instead", DeprecationWarning, stacklevel=2)
        args.group_write_from_file = args.write_to_group
    if args.show_consumer_lag:
        warnings.warn("--show-consumer-lag is deprecated, use --group-lag instead", DeprecationWarning, stacklevel=2)
        args.group_lag = args.show_consumer_lag
    
    # Consumer Control legacy arguments
    if args.seek_to_offset:
        warnings.warn("--seek-to-offset is deprecated, use --consumer-seek-offset instead", DeprecationWarning, stacklevel=2)
        args.consumer_seek_offset = args.seek_to_offset
    if args.seek_to_timestamp:
        warnings.warn("--seek-to-timestamp is deprecated, use --consumer-seek-timestamp instead", DeprecationWarning, stacklevel=2)
        args.consumer_seek_timestamp = args.seek_to_timestamp
    if args.pause_partitions:
        warnings.warn("--pause-partitions is deprecated, use --consumer-pause-partitions instead", DeprecationWarning, stacklevel=2)
        args.consumer_pause_partitions = args.pause_partitions
    if args.resume_partitions:
        warnings.warn("--resume-partitions is deprecated, use --consumer-resume-partitions instead", DeprecationWarning, stacklevel=2)
        args.consumer_resume_partitions = args.resume_partitions
    if args.get_watermarks:
        warnings.warn("--get-watermarks is deprecated, use --consumer-get-watermarks instead", DeprecationWarning, stacklevel=2)
        args.consumer_get_watermarks = args.get_watermarks
    if args.consumer_group:
        warnings.warn("--consumer-group is deprecated, use --consumer-group-id instead", DeprecationWarning, stacklevel=2)
        args.consumer_group_id = args.consumer_group
    if args.from_beginning:
        warnings.warn("--from-beginning is deprecated, use --consumer-from-beginning instead", DeprecationWarning, stacklevel=2)
        args.consumer_from_beginning = True
    if args.max_messages:
        warnings.warn("--max-messages is deprecated, use --consumer-max-messages instead", DeprecationWarning, stacklevel=2)
        args.consumer_max_messages = args.max_messages
    if args.timeout:
        warnings.warn("--timeout is deprecated, use --consumer-timeout instead", DeprecationWarning, stacklevel=2)
        args.consumer_timeout = args.timeout
    if args.isolation_level:
        warnings.warn("--isolation-level is deprecated, use --consumer-isolation-level instead", DeprecationWarning, stacklevel=2)
        args.consumer_isolation_level = args.isolation_level
    
    # Cluster & Broker Management legacy arguments
    if args.list_brokers:
        warnings.warn("--list-brokers is deprecated, use --cluster-list-brokers instead", DeprecationWarning, stacklevel=2)
        args.cluster_list_brokers = True
    if args.broker_configs:
        warnings.warn("--broker-configs is deprecated, use --cluster-broker-configs instead", DeprecationWarning, stacklevel=2)
        args.cluster_broker_configs = True
    if args.show_broker_health:
        warnings.warn("--show-broker-health is deprecated, use --cluster-broker-health instead", DeprecationWarning, stacklevel=2)
        args.cluster_broker_health = True
    
    # Security & Access Control legacy arguments
    if args.acls:
        warnings.warn("--acls is deprecated, use --security-acl-list instead", DeprecationWarning, stacklevel=2)
        args.security_acl_list = True
    if args.create_acl:
        warnings.warn("--create-acl is deprecated, use --security-acl-create instead", DeprecationWarning, stacklevel=2)
        args.security_acl_create = args.create_acl
    if args.delete_acl:
        warnings.warn("--delete-acl is deprecated, use --security-acl-delete instead", DeprecationWarning, stacklevel=2)
        args.security_acl_delete = args.delete_acl
    if args.user_credentials:
        warnings.warn("--user-credentials is deprecated, use --security-user-credentials instead", DeprecationWarning, stacklevel=2)
        args.security_user_credentials = True
    if args.alter_user_credentials:
        warnings.warn("--alter-user-credentials is deprecated, use --security-user-credentials-alter instead", DeprecationWarning, stacklevel=2)
        args.security_user_credentials_alter = args.alter_user_credentials
    if args.delete_user_credentials:
        warnings.warn("--delete-user-credentials is deprecated, use --security-user-credentials-delete instead", DeprecationWarning, stacklevel=2)
        args.security_user_credentials_delete = args.delete_user_credentials
    if args.describe_user_credentials:
        warnings.warn("--describe-user-credentials is deprecated, use --security-user-credentials-describe instead", DeprecationWarning, stacklevel=2)
        args.security_user_credentials_describe = args.describe_user_credentials
    if args.test_permissions:
        warnings.warn("--test-permissions is deprecated, use --security-test-permissions instead", DeprecationWarning, stacklevel=2)
        args.security_test_permissions = True
    if args.audit_security:
        warnings.warn("--audit-security is deprecated, use --security-audit instead", DeprecationWarning, stacklevel=2)
        args.security_audit = True
    if args.enumerate_sensitive:
        warnings.warn("--enumerate-sensitive is deprecated, use --security-enumerate-sensitive instead", DeprecationWarning, stacklevel=2)
        args.security_enumerate_sensitive = True
    if args.test_injection:
        warnings.warn("--test-injection is deprecated, use --security-test-injection instead", DeprecationWarning, stacklevel=2)
        args.security_test_injection = True
    if args.full_security_audit:
        warnings.warn("--full-security-audit is deprecated, use --security-full-audit instead", DeprecationWarning, stacklevel=2)
        args.security_full_audit = True
    
    # Monitoring & Metrics legacy arguments
    if args.show_consumer_metrics:
        warnings.warn("--show-consumer-metrics is deprecated, use --monitor-consumer-metrics instead", DeprecationWarning, stacklevel=2)
        args.monitor_consumer_metrics = True
    if args.show_producer_metrics:
        warnings.warn("--show-producer-metrics is deprecated, use --monitor-producer-metrics instead", DeprecationWarning, stacklevel=2)
        args.monitor_producer_metrics = True
    if args.all:
        warnings.warn("--all is deprecated, use --monitor-all instead", DeprecationWarning, stacklevel=2)
        args.monitor_all = True
    
    # Transaction Management legacy arguments
    if args.begin_transaction:
        warnings.warn("--begin-transaction is deprecated, use --transaction-begin instead", DeprecationWarning, stacklevel=2)
        args.transaction_begin = True
    if args.commit_transaction:
        warnings.warn("--commit-transaction is deprecated, use --transaction-commit instead", DeprecationWarning, stacklevel=2)
        args.transaction_commit = True
    if args.abort_transaction:
        warnings.warn("--abort-transaction is deprecated, use --transaction-abort instead", DeprecationWarning, stacklevel=2)
        args.transaction_abort = True
    
    return args

def main():
    args = parse_args()
    
    # Handle legacy arguments for backward compatibility
    args = handle_legacy_args(args)
    
    # Configuration
    bootstrap_servers = args.server
    
    # Build configuration based on TLS settings
    if args.connection_tls:
        # TLS/SSL connection
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SSL',
            'ssl.endpoint.identification.algorithm': 'none',  # Disable hostname verification
        }
        if args.connection_ca_cert:
            conf['ssl.ca.location'] = args.connection_ca_cert
        elif args.connection_client_cert:
            conf['ssl.ca.location'] = args.connection_client_cert
        if args.connection_client_cert:
            conf['ssl.certificate.location'] = args.connection_client_cert
            conf['ssl.key.location'] = args.connection_client_cert
        print(f"Connecting to {bootstrap_servers} using SSL/TLS" + (" with client certificate" if args.connection_client_cert else " (no client certificate)"))
    else:
        # Plaintext connection without TLS
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'PLAINTEXT',
        }
        print(f"Connecting to {bootstrap_servers} using PLAINTEXT (no TLS)")

    # Add broker-sticking configuration if requested
    if args.connection_stick_to_broker:
        # Configuration to stay connected to the initial broker only
        conf.update({
            'metadata.max.age.ms': '1',  # Set to minimum allowed value to minimize metadata refresh
            'reconnect.backoff.ms': '0',  # Disable reconnection backoff
            'reconnect.backoff.max.ms': '0',  # Disable max reconnection backoff
            'connections.max.idle.ms': '0',  # Keep connections alive indefinitely
            'request.timeout.ms': '30000',  # Increase request timeout
            'socket.timeout.ms': '30000',  # Increase socket timeout
        })
        print(f"[+] Configured to stick to initial broker: {bootstrap_servers}")
        print("  - Disabled broker discovery and load balancing")
        print("  - Disabled automatic reconnection to other brokers")
        print("  - Will only use the initially connected broker")

    # If subscribing to a topic, set up consumer
    if args.consume_subscribe:
        consumer_conf = conf.copy()
        consumer_conf.update({
            'group.id': args.consumer_group_id,
            'auto.offset.reset': 'earliest' if args.consumer_from_beginning else 'latest',
        })
        
        # Add additional broker-sticking config for consumers
        if args.connection_stick_to_broker:
            consumer_conf.update({
                'enable.auto.commit': 'false',  # Disable auto commit to avoid broker switching
                'auto.commit.interval.ms': '0',  # Disable auto commit interval
            })
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([args.consume_subscribe])
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        
        # Start consuming
        subscribe_messages(consumer, args.consume_subscribe, args.consumer_max_messages, args.consumer_timeout)
        return

    # If subscribing to topics with wildcard, set up consumer
    if args.consume_subscribe_wildcard:
        # Admin client for topic discovery
        admin_client = AdminClient(conf)
        
        # Find topics matching the prefix
        matching_topics = find_topics_by_prefix(admin_client, args.consume_subscribe_wildcard)
        
        if not matching_topics:
            print(f"No topics found starting with prefix '{args.consume_subscribe_wildcard}'")
            return
        
        print(f"Found {len(matching_topics)} topics matching prefix '{args.consume_subscribe_wildcard}':")
        for topic in matching_topics:
            print(f"  - {topic}")
        
        consumer_conf = conf.copy()
        consumer_conf.update({
            'group.id': args.consumer_group_id,
            'auto.offset.reset': 'earliest' if args.consumer_from_beginning else 'latest',
        })
        
        # Add additional broker-sticking config for consumers
        if args.connection_stick_to_broker:
            consumer_conf.update({
                'enable.auto.commit': 'false',  # Disable auto commit to avoid broker switching
                'auto.commit.interval.ms': '0',  # Disable auto commit interval
            })
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe(matching_topics)
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        
        # Start consuming
        subscribe_messages(consumer, matching_topics, args.consumer_max_messages, args.consumer_timeout)
        return

    # If browsing a consumer group, set up browser
    if args.group_browse:
        # Admin client for group browsing
        admin_client = AdminClient(conf)
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        
        # Start browsing
        browse_group(admin_client, args.group_browse, args.group_browse_max_messages, args.group_browse_timeout)
        return

    # If producing to a topic, set up producer
    if args.produce_message:
        producer_conf = conf.copy()
        
        # Add additional broker-sticking config for producers
        if args.connection_stick_to_broker:
            producer_conf.update({
                'acks': '1',  # Use leader-only acknowledgment to avoid broker switching
                'retries': '0',  # Disable retries to avoid broker switching
                'enable.idempotence': 'false',  # Disable idempotence to avoid broker switching
            })
        
        producer = Producer(producer_conf)
        
        if not (args.produce_value or args.produce_json_value):
            print("Error: Either --produce-value or --produce-json-value must be provided with --produce-message")
            return
            
        if args.produce_json_value:
            try:
                json_data = json.loads(args.produce_json_value)
            except json.JSONDecodeError as e:
                print(f"Error: Invalid JSON value: {e}")
                return
            success = produce_message(producer, args.produce_message, args.produce_key, json_value=json_data)
        else:
            success = produce_message(producer, args.produce_message, args.produce_key, value=args.produce_value)
            
        if success:
            print(f"[+] Successfully produced message to topic '{args.produce_message}'")
        return

    # Admin client for server information
    admin_client = AdminClient(conf)

    # Handle topic creation if requested
    if args.topic_create:
        create_topic(admin_client, args.topic_create)
        return

    # Handle topic deletion if requested
    if args.topic_delete:
        delete_topic(admin_client, args.topic_delete)
        return

    # Handle consumer group creation if requested
    if args.group_create:
        create_consumer_group(admin_client, conf, args.group_create)
        return

    # Handle writing messages from files if requested
    if args.produce_from_file:
        write_message_to_topic(admin_client, conf, args.produce_from_file)
        return

    if args.group_write_from_file:
        write_message_to_group(admin_client, conf, args.group_write_from_file)
        return

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
        print("    advertised.listeners=PLAINTEXT://<BROKER_IP>:9092")

    # Handle topic listing based on options
    if args.topic_list:
        print("\nTopics:")
        for topic_name in metadata.topics.keys():
            print(f" - {topic_name}")
    elif args.topic_list_partitions:
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
    if args.cluster_list_brokers:
        print("\nBrokers:")
        for broker in metadata.brokers.values():
            print(f" - id: {broker.id}, host: {broker.host}, port: {broker.port}")

    # List consumer groups if requested
    groups_result = None
    if args.group_list:
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
    if args.security_full_audit:
        test_permissions(admin_client, metadata, conf)
        audit_security_configs(admin_client, metadata)
        enumerate_sensitive_data(admin_client, metadata)
        test_message_injection(admin_client, metadata, conf)
    else:
        if args.security_test_permissions:
            test_permissions(admin_client, metadata, conf)
        if args.security_audit:
            audit_security_configs(admin_client, metadata)
        if args.security_enumerate_sensitive:
            enumerate_sensitive_data(admin_client, metadata)
        if args.security_test_injection:
            test_message_injection(admin_client, metadata, conf)

    # Show topic configurations if requested
    if args.topic_configs or args.monitor_all:
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
    if args.cluster_info or args.monitor_all:
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
    if args.security_acl_list or args.monitor_all:
        try:
            # Check if we have metadata (needed for ACL operations)
            if metadata is None:
                print(f"\nAccess Control Lists (ACLs):")
                print("  Cannot retrieve ACLs: Metadata connection failed")
                print("  This may be due to broker configuration (advertised.listeners)")
                print("  Try fixing broker's server.properties:")
                print("    advertised.listeners=PLAINTEXT://<BROKER_IP>:9092")
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
                print("    advertised.listeners=PLAINTEXT://<BROKER_IP>:9092")
            else:
                import traceback
                print(f"Full traceback:")
                traceback.print_exc()

    # Show detailed consumer groups if requested
    if args.group_list_detailed or args.monitor_all:
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
    if args.security_user_credentials or args.monitor_all:
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
    if args.cluster_broker_configs or args.monitor_all:
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
    if args.topic_offsets or args.monitor_all:
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
    if args.group_delete:
        delete_consumer_group(admin_client, args.group_delete)
        return
    
    if args.group_describe:
        describe_consumer_group_details(admin_client, args.group_describe)
        return

    if args.group_create_test:
        group_name, topic_name = args.group_create_test.split(':')
        create_test_consumer_group(admin_client, conf, group_name, topic_name)
        return

    if args.consume_scan_available:
        scan_available_messages(conf)
        return

    # =============================================================================
    # NEW ADVANCED FUNCTIONALITY HANDLERS
    # =============================================================================
    
    # ACL Management
    if args.security_acl_create:
        create_acl(admin_client, args.security_acl_create)
        return
    
    if args.security_acl_delete:
        delete_acl(admin_client, args.security_acl_delete)
        return
    
    # Topic Management
    if args.topic_add_partitions:
        add_partitions(admin_client, args.topic_add_partitions)
        return
    
    if args.topic_alter_config:
        alter_topic_config(admin_client, args.topic_alter_config)
        return
    
    if args.topic_delete_records:
        delete_records(admin_client, args.topic_delete_records)
        return
    
    if args.topic_elect_leaders:
        elect_leaders(admin_client, args.topic_elect_leaders)
        return
    
    # Consumer Group Management
    if args.group_alter_offsets:
        alter_consumer_group_offsets(admin_client, args.group_alter_offsets)
        return
    
    # User SCRAM Credentials
    if args.security_user_credentials_alter:
        alter_user_scram_credentials(admin_client, args.security_user_credentials_alter)
        return
    
    if args.security_user_credentials_delete:
        delete_user_scram_credentials(admin_client, args.security_user_credentials_delete)
        return
    
    if args.security_user_credentials_describe:
        describe_user_scram_credentials(admin_client, args.security_user_credentials_describe)
        return
    
    # Consumer Control (requires consumer instance)
    if args.consumer_seek_offset or args.consumer_seek_timestamp or args.consumer_pause_partitions or args.consumer_resume_partitions or args.consumer_get_watermarks:
        # Create consumer for these operations
        consumer_conf = conf.copy()
        consumer_conf.update({
            'group.id': args.consumer_group_id,
            'auto.offset.reset': 'earliest' if args.consumer_from_beginning else 'latest',
        })
        
        if args.consumer_isolation_level:
            if args.consumer_isolation_level == 'read_committed':
                consumer_conf['isolation.level'] = 'read_committed'
            elif args.consumer_isolation_level == 'read_uncommitted':
                consumer_conf['isolation.level'] = 'read_uncommitted'
        
        consumer = Consumer(consumer_conf)
        
        if args.consumer_seek_offset:
            seek_to_offset(consumer, args.consumer_seek_offset)
            consumer.close()
            return
        
        if args.consumer_seek_timestamp:
            seek_to_timestamp(consumer, args.consumer_seek_timestamp)
            consumer.close()
            return
        
        if args.consumer_pause_partitions:
            pause_partitions(consumer, args.consumer_pause_partitions)
            consumer.close()
            return
        
        if args.consumer_resume_partitions:
            resume_partitions(consumer, args.consumer_resume_partitions)
            consumer.close()
            return
        
        if args.consumer_get_watermarks:
            get_watermark_offsets(consumer, args.consumer_get_watermarks)
            consumer.close()
            return
    
    # Batch Operations
    if args.produce_batch:
        producer = Producer(conf)
        batch_produce_messages(producer, args.produce_batch)
        return
    
    if args.consume_batch:
        consumer_conf = conf.copy()
        consumer_conf.update({
            'group.id': args.consumer_group_id,
            'auto.offset.reset': 'earliest' if args.consumer_from_beginning else 'latest',
        })
        
        if args.consumer_isolation_level:
            if args.consumer_isolation_level == 'read_committed':
                consumer_conf['isolation.level'] = 'read_committed'
            elif args.consumer_isolation_level == 'read_uncommitted':
                consumer_conf['isolation.level'] = 'read_uncommitted'
        
        consumer = Consumer(consumer_conf)
        batch_consume_messages(consumer, args.consume_batch)
        return
    
    # Monitoring and Metrics
    if args.monitor_consumer_metrics:
        consumer_conf = conf.copy()
        consumer_conf.update({
            'group.id': args.consumer_group_id,
            'auto.offset.reset': 'earliest' if args.consumer_from_beginning else 'latest',
        })
        
        if args.consumer_isolation_level:
            if args.consumer_isolation_level == 'read_committed':
                consumer_conf['isolation.level'] = 'read_committed'
            elif args.consumer_isolation_level == 'read_uncommitted':
                consumer_conf['isolation.level'] = 'read_uncommitted'
        
        consumer = Consumer(consumer_conf)
        show_consumer_metrics(consumer)
        consumer.close()
        return
    
    if args.monitor_producer_metrics:
        producer = Producer(conf)
        show_producer_metrics(producer)
        return
    
    if args.cluster_broker_health:
        show_broker_health(admin_client)
        return
    
    if args.group_lag:
        show_consumer_lag(admin_client, args.group_lag)
        return
    
    # Transaction Support (requires producer instance)
    if args.transaction_begin or args.transaction_commit or args.transaction_abort:
        producer = Producer(conf)
        
        if args.transaction_begin:
            try:
                producer.init_transactions()
                producer.begin_transaction()
                print("[+] Transaction begun successfully")
            except Exception as e:
                print(f"[-] Error beginning transaction: {e}")
            return
        
        if args.transaction_commit:
            try:
                producer.commit_transaction()
                print("[+] Transaction committed successfully")
            except Exception as e:
                print(f"[-] Error committing transaction: {e}")
            return
        
        if args.transaction_abort:
            try:
                producer.abort_transaction()
                print("[+] Transaction aborted successfully")
            except Exception as e:
                print(f"[-] Error aborting transaction: {e}")
            return

if __name__ == "__main__":
    main() 