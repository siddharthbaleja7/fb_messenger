"""
Script to generate test data for the Messenger application.
This script is a skeleton for students to implement.
"""
import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def generate_test_data(session):
    """
    Generate test data in Cassandra.
    
    Students should implement this function to generate test data based on their schema design.
    The function should create:
    - Users (with IDs 1-NUM_USERS)
    - Conversations between random pairs of users
    - Messages in each conversation with realistic timestamps
    """
    logger.info("Generating test data...")
    
    user_ids = []
    for i in range(NUM_USERS):
        user_id = uuid.uuid4()
        user_ids.append(user_id)
        session.execute(
            """
            INSERT INTO user_details (user_id, user_index,username, full_name, email)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (user_id, i,f"user{i+1}", f"Test User {i+1}", f"user{i+1}@example.com")
        )
    logger.info(f"Created {NUM_USERS} users.")
    for index, user_id in enumerate(user_ids):
        logger.info(f"USER_INDEX_MAP[{index}] = uuid.UUID('{user_id}')")

    for i in range(NUM_CONVERSATIONS):
        participants = random.sample(user_ids, random.randint(2, 4))
        conversation_id = uuid.uuid4()
        logger.info(f"Creating conversation {i+1} with ID {conversation_id} and {len(participants)} participants")

        for user_id in participants:
            session.execute(
                """
                INSERT INTO conversation_participants (conversation_id, user_id, joined_at)
                VALUES (%s, %s, %s)
                """,
                (conversation_id, user_id, datetime.utcnow())
            )

        session.execute(
            """
            INSERT INTO conversation_metadata (conversation_id, conversation_index)
            VALUES (%s, %s)
            """,
            (conversation_id, i + 1)
        )


        num_messages = random.randint(5, MAX_MESSAGES_PER_CONVERSATION)
        timestamp = datetime.utcnow()
        messages = []

        for j in range(num_messages):
            sender = random.choice(participants)
            receiver = random.choice([u for u in participants if u != sender])
            content = f"Message {j+1} in convo {i+1}"
            message_id = uuid.uuid4()
            timestamp = timestamp + timedelta(seconds=random.randint(10, 60))

            session.execute(
                """
                INSERT INTO messages_by_conversation (
                    conversation_id, timestamp, message_id, sender_id, receiver_id, content
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (conversation_id, timestamp, message_id, sender, receiver, content)
            )

            messages.append((sender, content, timestamp))

        last_sender, last_content, last_timestamp = messages[-1]
        for user_id in participants:
            others = set(participants)
            others.discard(user_id)
            session.execute(
                """
                INSERT INTO conversations_by_user (
                    user_id, conversation_id, last_updated_at, last_message, other_participants
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (user_id, conversation_id,last_timestamp, last_content, others)
            )
    
    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with messages")
    logger.info(f"User IDs range from 1 to {NUM_USERS}")
    logger.info("Use these IDs for testing the API endpoints")

def main():
    """Main function to generate test data."""
    cluster = None
    
    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()
        
        # Generate test data
        generate_test_data(session)
        
        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main() 