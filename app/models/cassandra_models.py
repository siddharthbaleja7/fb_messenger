"""
Sample models for interacting with Cassandra tables.
Students should implement these models based on their database schema design.
"""
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional,Tuple

from app.db.cassandra_client import cassandra_client
from cassandra.query import  dict_factory

session = cassandra_client.get_session()
session.row_factory = dict_factory

class MessageModel:
    """
    Message model for interacting with the messages table.
    Students will implement this as part of the assignment.
    
    They should consider:
    - How to efficiently store and retrieve messages
    - How to handle pagination of results
    - How to filter messages by timestamp
    """
    
    # TODO: Implement the following methods
    
    @staticmethod
    def create_message(conversation_id: uuid.UUID, sender_id: uuid.UUID, receiver_id: uuid.UUID, content: str) -> dict:
        message_id = uuid.uuid4()
        timestamp = datetime.utcnow()

        session.execute("""
            INSERT INTO messages_by_conversation (
                conversation_id, timestamp, message_id, sender_id, receiver_id, content
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (conversation_id, timestamp, message_id, sender_id, receiver_id, content))

        return {
            "id": message_id,
            "message_id": message_id,
            "conversation_id": conversation_id,
            "sender_id": sender_id,
            "content": content,
            "timestamp": timestamp
        }
    
    @staticmethod
    def get_conversation_messages(conversation_id: uuid.UUID, limit: int = 20, page: int = 1) -> List[dict]:
        try:
            fetch_limit = page * limit
            
            query = """
                SELECT * FROM messages_by_conversation
                WHERE conversation_id = %s
                LIMIT %s
            """
            print("Query:", query.strip())
            print("Params:", conversation_id, fetch_limit)
            
            result = session.execute(query, (conversation_id, fetch_limit))
            
            all_messages = list(result)
            start_idx = (page - 1) * limit
            
            return all_messages[start_idx:min(start_idx + limit, len(all_messages))]
        except Exception as e:
            print("Cassandra query error:", str(e))
            raise


    
    @staticmethod
    def get_messages_before_timestamp(conversation_id: uuid.UUID, before: datetime, limit: int = 20, page: int = 1) -> List[dict]:
        offset = (page - 1) * limit
        try:
            query = """
                SELECT * FROM messages_by_conversation
                WHERE conversation_id = %s AND timestamp < %s ALLOW FILTERING
            """
            print("Query:", query.strip())
            print("Params:", conversation_id, before)
            result = session.execute(query, (conversation_id, before))
            return list(result)[offset:offset + limit]
        except Exception as e:
            print("Cassandra query error:", str(e))
            raise




class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    Students will implement this as part of the assignment.
    
    They should consider:
    - How to efficiently store and retrieve conversations for a user
    - How to handle pagination of results
    - How to optimize for the most recent conversations
    """
    
    # TODO: Implement the following methods
    
    @staticmethod
    def get_user_conversations(user_id: uuid.UUID, limit: int = 20, page: int = 1) -> List[dict]:
        result = session.execute("""
            SELECT * FROM conversations_by_user
            WHERE user_id = %s
            LIMIT %s
        """, (user_id, limit))

        return list(result)

    
    @staticmethod
    def get_conversation(conversation_id: uuid.UUID) -> Optional[List[uuid.UUID]]:
        rows = session.execute("""
            SELECT user_id FROM conversation_participants
            WHERE conversation_id = %s
        """, (conversation_id,))
        return [r["user_id"] for r in rows]
    
    @staticmethod
    def create_or_get_conversation(user_ids: List[uuid.UUID]) -> uuid.UUID:
        conversation_id = uuid.uuid4()
        timestamp = datetime.utcnow()

        for user_id in user_ids:
            session.execute("""
                INSERT INTO conversation_participants (conversation_id, user_id, joined_at)
                VALUES (%s, %s, %s)
            """, (conversation_id, user_id, timestamp))

        return conversation_id
    
    @staticmethod
    def get_last_message_and_time(conversation_id: uuid.UUID) -> Tuple[Optional[str], Optional[datetime]]:
        row = session.execute("""
            SELECT content, timestamp FROM messages_by_conversation
            WHERE conversation_id = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (conversation_id,)).one()

        if row:
            return row["content"], row["timestamp"]
        return "", datetime.utcnow()

    
    @staticmethod
    def get_user_uuid_by_index(index: int) -> uuid.UUID:
        row = session.execute(
        "SELECT user_id FROM user_details WHERE user_index = %s ALLOW FILTERING", (index,)
        ).one()
        if row:
            return row["user_id"]
        raise ValueError(f"No user found for index {index}")

    @staticmethod
    def get_user_index_by_uuid(user_uuid: uuid.UUID) -> int:
        row = session.execute(
            "SELECT user_index FROM user_details WHERE user_id = %s", (user_uuid,)
        ).one()
        if row:
            return row["user_index"]
        raise ValueError(f"No index found for UUID {user_uuid}")


    @staticmethod
    def get_conversation_uuid_by_index(index: int) -> uuid.UUID:
        try:
            print(f"Looking up conversation_id for index {index}")
            
            # ALLOW FILTERING 
            query = """
                SELECT conversation_id FROM conversation_metadata 
                WHERE conversation_index = %s
                ALLOW FILTERING
            """
            
            print(f"⚙️ Executing query: {query.strip()}")
            result = session.execute(query, (index,))
            row = result.one()
            
            if row:
                print(f"Found conversation_id: {row['conversation_id']}")
                return row["conversation_id"]
            
            print(f"No conversation found for index {index}")
            raise ValueError(f"No conversation found for index {index}")
        except Exception as e:
            print(f"Error in get_conversation_uuid_by_index: {str(e)}")
            raise

    @staticmethod
    def get_conversation_index_by_uuid(conv_uuid: uuid.UUID) -> int:
        row = session.execute(
            "SELECT conversation_index FROM conversation_metadata WHERE conversation_id = %s", (conv_uuid,)
        ).one()
        if row:
            return row["conversation_index"]
        raise ValueError(f"No index found for UUID {conv_uuid}")
