from typing import Optional
from datetime import datetime
import uuid
from fastapi import HTTPException, status
from app.models.cassandra_models import MessageModel,ConversationModel

from app.schemas.message import MessageCreate, MessageResponse, PaginatedMessageResponse

class MessageController:
    """
    Controller for handling message operations
    This is a stub that students will implement
    """
    
    async def send_message(self, message_data: MessageCreate) -> MessageResponse:
        try:
            sender_uuid = ConversationModel.get_user_uuid_by_index(message_data.sender_id)
            receiver_uuid = ConversationModel.get_user_uuid_by_index(message_data.receiver_id)

            print("Sender UUID:", sender_uuid)
            print("Receiver UUID:", receiver_uuid)

            conversation_id = ConversationModel.create_or_get_conversation(
                user_ids=[sender_uuid, receiver_uuid]
            )

            message = MessageModel.create_message(
                conversation_id=conversation_id,
                sender_id=sender_uuid,
                receiver_id=receiver_uuid,
                content=message_data.content
            )
            return MessageResponse(
                id=1,
                sender_id=message_data.sender_id,
                receiver_id=message_data.receiver_id,
                content=message["content"],
                created_at=message["timestamp"],
                conversation_id=message_data.sender_id
            )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to send message: {str(e)}"
            )
    
    async def get_conversation_messages(
        self,
        conversation_id: int,
        page: int = 1,
        limit: int = 20
    ) -> PaginatedMessageResponse:
        try:
            print("Fetching UUID for conversation index:", conversation_id)
            conversation_uuid = ConversationModel.get_conversation_uuid_by_index(conversation_id)
            print("Found UUID:", conversation_uuid)

            print(f"Fetching messages: page={page}, limit={limit}")
            messages = MessageModel.get_conversation_messages(
                conversation_id=conversation_uuid,
                page=page,
                limit=limit
            )
            print(f"Retrieved {len(messages)} messages")

            formatted = []
            for m in messages:
                sender_index = ConversationModel.get_user_index_by_uuid(m["sender_id"])
                receiver_index = ConversationModel.get_user_index_by_uuid(m["receiver_id"])
                formatted.append(MessageResponse(
                    id=1,
                    content=m["content"],
                    sender_id=sender_index,
                    receiver_id=receiver_index,
                    created_at=m["timestamp"],
                    conversation_id=conversation_id
                ))
            print("Formatted all messages")

            return PaginatedMessageResponse(
                data=formatted,
                total=len(formatted),
                page=page,
                limit=limit
            )

        except Exception as e:
            print("Error in get_conversation_messages:", str(e)) 
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch messages: {str(e)}"
            )


    
    async def get_messages_before_timestamp(
        self,
        conversation_id: str,
        before_timestamp: datetime,
        page: int = 1,
        limit: int = 20
    ) -> PaginatedMessageResponse:
        try:
            conversation_uuid = ConversationModel.get_conversation_uuid_by_index(conversation_id)
            messages = MessageModel.get_messages_before_timestamp(
                conversation_id=conversation_uuid,
                before=before_timestamp,
                page=page,
                limit=limit
            )
            formatted = []
            for m in messages:
                formatted.append(MessageResponse(
                    id=1,
                    content=m["content"],
                    sender_id=ConversationModel.get_user_index_by_uuid(m["sender_id"]),
                    receiver_id=ConversationModel.get_user_index_by_uuid(m["receiver_id"]),
                    created_at=m["timestamp"],
                    conversation_id=conversation_id
                ))

            return PaginatedMessageResponse(
                data=formatted,
                total=len(formatted),
                page=page,
                limit=limit
            )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch messages before timestamp: {str(e)}"
            )