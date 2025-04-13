import uuid
from fastapi import HTTPException, status

from app.schemas.conversation import ConversationResponse, PaginatedConversationResponse
from app.models.cassandra_models import ConversationModel

class ConversationController:
    """
    Controller for handling conversation operations
    This is a stub that students will implement
    """
    
    async def get_user_conversations(
        self, 
        user_id: str,
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedConversationResponse:
        try:
            user_uuid = ConversationModel.get_user_uuid_by_index(user_id)
            conversations = ConversationModel.get_user_conversations(user_uuid, limit, page)
            formatted = []
            for convo in conversations:
                try:
                    convo_index = ConversationModel.get_conversation_index_by_uuid(convo["conversation_id"])
                    user1_index = ConversationModel.get_user_index_by_uuid(convo["user_id"])
                    other_user_ids = list(convo["other_participants"])
                    user2_index = ConversationModel.get_user_index_by_uuid(other_user_ids[0]) if other_user_ids else -1

                    formatted.append(ConversationResponse(
                        id=convo_index,
                        user1_id=user1_index,
                        user2_id=user2_index,
                        last_message_at=convo["last_updated_at"],
                        last_message_content=convo.get("last_message", "")
                    ))
                except Exception as e:
                    print(f"Skipping conversation due to error: {e}")
                    continue

            return PaginatedConversationResponse(
                total=len(formatted),
                page=page,
                limit=limit,
                data=formatted
            )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch conversations: {str(e)}"
            )
    
    async def get_conversation(self, conversation_id: str) -> ConversationResponse:
        try:
            convo_uuid = ConversationModel.get_conversation_uuid_by_index(int(conversation_id))
            participants = ConversationModel.get_conversation(convo_uuid)

            if not participants:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Conversation not found"
                )

            user1_index = ConversationModel.get_user_index_by_uuid(participants[0])
            user2_index = ConversationModel.get_user_index_by_uuid(participants[1]) if len(participants) > 1 else -1
            convo_index = ConversationModel.get_conversation_index_by_uuid(convo_uuid)

            last_message, last_time = ConversationModel.get_last_message_and_time(convo_uuid)

            return ConversationResponse(
                id=convo_index,
                user1_id=user1_index,
                user2_id=user2_index,
                last_message_at=last_time,
                last_message_content=last_message
            )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch conversation: {str(e)}"
            )
