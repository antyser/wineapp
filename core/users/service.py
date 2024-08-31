from core.clients.supabase import get_client


async def delete_user(user_id: str):
    client = await get_client()
    await client.auth.admin.delete_user(user_id)
