from core.clients.supabase import get_client


def delete_user(user_id: str):
    client = get_client()
    response = client.auth.admin.delete_user(user_id)
    return response
