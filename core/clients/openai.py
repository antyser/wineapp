import openai
from langsmith.wrappers import wrap_openai

_client = None


def get_client():
    global _client
    if _client is None:
        _client = wrap_openai(openai.Client())
    return _client
