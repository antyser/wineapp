import openai
import tiktoken
from pinecone import Pinecone

TOKEN_LIMIT = 8191
INDEX = "wineknowledgetitle"
EMBEDDING_MODEL = "text-embedding-3-small"
pc = Pinecone(api_key="255b2419-7282-4973-bbcd-c01663bac986")


def count_tokens(text):
    enc = tiktoken.encoding_for_model(EMBEDDING_MODEL)
    return len(enc.encode(text))


def str_to_token(str, token_limit=TOKEN_LIMIT):
    enc = tiktoken.encoding_for_model(EMBEDDING_MODEL)
    tokens = enc.encode(str)
    return tokens[:token_limit]


def query_pinecone_index(query, limit=3):
    response = openai.embeddings.create(input=query, model=EMBEDDING_MODEL)
    query_embedding = response.data[0].embedding

    index = pc.Index(INDEX)
    query_results = index.query(
        vector=query_embedding, top_k=limit, include_metadata=True
    )

    return [result["metadata"]["content"] for result in query_results["matches"]]
