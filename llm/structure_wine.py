from typing import List, Optional

from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from langchain_openai import ChatOpenAI
from langsmith import traceable
from openai import OpenAI
from pydantic import BaseModel, Field

from core.wine.model import Wine
from core.wine.wine_searcher import batch_fetch_wines


class WineOutput(BaseModel):
    has_wine: bool = Field(
        description="Whether the context has information of wines. General grape or producer information is not a wine."
    )
    wines: Optional[List[str]] = Field(
        description="The wine names referred in the context"
    )


parser = PydanticOutputParser(pydantic_object=WineOutput)


def extract_wine_chain():
    model = ChatOpenAI(model_name="gpt-4o-mini", temperature=0)
    prompt = PromptTemplate(
        template="Given the following context, find the wine name in the context. A wine name usually includes winery, region and vintage. :\n <context>{context}</context>\n{format_instructions}",
        input_variables=["context"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )
    chain = prompt | model | parser
    return chain


@traceable(name="extract_wines")
def extract_wines_llm(
    text_input: Optional[str] = None, image_url: Optional[str] = None
) -> str:
    if not text_input and not image_url:
        raise ValueError("Either text_input or image_url must be provided")
    messages = [
        {
            "role": "system",
            "content": [
                {
                    "type": "text",
                    "text": """
                you are a wine expert. Your task is to extract the complete wine names given the context or image.
                A wine name usually includes winery, region and vintage.
            """,
                }
            ],
        },
    ]
    if image_url:
        messages.append(
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Extract the wine names from the image:"},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ],
            }
        )

    if text_input:
        messages.append(
            {"role": "user", "content": [{"type": "text", "text": text_input}]}
        )
    client = OpenAI()
    response = client.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0.7,
        response_format=WineOutput,
    )
    return response.choices[0].message.parsed


async def extact_wines(
    text_input: Optional[str] = None, image_url: Optional[str] = None
) -> List[Wine]:
    result = extract_wines_llm(text_input, image_url)
    if result.has_wine:
        wine_names = result.dict().get("wines", [])
        return await batch_fetch_wines(wine_names)
    return []
