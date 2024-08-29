from typing import Dict, List, Optional, Tuple

from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from langchain_openai import ChatOpenAI
from langsmith import traceable
from loguru import logger
from pydantic import BaseModel, Field

from core.clients.openai import get_client
from core.timer import timer
from core.wine.model import Wine
from core.wine.wine_searcher import batch_fetch_wines


class WineOutput(BaseModel):
    has_wine: bool = Field(
        description="Whether the context has information of wines. General grape or producer information is not a wine."
    )
    wines: Optional[List[str]] = Field(
        description="The wine names referred in the context"
    )
    need_further_action: bool = Field(
        description="Determine if the LLM needs further action to complete the task."
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
@timer
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
                Think step by step and provide the final output. The wine name must include winery. Most of the time, it also includes region and vintage.
                Don't include the format or status information such as Magnum, 750ml, OWC, etc.
                If the user has further request other than looking for the wine information, please set need_further_action to True.
                You don't need to proceed the request or explain it. Don't duplcate wine names.
                <Example>
                <Context>
                I am looking for a wine that is a red wine from California.
                </Context>
                <Output>
                {
                    "has_wine": False,
                    "wines": [],
                    "need_further_action": True
                }
                </Output>
                <Explanation>
                The user doesn't specify the wine, so we need to further investigate.
                </Explanation>
                </Example>
                
                <Example>
                <Context>
                Tell me about Opus One 2013.
                </Context>
                <Output>
                {
                    "has_wine": True,
                    "wines": ["Opus One 2013"],
                    "need_further_action": False
                }
                </Output>
                <Explanation>
                The user specifies the wine, so we don't need to further investigate.
                </Explanation>
                </Example>

                <Example>
                <Context>
                Which one is from Napa Valley, 2013 Opus One or 2013 Lafite Rothschild?
                </Context>
                <Output>
                {
                    "has_wine": True,
                    "wines": ["2013 Opus One", "2013 Lafite Rothschild"],
                    "need_further_action": True
                }
                </Output>
                <Explanation>
                The user specifies the wine, and ask which one is from Napa Valley. We need to further investigate.
                </Explanation>
                </Example>
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
    client = get_client()
    response = client.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0.7,
        response_format=WineOutput,
    )
    return response.choices[0].message.parsed


@timer
async def extract_wines(
    text_input: Optional[str] = None, image_url: Optional[str] = None
) -> Tuple[Dict[str, Optional[Wine]], bool]:
    result = extract_wines_llm(text_input, image_url)

    logger.info(f"extract_wines_llm result: {result}")

    if result.has_wine:
        wine_names = result.dict().get("wines", [])
        wines_dict = await batch_fetch_wines(wine_names, is_pro=True)
        return wines_dict, result.need_further_action

    return {}, False
