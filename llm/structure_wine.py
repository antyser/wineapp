from typing import List, Optional

from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.pydantic_v1 import BaseModel, Field
from langchain_openai import ChatOpenAI


class Wine(BaseModel):
    name: str = Field(
        description="The name of the wine. Usually it includes the winery, region and vintage. And there should be a space between wine name and the vintage. i.e. Opus One 2013 instead of Opus One2013"
    )
    image: Optional[str] = Field(
        description="The image of the wine. Leave blank if you cannot find it from the context"
    )
    region: Optional[str] = Field(description="The region of the wine")
    producer: Optional[str] = Field(description="The producer of the wine")
    vintage: Optional[str] = Field(description="The vintage of the wine")
    type: Optional[str] = Field(
        description="The type of the wine. If it is wine, specify the type of wine, e.g. red, white, sparkling, rose etc. Otherwise, try your best to fill it such as beer, sake, whiskey, etc. If you are not sure, leave it blank."
    )


class WineOutput(BaseModel):
    has_wine: bool = Field(description="Whether the context has wine information")
    wines: Optional[List[Wine]] = Field(description="The wines referred in the context")


parser = PydanticOutputParser(pydantic_object=WineOutput)


def extract_wine_chain():
    model = ChatOpenAI(model_name="gpt-4o-mini", temperature=0)
    prompt = PromptTemplate(
        template="Given the following context, the wine name and the wine image must exist in the context:\n <context>{context}</context>\n{format_instructions}",
        input_variables=["context"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )
    chain = prompt | model | parser
    return chain


def extact_wines(context: str) -> List[Wine]:
    chain = extract_wine_chain()
    result = chain.invoke({"context": context})
    if result.has_wine:
        return result.dict().get("wines", [])
    return []


# Example usage
if __name__ == "__main__":
    context = "which one is better? Opus one 2013, Lafite 2013"
    wines = extact_wines(context)
    if wines:
        print(wines)
