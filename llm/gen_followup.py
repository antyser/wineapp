from typing import Dict, List

from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.pydantic_v1 import BaseModel, Field
from langchain_openai import ChatOpenAI


class Followups(BaseModel):
    followups: List[str] = Field(description="List of follow-up questions")


parser = PydanticOutputParser(pydantic_object=Followups)


def create_followup_chain():
    model = ChatOpenAI(model_name="gpt-4o-mini", temperature=0.7)
    prompt = PromptTemplate(
        template="Given the following context, generate {n} follow-up questions and extract the wines referred in the context:\nContext: {context}\n{format_instructions}",
        input_variables=["context", "n"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )
    chain = prompt | model | parser
    return chain


def generate_followups(context: str, n: int) -> Dict:
    chain = create_followup_chain()
    result = chain.invoke({"context": context, "n": n})
    return result.dict()


# Example usage
if __name__ == "__main__":
    context = "which one is better? Opus one 2013, Lafite 2013"
    n = 3
    followup_questions = generate_followups(context, n)
    if followup_questions:
        print(followup_questions)
