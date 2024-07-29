from typing import List

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
        template="Given the following context, generate {n} follow-up questions:\nContext: {context}\n{format_instructions}",
        input_variables=["context", "n"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )
    chain = prompt | model | parser
    return chain


def generate_followup_questions(context: str, n: int) -> List[str]:
    chain = create_followup_chain()
    result = chain.invoke({"context": context, "n": n})
    return result.followups


# Example usage
if __name__ == "__main__":
    context = "Wine is a popular alcoholic beverage made from fermented grapes."
    n = 3
    followup_questions = generate_followup_questions(context, n)
    if followup_questions:
        print(followup_questions)
