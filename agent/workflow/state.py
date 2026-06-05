from typing import TypedDict, Optional


class AgentState(TypedDict):
    """
    Represents the shared memory and context state for the LangGraph agent.

    Attributes:
        question (str): The active query from the user.
        thought (str): Chain-of-Thought (CoT) analytical planning reasoning.
        intent (str): The classified query intent (strict_quantitative, vague_analytical, refusal).
        planned_sql (str): Executable and safe ClickHouse SQL query syntax.
        sql_results (str): Formatted rows returned from ClickHouse.
        row_count (int): Length of dataset rows retrieved.
        execution_error (Optional[str]): Failure descriptions if safety checks or queries error out.
        response (str): Final synthesized markdown dashboard or refuse statement.
    """
    question: str
    thought: str
    intent: str
    planned_sql: str
    sql_results: str
    row_count: int
    execution_error: Optional[str]
    response: str
