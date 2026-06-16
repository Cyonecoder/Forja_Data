from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode

from forjaagent.schemas.state import AgentState
from forjaagent.graph.nodes import (
    inputguardrail_fn,
    outputguardrail_fn,
    supervisor_fn,
    toolaudit_fn,
    toolgate_fn,
)
from forjaagent.tools.pipeline import getpipelinestatus


tools = [getpipelinestatus]


def route_after_inputguardrail(state):
    return END if state.get("refusal") else "supervisor"


def route_after_supervisor(state):
    last = state["messages"][-1]
    tool_calls = getattr(last, "tool_calls", []) or []
    return "toolgate" if tool_calls else "outputguardrail"


def route_after_toolgate(state):
    return "toolnode"


def build_graph():
    graph = StateGraph(AgentState)

    graph.add_node("inputguardrail", inputguardrail_fn)
    graph.add_node("supervisor", supervisor_fn)
    graph.add_node("toolgate", toolgate_fn)
    graph.add_node("toolnode", ToolNode(tools))
    graph.add_node("toolaudit", toolaudit_fn)
    graph.add_node("outputguardrail", outputguardrail_fn)

    graph.add_edge(START, "inputguardrail")
    graph.add_conditional_edges("inputguardrail", route_after_inputguardrail)
    graph.add_conditional_edges("supervisor", route_after_supervisor)
    graph.add_conditional_edges("toolgate", route_after_toolgate)
    graph.add_edge("toolnode", "toolaudit")
    graph.add_edge("toolaudit", "supervisor")
    graph.add_edge("outputguardrail", END)

    return graph.compile()