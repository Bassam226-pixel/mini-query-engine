from engine.optimizer import optimize
from engine.planner import build


def run(logical_plan):
    optimized = optimize(logical_plan)
    physical = build(optimized)
    return physical.execute()


def explain(logical_plan):
    """Optimize and return the plan without executing it."""
    return optimize(logical_plan)