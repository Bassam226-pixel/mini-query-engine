from engine.optimizer import optimize
from engine.planner import build


def run(logical_plan):
    optimized = optimize(logical_plan)
    physical = build(optimized)
    return physical.execute()


def explain(logical_plan):
    """Optimize and return the plan without executing it."""
    return optimize(logical_plan)


def sql(query, catalog):
    """Parse SQL, optimize, and execute. Returns a batch generator."""
    from engine.sql import to_logical_plan
    return run(to_logical_plan(query, catalog))