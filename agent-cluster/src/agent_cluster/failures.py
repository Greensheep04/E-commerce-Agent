def error_leaves(error):
    if isinstance(error, BaseExceptionGroup):
        return [leaf for child in error.exceptions for leaf in error_leaves(child)]
    return [error]


def failure_status(error):
    leaves = error_leaves(error)
    names = {type(leaf).__name__ for leaf in leaves}
    if names & {"UnknownRequest", "ReadTimeout", "TimeoutError", "MCPError"}:
        return "unknown"
    if names & {"BudgetExceeded", "RequestLimit"}:
        return "budget_exhausted"
    return "failed"


def failure_detail(error):
    return " | ".join(f"{type(leaf).__name__}: {leaf}" for leaf in error_leaves(error))
