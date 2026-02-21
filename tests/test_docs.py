import pytest
from pytest_examples import CodeExample, EvalExample, find_examples


# @pytest.mark.parametrize("example", find_examples("docs", "docs/api.md"), ids=str)
@pytest.mark.parametrize("example", find_examples("docs/index.md"), ids=str)
def test_index_docs(example: CodeExample, eval_example: EvalExample):
    # eval_example.lint(example)
    eval_example.run(example)
