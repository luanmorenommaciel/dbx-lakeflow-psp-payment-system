# Evaluation Framework Pattern

> **Purpose**: LLM and RAG evaluation using LLM-as-judge, RAGAS, DeepEval, Braintrust, and automated quality pipelines
> **MCP Validated**: 2026-03-26

## When to Use

- Measuring RAG pipeline quality (faithfulness, relevance, recall)
- Evaluating LLM output quality without manual annotation
- A/B testing prompt versions or model changes
- Continuous quality monitoring in production
- Agent task completion evaluation

## Implementation

```python
from dataclasses import dataclass, field
from typing import Optional, Callable

@dataclass
class EvalSample:
    question: str
    contexts: list[str]
    answer: str
    ground_truth: Optional[str] = None

@dataclass
class EvalResult:
    sample: EvalSample
    scores: dict[str, float]
    feedback: dict[str, str] = field(default_factory=dict)

class EvaluationFramework:
    def __init__(self, judge_model: str = "claude-opus-4-6"):
        self.judge_model = judge_model
        self.metrics: dict[str, Callable] = {}
        self._register_defaults()

    def _register_defaults(self):
        self.metrics["faithfulness"] = self._eval_faithfulness
        self.metrics["answer_relevancy"] = self._eval_answer_relevancy
        self.metrics["context_precision"] = self._eval_context_precision
        self.metrics["hallucination"] = self._eval_hallucination

    def evaluate(self, samples: list[EvalSample],
                 metrics: Optional[list[str]] = None) -> list[EvalResult]:
        """Evaluate a batch of samples across specified metrics."""
        metrics = metrics or list(self.metrics.keys())
        results = []
        for sample in samples:
            scores, feedback = {}, {}
            for name in metrics:
                scorer = self.metrics.get(name)
                if scorer:
                    score, reason = scorer(sample)
                    scores[name] = score
                    feedback[name] = reason
            results.append(EvalResult(sample=sample, scores=scores, feedback=feedback))
        return results

    def _eval_faithfulness(self, sample: EvalSample) -> tuple[float, str]:
        """Check if answer is grounded in provided contexts."""
        prompt = f"""Judge faithfulness. What fraction of claims in the ANSWER
are supported by the CONTEXT?
CONTEXT: {chr(10).join(sample.contexts)}
ANSWER: {sample.answer}
Score 0.0-1.0. JSON: {{"score": <float>, "reason": "<str>"}}"""
        result = self._call_judge(prompt)
        return result["score"], result["reason"]

    def _eval_hallucination(self, sample: EvalSample) -> tuple[float, str]:
        """Detect claims not supported by context (inverse of faithfulness)."""
        prompt = f"""Identify unsupported claims in the ANSWER.
CONTEXT: {chr(10).join(sample.contexts)}
ANSWER: {sample.answer}
Score 0.0 (no hallucination) to 1.0 (fully hallucinated).
JSON: {{"score": <float>, "reason": "<str>"}}"""
        result = self._call_judge(prompt)
        return result["score"], result["reason"]
```

## Evaluation Tools Comparison (2026)

| Tool | Type | Strengths | Best For |
|------|------|-----------|----------|
| RAGAS | OSS | Standard RAG metrics, component-level eval | RAG pipeline quality |
| DeepEval | OSS | 14+ metrics, pytest integration, CI/CD | Testing-first teams |
| Braintrust | Commercial | Best eval pipeline, datasets, experiments | Enterprise eval |
| Langfuse | OSS | Tracing + evals, prompt management | Observability + eval |
| Arize Phoenix | OSS | Traces, embeddings, LLM evals | Visual debugging |
| LangSmith | Commercial | LangChain integration, playground | LangChain teams |

## RAGAS Integration

```python
from ragas import evaluate
from ragas.metrics import faithfulness, answer_relevancy, context_precision, context_recall
from datasets import Dataset

def evaluate_with_ragas(questions, contexts, answers, ground_truths=None):
    """Evaluate RAG pipeline using RAGAS framework."""
    data = {"question": questions, "contexts": contexts, "answer": answers}
    if ground_truths:
        data["ground_truth"] = ground_truths
    dataset = Dataset.from_dict(data)
    metrics = [faithfulness, answer_relevancy, context_precision]
    if ground_truths:
        metrics.append(context_recall)
    return evaluate(dataset=dataset, metrics=metrics)
```

## DeepEval Integration

```python
from deepeval import evaluate
from deepeval.metrics import FaithfulnessMetric, AnswerRelevancyMetric, HallucinationMetric
from deepeval.test_case import LLMTestCase

test_case = LLMTestCase(
    input="What is RAG?",
    actual_output="RAG retrieves documents before generation.",
    retrieval_context=["RAG augments LLM responses with retrieved knowledge."],
    expected_output="RAG retrieves external knowledge to augment generation.",
)

faithfulness = FaithfulnessMetric(threshold=0.85, model="claude-opus-4-6")
relevancy = AnswerRelevancyMetric(threshold=0.80)
hallucination = HallucinationMetric(threshold=0.3)

evaluate([test_case], [faithfulness, relevancy, hallucination])
```

## LLM-as-Judge Best Practices

| Practice | Description |
|----------|-------------|
| Use structured output | JSON responses with score + reason |
| Calibrate with humans | Align judge scores with expert annotations |
| Use frontier models | Claude Opus 4.6 or GPT-4o for judging |
| Include rubrics | Explicit scoring criteria in the prompt |
| Multi-judge consensus | Average scores from 3+ judge runs |
| Pairwise comparison | Compare two outputs side-by-side (more reliable) |

## CI/CD Quality Gate

```python
def ci_evaluation_gate(pipeline, test_dataset, thresholds):
    """Fail build if quality drops below thresholds."""
    results = evaluate_pipeline(pipeline, test_dataset)
    avg_scores = aggregate_scores(results)
    failures = []
    for metric, threshold in thresholds.items():
        if avg_scores.get(metric, 0) < threshold:
            failures.append(f"{metric}: {avg_scores[metric]:.2f} < {threshold}")
    if failures:
        raise QualityGateError(f"Quality gate failed: {'; '.join(failures)}")
    return avg_scores

thresholds = {
    "faithfulness": 0.85,
    "answer_relevancy": 0.80,
    "context_precision": 0.75,
    "hallucination": 0.20,  # max acceptable hallucination rate
}
ci_evaluation_gate(rag_pipeline, test_set, thresholds)
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `judge_model` | `claude-opus-4-6` | Model used for LLM-as-judge |
| `batch_size` | `50` | Samples per evaluation batch |
| `min_faithfulness` | `0.85` | Alert threshold for faithfulness |
| `min_relevancy` | `0.80` | Alert threshold for relevancy |
| `max_hallucination` | `0.20` | Alert threshold for hallucination |

## See Also

- [RAG Pipeline](../patterns/rag-pipeline.md)
- [RAG Architecture](../concepts/rag-architecture.md)
- [Guardrails](../concepts/guardrails.md)
