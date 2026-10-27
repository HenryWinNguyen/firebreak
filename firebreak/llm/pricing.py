"""Cost math from `pricing.yaml`."""

from pathlib import Path

import yaml
from pydantic import BaseModel

from firebreak.llm.errors import PricingError
from firebreak.models.llm import Usage

_MTOK = 1_000_000


class PriceTier(BaseModel):
    max_prompt_tokens: int | None = None
    input: float
    cache_write: float
    cache_read: float
    output: float


class ModelPricing(BaseModel):
    tiers: list[PriceTier]


class PricingTable(BaseModel):
    models: dict[str, ModelPricing]

    @classmethod
    def load(cls, path: Path) -> "PricingTable":
        return cls.model_validate(yaml.safe_load(path.read_text()))

    def cost_usd(self, model: str, usage: Usage) -> float:
        pricing = self.models.get(model)
        if pricing is None:
            raise PricingError(f"no pricing configured for model {model!r}")
        prompt_tokens = usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens
        tier = next(
            (
                t
                for t in pricing.tiers
                if t.max_prompt_tokens is None or prompt_tokens <= t.max_prompt_tokens
            ),
            None,
        )
        if tier is None:
            raise PricingError(f"no price tier covers {prompt_tokens} prompt tokens for {model!r}")
        return (
            usage.input_tokens * tier.input
            + usage.cache_write_tokens * tier.cache_write
            + usage.cache_read_tokens * tier.cache_read
            + usage.output_tokens * tier.output
        ) / _MTOK
