from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime
from decimal import Decimal
from uuid import UUID


class AssetMarketData(BaseModel):
    asset_id: UUID
    source: str

    price_usd: Decimal = Field(gt=0)
    market_cap_usd: Decimal | None = Field(default=None, ge=0)
    volume_24h_usd: Decimal | None = Field(default=None, ge=0)

    last_updated: datetime
    created_at: datetime

    model_config = ConfigDict(extra="forbid")
