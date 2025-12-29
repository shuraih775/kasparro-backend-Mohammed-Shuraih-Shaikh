"""add asset_sources table and normalize asset_id to uuid

Revision ID: dd2aab32ebbf
Revises: 2e958dcfba83
"""

from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "dd2aab32ebbf"
down_revision: Union[str, Sequence[str], None] = "2e958dcfba83"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')

    # 1. Drop FK safely using batch mode
    with op.batch_alter_table("asset_market_data") as batch_op:
        batch_op.drop_constraint(
            "asset_market_data_asset_id_fkey",
            type_="foreignkey",
        )

    # 2. Convert parent PK FIRST
    op.alter_column(
        "assets",
        "asset_id",
        existing_type=sa.TEXT(),
        type_=postgresql.UUID(),
        postgresql_using="asset_id::uuid",
        nullable=False,
    )

    # 3. Convert child FK column
    op.alter_column(
        "asset_market_data",
        "asset_id",
        existing_type=sa.TEXT(),
        type_=postgresql.UUID(),
        postgresql_using="asset_id::uuid",
        nullable=False,
    )

    # 4. Recreate FK
    with op.batch_alter_table("asset_market_data") as batch_op:
        batch_op.create_foreign_key(
            "asset_market_data_asset_id_fkey",
            "assets",
            ["asset_id"],
            ["asset_id"],
            ondelete="CASCADE",
        )

    # 5. Create asset_sources LAST
    op.create_table(
        "asset_sources",
        sa.Column("asset_id", postgresql.UUID(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_asset_id", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["asset_id"],
            ["assets.asset_id"],
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "source",
            "source_asset_id",
            name="uq_asset_sources_source_identity",
        ),
    )


def downgrade() -> None:
    op.drop_table("asset_sources")

    with op.batch_alter_table("asset_market_data") as batch_op:
        batch_op.drop_constraint(
            "asset_market_data_asset_id_fkey",
            type_="foreignkey",
        )

    op.alter_column(
        "asset_market_data",
        "asset_id",
        existing_type=postgresql.UUID(),
        type_=sa.TEXT(),
        nullable=False,
    )

    with op.batch_alter_table("asset_market_data") as batch_op:
        batch_op.create_foreign_key(
            "asset_market_data_asset_id_fkey",
            "assets",
            ["asset_id"],
            ["asset_id"],
        )

    op.alter_column(
        "assets",
        "asset_id",
        existing_type=postgresql.UUID(),
        type_=sa.TEXT(),
        nullable=False,
    )
