"""add artist and release tables

Revision ID: ed4bbf36b3ab
Revises: 
Create Date: 2022-12-28 15:04:47.659666

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ed4bbf36b3ab'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('artist',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=256), nullable=False),
    sa.Column('discogs_id', sa.Integer(), nullable=False),
    sa.Column('page_url', sa.String(length=256), nullable=False),
    sa.Column('image_url', sa.String(length=256), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_artist_discogs_id'), 'artist', ['discogs_id'], unique=False)
    op.create_index(op.f('ix_artist_id'), 'artist', ['id'], unique=False)
    op.create_table('release',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=256), nullable=False),
    sa.Column('discogs_id', sa.Integer(), nullable=False),
    sa.Column('page_url', sa.String(length=256), nullable=False),
    sa.Column('year', sa.SmallInteger(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_release_discogs_id'), 'release', ['discogs_id'], unique=False)
    op.create_index(op.f('ix_release_id'), 'release', ['id'], unique=False)
    op.create_table('artist_release',
    sa.Column('artist_id', sa.Integer(), nullable=True),
    sa.Column('release_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['artist_id'], ['artist.id'], ),
    sa.ForeignKeyConstraint(['release_id'], ['release.id'], )
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('artist_release')
    op.drop_index(op.f('ix_release_id'), table_name='release')
    op.drop_index(op.f('ix_release_discogs_id'), table_name='release')
    op.drop_table('release')
    op.drop_index(op.f('ix_artist_id'), table_name='artist')
    op.drop_index(op.f('ix_artist_discogs_id'), table_name='artist')
    op.drop_table('artist')
    # ### end Alembic commands ###
