"""Add processed_filename to vendor_file

Revision ID: 697f20501d64
Revises: 6fcfcb9d2ee4
Create Date: 2023-06-08 14:58:42.519434

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '697f20501d64'
down_revision = '6fcfcb9d2ee4'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('vendor_files', sa.Column('processed_filename', sa.String(length=250), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('vendor_files', 'processed_filename')
    # ### end Alembic commands ###
