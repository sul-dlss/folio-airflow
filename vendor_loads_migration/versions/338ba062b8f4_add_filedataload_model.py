"""add FileDataLoad model

Revision ID: 338ba062b8f4
Revises: 0690013481e5
Create Date: 2023-05-08 17:19:08.865870

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '338ba062b8f4'
down_revision = '0690013481e5'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('file_data_loads',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('created', sa.DateTime(), nullable=False),
    sa.Column('updated', sa.DateTime(), nullable=False),
    sa.Column('vendor_file_id', sa.Integer(), nullable=True),
    sa.Column('dag_run_id', sa.String(length=36), nullable=False),
    sa.Column('dag_run_starttime', sa.DateTime(), nullable=True),
    sa.Column('dag_run_endtime', sa.DateTime(), nullable=True),
    sa.Column('status', sa.Enum('not_loaded', 'loading_error', 'loaded', name='dataloadstatus'), server_default='not_loaded', nullable=False),
    sa.Column('additional_info', sa.Text(), nullable=True),
    sa.ForeignKeyConstraint(['vendor_file_id'], ['vendor_files.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('dag_run_id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('file_data_loads')
    # ### end Alembic commands ###
