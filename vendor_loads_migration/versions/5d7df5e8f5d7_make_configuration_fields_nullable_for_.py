"""Make configuration fields nullable for setup

Revision ID: 5d7df5e8f5d7
Revises: 55557ab268c9
Create Date: 2023-05-05 08:58:19.219506

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5d7df5e8f5d7'
down_revision = '55557ab268c9'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('vendor_interfaces', 'folio_data_import_profile_uuid',
               existing_type=sa.VARCHAR(length=36),
               nullable=True)
    op.alter_column('vendor_interfaces', 'folio_data_import_processing_name',
               existing_type=sa.VARCHAR(length=50),
               nullable=True)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('vendor_interfaces', 'folio_data_import_processing_name',
               existing_type=sa.VARCHAR(length=50),
               nullable=False)
    op.alter_column('vendor_interfaces', 'folio_data_import_profile_uuid',
               existing_type=sa.VARCHAR(length=36),
               nullable=False)
    # ### end Alembic commands ###