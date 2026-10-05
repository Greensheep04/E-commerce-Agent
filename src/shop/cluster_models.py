from agent_cluster.store import Base as ClusterBase

from shop.models import Base

for table in ClusterBase.metadata.sorted_tables:
    table.to_metadata(Base.metadata)
