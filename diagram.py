from diagrams import Cluster, Diagram
from diagrams.k8s.compute import Pod
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.network import Nginx
from diagrams.generic.device import Mobile

graph_attr = {"fontsize": "22"}

with Diagram(
    "Telemetry Aggregator Architecture",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    outformat=["png"],
    filename="img/telemetry_aggregator_architecture.png",
):
    with Cluster("Edge Devices"):
        agent = Mobile("Telemetry Agent\n(Edge Kiosk)")

    with Cluster("Edge Gateway"):
        nginx = Nginx("API Gateway\n(Layer 7 / SSL)")

    with Cluster("Ingestion Cluster"):
        workers = Pod("Worker Pool\n(50 Workers)")

    with Cluster("Query Cluster"):
        api = Pod("API Server")

    with Cluster("Infrastructure Cluster"):
        db = PostgreSQL("Cold Storage")
        cache = Redis("Speed Layer")

    # Flow
    agent >> nginx >> workers
    workers >> db
    workers >> cache
    api << cache
