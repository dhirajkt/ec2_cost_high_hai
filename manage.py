import typer

from hackthon.metric import MetricDump
from hackthon.utilization import CalculateUtilization

app = typer.Typer()


@app.command()
def get_metrics():
    MetricDump().dump()


@app.command()
def calculate_utilization():
    CalculateUtilization('warehouse').calculate_metrics()


if __name__ == '__main__':
    app()
