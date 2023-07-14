import typer

from hackthon.s3.metric import MetricDump

app = typer.Typer()


@app.command()
def get_metrics():
    MetricDump().dump()


if __name__ == '__main__':
    app()
