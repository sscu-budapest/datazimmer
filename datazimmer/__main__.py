import typer

from .explorer import build

typer_app = typer.Typer()


@typer_app.command()
def hello(name: str):
    typer.echo(f"Hello {name}")


@typer_app.command()
def goodbye(name: str, formal: bool = False):
    if formal:
        typer.echo(f"Goodbye Ms. {name}. Have a good day.")
    else:
        typer.echo(f"Bye {name}!")


@typer_app.command()
def build_explorer():
    typer.echo("Building thing")
    build()


if __name__ == "__main__":
    typer_app()
