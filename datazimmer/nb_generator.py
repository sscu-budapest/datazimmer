import json


def get_nb_string(title, sources):
    return json.dumps(get_nb_dic(title, sources))


def get_nb_dic(title, sources):
    return {
        "cells": [
            {
                "cell_type": "markdown",
                "id": "title-cell",
                "metadata": {},
                "source": [f"# {title}"],
            },
            *[
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "id": f"t-{hex(i)}",
                    "metadata": {},
                    "outputs": [],
                    "source": s,
                }
                for i, s in enumerate(sources)
            ],
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.8.10",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
