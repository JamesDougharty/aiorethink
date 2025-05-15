#!/bin/bash

# Poetry automatically manages the virtual environment
# No need to check for active virtualenv

arg="tests/"
if [[ -n "$@" ]]; then
    arg="$@"
fi

poetry run pytest -s -x --cov=aiorethink --cov-report html --cov-report term --durations=10 $arg
