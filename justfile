#!/usr/bin/env just --justfile

default:
    just --list

lint:
    poetry run flake8

format_all:
    poetry run black .
    poetry run isort --profile black .

all: lint format_all