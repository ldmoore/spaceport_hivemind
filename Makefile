.PHONY: generate test demo
PYTHON ?= .venv/bin/python

generate:
	$(PYTHON) -m grpc_tools.protoc -I proto --python_out=src/spaceport_hivemind/generated proto/bazaar.proto

test:
	$(PYTHON) -m pytest

demo:
	$(PYTHON) -m spaceport_hivemind.demo
