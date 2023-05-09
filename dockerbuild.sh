#!/bin/bash
IMAGE_NAME=etl-modules
DOCKER_BUILDKIT=1 docker build --no-cache -t $IMAGE_NAME .