GIT_COMMIT = $(shell git log -1 --pretty=format:%h)
BUILD_DATE = $(shell date +%Y%m%d)
IMAGE_TAG = ${GIT_COMMIT}-${BUILD_DATE}

# Image URL to use all building/pushing image targets
CONTROLLER_IMAGE ?= docker-image:${IMAGE_TAG}


# Build the mananger docker image
docker-build-manager:
	docker build . -f Dockerfile -t ${CONTROLLER_IMAGE}
	@echo "updating deploy file for manager controller"
	sed -i'' -e 's@image: .*@image: '"${CONTROLLER_IMAGE}"'@' ./operator.yml
	docker push ${CONTROLLER_IMAGE}

docker-push:
	docker push ${CONTROLLER_IMAGE}