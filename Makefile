all:

.PHONY: test
test:
	docker build -t wes-data-sharing-service .
	docker run --rm --network host --entrypoint=sh wes-data-sharing-service -c 'coverage run -m unittest -v test.py; coverage report'

.PHONY: svc-up
svc-up:
	docker-compose up -d

.PHONY: svc-down
svc-down:
	docker-compose down --volumes
