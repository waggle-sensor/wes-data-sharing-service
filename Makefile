all:

.PHONY: test
test:
	docker build -t wes-data-sharing-service .
	docker run --rm --network host --entrypoint=python3 wes-data-sharing-service -m unittest -v test.py

.PHONY: svc-up
svc-up:
	docker-compose up -d

.PHONY: svc-down
svc-down:
	docker-compose down --volumes
