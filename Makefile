all:

.PHONY: test
test:
	docker-compose exec -T -- wes-data-sharing-service python3 test.py

.PHONY: svc-up
svc-up:
	docker-compose up -d --build

.PHONY: svc-down
svc-down:
	docker-compose down --volumes
