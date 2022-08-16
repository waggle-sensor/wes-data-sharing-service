all:

.PHONY: test
test:
	python3 test.py

.PHONY: svc-up
svc-up:
	docker-compose up -d --build

.PHONY: svc-down
svc-down:
	docker-compose down --volumes
