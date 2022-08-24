all:

.PHONY: test
test:
	python3 -m unittest -v test.py

.PHONY: svc-up
svc-up:
	docker-compose up -d

.PHONY: svc-down
svc-down:
	docker-compose down --volumes
