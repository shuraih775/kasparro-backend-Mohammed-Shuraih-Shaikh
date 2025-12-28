up:
	docker-compose  up -d --build db api

run-etl-cron:
	docker-compose up -d --build etl-cron

run-etl:
	docker-compose run --rm --build etl-once

down:
	docker-compose  down -v

test:
	docker-compose up -d db 
	docker-compose run --rm --build test
	docker-compose down -v


migrate-new:
	@if [ -z "$(msg)" ]; then \
		echo "ERROR: msg is required. Usage: make migrate-new msg=\"message\""; \
		exit 1; \
	fi
	docker-compose  exec api alembic revision --autogenerate -m "$(msg)"

migrate:
	docker-compose  exec api alembic upgrade head

