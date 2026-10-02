.PHONY: up down logs seed demo check backup restore-verify kind-up
up:
	docker pull grafana/k6:0.57.0
	docker compose up -d --build --wait --wait-timeout 180
down:
	python3 scripts/shutdown.py
	docker compose down
logs:
	docker compose logs -f --tail=100
seed:
	docker compose exec -T postgres psql -U postgres -d commerce < db/seeds/products.sql
demo:
	python3 scripts/demo.py
check:
	docker compose config --quiet
	docker run --rm -v "$(CURDIR):/src" -v reliability-go-mod:/go/pkg/mod -v reliability-go-build:/root/.cache/go-build -w /src golang:1.24.2-alpine sh -c 'test -z "$$(gofmt -l internal services)" && GOMAXPROCS=2 go test -p 2 ./... && GOMAXPROCS=2 go build -p 2 ./...'
	docker run --rm -v "$(CURDIR)/apps/dashboard:/app" -v reliability-dashboard-deps:/app/node_modules -v reliability-dashboard-dist:/app/dist -w /app node:22.14.0-alpine sh -c 'npm ci && npm run build'
	python3 -m py_compile scripts/runner.py scripts/demo.py scripts/recovery.py scripts/shutdown.py scripts/replay.py
backup:
	python3 scripts/recovery.py backup
restore-verify:
	python3 scripts/recovery.py restore-verify
kind-up:
	bash scripts/kind-up.sh

replay-deadletters:
	python3 scripts/replay.py
