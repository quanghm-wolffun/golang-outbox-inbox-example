infras:
	docker compose -f ./docker-compose.infras.yaml up -d

dev:
	docker compose up

build:
	docker compose build app

clean:
	docker compose down && docker compose -f ./docker-compose.infras.yaml down