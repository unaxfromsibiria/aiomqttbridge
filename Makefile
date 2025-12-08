lint:
	flake8 ./

isort:
	isort -c --diff ./

fisort:
	isort -rc ./

example_server:
	mkdir -p server
	rm -rf server/*
	cp example/server-docker-compose.yml server/docker-compose.yml
	cp example/server.Dockerfile server/server.Dockerfile
	cp example/mosquitto.Dockerfile server/mosquitto.Dockerfile
	cp requirements.txt server/requirements.txt
	cd server && mkdir .mqtt_pass && touch .mqtt_pass/password.txt
	cp -R aiomsgbridge server/aiomsgbridge
	@echo 'Edit all lines:'
	cat server/docker-compose.yml | grep -e '>' | grep -e '<'

example_client:
	mkdir -p client
	rm -rf client/*
	cp example/client-docker-compose.yml client/docker-compose.yml
	cp example/client.Dockerfile client/client.Dockerfile
	cp requirements.txt client/requirements.txt
	cp -R aiomsgbridge client/aiomsgbridge
	@echo 'Edit all lines:'
	cat client/docker-compose.yml | grep -e '>' | grep -e '<'

mqttpassword:
	@echo 'New password for user "connection" (you can edit the name)'
	cd server/ && docker compose exec mqtt-server /opt/make_user.sh connection
