.PHONY: build
build:
	sam build --parallel --cached

.PHONY: init
init:
	sam build --parallel --cached
	sam deploy -g

.PHONY: deploy
deploy:
	sam build --parallel --cached
	sam deploy

.PHONY: local
local:
	sam build --parallel --cached
	sam local invoke