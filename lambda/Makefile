build-FirewallaExporter:
	GOARCH=arm64 GOOS=linux go build -tags lambda.norpc -o bootstrap 
	upx -9 bootstrap
	cp ./bootstrap $(ARTIFACTS_DIR)/.

build:
	GOARCH=arm64 GOOS=linux go build -tags lambda.norpc -o bootstrap 
	upx -9 bootstrap
	cp ./bootstrap $(ARTIFACTS_DIR)/.

local: build
	sam local invoke

deploy: build
	sam deploy