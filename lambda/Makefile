build-FirewallaExporter:
	GOARCH=arm64 GOOS=linux go build -tags lambda.norpc -o bootstrap 
	upx -9 bootstrap
	chmod +x bootstrap

build:
	GOARCH=arm64 GOOS=linux go build -tags lambda.norpc -o bootstrap 
	upx -9 bootstrap
	chmod +x bootstrap
