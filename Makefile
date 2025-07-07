all: build

build:
	go build .

test:
	go test -v ./...

clean:
	rm ./kvd
	rm -r ./node*