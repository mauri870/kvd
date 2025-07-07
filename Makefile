all: build

build:
	go build .

test:
	go test -v ./...

clean:
	rm ./kvd || true
	rm -r ./node* || true