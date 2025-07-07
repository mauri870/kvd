all: build

build:
	go build .

test:
	go test -v ./...

cluster:
	go tool goreman start

clean:
	rm ./kvd || true
	rm -r ./node* || true
