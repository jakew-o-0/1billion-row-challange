run:
	go run ./cmd/.

errless:
	go run ./cmd/. 2>&1 | less

test:
	go test ./cmd/. -bench="./cmd/." -cpuprofile='./profiles/cpu.prof' -memprofile='./profiles/mem.prof'
	go tool pprof -raw -output=./profiles/cpu.txt ./profiles/cpu.prof
	./tools/stackcollapse-go.pl ./profiles/cpu.txt | ./tools/flamegraph.pl > ./profiles/cpu.svg
	firefox ./profiles/cpu.svg
	go tool pprof -http=: ./profiles/mem.prof
