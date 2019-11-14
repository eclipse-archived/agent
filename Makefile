all:
	dune build
clean:
	dune clean


install:
	sudo cp _build/default/fos-agent/fos-agent.exe /etc/fos/agent