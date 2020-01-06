FOS_CONF_FILE = /etc/fos/agent.json


all:

ifeq "$(INTRAVIS)" "true"
	echo "Nothing to do"
else
	dune build
endif

clean:

ifeq "$(INTRAVIS)" "true"
	echo "Nothing to do"
else
	dune clean
endif

test:
	echo "Nothing to do"

install:
	sudo cp _build/default/fos-agent/fos_agent.exe /etc/fos/agent
	sudo cp etc/fos_agent.service /lib/systemd/system/
	sudo cp etc/fos_agent.target /lib/systemd/system/
ifeq "$(wildcard $(FOS_CONF_FILE))" ""
	sudo cp etc/agent.json /etc/fos/agent.json
endif