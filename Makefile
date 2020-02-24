

FOS_DIR = /etc/fos
SYSTEMD_DIR = /lib/systemd/system
FOS_CONF_FILE = $(FOS_DIR)/agent.json


all:

	dune build

clean:
	dune clean


test:
	echo "Nothing to do"

install:
	install -m 0755 _build/default/fos-agent/fos_agent.exe $(FOS_DIR)/agent
	install etc/fos_agent.service $(SYSTEMD_DIR)
	install etc/fos_agent.target $(SYSTEMD_DIR)
ifeq "$(wildcard $(FOS_CONF_FILE))" ""
	install etc/agent.json  $(FOS_DIR)/agent.json
endif