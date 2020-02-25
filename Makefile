

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
	mkdir -p $(FOS_DIR)
	install -m 0755 _build/default/fos-agent/fos_agent.exe $(FOS_DIR)/agent
	install -m 0755 to_uuid.sh $(FOS_DIR)/to_uuid.sh
	install etc/fos_agent.service $(SYSTEMD_DIR)
	install etc/fos_agent.target $(SYSTEMD_DIR)
ifeq "$(wildcard $(FOS_CONF_FILE))" ""
	install etc/agent.json  $(FOS_DIR)/agent.json
endif