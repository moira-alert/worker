VERSION := $(shell git describe --always --tags --abbrev=0 | tail -c +2)
RELEASE := $(shell git describe --always --tags | awk -F- '{ if ($$2) dot="."} END { printf "1%s%s%s%s\n",dot,$$2,dot,$$3}')
PIP_VERSION := $(shell git describe --always --tags | tail -c +2 | awk -F- '{ if ($$2) printf "%s.dev%s-%s\n",$$1,$$2,$$3; else print $$1 }')
TRIAL := $(shell which trial)
PYTHON := $(shell which python)
PIP := $(shell which pip)

VENDOR := "SKB Kontur"
URL := "https://github.com/moira-alert"
LICENSE := "GPLv3"

default: clean prepare test pip

version:
	echo $(PIP_VERSION) > version.txt

prepare:
	$(PIP) install -r requirements.txt

test:
	cd tests && coverage run --source="../moira/" --omit="../moira/graphite/*,../moira/metrics/*" $(TRIAL) functional cache expression

pip: version
	$(PYTHON) setup.py sdist

clean:
	rm -rf build dist moira_worker.egg-info tests/_trial_temp

rpm: version
	fpm -t rpm \
		-s "python" \
		--description "Moira Worker" \
		--vendor $(VENDOR) \
		--url $(URL) \
		--license $(LICENSE) \
		--name "moira-worker" \
		--version "$(VERSION)" \
		--iteration "$(RELEASE)" \
		--after-install "./pkg/postinst" \
		--no-python-dependencies \
		-p build \
		setup.py
