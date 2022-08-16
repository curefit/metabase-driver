.PHONY: clone_metabase_if_missing clean link_to_driver front_end driver update_deps_files server test all release
.DEFAULT_GOAL := all
export MB_EDITION=ee

metabase_curefit_branch := version0.43.4

trino_version := $(shell jq '.trino' app_versions.json)
metabase_version := $(shell jq '.metabase' app_versions.json)

makefile_dir := $(dir $(realpath $(lastword $(MAKEFILE_LIST))))
is_trino_started := $(shell curl --fail --silent --insecure http://localhost:8080/v1/info | jq '.starting')

clone_metabase_if_missing:
ifeq ($(wildcard $(metabase_curefit_branch)/metabase/.),)
	@echo "Did not find metabase repo, cloning version $(metabase_version)..."; git clone -b $(metabase_curefit_branch) --single-branch https://github.com/curefit/metabase.git
else
	@echo "Found metabase repo, skipping initialization."
endif

checkout_latest_metabase_tag: clone_metabase_if_missing clean
	cd $(makefile_dir)/metabase; git fetch --all --tags;
	$(eval latest_metabase_version=$(shell cd $(makefile_dir)/metabase; git tag | egrep 'v[0-9]+\.[0-9]+\.[0-9]+' | sort | tail -n 1))
	@echo "Checking out latest metabase tag: $(latest_metabase_version)"
	cd $(makefile_dir)/metabase/modules/drivers && git checkout $(latest_metabase_version);
	sed -i.bak 's/metabase\": \".*\"/metabase\": \"$(latest_metabase_version)\"/g' app_versions.json; rm  ./app_versions.json.bak

start_trino_if_missing:
ifeq ($(is_trino_started),)
	@echo "Trino not started, starting using version $(trino_version)...";
	cd $(makefile_dir)/resources/docker/trino; docker build -t trino-metabase-test . --build-arg version=$(trino_version); docker run --rm -d  -p 8080:8080/tcp trino-metabase-test:latest
else
	@echo "Trino started, skipping initialization."
endif

clean:
	@echo "Force cleaning Metabase repo..."
	cd $(metabase_curefit_branch)metabase/modules/drivers && git reset --hard && git clean -f
	@echo "Checking out metabase at: $(metabase_version)"
	cd $(metabase_curefit_branch)metabase/modules/drivers && git checkout $(metabase_version);

link_to_driver:
ifeq ($(wildcard $(metabase_curefit_branch)metabase/modules/drivers/starburst/src),)
	@echo "Adding link to driver..."; ln -s ../../../drivers/starburst $(metabase_curefit_branch)/metabase/modules/drivers
else
	@echo "Driver found, skipping linking."
endif
	

front_end:
	@echo "Building Front End..."
	cd $(metabase_curefit_branch)/metabase/; yarn build && yarn build-static-viz

driver: update_deps_files
	@echo "Building Starburst driver..."
	cd $(metabase_curefit_branch)/metabase/; ./bin/build-driver.sh starburst

server: 
	@echo "Starting metabase..."
	cd $(metabase_curefit_branch)/metabase/; clojure -M:run

# This command adds the require starburst driver dependencies to the metabase repo.
update_deps_files:
	@if cd $(metabase_curefit_branch)/metabase && grep -q starburst deps.edn; \
		then \
			echo "Metabase deps file updated, skipping..."; \
		else \
			echo "Updating metabase deps file..."; \
			cd $(metabase_curefit_branch)/metabase/; sed -i.bak 's/\/test\"\]\}/\/test\" \"modules\/drivers\/starburst\/test\"\]\}/g' deps.edn; \
	fi

	@if cd $(metabase_curefit_branch)/metabase/modules/drivers && grep -q starburst deps.edn; \
		then \
			echo "Metabase driver deps file updated, skipping..."; \
		else \
			echo "Updating metabase driver deps file..."; \
			cd $(metabase_curefit_branch)/metabase/modules/drivers/; sed -i.bak "s/\}\}\}/\} \metabase\/starburst \{:local\/root \"starburst\"\}\}\}/g" deps.edn; \
	fi

test: start_trino_if_missing link_to_driver update_deps_files
	@echo "Testing Starburst driver..."
	cd $(metabase_curefit_branch)/metabase/; DRIVERS=starburst clojure -X:dev:drivers:drivers-dev:test

build: clone_metabase_if_missing update_deps_files link_to_driver front_end driver

docker-image:
	cd $(metabase_curefit_branch)/metabase/; export MB_EDITION=ee && ./bin/build && mv target/uberjar/metabase.jar bin/docker/ && docker build -t metabase-dev --build-arg MB_EDITION=ee ./bin/docker/