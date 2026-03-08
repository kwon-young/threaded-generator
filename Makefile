.PHONY: docs release

docs:
	sphinx-build -b html doc_source docs

release: docs
	$(eval VERSION := $(shell python -c "import tomllib; print(tomllib.load(open('pyproject.toml', 'rb'))['project']['version'])"))
	git commit -am "build: update documentation for v$(VERSION)" || true
	git push
	git tag v$(VERSION)
	git push origin v$(VERSION)
