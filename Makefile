all: ebin/
	(cd src;$(MAKE) all)

clean:
	(cd src;$(MAKE) clean)

ebin/:
	@mkdir -p ebin
