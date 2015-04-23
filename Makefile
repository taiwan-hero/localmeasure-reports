.PHONY: test

clean:
	find . -name \*.pyc -exec rm {\} \;
run:
	python app.py
test:
	python setup.py test
ci-test:
	python setup.py test --with-xunit
develop:
	mkdir -p hadoop-binaries
	cd hadoop-binaries
	wget http://archive.apache.org/dist/hadoop/common/hadoop-2.4.1/hadoop-2.4.1.tar.gz
	wget https://archive.apache.org/dist/pig/pig-0.13.0/pig-0.13.0.tar.gz
	tar xzf hadoop-2.4.1.tar.gz
	tar xzf pig-0.13.0.tar.gz
