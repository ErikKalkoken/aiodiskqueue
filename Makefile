coverage:
	coverage run -m unittest discover -v && coverage html && coverage report -m

pylint:
	pylint src/aiodiskqueue
