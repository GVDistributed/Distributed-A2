CS 454 Distributed - Assignment #2
Distributed Hash Table README.txt

Group:
  Aaron Voelker (20307917)
  Guru Guruganesh (20311789)

Steps To Install:
  1. Let <project> be the directory for the project
  2. Verify that Python2.6+ is installed via `python --version`
  3. http://www.cs.uwaterloo.ca/~bernard/courses/cs454/tutorial.pdf
       Remove --with-python=no
       And run the following before 'configure':
       export PY_PREFIX=<project>
  4. Add the Thrift library to Python's path:
       export PYTHONPATH=$PYTHONPATH:<project>/lib/python2.6/site-packages 

Steps To Run (Nothing different here):
  ./server node_id ip port
  ./server node_id ip port existing_ip existing_port

To Kill Servers (Note: only tested on school's linux environment):
  ./kill-server  (Kills all servers)
  ./kill-one-server NodeID

Miscellaneous Test Scripts:
  ./test-putget.py
  ./test-big.py
  ./test-migrate.py
  ./test-migrate-hard.py
  ./test-failure.py
  ./test-large-failure.py

To View Logged Output:
  tail -f log
