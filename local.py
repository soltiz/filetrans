import sys
import subprocess
import os
import hashlib
import time

verbose = False

def send_data(data):
	if type(data) is str:
		remote.stdin.write((data+'\n').encode('ascii'))
	else:
		remote.stdin.write(data)
	remote.stdin.flush()


def transmit_file_data(path, block_size, start_from):
	file_size = os.path.getsize(path)
	if start_from != 0:
		print("Restarting data sending from position %d ..."%(start_from))
	start_time = time.time()
	total_sent=0
	with open(path, 'rb') as f:
		remaining = file_size - start_from
		f.seek(start_from)
		while remaining > 0:
			data = f.read(block_size)
			send_data(data)
			sent_bytes = len(data)
			remaining = remaining - sent_bytes
			total_sent = total_sent + sent_bytes
			curtime = time.time()
			elapsed = int(curtime - start_time)
			remaining_estimate = (remaining * elapsed) // total_sent
			if remaining_estimate > 10:
				if elapsed > 6:
					if remaining_estimate > 90:
						remaining_time = " (%d minutes remaining)"%(remaining_estimate//60 + 1)
					else:
						remaining_time = " (%s seconds remaining)"%(remaining_estimate)
				else:
					remaining_time = " (Computing remaining time estimation...)"
			else:
				remaining_time = ""

			print("Remaining bytes to send: %d%s"%(remaining, remaining_time))

def print_exception_and_exit():
	sys.stderr.write("REMOTE EXCEPTION !\n")
	for line in remote.stdout:
		sys.stderr.write(line.decode('ascii'))
	sys.stderr.flush()
	sys.exit(99)
	

def debug(message):
	if verbose:
		print(message)



def blocks_hashes(path, blocks_size):
	print("Computing block hashes...")
	hashes=[]
	with open(path, "rb") as source_file:
		remaining = os.path.getsize(path)
		block = 0
		while remaining > 0:
			m = hashlib.sha256()
			print("Remaining bytes to hash: %d"%(remaining))
			data = source_file.read(min (remaining, blocks_size))
			m.update(data)
			computed_hash = m.hexdigest()
			hashes.append(computed_hash)
			debug("Block #%d hash is '%s'"%(block, computed_hash))
			remaining = remaining - len(data)
			block = block + 1
	return hashes

sys.argv=sys.argv[1:]
file_path=None



while len(sys.argv) > 0:
	arg = sys.argv[0]
	sys.argv=sys.argv[1:]
	if arg.startswith('-'):
		if arg == '-v':
			verbose = True
			print("DEBUG/VERBOSE mode is active.")
		else:
			raise(Exception("Unknown option '%s'."%(arg)))
	else:
		file_path = arg

if not file_path:
	raise(Exception("Expected file path to send"))

with open('remote.py', 'r') as f:
    remote_script = f.read() # Read whole file in the file_content string


blocks_size=1024*1024
remote = subprocess.Popen(['ssh', 'tpdep01', 'python', '-c' , '\'' + remote_script + '\''],stdin=subprocess.PIPE, stdout=subprocess.PIPE, bufsize = 10 * blocks_size)
file_size = os.path.getsize(file_path)




ignored_statements = 0
while ignored_statements < 10:
	statement = remote.stdout.readline().decode('ascii').replace("\n","");
	#print('Received statement "%s"'%(statement))
	args=statement.split(" ");
	cmd = args[0];
	if cmd == '':
		print("Ignoring empty remote statement")
	elif cmd == 'GET_METADATA':
		metadata={'path': "hello.txt", 'file_size': file_size, 'blocks_size': blocks_size}
		send_data(str(metadata))
	elif cmd == 'GET_HASHES':
		hashes = blocks_hashes(file_path, blocks_size)
		send_data(str(hashes))
	elif cmd == 'QUIT':
		print("Qutting with remote rc=%s"%(args[1]))
		sys.exit(int(args[1]));
	elif cmd == 'PRINT':
		print("Log from remote: '%s'"%(" ".join(args[1:])));
	elif cmd == 'DEBUG':
		if verbose:
			print("DEBUG from remote: '%s'"%(" ".join(args[1:])));
	elif cmd == 'EXCEPTION':
		print_exception_and_exit()
	elif cmd == 'GET_DATA':
		start_from = int(args[1])
		transmit_file_data(file_path, blocks_size, start_from)
	else:
		raise Exception("Unknown remote statement '%s' (command '%s')."%(statement,cmd))


raise Exception("Received more than 10 empty remote statements")
