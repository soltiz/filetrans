
import sys
import fcntl
import os
import traceback
import hashlib



def log(message):
	send_command("PRINT %s"%(message))

def debug(message):
	send_command("DEBUG %s"%(message))

def errlog(message):
	sys.stderr.write(message)
	sys.stderr.flush()

def send_command(cmd):
	sys.stdout.write(cmd + "\n")
	sys.stdout.flush()

def receive_blocks(target, file_size, blocks_size, start_from):
	if start_from:
		send_command("PRINT Will continue file \"%s\" starting at offset %d on a total of %d bytes."%(file_path,start_from, file_size))
	else:
		send_command("PRINT Will write file \"%s\" with %d bytes."%(file_path,file_size))
		
	send_command("GET_DATA %d"%(start_from))
	with open(target, "wb") as output_file:
		remaining = file_size - start_from
		output_file.seek(start_from)
		block = start_from // blocks_size
		while remaining > blocks_size:
			data = get_incoming_data(blocks_size)
			output_file.write(data)
			remaining = remaining - blocks_size
			m = hashlib.sha256()
			m.update(data)
			debug("Written block #%d with hash : '%s'"%(block, str(m.hexdigest())))
			block = block + 1
		if remaining > 0:
			data = get_incoming_data(remaining)
			output_file.write(data)
	log("End of file reception.")


def check_blocks(path, blocks_size, hashes_to_check):
	log("Checking existing blocks hashes...")
	correct_data_size = 0
	try:
		with open(path, "rb") as target_file:
			remaining = os.path.getsize(path)
			block_index=0
			while remaining > 0:
				m = hashlib.sha256()
				data = target_file.read(min (remaining, blocks_size))
				m.update(data)
				computed_hash = m.hexdigest()
				debug("checking block #%d (remaining=%d)..."%(block_index,remaining))
				reference_hash = hashes_to_check[block_index]
				if computed_hash != reference_hash:
					debug("Bad hash at block #%d: '%s' instead of '%s'"%(block_index, computed_hash, reference_hash))
					break;
				debug("Block #%d is correct"%(block_index))
				read_bytes_count = len(data)
				correct_data_size = correct_data_size + read_bytes_count
				block_index = block_index + 1
				remaining = remaining - read_bytes_count
	except IOError as e:
		debug("Unable to access target file '%s'."%(path))
	return correct_data_size




def get_incoming_data(limit=0):
	if limit:
		return sys.stdin.read(limit)
	else:
		return sys.stdin.readline()


def quit(rc):
	send_command("QUIT %d"%(rc))
	sys.exit(rc)


try:

	send_command("PRINT Connected")
	send_command("GET_METADATA")
	metadata = eval(get_incoming_data())
	
	try:
		file_path = "/tmp/" + metadata["path"]
		file_size = metadata["file_size"]
		blocks_size= metadata["blocks_size"]

		hashes = metadata.get("blocks_hashes")
	except:
		sys.stderr.write("Error on received metadata: %s"%(str(metadata)))
		sys.stderr.flush()
		raise




	if os.path.exists(file_path):
		send_command("GET_HASHES")
		hashes=eval(get_incoming_data())		
		correct_data_size = check_blocks(file_path, blocks_size, hashes)
	else:
		correct_data_size = 0


	if correct_data_size == file_size:
		log("File is already correct on filesystem.")
	else:
		receive_blocks(file_path, file_size, blocks_size, correct_data_size)

	quit(0)
except Exception as e:
	send_command("EXCEPTION")
	raise
