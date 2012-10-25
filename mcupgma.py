#!/usr/bin/python

import subprocess
import time
import sys
import os

def run_upgma(fasta_file, workdir=".", cluster_method="upgma", clust_file=None, id_mapping_file=None, sample_mapping_file=None, derep_file=None, matrix_file=None, mask_seq=None, jar_loc="Clustering.jar"):
	ret = dict()
	abs_start_time = time.time()
	file_stem = os.path.split(fasta_file)[1].split(".")[0]

	if id_mapping_file == None:
		id_mapping_file = os.path.join(workdir, file_stem + ".id_mapping")
	if sample_mapping_file == None:
		sample_mapping_file = os.path.join(workdir, file_stem + ".sample_mapping")
	if derep_file == None:
		derep_file = os.path.join(workdir, file_stem + ".derep")
	if matrix_file == None:
		matrix_file = os.path.join(workdir, file_stem + ".matrix")
	if clust_file == None:
		clust_file = os.path.join(workdir, file_stem + ".clust")

	start_time = time.time()
	derep_stream = open(derep_file, "w")
	if mask_seq == None:
		derep_process = subprocess.Popen(['java', '-Xmx2g', '-jar', jar_loc, "derep", "--aligned", id_mapping_file, sample_mapping_file, fasta_file], stdout=derep_stream)
	else:
		derep_process = subprocess.Popen(['java', '-Xmx2g', '-jar', jar_loc, "derep", "--model-only=" + mask_seq, id_mapping_file, sample_mapping_file, fasta_file], stdout=derep_stream)
	derep_process.wait()
	derep_stream.close()

	end_time = time.time()
	ret["derep"] = end_time - start_time

	print "Derep completed\t" + str(end_time - start_time)

	start_time = time.time()
	subprocess.check_call(['java', '-Xmx2g', '-jar', jar_loc, "dmatrix", "-i", id_mapping_file, "-o", matrix_file, "-in", derep_file, "-w", workdir])
	end_time = time.time()
	ret["matrix"] = end_time - start_time

	print "Distance matrix computed in\t" + str(end_time - start_time)

	start_time = time.time()
	subprocess.check_call(['java', '-Xmx2g', '-jar', jar_loc, 'cluster', '-m', cluster_method, '-i', id_mapping_file, '-s', sample_mapping_file, '-d', matrix_file, '-o', clust_file])
	end_time = time.time()
	ret["cluster"] = end_time - start_time

	print "Clustering completed in\t" + str(end_time - start_time)

	abs_end_time = time.time()
	ret["total"] = abs_end_time - abs_start_time


	print "Completed in\t" + str(abs_end_time - abs_start_time)

if __name__ == "__main__":
	if len(sys.argv) > 2:
                method = sys.argv[2];
		if not method in ['single', 'upgma', 'complete']:
			print "Valid methods are single, upgma, or complete"
		else:
			workdir = "."
			mask_seq = None
			if len(sys.argv) > 3:
				if "-mask=" in sys.argv[3]:
					mask_seq = sys.argv[3].replace("-mask=", "")
					if len(sys.argv) > 4:
						workdir = sys.argv[4]
				else:
					workdir = sys.argv[3]
			run_upgma(sys.argv[1], cluster_method=method, workdir=workdir, mask_seq=mask_seq)
	else:
		print "USAGE: mcupgma.py <fasta_file> <single,upgma,complete> [-mask=<maskseq>] [working_directory]"
