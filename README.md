## RDP memory-constrained hierarchical clustering tools (mcClust)

### Intro

RDP mcClust is an efficient implementation of a single round memory-constrained clustering algorithm proposed by Loewenstein (Loewenstein et al., 2008, Bioinformatics 24:i41-i49). 
It offers separate programs that can be combined to pre-process (dereplication and distance calculation)
 or post-process clustering results ( converting to biom output and finding representative sequences). 
 The clustering result can be used by AbundanceStats (https://github.com/rdpstaff/AbundanceStats) to calculate diversity measurements.

### Setup
Some commands in this tutorial depend on RDP AlignmentTools. See RDPTools (https://github.com/rdpstaff/RDPTools) to install.

### Usage

* Cluster sample(s)

	This tool performs clustering of one or more aligned sequence files. The distance is calculated using straight percent identity (does not take ambiguity codons in to account), 
ignoring positions where either or both sequences have a gap. Sequences must overlap by at least 25 bases or else distance calculation will fail. Three steps are involved to do the clustering:

	* dereplicate aligned sequences
	
  	An example command to run deprelicate with multiple aligned sequence files with mask sequence "#=GC_RF".
		
			java -Xmx2g -jar /path/to/Clustering.jar derep -m '#=GC_RF' -o derep.fa all_seqs.ids all_seqs.samples sample_1_aligned.fasta sample_2_aligned.fasta
	
	* calculate sequence distances
		
			java -Xmx2g -jar /path/to/Clustering.jar dmatrix --id-mapping all_seqs.ids --in derep.fa --outfile derep_matrix.bin -l 200 --dist-cutoff 0.15

	* perform clustering
	
		Three clustering algorithms are available: complete, single and average linkage algorithm.
		
			java -Xmx2g -jar /path/to/Clustering.jar cluster --dist-file derep_matrix.bin --id-mapping all_seqs.ids --sample-mapping all_seqs.samples --method complete --outfile all_seq_complete.clust

* Convert a cluster file to a R compatible community data matrix file
 			
 		java -Xmx2g -jar /path/to/Clustering.jar cluster_to_Rformat all_seq_complete.clust . 0.01 0.03
 		
 	The resulting output contains a list of tab delimited files containing the number of sequences for each sample for each OTU to each distance within the distance cutoff range. 
 	The word "aligned_" or "_trimmed" will be removed if present in the sample name. For ex:
 		
 			OTU_0000	OTU_0001	OTU_0002	OTU_0003
 		USGA_2_7_A	5	8	0	1
 		Native_1_4_A	4	13	8	5
 			
* Convert a cluster file to a biom otu table

		java -Xmx2g -jar /path/to/Clustering.jar/Clustering.jar cluster-to-biom all_seq_complete.clust 0.03 > all_seq_complete.clust.biom 

* Get represenative sequences from a cluster file

	If there are more than one alignment files, need to merge them into one file first.
		
		java -jar /path/to/AlignmentTools.jar alignment-merger alignment merged_aligned.fasta
		java -Xmx2g -jar /path/to/Clustering.jar rep-seqs --id-mapping all_seqs.ids --one-rep-per-otu all_seq_complete.clust 0.03 merged_aligned.fasta
		
* Select or exclude sequences from a file
	
		java -Xmx2g -jar /path/to/Clustering.jar filter-seqs selected.id merged_aligned.fasta false > selected.fasta

* Explode a dereplicated sequence file back to sample replicated files

		java -Xmx2g -jar /path/to/Clustering.jar explode-mappings --out-dir explode-mappings all_seqs.ids all_seqs.samples selected.fasta

* Convert a sequence file to fasta format		

		java -Xmx2g -jar /path/to/Clustering.jar to-fasta sample_1_aligned.stk sample_1_aligned.fasta
		
* Remove mapping entries for sequences externally filtered

		java -Xmx2g -jar /path/to/Clustering.jar refresh-mappings selected.fasta all_seqs.ids all_seqs.samples selected_seqs.ids selected_seqs.sample

* Dumps a binary distance file to flat text

		java -Xmx2g -jar /path/to/Clustering.jar dump-edges derep_matrix.bin > derep_matrix.txt
		
* Calculate distances using hadoop distance calculator	

		java -Xmx2g -jar /path/to/Clustering.jar hadoop
