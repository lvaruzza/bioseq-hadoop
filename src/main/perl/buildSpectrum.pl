#!/bin/perl -w
 
use Bio::Perl;
use Bio::SeqIO;
use strict;

my $input = $ARGV[0];
my $k = $ARGV[1];

my $seqio = Bio::SeqIO->new(-file => $input, -format => "fasta" );

my %h;

while (my $seq = $seqio->next_seq){
    my $s = $seq->seq;
    for (my $i=0;$i<length($s)-$k+1;$i++) {
	my $f = uc(substr($s,$i,$k));
	$f =~ s/[^ACGT0123\.]/N/g;
	$h{$f}++;	
	$h{revcom($f)->seq}++;	
    }
}

my $total = 0;
my $oneCount =0;

while (my ($ff,$c) = each %h) {
    print "$ff\t$c\n";
    $total ++;
    if ($c == 1) {
	$oneCount ++;
    }
}

print STDERR  "Total kmers   = $total\n";
if ($total != 0) {
    print STDERR  sprintf("1-count kmers = %d (%.2f%%)\n",$oneCount,$oneCount*100.0/$total);
}
