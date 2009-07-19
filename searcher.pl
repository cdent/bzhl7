#!/usr/bin/env perl
#
#
# Do a search across all the values, and return all the record ids 
# that match.

use strict;
use warnings;

use GEC;

our $DSN = GEC->ReadDSN('gec');
our $USER = 'cdent';
our $GEC = GEC->new(ename => 'hl7', dsn => $DSN, user => $USER);

my $record_ids = $GEC->search($ARGV[0]);

foreach my $record (@$record_ids) {
    print "$record\n";
}
